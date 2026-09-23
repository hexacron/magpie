"""Storage layer.

The filesystem is the source of truth: every capture is a self-contained
package directory under ``settings.captures_dir``. SQLite is only a derived
search index over those packages and can be thrown away and rebuilt with
:meth:`Store.reindex` at any time.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import shutil
import sqlite3
import tempfile
import threading
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .hashing import sha256_file, verify_package
from .models import IndexRow, Manifest, TweetRef, VerifyResult

COLUMNS: tuple[str, ...] = (
    "folder",
    "capture_time_utc",
    "tweet_id",
    "screen_name",
    "name",
    "text",
    "created_at_utc",
    "available_sources",
    "media_count",
    "flags",
    "status",
    "tags",
    "note",
    "ocr_text",
    "manifest_sha256",
    "timestamped",
    "version",
)

FTS_COLUMNS: tuple[str, ...] = ("text", "screen_name", "name", "tags", "note", "ocr_text")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS captures (
    folder            TEXT PRIMARY KEY,
    capture_time_utc  TEXT,
    tweet_id          TEXT,
    screen_name       TEXT,
    name              TEXT,
    text              TEXT,
    created_at_utc    TEXT,
    available_sources TEXT,
    media_count       INTEGER DEFAULT 0,
    flags             TEXT,
    status            TEXT,
    tags              TEXT,
    note              TEXT,
    ocr_text          TEXT,
    manifest_sha256   TEXT,
    timestamped       INTEGER DEFAULT 0,
    version           INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS captures_tweet_id ON captures(tweet_id);
CREATE INDEX IF NOT EXISTS captures_screen_name ON captures(screen_name);
CREATE INDEX IF NOT EXISTS captures_capture_time ON captures(capture_time_utc);
"""

# External-content FTS plus the three sync triggers sqlite's docs prescribe.
_FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS captures_fts USING fts5(
    text, screen_name, name, tags, note, ocr_text,
    content='captures', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS captures_fts_ai AFTER INSERT ON captures BEGIN
    INSERT INTO captures_fts(rowid, text, screen_name, name, tags, note, ocr_text)
    VALUES (new.rowid, new.text, new.screen_name, new.name, new.tags, new.note, new.ocr_text);
END;
CREATE TRIGGER IF NOT EXISTS captures_fts_ad AFTER DELETE ON captures BEGIN
    INSERT INTO captures_fts(captures_fts, rowid, text, screen_name, name, tags, note, ocr_text)
    VALUES ('delete', old.rowid, old.text, old.screen_name, old.name, old.tags, old.note,
            old.ocr_text);
END;
CREATE TRIGGER IF NOT EXISTS captures_fts_au AFTER UPDATE ON captures BEGIN
    INSERT INTO captures_fts(captures_fts, rowid, text, screen_name, name, tags, note, ocr_text)
    VALUES ('delete', old.rowid, old.text, old.screen_name, old.name, old.tags, old.note,
            old.ocr_text);
    INSERT INTO captures_fts(rowid, text, screen_name, name, tags, note, ocr_text)
    VALUES (new.rowid, new.text, new.screen_name, new.name, new.tags, new.note, new.ocr_text);
END;
"""

CSV_HEADER: tuple[str, ...] = (
    "folder",
    "capture_time_utc",
    "screen_name",
    "name",
    "tweet_id",
    "created_at_utc",
    "status",
    "media_count",
    "manifest_sha256",
    "timestamped",
    "version",
    "tags",
    "flags",
    "text",
)

#: A capture folder is always ``<stamp>_<handle>_<id>``; nothing else is served.
#: Lives here beside ``package_path``, the function it guards, so the web UI and
#: the API cannot drift apart on what a servable folder name looks like.
FOLDER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,160}$")

_HANDLE_RE = re.compile(r"[^A-Za-z0-9_]")
_DIGITS_RE = re.compile(r"[^0-9]")
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _load_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw]
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _fts_query(q: str) -> str:
    """Quote every token so user input can never be FTS5 syntax."""
    tokens = [t for t in re.split(r"\s+", q.strip()) if t]
    quoted = []
    for token in tokens:
        cleaned = token.replace('"', '""')
        if cleaned.strip('"'):
            quoted.append(f'"{cleaned}"')
    return " ".join(quoted)


def normalise_tags(tags: list[str] | None) -> list[str]:
    """Strip, drop empties, dedupe, keep order."""
    out: list[str] = []
    seen: set[str] = set()
    for tag in tags or []:
        cleaned = str(tag).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


class Store:
    """Package directories on disk + a rebuildable sqlite index over them."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        settings.ensure_dirs()
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(settings.index_db), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self.fts_enabled = False
        self._create_schema()

    # ------------------------------------------------------------------ db

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            try:
                self._conn.executescript(_FTS_SCHEMA)
            except sqlite3.Error:
                # sqlite built without FTS5: search falls back to LIKE.
                self._conn.rollback()
                self.fts_enabled = False
            else:
                self.fts_enabled = True
            self._conn.commit()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --------------------------------------------------------------- paths

    def package_path(self, folder: str) -> Path:
        """Resolve a package directory, refusing anything that escapes the root."""
        name = (folder or "").strip()
        if not name or name in (".", ".."):
            raise ValueError("invalid folder name")
        if "/" in name or "\\" in name or ".." in name or "\x00" in name:
            raise ValueError(f"invalid folder name: {folder!r}")
        return self.settings.captures_dir / name

    def allocate(self, ref: TweetRef, capture_time: datetime) -> tuple[str, Path]:
        """Create the next free package directory for this reference."""
        when = capture_time
        if when.tzinfo is not None:
            when = when.astimezone(timezone.utc)
        handle = _HANDLE_RE.sub("", (ref.screen_name or "").lstrip("@")) or "unknown"
        tweet_id = _DIGITS_RE.sub("", ref.tweet_id or "") or "unknown"
        base = f"{when:%Y%m%dT%H%M%SZ}_{handle}_{tweet_id}"
        folder = base
        suffix = 2
        while (self.settings.captures_dir / folder).exists():
            folder = f"{base}-{suffix}"
            suffix += 1
        path = self.settings.captures_dir / folder
        (path / "media").mkdir(parents=True)
        return folder, path

    # ------------------------------------------------------------ manifest

    def write_manifest(self, pkg_dir: Path, manifest: Manifest) -> str:
        path = pkg_dir / "manifest.json"
        path.write_text(
            json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        sha = sha256_file(path)
        (pkg_dir / "MANIFEST.sha256").write_text(f"{sha}  manifest.json\n", encoding="utf-8")
        return sha

    def get_manifest(self, folder: str) -> dict[str, Any] | None:
        path = self.package_path(folder) / "manifest.json"
        return _read_manifest(path)

    # ---------------------------------------------------------------- meta

    def meta(self, folder: str) -> dict[str, Any]:
        return _read_meta(self.package_path(folder))

    def set_meta(self, folder: str, tags: list[str], note: str) -> None:
        """Tags/notes live outside the hash chain on purpose: they are mutable."""
        pkg_dir = self.package_path(folder)
        if not pkg_dir.is_dir():
            raise FileNotFoundError(folder)
        payload = {"tags": normalise_tags(tags), "note": note or ""}
        (pkg_dir / "meta.json").write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE captures SET tags = ?, note = ? WHERE folder = ?",
                (_dump(payload["tags"]), payload["note"], folder),
            )
            updated = cur.rowcount
        if not updated:
            data = _read_manifest(pkg_dir / "manifest.json")
            if data is not None:
                self._write_row(_row_from_manifest(folder, data, pkg_dir))

    # --------------------------------------------------------------- index

    def index_upsert(self, manifest: Manifest) -> IndexRow:
        folder = manifest.folder
        pkg_dir = self.package_path(folder)
        row = _row_from_manifest(folder, manifest.to_dict(), pkg_dir)
        self._write_row(row)
        return row

    def _write_row(self, row: IndexRow) -> None:
        values = _row_values(row)
        placeholders = ", ".join("?" for _ in COLUMNS)
        updates = ", ".join(f"{c} = excluded.{c}" for c in COLUMNS if c != "folder")
        sql = (
            f"INSERT INTO captures ({', '.join(COLUMNS)}) VALUES ({placeholders}) "
            f"ON CONFLICT(folder) DO UPDATE SET {updates}"
        )
        with self._tx() as conn:
            conn.execute(sql, values)

    def get_row(self, folder: str) -> IndexRow | None:
        self.package_path(folder)  # reject traversal even on a pure read
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM captures WHERE folder = ?", (folder,)
            ).fetchone()
        return _row_from_db(row) if row else None

    def search(
        self,
        q: str | None = None,
        user: str | None = None,
        date: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[IndexRow], int]:
        where: list[str] = []
        params: list[Any] = []

        if user and user.strip():
            where.append("lower(screen_name) = ?")
            params.append(user.strip().lstrip("@").lower())
        if date and date.strip():
            where.append("capture_time_utc LIKE ? ESCAPE '\\'")
            params.append(f"{_like_escape(date.strip())}%")
        if tag and tag.strip():
            where.append("tags LIKE ? ESCAPE '\\'")
            params.append(f"%{_like_escape(_dump(tag.strip()))}%")
        if status and status.strip():
            where.append("status = ?")
            params.append(status.strip())
        if q and q.strip():
            match = _fts_query(q) if self.fts_enabled else ""
            if match:
                where.append("rowid IN (SELECT rowid FROM captures_fts WHERE captures_fts MATCH ?)")
                params.append(match)
            else:
                like = f"%{_like_escape(q.strip())}%"
                where.append(
                    "("
                    + " OR ".join(f"{col} LIKE ? ESCAPE '\\'" for col in FTS_COLUMNS)
                    + ")"
                )
                params.extend([like] * len(FTS_COLUMNS))

        clause = f" WHERE {' AND '.join(where)}" if where else ""
        limit = max(0, int(limit))
        offset = max(0, int(offset))
        with self._lock:
            total = int(
                self._conn.execute(f"SELECT COUNT(*) FROM captures{clause}", params).fetchone()[0]
            )
            rows = self._conn.execute(
                f"SELECT * FROM captures{clause} "
                "ORDER BY capture_time_utc DESC, folder DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        return [_row_from_db(r) for r in rows], total

    def recent(self, limit: int = 8) -> list[IndexRow]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM captures ORDER BY capture_time_utc DESC, folder DESC LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
        return [_row_from_db(r) for r in rows]

    # ------------------------------------------------------------ versions

    def previous_capture(self, tweet_id: str, exclude_folder: str) -> IndexRow | None:
        if not tweet_id:
            return None
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM captures WHERE tweet_id = ? AND folder <> ? "
                "ORDER BY capture_time_utc DESC, folder DESC LIMIT 1",
                (tweet_id, exclude_folder or ""),
            ).fetchone()
        return _row_from_db(row) if row else None

    def versions(self, tweet_id: str) -> list[IndexRow]:
        if not tweet_id:
            return []
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM captures WHERE tweet_id = ? "
                "ORDER BY capture_time_utc ASC, folder ASC",
                (tweet_id,),
            ).fetchall()
        return [_row_from_db(r) for r in rows]

    # -------------------------------------------------------------- export

    def export_rows(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM captures ORDER BY capture_time_utc DESC, folder DESC"
            ).fetchall()
        return [_row_from_db(r).to_dict() for r in rows]

    def export_csv(self) -> str:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(CSV_HEADER)
        for data in self.export_rows():
            writer.writerow(
                [
                    data["folder"],
                    data["capture_time_utc"],
                    data["screen_name"] or "",
                    data["name"] or "",
                    data["tweet_id"] or "",
                    data["created_at_utc"] or "",
                    data["status"],
                    data["media_count"],
                    data["manifest_sha256"],
                    "1" if data["timestamped"] else "0",
                    data["version"],
                    ";".join(data["tags"]),
                    ";".join(data["flags"]),
                    data["text"],
                ]
            )
        return buf.getvalue()

    def make_zip(self, folder: str) -> Path:
        """Deterministic zip of one package. The caller deletes the temp file."""
        pkg_dir = self.package_path(folder)
        if not pkg_dir.is_dir():
            raise FileNotFoundError(folder)
        handle = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
        handle.close()
        out = Path(handle.name)
        files = sorted(
            (p for p in pkg_dir.rglob("*") if p.is_file()),
            key=lambda p: p.relative_to(pkg_dir).as_posix(),
        )
        with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in files:
                arcname = f"{folder}/{path.relative_to(pkg_dir).as_posix()}"
                info = zipfile.ZipInfo(arcname, date_time=_ZIP_EPOCH)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                zf.writestr(info, path.read_bytes())
        return out

    # ---------------------------------------------------------- maintenance

    def verify(self, folder: str) -> VerifyResult:
        return verify_package(self.package_path(folder))

    def reindex(self) -> int:
        """Rebuild the whole table from disk. Corrupt packages are skipped."""
        rows: list[IndexRow] = []
        captures_dir = self.settings.captures_dir
        if captures_dir.is_dir():
            for pkg_dir in sorted(captures_dir.iterdir(), key=lambda p: p.name):
                if not pkg_dir.is_dir():
                    continue
                data = _read_manifest(pkg_dir / "manifest.json")
                if data is None:
                    continue
                try:
                    rows.append(_row_from_manifest(pkg_dir.name, data, pkg_dir))
                except Exception:
                    continue
        placeholders = ", ".join("?" for _ in COLUMNS)
        sql = f"INSERT INTO captures ({', '.join(COLUMNS)}) VALUES ({placeholders})"
        with self._tx() as conn:
            conn.execute("DELETE FROM captures")
            conn.executemany(sql, [_row_values(r) for r in rows])
        return len(rows)

    def delete(self, folder: str) -> bool:
        pkg_dir = self.package_path(folder)
        existed = pkg_dir.exists()
        if existed:
            shutil.rmtree(pkg_dir, ignore_errors=True)
        with self._tx() as conn:
            cur = conn.execute("DELETE FROM captures WHERE folder = ?", (folder,))
            removed = cur.rowcount
        return bool(existed or removed)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS captures,"
                " SUM(status = 'live') AS live,"
                " SUM(status = 'unavailable') AS unavailable,"
                " SUM(flags IS NOT NULL AND flags <> '' AND flags <> '[]') AS flagged,"
                " SUM(timestamped) AS timestamped,"
                " COUNT(DISTINCT screen_name) AS authors,"
                " MIN(capture_time_utc) AS first_capture,"
                " MAX(capture_time_utc) AS last_capture"
                " FROM captures"
            ).fetchone()
        total_bytes = 0
        captures_dir = self.settings.captures_dir
        if captures_dir.is_dir():
            for dirpath, _dirnames, filenames in os.walk(captures_dir):
                for name in filenames:
                    try:
                        total_bytes += os.path.getsize(os.path.join(dirpath, name))
                    except OSError:
                        continue
        return {
            "captures": int(row["captures"] or 0),
            "live": int(row["live"] or 0),
            "unavailable": int(row["unavailable"] or 0),
            "flagged": int(row["flagged"] or 0),
            "timestamped": int(row["timestamped"] or 0),
            "authors": int(row["authors"] or 0),
            "bytes": total_bytes,
            "first_capture": row["first_capture"],
            "last_capture": row["last_capture"],
        }


# --------------------------------------------------------------- row helpers


def _read_manifest(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _read_meta(pkg_dir: Path) -> dict[str, Any]:
    try:
        data = json.loads((pkg_dir / "meta.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        data = None
    if not isinstance(data, dict):
        return {"tags": [], "note": ""}
    return {"tags": normalise_tags(_load_list(data.get("tags"))), "note": str(data.get("note") or "")}


def _recorded_sha(pkg_dir: Path) -> str:
    try:
        line = (pkg_dir / "MANIFEST.sha256").read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    return line.split()[0] if line else ""


def _is_timestamped(pkg_dir: Path) -> bool:
    """A 200 from the TSA is kept as evidence even when it carries no token,
    so the sidecar's verdict wins whenever it exists."""
    try:
        sidecar = json.loads((pkg_dir / "timestamp.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        sidecar = None
    if isinstance(sidecar, dict) and "ok" in sidecar:
        return bool(sidecar.get("ok"))
    return (pkg_dir / "timestamp.tsr").exists()


def _row_from_manifest(folder: str, data: dict[str, Any], pkg_dir: Path) -> IndexRow:
    post = data.get("post") or {}
    if not isinstance(post, dict):
        post = {}
    crosscheck = data.get("crosscheck") or {}
    if not isinstance(crosscheck, dict):
        crosscheck = {}
    available = _load_list(post.get("available_sources"))
    if not available:
        available = [
            str(s.get("name"))
            for s in (data.get("sources") or [])
            if isinstance(s, dict) and s.get("ok") and s.get("name")
        ]
    version = data.get("version") or {}
    meta = _read_meta(pkg_dir)
    return IndexRow(
        folder=folder,
        capture_time_utc=str(data.get("capture_time_utc") or ""),
        tweet_id=data.get("tweet_id") or post.get("id") or None,
        screen_name=data.get("screen_name") or post.get("screen_name") or None,
        name=post.get("name") or None,
        text=str(post.get("text") or ""),
        created_at_utc=post.get("created_at_utc") or None,
        available_sources=available,
        media_count=len(post.get("media") or []),
        flags=_load_list(crosscheck.get("flags")),
        status=str(data.get("status") or "unavailable"),
        tags=meta["tags"],
        note=meta["note"],
        ocr_text=str(post.get("ocr_text") or ""),
        manifest_sha256=_recorded_sha(pkg_dir),
        # The RFC 3161 token is fetched after manifest.json is sealed, so the
        # manifest can never record it: disk is the only honest answer.
        timestamped=_is_timestamped(pkg_dir),
        version=int(version.get("version") or 1) if isinstance(version, dict) else 1,
    )


def _row_values(row: IndexRow) -> tuple[Any, ...]:
    return (
        row.folder,
        row.capture_time_utc,
        row.tweet_id,
        row.screen_name,
        row.name,
        row.text,
        row.created_at_utc,
        _dump(row.available_sources),
        int(row.media_count),
        _dump(row.flags),
        row.status,
        _dump(row.tags),
        row.note,
        row.ocr_text,
        row.manifest_sha256,
        1 if row.timestamped else 0,
        int(row.version),
    )


def _row_from_db(row: sqlite3.Row) -> IndexRow:
    return IndexRow(
        folder=row["folder"],
        capture_time_utc=row["capture_time_utc"] or "",
        tweet_id=row["tweet_id"],
        screen_name=row["screen_name"],
        name=row["name"],
        text=row["text"] or "",
        created_at_utc=row["created_at_utc"],
        available_sources=_load_list(row["available_sources"]),
        media_count=int(row["media_count"] or 0),
        flags=_load_list(row["flags"]),
        status=row["status"] or "unavailable",
        tags=_load_list(row["tags"]),
        note=row["note"] or "",
        ocr_text=row["ocr_text"] or "",
        manifest_sha256=row["manifest_sha256"] or "",
        timestamped=bool(row["timestamped"]),
        version=int(row["version"] or 1),
    )
