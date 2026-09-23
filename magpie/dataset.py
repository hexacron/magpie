"""Analytical dataset: a queryable sqlite table of posts and users.

This is the data-extraction side of the tool and is deliberately independent
of the evidence store: no packages, no hashes, no renders, no timestamps.
One row per tweet id, re-polled freely — metrics are expected to move, the
first sighting is not.
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

from .config import Settings
from .models import Post, Profile

SCHEMA_VERSION = 1

# sqlite's default SQLITE_MAX_VARIABLE_NUMBER is 999 on older builds; 500 ids
# per IN clause keeps the crawler's skip check safe everywhere.
_ID_CHUNK = 500

POST_COLUMNS: tuple[str, ...] = (
    "id",
    "screen_name",
    "name",
    "text",
    "created_at_utc",
    "lang",
    "likes",
    "retweets",
    "replies",
    "quotes",
    "views",
    "bookmarks",
    "media_count",
    "has_media",
    "quoted_id",
    "reply_to_id",
    "reply_to_screen_name",
    "community_note",
    "text_source",
    "sources",
    "source_url",
    "first_seen_utc",
    "last_seen_utc",
    "raw",
)

# Everything except the identity and the first sighting is refreshed on
# re-ingest: counts are the whole reason to poll a post twice.
_POST_FROZEN = frozenset({"id", "first_seen_utc"})

LIST_COLUMNS: tuple[str, ...] = tuple(c for c in POST_COLUMNS if c != "raw")

USER_COLUMNS: tuple[str, ...] = (
    "screen_name",
    "name",
    "description",
    "location",
    "website",
    "joined",
    "followers",
    "following",
    "tweets",
    "likes",
    "media_count",
    "protected",
    "verified",
    "avatar_url",
    "banner_url",
    "first_seen_utc",
    "last_seen_utc",
    "raw",
)

_USER_FROZEN = frozenset({"screen_name", "first_seen_utc"})

MEDIA_COLUMNS: tuple[str, ...] = (
    "post_id",
    "idx",
    "type",
    "url",
    "thumb_url",
    "width",
    "height",
    "duration_s",
    "alt",
)

CURSOR_FIELDS: tuple[str, ...] = ("last_seen_id", "last_poll_utc", "new_count", "polls", "last_error")

CSV_HEADER: tuple[str, ...] = (
    "id",
    "created_at_utc",
    "screen_name",
    "name",
    "text",
    "likes",
    "retweets",
    "replies",
    "quotes",
    "views",
    "bookmarks",
    "media_count",
    "quoted_id",
    "reply_to_id",
    "source_url",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id                   TEXT PRIMARY KEY,
    screen_name          TEXT,
    name                 TEXT,
    text                 TEXT,
    created_at_utc       TEXT,
    lang                 TEXT,
    likes                INTEGER,
    retweets             INTEGER,
    replies              INTEGER,
    quotes               INTEGER,
    views                INTEGER,
    bookmarks            INTEGER,
    media_count          INTEGER DEFAULT 0,
    has_media            INTEGER DEFAULT 0,
    quoted_id            TEXT,
    reply_to_id          TEXT,
    reply_to_screen_name TEXT,
    community_note       TEXT,
    text_source          TEXT,
    sources              TEXT,
    source_url           TEXT,
    first_seen_utc       TEXT,
    last_seen_utc        TEXT,
    raw                  TEXT
);
CREATE INDEX IF NOT EXISTS posts_screen_name ON posts(screen_name);
CREATE INDEX IF NOT EXISTS posts_created_at ON posts(created_at_utc);
CREATE INDEX IF NOT EXISTS posts_reply_to_id ON posts(reply_to_id);
CREATE INDEX IF NOT EXISTS posts_quoted_id ON posts(quoted_id);

CREATE TABLE IF NOT EXISTS users (
    screen_name    TEXT PRIMARY KEY,
    name           TEXT,
    description    TEXT,
    location       TEXT,
    website        TEXT,
    joined         TEXT,
    followers      INTEGER,
    following      INTEGER,
    tweets         INTEGER,
    likes          INTEGER,
    media_count    INTEGER,
    protected      INTEGER,
    verified       INTEGER,
    avatar_url     TEXT,
    banner_url     TEXT,
    first_seen_utc TEXT,
    last_seen_utc  TEXT,
    raw            TEXT
);

CREATE TABLE IF NOT EXISTS media (
    post_id    TEXT NOT NULL,
    idx        INTEGER NOT NULL,
    type       TEXT,
    url        TEXT,
    thumb_url  TEXT,
    width      INTEGER,
    height     INTEGER,
    duration_s REAL,
    alt        TEXT,
    PRIMARY KEY (post_id, idx)
);

CREATE TABLE IF NOT EXISTS cursors (
    handle        TEXT PRIMARY KEY,
    last_seen_id  TEXT,
    last_poll_utc TEXT,
    new_count     INTEGER DEFAULT 0,
    polls         INTEGER DEFAULT 0,
    last_error    TEXT
);
"""

# External-content FTS plus the three sync triggers sqlite's docs prescribe.
_FTS_SCHEMA = """
CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(
    text, screen_name, name, content='posts', content_rowid='rowid'
);
CREATE TRIGGER IF NOT EXISTS posts_fts_ai AFTER INSERT ON posts BEGIN
    INSERT INTO posts_fts(rowid, text, screen_name, name)
    VALUES (new.rowid, new.text, new.screen_name, new.name);
END;
CREATE TRIGGER IF NOT EXISTS posts_fts_ad AFTER DELETE ON posts BEGIN
    INSERT INTO posts_fts(posts_fts, rowid, text, screen_name, name)
    VALUES ('delete', old.rowid, old.text, old.screen_name, old.name);
END;
CREATE TRIGGER IF NOT EXISTS posts_fts_au AFTER UPDATE ON posts BEGIN
    INSERT INTO posts_fts(posts_fts, rowid, text, screen_name, name)
    VALUES ('delete', old.rowid, old.text, old.screen_name, old.name);
    INSERT INTO posts_fts(rowid, text, screen_name, name)
    VALUES (new.rowid, new.text, new.screen_name, new.name);
END;
"""

_FTS_SEARCH_COLUMNS: tuple[str, ...] = ("text", "screen_name", "name")
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_LEAD_KEYWORD_RE = re.compile(r"^(select|with)\b", re.IGNORECASE)


# --------------------------------------------------------------- helpers


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _loads(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def _float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _flag(value: Any) -> int | None:
    return None if value is None else (1 if value else 0)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text != "" else None


def handle_key(handle: str | None) -> str:
    """Canonical comparison form for a screen name: no '@', lower case."""
    return (handle or "").strip().lstrip("@").lower()


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


_FTS_OPERATORS = {"AND", "OR", "NOT"}


def _fts_query(q: str) -> str:
    """Quote every token so user input can never be interpreted as FTS syntax.

    Bare uppercase AND/OR/NOT survive as operators -- people type
    ``Trump OR Gaza`` and expect a union, not a search for the literal word
    "OR". A trailing/leading operator is dropped rather than passed through,
    since FTS5 raises a syntax error on those.
    """
    tokens = [t for t in re.split(r"\s+", q.strip()) if t]
    out: list[str] = []
    for token in tokens:
        if token in _FTS_OPERATORS:
            if out and out[-1] not in _FTS_OPERATORS:
                out.append(token)
            continue
        cleaned = token.replace('"', "")
        if cleaned:
            out.append(f'"{cleaned}"')
    while out and out[-1] in _FTS_OPERATORS:
        out.pop()
    return " ".join(out)


def _since_bound(value: str) -> str:
    return value.strip().replace(" ", "T")


def _until_bound(value: str) -> str:
    """A bare date means the whole day; '9' sorts above every digit."""
    bound = value.strip().replace(" ", "T")
    return f"{bound}T99:99:99Z" if _DATE_ONLY_RE.match(bound) else bound


def _strip_sql_lead(sql: str) -> str:
    """Drop leading whitespace and comments so the first keyword is visible."""
    text = sql.lstrip()
    while True:
        if text.startswith("--"):
            nl = text.find("\n")
            text = "" if nl == -1 else text[nl + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            text = "" if end == -1 else text[end + 2 :].lstrip()
            continue
        return text


def _is_multi_statement(sql: str) -> bool:
    """True when a ';' separates statements (a single trailing one is fine)."""
    text = sql.rstrip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch in "'\"`":
            quote = ch
            i += 1
            while i < n:
                if text[i] == quote:
                    if i + 1 < n and text[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if ch == "[":
            close = text.find("]", i)
            i = n if close == -1 else close + 1
            continue
        if text.startswith("--", i):
            nl = text.find("\n", i)
            i = n if nl == -1 else nl + 1
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        if ch == ";":
            return True
        i += 1
    return False


def _post_values(post: Post, now: str) -> tuple[Any, ...]:
    counts = post.counts if isinstance(post.counts, dict) else {}
    media = list(post.media or [])
    return (
        str(post.id).strip(),
        _text(post.screen_name),
        _text(post.name),
        post.text,
        _text(post.created_at_utc),
        _text(post.lang),
        _int(counts.get("likes")),
        _int(counts.get("retweets")),
        _int(counts.get("replies")),
        _int(counts.get("quotes")),
        _int(counts.get("views")),
        _int(counts.get("bookmarks")),
        len(media),
        1 if media else 0,
        _text(post.quoted_id),
        _text(post.reply_to_id),
        _text(post.reply_to_screen_name),
        _text(post.community_note),
        _text(post.text_source),
        _dump(list(post.available_sources or [])),
        _text(post.source_url),
        now,
        now,
        _dump(post.to_dict()),
    )


def _media_values(post_id: str, post: Post) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for idx, item in enumerate(post.media or []):
        rows.append(
            (
                post_id,
                idx,
                _text(getattr(item, "type", None)),
                _text(getattr(item, "best_url", None)),
                _text(getattr(item, "thumb_url", None)),
                _int(getattr(item, "width", None)),
                _int(getattr(item, "height", None)),
                _float(getattr(item, "duration_s", None)),
                _text(getattr(item, "alt", None)),
            )
        )
    return rows


def _user_values(profile: Profile, now: str) -> tuple[Any, ...]:
    return (
        handle_key(profile.screen_name),
        _text(profile.name),
        _text(profile.description),
        _text(profile.location),
        _text(profile.website),
        _text(profile.joined),
        _int(profile.followers),
        _int(profile.following),
        _int(profile.tweets),
        _int(profile.likes),
        _int(profile.media_count),
        _flag(profile.protected),
        _flag(profile.verified),
        _text(profile.avatar_url),
        _text(profile.banner_url),
        now,
        now,
        _dump(profile.to_dict()),
    )


def _upsert_sql(table: str, columns: Sequence[str], key: str, frozen: Iterable[str]) -> str:
    frozen_set = set(frozen)
    placeholders = ", ".join("?" for _ in columns)
    updates = ", ".join(f"{c} = excluded.{c}" for c in columns if c not in frozen_set)
    return (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        f"ON CONFLICT({key}) DO UPDATE SET {updates}"
    )


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = {k: row[k] for k in row.keys()}
    if "sources" in data:
        data["sources"] = _loads(data["sources"], [])
    if "has_media" in data:
        data["has_media"] = bool(data["has_media"])
    if "raw" in data:
        data["raw"] = _loads(data["raw"], None)
    return data


# --------------------------------------------------------------- dataset


class Dataset:
    """One sqlite file holding every post and user the puller has extracted."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.path = Path(settings.data_dir) / "dataset.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self.fts_enabled = False
        self._create_schema()

    # ------------------------------------------------------------- db

    def _create_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)
            try:
                self._conn.executescript(_FTS_SCHEMA)
            except sqlite3.Error:
                # sqlite built without FTS5: text search falls back to LIKE.
                self._conn.rollback()
                self._conn.executescript(_SCHEMA)
                self.fts_enabled = False
            else:
                self.fts_enabled = True
            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0] or 0)
            if version != SCHEMA_VERSION:
                # Every statement above is CREATE IF NOT EXISTS, so opening an
                # older file is the migration; newer files are left alone.
                self._conn.execute(f"PRAGMA user_version = {max(version, SCHEMA_VERSION)}")
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

    def _reader(self) -> sqlite3.Connection:
        """A separate read-only connection, so long reads never hold the lock."""
        conn = sqlite3.connect(str(self.path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --------------------------------------------------------- writing

    def upsert_post(self, post: Post) -> bool:
        """Store a post. True only the first time this id is seen."""
        tweet_id = str(post.id or "").strip()
        if not tweet_id:
            raise ValueError("post.id is required")
        values = _post_values(post, _now())
        media = _media_values(tweet_id, post)
        sql = _upsert_sql("posts", POST_COLUMNS, "id", _POST_FROZEN)
        with self._tx() as conn:
            existed = conn.execute("SELECT 1 FROM posts WHERE id = ?", (tweet_id,)).fetchone()
            conn.execute(sql, values)
            conn.execute("DELETE FROM media WHERE post_id = ?", (tweet_id,))
            if media:
                conn.executemany(
                    f"INSERT INTO media ({', '.join(MEDIA_COLUMNS)}) "
                    f"VALUES ({', '.join('?' for _ in MEDIA_COLUMNS)})",
                    media,
                )
        return existed is None

    def upsert_user(self, profile: Profile) -> bool:
        """Store a profile keyed by lower-case handle. True on first sighting."""
        handle = handle_key(profile.screen_name)
        if not handle:
            raise ValueError("profile.screen_name is required")
        values = _user_values(profile, _now())
        sql = _upsert_sql("users", USER_COLUMNS, "screen_name", _USER_FROZEN)
        with self._tx() as conn:
            existed = conn.execute(
                "SELECT 1 FROM users WHERE screen_name = ?", (handle,)
            ).fetchone()
            conn.execute(sql, values)
        return existed is None

    # --------------------------------------------------------- reading

    def known(self, ids: Iterable[str]) -> set[str]:
        """Which of these ids are already stored. The crawler's skip check."""
        wanted: list[str] = []
        seen: set[str] = set()
        for raw in ids:
            value = str(raw or "").strip()
            if value and value not in seen:
                seen.add(value)
                wanted.append(value)
        if not wanted:
            return set()
        found: set[str] = set()
        with self._lock:
            for start in range(0, len(wanted), _ID_CHUNK):
                chunk = wanted[start : start + _ID_CHUNK]
                placeholders = ", ".join("?" for _ in chunk)
                rows = self._conn.execute(
                    f"SELECT id FROM posts WHERE id IN ({placeholders})", chunk
                ).fetchall()
                found.update(str(r["id"]) for r in rows)
        return found

    def seen_ids(self, handle: str) -> set[str]:
        key = handle_key(handle)
        if not key:
            return set()
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM posts WHERE lower(screen_name) = ?", (key,)
            ).fetchall()
        return {str(r["id"]) for r in rows}

    def _post_filters(
        self,
        handle: str | None,
        q: str | None,
        since: str | None,
        until: str | None,
        has_media: bool | None,
    ) -> tuple[str, list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        if handle and handle.strip():
            where.append("lower(screen_name) = ?")
            params.append(handle_key(handle))
        if since and since.strip():
            where.append("created_at_utc >= ?")
            params.append(_since_bound(since))
        if until and until.strip():
            where.append("created_at_utc <= ?")
            params.append(_until_bound(until))
        if has_media is not None:
            where.append("has_media = ?")
            params.append(1 if has_media else 0)
        if q and q.strip():
            match = _fts_query(q) if self.fts_enabled else ""
            if match:
                where.append("rowid IN (SELECT rowid FROM posts_fts WHERE posts_fts MATCH ?)")
                params.append(match)
            else:
                like = f"%{_like_escape(q.strip())}%"
                where.append(
                    "("
                    + " OR ".join(f"{c} LIKE ? ESCAPE '\\'" for c in _FTS_SEARCH_COLUMNS)
                    + ")"
                )
                params.extend([like] * len(_FTS_SEARCH_COLUMNS))
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        return clause, params

    def posts(
        self,
        handle: str | None = None,
        q: str | None = None,
        since: str | None = None,
        until: str | None = None,
        has_media: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        clause, params = self._post_filters(handle, q, since, until, has_media)
        limit = max(0, int(limit))
        offset = max(0, int(offset))
        columns = ", ".join(LIST_COLUMNS)
        with self._lock:
            total = int(
                self._conn.execute(f"SELECT COUNT(*) FROM posts{clause}", params).fetchone()[0]
            )
            rows = self._conn.execute(
                f"SELECT {columns} FROM posts{clause} "
                "ORDER BY created_at_utc DESC, id DESC LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        return [_row_dict(r) for r in rows], total

    def post(self, tweet_id: str) -> dict[str, Any] | None:
        key = str(tweet_id or "").strip()
        if not key:
            return None
        with self._lock:
            row = self._conn.execute("SELECT * FROM posts WHERE id = ?", (key,)).fetchone()
            if row is None:
                return None
            media = self._conn.execute(
                "SELECT * FROM media WHERE post_id = ? ORDER BY idx", (key,)
            ).fetchall()
        data = _row_dict(row)
        data["media"] = [dict(m) for m in media]
        return data

    def users(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM users ORDER BY followers DESC, screen_name ASC LIMIT ?",
                (max(0, int(limit)),),
            ).fetchall()
        return [_row_dict(r) for r in rows]

    def query(
        self,
        sql: str,
        params: tuple = (),
        *,
        timeout: float | None = None,
        max_rows: int | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Read-only escape hatch: one SELECT/WITH statement, nothing else.

        Returns ``(rows, truncated)``. ``query_only`` stops writes but not work:
        ``WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c)`` never
        terminates and a self-join materialises arbitrarily many rows. `timeout`
        aborts the statement from sqlite's progress handler; `max_rows` stops
        reading rather than letting the result set define the memory ceiling.
        """
        text = _strip_sql_lead(sql or "")
        if not text:
            raise ValueError("empty query")
        if not _LEAD_KEYWORD_RE.match(text):
            raise ValueError("only SELECT / WITH queries are allowed")
        if _is_multi_statement(text):
            raise ValueError("only a single statement is allowed")
        conn = self._reader()
        try:
            if timeout and timeout > 0:
                deadline = time.monotonic() + float(timeout)
                # Returning true from the handler aborts the statement; 10k VM
                # steps is often enough to catch a runaway loop, rare enough not
                # to cost a normal query anything measurable.
                conn.set_progress_handler(lambda: time.monotonic() > deadline, 10_000)
            cursor = conn.execute(text, tuple(params))
            if max_rows is not None and max_rows >= 0:
                rows = cursor.fetchmany(max_rows + 1)
                truncated = len(rows) > max_rows
                rows = rows[:max_rows]
            else:
                rows = cursor.fetchall()
                truncated = False
        finally:
            conn.set_progress_handler(None, 0)
            conn.close()
        return [{k: r[k] for k in r.keys()} for r in rows], truncated

    # ---------------------------------------------------------- export

    def iter_export(self, handle: str | None = None) -> Iterator[dict[str, Any]]:
        """Stream export rows, one post at a time, so nothing buffers them all."""
        clause, params = self._post_filters(handle, None, None, None, None)
        columns = ", ".join(LIST_COLUMNS)
        conn = self._reader()
        try:
            cursor = conn.execute(
                f"SELECT {columns} FROM posts{clause} ORDER BY created_at_utc DESC, id DESC",
                params,
            )
            media_sql = "SELECT * FROM media WHERE post_id = ? ORDER BY idx"
            for row in cursor:
                data = _row_dict(row)
                data["media"] = [
                    dict(m) for m in conn.execute(media_sql, (data["id"],)).fetchall()
                ]
                yield data
        finally:
            conn.close()

    def _write_export(self, path: str | None, emit: Callable[[IO[str]], None]) -> str:
        if path is None:
            buf = io.StringIO()
            emit(buf)
            return buf.getvalue()
        target = Path(path)
        if target.parent and not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8", newline="") as handle:
            emit(handle)
        return str(target)

    def export_jsonl(self, path: str | None = None, handle: str | None = None) -> str:
        def emit(out: IO[str]) -> None:
            for row in self.iter_export(handle):
                out.write(json.dumps(row, ensure_ascii=False))
                out.write("\n")

        return self._write_export(path, emit)

    def export_csv(self, path: str | None = None, handle: str | None = None) -> str:
        def emit(out: IO[str]) -> None:
            writer = csv.writer(out)
            writer.writerow(CSV_HEADER)
            for row in self.iter_export(handle):
                writer.writerow(["" if row.get(c) is None else row.get(c) for c in CSV_HEADER])

        return self._write_export(path, emit)

    # ---------------------------------------------------------- cursors

    def get_cursor(self, handle: str) -> dict[str, Any]:
        key = handle_key(handle)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM cursors WHERE handle = ?", (key,)
            ).fetchone()
        if row is None:
            return {
                "handle": key,
                "last_seen_id": None,
                "last_poll_utc": None,
                "new_count": 0,
                "polls": 0,
                "last_error": None,
            }
        return {k: row[k] for k in row.keys()}

    def set_cursor(self, handle: str, **fields: Any) -> None:
        """Absolute values only: the caller owns the arithmetic."""
        key = handle_key(handle)
        if not key:
            raise ValueError("handle is required")
        unknown = [name for name in fields if name not in CURSOR_FIELDS]
        if unknown:
            raise ValueError(f"unknown cursor field(s): {', '.join(sorted(unknown))}")
        with self._tx() as conn:
            conn.execute("INSERT OR IGNORE INTO cursors (handle) VALUES (?)", (key,))
            if fields:
                assignments = ", ".join(f"{name} = ?" for name in fields)
                conn.execute(
                    f"UPDATE cursors SET {assignments} WHERE handle = ?",
                    (*fields.values(), key),
                )

    # ------------------------------------------------------------ stats

    def stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS posts, "
                "COUNT(DISTINCT CASE WHEN screen_name IS NOT NULL AND screen_name <> '' "
                "THEN lower(screen_name) END) AS authors, "
                "SUM(CASE WHEN has_media = 1 THEN 1 ELSE 0 END) AS with_media, "
                "MIN(CASE WHEN created_at_utc <> '' THEN created_at_utc END) AS first_post_utc, "
                "MAX(created_at_utc) AS last_post_utc, "
                "MIN(first_seen_utc) AS first_seen_utc, "
                "MAX(last_seen_utc) AS last_seen_utc "
                "FROM posts"
            ).fetchone()
            users = int(self._conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])
            watched = int(self._conn.execute("SELECT COUNT(*) FROM cursors").fetchone()[0])
        # WAL keeps recent writes outside the main file; report the real
        # on-disk footprint, not a 4 KiB header.
        db_bytes = 0
        for suffix in ("", "-wal"):
            try:
                db_bytes += os.path.getsize(f"{self.path}{suffix}")
            except OSError:
                pass
        return {
            "posts": int(row["posts"] or 0),
            "users": users,
            "authors": int(row["authors"] or 0),
            "with_media": int(row["with_media"] or 0),
            "first_post_utc": row["first_post_utc"],
            "last_post_utc": row["last_post_utc"],
            "first_seen_utc": row["first_seen_utc"],
            "last_seen_utc": row["last_seen_utc"],
            "db_bytes": db_bytes,
            "handles_watched": watched,
        }
