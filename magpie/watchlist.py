"""Persisted watchlist: which accounts to poll, how often, and how that cadence
self-tunes.

The unauthenticated profile page exposes 5-6 post ids, so coverage is a pure
function of cadence: an account that posts faster than a page per interval
loses posts between polls. The watchlist therefore stores a per-account
interval and reacts to the puller's rollover signal (a round where *every*
discovered id was new -> the page turned over -> posts were missed) by halving
that interval, then relaxing it back toward the default once the account goes
quiet again.

It lives in the same sqlite file as the dataset (WAL makes that safe) but owns
its own connection and its own table, and it never imports the puller or the
CLI: this module is state, not policy.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
import threading
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings
from .dataset import handle_key
from .models import Model

# A poll costs ~1-2 s of latency; anything below a minute per account is a
# self-inflicted rate limit, not better coverage.
INTERVAL_FLOOR = 60.0
# Consecutive clean (non-rollover, non-error) polls before the interval relaxes.
RELAX_AFTER = 5
# How much one relaxation step widens the interval.
RELAX_FACTOR = 1.25
# Posts sampled by suggest_interval(); ~50 covers a week for a busy account.
_SUGGEST_SAMPLE = 50
# Fewer gaps than this and the median is noise, not a posting rhythm.
_SUGGEST_MIN_POSTS = 5

WATCH_COLUMNS: tuple[str, ...] = (
    "handle",
    "display",
    "interval",
    "enabled",
    "tags",
    "note",
    "added_utc",
    "last_poll_utc",
    "next_due_utc",
    "polls",
    "new_posts",
    "last_new_utc",
    "rollovers",
    "last_error",
    "consecutive_errors",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watchlist (
    handle             TEXT PRIMARY KEY,
    display            TEXT,
    interval           REAL,
    enabled            INTEGER DEFAULT 1,
    tags               TEXT,
    note               TEXT,
    added_utc          TEXT,
    last_poll_utc      TEXT,
    next_due_utc       TEXT,
    polls              INTEGER DEFAULT 0,
    new_posts          INTEGER DEFAULT 0,
    last_new_utc       TEXT,
    rollovers          INTEGER DEFAULT 0,
    last_error         TEXT,
    consecutive_errors INTEGER DEFAULT 0,
    clean_polls        INTEGER DEFAULT 0,
    auto_tuned         INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS watchlist_due ON watchlist(enabled, next_due_utc);
"""


# --------------------------------------------------------------- helpers


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch(value: str | None) -> float | None:
    """Parse a stored timestamp back to epoch seconds. Junk reads as unknown."""
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _text(value: Any) -> str | None:
    if value is None:
        return None
    out = str(value).strip()
    return out or None


def _interval(value: Any) -> float | None:
    """Intervals are positive seconds or 'use the default'."""
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _norm_tags(tags: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    for raw in tags or ():
        tag = str(raw or "").strip().lstrip("#").lower()
        if tag and tag not in out:
            out.append(tag)
    return out


def _load_tags(raw: Any) -> list[str]:
    if raw in (None, ""):
        return []
    if isinstance(raw, list):
        return _norm_tags(raw)
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        # A hand-edited row must degrade, never kill the loop.
        return _norm_tags(str(raw).split(","))
    return _norm_tags(value) if isinstance(value, list) else []


def _display(handle: str | None, fallback: str) -> str:
    return (handle or "").strip().lstrip("@") or fallback


def _median(values: Sequence[float]) -> float:
    return float(statistics.median(values))


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


# ----------------------------------------------------------------- model


@dataclass
class Watch(Model):
    """One monitored account: identity, cadence, and what polling has learned."""

    handle: str
    display: str
    interval: float | None = None
    enabled: bool = True
    tags: list[str] = field(default_factory=list)
    note: str | None = None
    added_utc: str | None = None
    last_poll_utc: str | None = None
    next_due_utc: str | None = None
    polls: int = 0
    new_posts: int = 0
    last_new_utc: str | None = None
    rollovers: int = 0
    last_error: str | None = None
    consecutive_errors: int = 0

    def effective_interval(self, default: float) -> float:
        """The cadence this account is actually polled at."""
        return self.interval if self.interval and self.interval > 0 else float(default)

    def due_at(self, default: float) -> float | None:
        """Epoch seconds this account is next due; None means 'never polled'."""
        explicit = _epoch(self.next_due_utc)
        if explicit is not None:
            return explicit
        last = _epoch(self.last_poll_utc)
        if last is None:
            return None
        return last + self.effective_interval(default)


def _row_watch(row: sqlite3.Row) -> Watch:
    return Watch(
        handle=str(row["handle"] or ""),
        display=str(row["display"] or row["handle"] or ""),
        interval=_interval(row["interval"]),
        enabled=bool(row["enabled"]),
        tags=_load_tags(row["tags"]),
        note=_text(row["note"]),
        added_utc=_text(row["added_utc"]),
        last_poll_utc=_text(row["last_poll_utc"]),
        next_due_utc=_text(row["next_due_utc"]),
        polls=int(row["polls"] or 0),
        new_posts=int(row["new_posts"] or 0),
        last_new_utc=_text(row["last_new_utc"]),
        rollovers=int(row["rollovers"] or 0),
        last_error=_text(row["last_error"]),
        consecutive_errors=int(row["consecutive_errors"] or 0),
    )


# ------------------------------------------------------------- watchlist


class Watchlist:
    """The set of accounts a monitor polls, and the cadence it polls them at."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.path = Path(settings.data_dir) / "dataset.db"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.default_interval = float(getattr(settings, "watch_interval", 300.0) or 300.0)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            # CREATE IF NOT EXISTS only: user_version belongs to Dataset, which
            # shares this file.
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # -------------------------------------------------------------- db

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _fetch(self, key: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM watchlist WHERE handle = ?", (key,)
            ).fetchone()

    def _update(self, key: str, fields: dict[str, Any]) -> bool:
        if not key:
            return False
        if not fields:
            return self._fetch(key) is not None
        assignments = ", ".join(f"{name} = ?" for name in fields)
        with self._tx() as conn:
            cur = conn.execute(
                f"UPDATE watchlist SET {assignments} WHERE handle = ?",
                (*fields.values(), key),
            )
        return cur.rowcount > 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # --------------------------------------------------------- editing

    def add(
        self,
        handle: str,
        *,
        interval: float | None = None,
        tags: Iterable[str] | None = None,
        note: str | None = None,
        enabled: bool = True,
    ) -> Watch:
        """Idempotent upsert. Re-adding updates the row; counters survive."""
        key = handle_key(handle)
        if not key:
            raise ValueError("handle is required")
        display = _display(handle, key)
        seconds = _interval(interval)
        with self._tx() as conn:
            row = conn.execute("SELECT 1 FROM watchlist WHERE handle = ?", (key,)).fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO watchlist "
                    "(handle, display, interval, enabled, tags, note, added_utc) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        key,
                        display,
                        seconds,
                        1 if enabled else 0,
                        json.dumps(_norm_tags(tags), ensure_ascii=False),
                        _text(note),
                        _iso(time.time()),
                    ),
                )
            else:
                # Only what the caller actually passed is overwritten: a bare
                # re-add must not silently wipe an interval, tags or a note.
                fields: dict[str, Any] = {"display": display, "enabled": 1 if enabled else 0}
                if interval is not None:
                    fields["interval"] = seconds
                    fields["auto_tuned"] = 0
                if tags is not None:
                    fields["tags"] = json.dumps(_norm_tags(tags), ensure_ascii=False)
                if note is not None:
                    fields["note"] = _text(note)
                assignments = ", ".join(f"{name} = ?" for name in fields)
                conn.execute(
                    f"UPDATE watchlist SET {assignments} WHERE handle = ?",
                    (*fields.values(), key),
                )
        entry = self.get(key)
        if entry is None:  # pragma: no cover - the row was just written
            raise RuntimeError(f"watchlist row vanished: {key}")
        return entry

    def remove(self, handle: str) -> bool:
        key = handle_key(handle)
        if not key:
            return False
        with self._tx() as conn:
            cur = conn.execute("DELETE FROM watchlist WHERE handle = ?", (key,))
        return cur.rowcount > 0

    def enable(self, handle: str, on: bool = True) -> bool:
        return self._update(handle_key(handle), {"enabled": 1 if on else 0})

    def set_interval(self, handle: str, interval: float | None) -> bool:
        """Set (or clear) the per-account override. Clears auto-tuning."""
        return self._update(
            handle_key(handle), {"interval": _interval(interval), "auto_tuned": 0}
        )

    # --------------------------------------------------------- reading

    def get(self, handle: str) -> Watch | None:
        key = handle_key(handle)
        if not key:
            return None
        row = self._fetch(key)
        return None if row is None else _row_watch(row)

    def list(self, *, enabled_only: bool = False, tag: str | None = None) -> list[Watch]:
        clause = " WHERE enabled = 1" if enabled_only else ""
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM watchlist{clause} ORDER BY handle ASC"
            ).fetchall()
        entries = [_row_watch(r) for r in rows]
        wanted = (tag or "").strip().lstrip("#").lower()
        if wanted:
            entries = [e for e in entries if wanted in e.tags]
        return entries

    def due(self, now: float | None = None, *, default_interval: float) -> list[Watch]:
        """Enabled entries whose next poll is owed, most overdue first.

        A never-polled entry is maximally overdue, which is what makes a fresh
        `magpie watch` pick every account up on its first round.
        """
        moment = time.time() if now is None else float(now)
        default = float(default_interval) if default_interval and default_interval > 0 else (
            self.default_interval
        )
        ranked: list[tuple[float, str, Watch]] = []
        for entry in self.list(enabled_only=True):
            due_at = entry.due_at(default)
            if due_at is None:
                # float('inf') sorts ahead of every real overdue amount.
                ranked.append((float("inf"), entry.handle, entry))
            elif due_at <= moment:
                ranked.append((moment - due_at, entry.handle, entry))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [entry for _, _, entry in ranked]

    # --------------------------------------------------------- polling

    def record_poll(
        self,
        handle: str,
        *,
        new_posts: int = 0,
        rollover: bool = False,
        error: str | None = None,
        now: float | None = None,
        default_interval: float | None = None,
    ) -> None:
        """Fold one poll's outcome into the entry and schedule the next one.

        Never raises: a monitor that dies because a counter update failed is a
        worse outcome than a lost counter.
        """
        key = handle_key(handle)
        if not key:
            return
        moment = time.time() if now is None else float(now)
        default = _interval(default_interval) or self.default_interval
        try:
            row = self._fetch(key)
            if row is None:
                return
            entry = _row_watch(row)
            effective = entry.effective_interval(default)
            auto_tuned = bool(row["auto_tuned"])
            clean = int(row["clean_polls"] or 0)
            stored = entry.interval

            if rollover:
                # The page rolled over between polls: posts were missed, so
                # poll twice as often and remember that we chose this.
                stored = max(INTERVAL_FLOOR, effective / 2.0)
                auto_tuned = True
                clean = 0
            elif error:
                clean = 0
            else:
                clean += 1
                if clean >= RELAX_AFTER and auto_tuned and stored is not None:
                    relaxed = min(default, stored * RELAX_FACTOR)
                    # Never past the default, and never below the floor.
                    stored = max(INTERVAL_FLOOR, relaxed)
                    if stored >= default:
                        # Back at the default: stop tuning, drop the override.
                        stored = None
                        auto_tuned = False
                    clean = 0

            gained = max(0, int(new_posts or 0))
            message = _text(error)
            interval_now = stored if stored and stored > 0 else default
            fields: dict[str, Any] = {
                "interval": stored,
                "auto_tuned": 1 if auto_tuned else 0,
                "clean_polls": clean,
                "polls": entry.polls + 1,
                "new_posts": entry.new_posts + gained,
                "last_poll_utc": _iso(moment),
                "next_due_utc": _iso(moment + interval_now),
                "rollovers": entry.rollovers + (1 if rollover else 0),
                "last_error": message,
                "consecutive_errors": entry.consecutive_errors + 1 if message else 0,
            }
            if gained:
                fields["last_new_utc"] = _iso(moment)
            self._update(key, fields)
        except sqlite3.Error:
            # Degrade to a missed bookkeeping update, not a dead loop.
            return

    def suggest_interval(
        self,
        handle: str,
        dataset: Any,
        *,
        floor: float = 60.0,
        ceiling: float = 3600.0,
        ids_per_page: int = 5,
        window_days: float = 14.0,
    ) -> float | None:
        """Cadence that keeps a page of ids from rolling over, from history.

        Median gap between this handle's recent posts times the ids one page
        exposes, halved: poll at twice the rate the account actually needs.
        None when there is not enough recent history to measure.

        Only posts from the last `window_days` count. A `--deep` backfill can
        drop years of archive into the dataset, and measured against @AFP that
        dragged the median gap from minutes to months, pinning the suggestion
        at the 3600 s ceiling for an account that actually posts every few
        minutes. Current cadence must be measured from current behaviour.
        """
        key = handle_key(handle)
        if not key or dataset is None:
            return None
        try:
            rows, _total = dataset.posts(handle=key, limit=_SUGGEST_SAMPLE)
        except Exception:
            return None
        cutoff = time.time() - float(window_days) * 86400.0
        stamps: list[float] = []
        for row in rows or ():
            moment = _epoch(row.get("created_at_utc") if isinstance(row, dict) else None)
            if moment is not None and moment >= cutoff:
                stamps.append(moment)
        if len(stamps) < _SUGGEST_MIN_POSTS:
            return None
        stamps.sort(reverse=True)
        gaps = [
            earlier - later
            for earlier, later in zip(stamps, stamps[1:])
            if earlier - later > 0
        ]
        if not gaps:
            return None
        pages = max(1, int(ids_per_page or 1))
        return _clamp(_median(gaps) * pages * 0.5, float(floor), float(ceiling))

    # ----------------------------------------------------------- stats

    def stats(self) -> dict[str, Any]:
        entries = self.list()
        now = time.time()
        tags: dict[str, int] = {}
        for entry in entries:
            for tag in entry.tags:
                tags[tag] = tags.get(tag, 0) + 1
        enabled = [e for e in entries if e.enabled]
        due_now = self.due(now, default_interval=self.default_interval)
        next_due = [
            e.due_at(self.default_interval)
            for e in enabled
            if e.due_at(self.default_interval) is not None
        ]
        return {
            "total": len(entries),
            "enabled": len(enabled),
            "disabled": len(entries) - len(enabled),
            "due": len(due_now),
            "polls": sum(e.polls for e in entries),
            "new_posts": sum(e.new_posts for e in entries),
            "rollovers": sum(e.rollovers for e in entries),
            "erroring": sum(1 for e in entries if e.consecutive_errors > 0),
            "tuned": sum(1 for e in entries if e.interval is not None),
            "default_interval": self.default_interval,
            "tags": dict(sorted(tags.items())),
            "next_due_utc": _iso(min(next_due)) if next_due else None,
            "last_poll_utc": max((e.last_poll_utc for e in entries if e.last_poll_utc), default=None),
        }
