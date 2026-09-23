"""Dataset tests: re-ingest semantics, the crawler's skip check, filters, safety."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from pathlib import Path

import pytest

from magpie import dataset as dataset_mod
from magpie.config import Settings
from magpie.dataset import Dataset
from magpie.models import MediaItem, Post, Profile


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path)


@pytest.fixture
def ds(settings: Settings) -> Dataset:
    d = Dataset(settings)
    yield d
    d.close()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch):
    """Deterministic, advanceable _now() — real time is second-resolution."""

    state = {"t": "2026-03-01T00:00:00Z"}
    monkeypatch.setattr(dataset_mod, "_now", lambda: state["t"])
    return state


def make_post(
    tweet_id: str = "1",
    handle: str = "alpha",
    text: str = "hello world",
    created: str = "2026-03-01T10:00:00Z",
    likes: int = 1,
    media: list[MediaItem] | None = None,
) -> Post:
    return Post(
        id=tweet_id,
        screen_name=handle,
        name=handle.title(),
        text=text,
        created_at_utc=created,
        lang="en",
        counts={"likes": likes, "retweets": likes * 2, "views": likes * 100},
        media=list(media or []),
        source_url=f"https://x.com/{handle}/status/{tweet_id}",
        available_sources=["syndication", "fxtwitter"],
    )


def photo(url: str) -> MediaItem:
    return MediaItem(type="photo", best_url=url, thumb_url=f"{url}?thumb", width=800, height=600)


def test_reingest_keeps_first_seen_and_refreshes_metrics(ds: Dataset, clock) -> None:
    assert ds.upsert_post(make_post(likes=10)) is True
    assert ds.upsert_post(make_post(likes=10)) is False

    first = ds.post("1")
    assert first["first_seen_utc"] == "2026-03-01T00:00:00Z"
    assert first["last_seen_utc"] == "2026-03-01T00:00:00Z"
    assert first["likes"] == 10

    clock["t"] = "2026-03-02T00:00:00Z"
    assert ds.upsert_post(make_post(likes=99, text="edited text")) is False

    again = ds.post("1")
    assert again["first_seen_utc"] == "2026-03-01T00:00:00Z"
    assert again["last_seen_utc"] == "2026-03-02T00:00:00Z"
    assert again["likes"] == 99
    assert again["retweets"] == 198
    assert again["text"] == "edited text"
    assert ds.stats()["posts"] == 1


def test_reingest_replaces_media_rows(ds: Dataset) -> None:
    ds.upsert_post(make_post(media=[photo("https://img/a.jpg"), photo("https://img/b.jpg")]))
    ds.upsert_post(make_post(media=[photo("https://img/c.jpg")]))

    row = ds.post("1")
    assert [m["url"] for m in row["media"]] == ["https://img/c.jpg"]
    assert row["media_count"] == 1
    assert row["has_media"] is True

    ds.upsert_post(make_post(media=[]))
    stripped = ds.post("1")
    assert stripped["media"] == []
    assert stripped["has_media"] is False


def test_known_returns_stored_subset_across_chunk_boundary(ds: Dataset) -> None:
    stored = {str(100 + i) for i in range(0, 1300, 7)}
    for tid in sorted(stored):
        ds.upsert_post(make_post(tweet_id=tid))

    probe = [str(100 + i) for i in range(1300)] + ["999999", "999999"]
    assert len(probe) > dataset_mod._ID_CHUNK
    assert ds.known(probe) == stored
    assert ds.known([]) == set()
    assert ds.known(["", None]) == set()


def test_posts_filters_and_total(ds: Dataset) -> None:
    ds.upsert_post(
        make_post("1", "Alpha", "cats and dogs", "2026-03-01T10:00:00Z", media=[photo("u")])
    )
    ds.upsert_post(make_post("2", "Alpha", "rockets to orbit", "2026-03-05T09:00:00Z"))
    ds.upsert_post(make_post("3", "beta", "cats in space", "2026-02-01T08:00:00Z"))

    rows, total = ds.posts()
    assert total == 3
    assert [r["id"] for r in rows] == ["2", "1", "3"]  # newest first

    for spelling in ("alpha", "@ALPHA", "Alpha"):
        rows, total = ds.posts(handle=spelling)
        assert total == 2, spelling
        assert {r["id"] for r in rows} == {"1", "2"}

    rows, total = ds.posts(q="cats")
    assert {r["id"] for r in rows} == {"1", "3"}

    rows, total = ds.posts(since="2026-03-01", until="2026-03-01")
    assert [r["id"] for r in rows] == ["1"]  # bare until covers the whole day

    rows, total = ds.posts(since="2026-02-15T00:00:00Z")
    assert {r["id"] for r in rows} == {"1", "2"}

    rows, _ = ds.posts(has_media=True)
    assert [r["id"] for r in rows] == ["1"]
    rows, _ = ds.posts(has_media=False)
    assert {r["id"] for r in rows} == {"2", "3"}

    rows, total = ds.posts(limit=1)
    assert len(rows) == 1 and total == 3
    rows, total = ds.posts(limit=1, offset=1)
    assert [r["id"] for r in rows] == ["1"] and total == 3


def test_query_is_read_only(ds: Dataset) -> None:
    ds.upsert_post(make_post("1", "alpha"))

    with pytest.raises(ValueError):
        ds.query("DELETE FROM posts")
    with pytest.raises(ValueError):
        ds.query("SELECT 1; DROP TABLE posts")
    with pytest.raises(ValueError):
        ds.query("  -- comment\n PRAGMA user_version")

    rows, truncated = ds.query(
        "SELECT id, screen_name FROM posts WHERE screen_name = ?", ("alpha",)
    )
    assert rows == [{"id": "1", "screen_name": "alpha"}] and truncated is False
    assert ds.query("SELECT COUNT(*) AS n FROM posts;")[0][0]["n"] == 1
    assert ds.stats()["posts"] == 1


def test_query_bounds_runaway_work(ds: Dataset) -> None:
    """query_only stops writes, not work: a recursive CTE otherwise never ends."""
    for n in range(5):
        ds.upsert_post(make_post(str(n), "alpha"))

    rows, truncated = ds.query("SELECT id FROM posts", max_rows=2)
    assert len(rows) == 2 and truncated is True

    with pytest.raises(sqlite3.OperationalError):
        ds.query(
            "WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c) "
            "SELECT count(*) FROM c",
            timeout=0.5,
        )


def test_cursor_roundtrip_for_unseen_handle(ds: Dataset) -> None:
    empty = ds.get_cursor("@NeverPolled")
    assert empty["last_seen_id"] is None
    assert empty["polls"] == 0
    assert empty["new_count"] == 0

    ds.set_cursor("@NeverPolled", last_seen_id="42", polls=1, new_count=3)
    ds.set_cursor("neverpolled", last_poll_utc="2026-03-01T00:00:00Z")

    state = ds.get_cursor("NEVERPOLLED")
    assert state["last_seen_id"] == "42"
    assert state["polls"] == 1
    assert state["new_count"] == 3
    assert state["last_poll_utc"] == "2026-03-01T00:00:00Z"
    assert ds.stats()["handles_watched"] == 1

    with pytest.raises(ValueError):
        ds.set_cursor("neverpolled", bogus=1)


def test_exports_stream_rows_and_respect_handle(ds: Dataset, tmp_path: Path) -> None:
    ds.upsert_post(make_post("1", "alpha", "first", "2026-03-01T10:00:00Z", media=[photo("u")]))
    ds.upsert_post(make_post("2", "beta", "second", "2026-03-02T10:00:00Z"))

    lines = [json.loads(line) for line in ds.export_jsonl().splitlines()]
    assert [r["id"] for r in lines] == ["2", "1"]
    assert lines[1]["media"][0]["url"] == "u"

    out = tmp_path / "sub" / "alpha.csv"
    assert ds.export_csv(str(out), handle="@Alpha") == str(out)
    rows = list(csv.reader(io.StringIO(out.read_text(encoding="utf-8"))))
    assert rows[0] == list(dataset_mod.CSV_HEADER)
    assert len(rows) == 2
    assert rows[1][0] == "1" and rows[1][2] == "alpha"


def test_upsert_user_and_id_guard(ds: Dataset) -> None:
    assert ds.upsert_user(Profile(screen_name="@Alpha", name="A", followers=10)) is True
    assert ds.upsert_user(Profile(screen_name="alpha", name="A2", followers=20)) is False
    users = ds.users()
    assert len(users) == 1
    assert users[0]["screen_name"] == "alpha"
    assert users[0]["followers"] == 20

    with pytest.raises(ValueError):
        ds.upsert_post(make_post(tweet_id=""))
    assert ds.stats()["posts"] == 0
