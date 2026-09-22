"""Store tests: index behaviour, meta outside the hash chain, rebuildability."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from magpie.config import Settings
from magpie.hashing import sha256_file
from magpie.models import CrossCheck, IndexRow, Manifest, MediaItem, Post, TweetRef
from magpie.store import Store

T0 = datetime(2026, 3, 1, 9, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path)


@pytest.fixture
def store(settings: Settings) -> Store:
    s = Store(settings)
    yield s
    s.close()


def add(
    store: Store,
    tweet_id: str,
    handle: str,
    text: str,
    when: datetime,
    *,
    flags: list[str] | None = None,
    timestamped: bool = False,
) -> IndexRow:
    ref = TweetRef(
        input=f"https://x.com/{handle}/status/{tweet_id}",
        tweet_id=tweet_id,
        screen_name=handle,
    )
    folder, pkg_dir = store.allocate(ref, when)
    manifest = Manifest(
        capture_time_utc=when.strftime("%Y-%m-%dT%H:%M:%SZ"),
        input=ref.input,
        tweet_id=tweet_id,
        screen_name=handle,
        folder=folder,
        status="live",
        post=Post(
            id=tweet_id,
            screen_name=handle,
            name=handle.title(),
            text=text,
            created_at_utc="2026-02-28T12:00:00Z",
            available_sources=["syndication", "fxtwitter"],
            media=[MediaItem(type="photo", best_url="https://pbs.twimg.com/media/x.jpg")],
        ),
        crosscheck=CrossCheck(flags=flags or []),
    )
    store.write_manifest(pkg_dir, manifest)
    if timestamped:
        (pkg_dir / "timestamp.tsr").write_bytes(b"\x30\x82fake-token")
    return store.index_upsert(manifest)


def _tsa_reply(pkg_dir: Path, *, ok: bool | None, token: bytes = b"\x30\x82reply") -> None:
    (pkg_dir / "timestamp.tsr").write_bytes(token)
    if ok is not None:
        (pkg_dir / "timestamp.json").write_text(
            json.dumps({"enabled": True, "ok": ok, "tsa_url": "https://freetsa.org/tsr"}),
            encoding="utf-8",
        )


def test_a_rejected_tsa_reply_is_not_counted_as_timestamped(store: Store) -> None:
    # The TSA's raw 200 is kept as evidence even when it carries no token,
    # so file existence alone must not decide this.
    bare = add(store, "111", "alice", "token only, no sidecar", T0, timestamped=True)
    rejected = add(store, "222", "bob", "tsa said no", T0 + timedelta(minutes=1))
    signed = add(store, "333", "carol", "properly anchored", T0 + timedelta(minutes=2))
    _tsa_reply(store.package_path(rejected.folder), ok=False)
    _tsa_reply(store.package_path(signed.folder), ok=True)

    assert store.reindex() == 3
    assert store.get_row(bare.folder).timestamped is True  # sidecar absent -> file fallback
    assert store.get_row(rejected.folder).timestamped is False
    assert store.get_row(signed.folder).timestamped is True
    assert store.stats()["timestamped"] == 2


def test_search_filters_by_handle_tag_and_free_text(store: Store) -> None:
    alice = add(store, "111", "alice", "Falcon 9 launch cadence keeps climbing", T0)
    bob = add(store, "222", "bob", "Sourdough starter on day nine", T0 + timedelta(minutes=5))

    rows, total = store.search(user="@Alice")
    assert [r.folder for r in rows] == [alice.folder]
    assert total == 1

    rows, _ = store.search(q="sourdough")
    assert [r.folder for r in rows] == [bob.folder]

    store.set_meta(alice.folder, ["space", "logistics"], "")
    rows, total = store.search(tag="space")
    assert [r.folder for r in rows] == [alice.folder]
    assert total == 1

    # tag matching is exact, not a prefix scan over the JSON blob
    assert store.search(tag="spa")[1] == 0

    assert store.search(date="2026-03-01")[1] == 2
    assert store.search(date="2026-03-02")[1] == 0

    rows, _ = store.search()
    assert [r.folder for r in rows] == [bob.folder, alice.folder]  # newest first


def test_search_pagination_reports_full_total(store: Store) -> None:
    for i in range(5):
        add(store, f"90{i}", "dana", f"post number {i}", T0 + timedelta(minutes=i))

    rows, total = store.search(limit=2, offset=0)
    page2, total2 = store.search(limit=2, offset=2)
    assert total == total2 == 5
    assert len(rows) == 2
    assert not {r.folder for r in rows} & {r.folder for r in page2}


def test_search_survives_fts_absence_and_hostile_queries(store: Store) -> None:
    add(store, "111", "alice", "Falcon 9 launch cadence keeps climbing", T0)
    bob = add(store, "222", "bob", "Sourdough starter on day nine", T0 + timedelta(minutes=5))

    with_fts = [r.folder for r in store.search(q="sourdough")[0]]
    assert with_fts == [bob.folder]
    assert store.search(q='foo"bar OR')[1] == 0  # quoting, not a syntax error

    store.fts_enabled = False
    assert [r.folder for r in store.search(q="sourdough")[0]] == with_fts
    assert store.search(q="100%_")[1] == 0  # LIKE wildcards are escaped


def test_set_meta_is_searchable_and_leaves_the_hash_chain_alone(store: Store) -> None:
    row = add(store, "333", "carol", "Original post body", T0)
    pkg_dir = store.package_path(row.folder)
    sealed = sha256_file(pkg_dir / "manifest.json")
    assert row.manifest_sha256 == sealed

    store.set_meta(row.folder, ["  urgent ", "urgent", "", "litigation"], "subpoena reference")

    updated = store.get_row(row.folder)
    assert updated is not None
    assert updated.tags == ["urgent", "litigation"]  # stripped, deduped, ordered
    assert updated.note == "subpoena reference"
    assert updated.manifest_sha256 == sealed
    assert sha256_file(pkg_dir / "manifest.json") == sealed
    assert (pkg_dir / "MANIFEST.sha256").read_text() == f"{sealed}  manifest.json\n"

    assert [r.folder for r in store.search(tag="litigation")[0]] == [row.folder]
    assert [r.folder for r in store.search(q="subpoena")[0]] == [row.folder]

    with pytest.raises(FileNotFoundError):
        store.set_meta("20260301T090000Z_nobody_777", ["x"], "")


def test_version_chain_orders_captures_of_one_tweet(store: Store) -> None:
    first = add(store, "999", "carol", "v1 body", T0)
    second = add(store, "999", "carol", "v2 body", T0 + timedelta(hours=2))
    other = add(store, "888", "carol", "unrelated", T0 + timedelta(hours=3))

    assert first.folder != second.folder
    assert store.previous_capture("999", second.folder).folder == first.folder
    assert store.previous_capture("999", first.folder).folder == second.folder
    assert store.previous_capture("888", other.folder) is None
    assert [r.folder for r in store.versions("999")] == [first.folder, second.folder]


def test_allocate_never_reuses_a_directory(store: Store) -> None:
    ref = TweetRef(input="x", tweet_id="999", screen_name="carol")
    folder_a, path_a = store.allocate(ref, T0)
    folder_b, path_b = store.allocate(ref, T0)
    assert folder_a != folder_b
    assert path_a != path_b
    assert folder_b == f"{folder_a}-2"
    assert (path_b / "media").is_dir()


def test_reindex_rebuilds_an_identical_index_from_disk(store: Store, settings: Settings) -> None:
    a = add(store, "111", "alice", "Falcon 9 launch cadence", T0, flags=["source_truncated:syndication"])
    add(store, "222", "bob", "Sourdough starter", T0 + timedelta(minutes=5), timestamped=True)
    add(store, "111", "alice", "Falcon 9 launch cadence", T0 + timedelta(minutes=9))
    store.set_meta(a.folder, ["space"], "keep for the brief")
    before = store.export_rows()
    store.close()

    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(settings.index_db) + suffix)
        candidate.unlink(missing_ok=True)

    fresh = Store(settings)
    try:
        assert fresh.search(limit=100)[1] == 0
        assert fresh.reindex() == 3
        assert fresh.export_rows() == before
        assert fresh.get_row(a.folder).tags == ["space"]
        assert [r.timestamped for r in fresh.search(user="bob")[0]] == [True]
    finally:
        fresh.close()


def test_reindex_skips_corrupt_packages(store: Store, settings: Settings) -> None:
    add(store, "111", "alice", "good package", T0)
    broken = settings.captures_dir / "20260301T090500Z_bob_222"
    broken.mkdir()
    (broken / "manifest.json").write_text("{not json", encoding="utf-8")
    (settings.captures_dir / "20260301T090600Z_eve_333").mkdir()  # no manifest at all

    assert store.reindex() == 1


def test_delete_removes_package_and_row(store: Store) -> None:
    row = add(store, "111", "alice", "goodbye", T0)
    assert store.delete(row.folder) is True
    assert store.get_row(row.folder) is None
    assert not store.package_path(row.folder).exists()
    assert store.delete(row.folder) is False


def test_make_zip_is_deterministic_and_folder_prefixed(store: Store) -> None:
    row = add(store, "111", "alice", "zip me", T0)
    (store.package_path(row.folder) / "media" / "photo_01.jpg").write_bytes(b"jpegbytes")

    first = store.make_zip(row.folder)
    second = store.make_zip(row.folder)
    try:
        import zipfile

        with zipfile.ZipFile(first) as zf:
            names = zf.namelist()
        assert names == sorted(names)
        assert f"{row.folder}/manifest.json" in names
        assert f"{row.folder}/media/photo_01.jpg" in names
        assert first.read_bytes() == second.read_bytes()
    finally:
        first.unlink(missing_ok=True)
        second.unlink(missing_ok=True)


@pytest.mark.parametrize("bad", ["../etc", "..", "sub/dir", "sub\\dir", "", "   "])
def test_package_path_rejects_traversal(store: Store, bad: str) -> None:
    with pytest.raises(ValueError):
        store.package_path(bad)
