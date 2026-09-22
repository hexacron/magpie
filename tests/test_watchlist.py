"""Watchlist tests: idempotent membership, due-ordering, and cadence self-tuning.

Every test drives time explicitly (`now=`), so nothing here sleeps or touches
the network.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from magpie.config import Settings
from magpie.dataset import Dataset
from magpie.models import Post
from magpie.watchlist import INTERVAL_FLOOR, Watchlist

T0 = 1_700_000_000.0
DEFAULT = 300.0


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path)


@pytest.fixture
def wl(settings: Settings) -> Watchlist:
    w = Watchlist(settings)
    yield w
    w.close()


def roll(wl: Watchlist, handle: str, *, at: float, times: int = 1) -> None:
    for i in range(times):
        wl.record_poll(handle, rollover=True, now=at + i, default_interval=DEFAULT)


def clean(wl: Watchlist, handle: str, *, at: float, times: int = 1) -> None:
    for i in range(times):
        wl.record_poll(handle, new_posts=1, now=at + i, default_interval=DEFAULT)


# ------------------------------------------------------------ membership


def test_add_is_idempotent_and_normalises_the_handle(wl: Watchlist, settings: Settings) -> None:
    wl.add("@Handle", tags=["News"], note="first")
    wl.add("handle", note="second")
    wl.add("HANDLE", tags=["ops"])

    entries = wl.list()
    assert [e.handle for e in entries] == ["handle"]

    entry = entries[0]
    assert entry.display == "HANDLE"  # latest spelling wins
    assert entry.note == "second"  # updated, not duplicated
    assert entry.tags == ["ops"]
    assert wl.get("@handle").handle == "handle"

    # One physical row, in the dataset's own file.
    assert (Path(settings.data_dir) / "dataset.db").exists()
    assert len(wl.list()) == 1


def test_add_preserves_counters_and_unspecified_fields(wl: Watchlist) -> None:
    wl.add("alpha", interval=120.0, tags=["ops"], note="keep me")
    clean(wl, "alpha", at=T0, times=2)

    again = wl.add("alpha")  # bare re-add must not wipe anything

    assert again.polls == 2
    assert again.interval == 120.0
    assert again.tags == ["ops"]
    assert again.note == "keep me"
    assert again.new_posts == 2


def test_remove_and_enable(wl: Watchlist) -> None:
    wl.add("alpha")
    assert wl.enable("alpha", False) is True
    assert wl.get("alpha").enabled is False
    assert wl.remove("@Alpha") is True
    assert wl.remove("alpha") is False
    assert wl.get("alpha") is None


# ------------------------------------------------------------------ due


def test_due_respects_per_account_interval_and_orders_most_overdue_first(
    wl: Watchlist,
) -> None:
    wl.add("alpha")  # uses the default 300 s
    wl.add("beta", interval=60.0)
    wl.add("gamma", interval=900.0)
    wl.add("delta")  # never polled
    wl.add("epsilon", enabled=False)

    for handle in ("alpha", "beta", "gamma"):
        wl.record_poll(handle, now=T0, default_interval=DEFAULT)
    wl.record_poll("epsilon", now=T0, default_interval=DEFAULT)

    assert [e.handle for e in wl.due(T0 + 10, default_interval=DEFAULT)] == ["delta"]

    ordered = [e.handle for e in wl.due(T0 + 310, default_interval=DEFAULT)]
    # never-polled first, then beta (250 s overdue), then alpha (10 s overdue);
    # gamma's 900 s override is not up yet and the disabled row never appears.
    assert ordered == ["delta", "beta", "alpha"]

    late = [e.handle for e in wl.due(T0 + 1000, default_interval=DEFAULT)]
    assert late == ["delta", "beta", "alpha", "gamma"]


def test_due_is_exclusive_of_not_yet_due_entries(wl: Watchlist) -> None:
    wl.add("alpha", interval=60.0)
    wl.record_poll("alpha", now=T0, default_interval=DEFAULT)
    assert wl.due(T0 + 59, default_interval=DEFAULT) == []
    assert [e.handle for e in wl.due(T0 + 60, default_interval=DEFAULT)] == ["alpha"]


# -------------------------------------------------------------- tuning


def test_rollover_halves_the_interval_and_persists_it(
    wl: Watchlist, settings: Settings
) -> None:
    wl.add("alpha")
    wl.record_poll("alpha", new_posts=5, rollover=True, now=T0, default_interval=DEFAULT)

    entry = wl.get("alpha")
    assert entry.interval == DEFAULT / 2
    assert entry.rollovers == 1
    assert entry.new_posts == 5
    assert entry.next_due_utc is not None

    # Persisted: a fresh handle on the same file sees the tuned cadence.
    other = Watchlist(settings)
    try:
        assert other.get("alpha").interval == DEFAULT / 2
    finally:
        other.close()


def test_repeated_rollovers_stop_at_the_floor(wl: Watchlist) -> None:
    wl.add("alpha")
    seen: list[float] = []
    for i in range(8):
        wl.record_poll("alpha", rollover=True, now=T0 + i, default_interval=DEFAULT)
        seen.append(wl.get("alpha").interval)

    assert seen[:3] == [150.0, 75.0, INTERVAL_FLOOR]
    assert all(v >= INTERVAL_FLOOR for v in seen)
    assert seen[-1] == INTERVAL_FLOOR
    assert wl.get("alpha").rollovers == 8


def test_five_clean_polls_relax_the_interval_but_never_past_the_default(
    wl: Watchlist,
) -> None:
    wl.add("alpha")
    roll(wl, "alpha", at=T0)
    assert wl.get("alpha").interval == 150.0

    # Four clean polls are not enough to move it.
    clean(wl, "alpha", at=T0 + 10, times=4)
    assert wl.get("alpha").interval == 150.0

    # The fifth relaxes by 25%.
    clean(wl, "alpha", at=T0 + 20)
    assert wl.get("alpha").interval == pytest.approx(187.5)

    # Keep relaxing: monotonic upward, capped at the default.
    previous = 187.5
    for step in range(10):
        clean(wl, "alpha", at=T0 + 100 + step * 10, times=5)
        entry = wl.get("alpha")
        current = entry.effective_interval(DEFAULT)
        assert previous <= current <= DEFAULT
        previous = current
    assert wl.get("alpha").effective_interval(DEFAULT) == DEFAULT


def test_manual_interval_is_never_auto_relaxed(wl: Watchlist) -> None:
    wl.add("alpha")
    assert wl.set_interval("alpha", 90.0) is True
    clean(wl, "alpha", at=T0, times=20)
    assert wl.get("alpha").interval == 90.0

    assert wl.set_interval("alpha", None) is True
    assert wl.get("alpha").interval is None


def test_errors_count_consecutively_and_reset_without_disabling(wl: Watchlist) -> None:
    wl.add("alpha")
    wl.record_poll("alpha", error="503", now=T0, default_interval=DEFAULT)
    wl.record_poll("alpha", error="timeout", now=T0 + 1, default_interval=DEFAULT)

    entry = wl.get("alpha")
    assert entry.consecutive_errors == 2
    assert entry.last_error == "timeout"
    assert entry.enabled is True  # a 503 is not a suspension

    wl.record_poll("alpha", new_posts=1, now=T0 + 2, default_interval=DEFAULT)
    entry = wl.get("alpha")
    assert entry.consecutive_errors == 0
    assert entry.last_error is None
    assert entry.last_new_utc is not None


def test_record_poll_for_an_unknown_handle_is_a_noop(wl: Watchlist) -> None:
    wl.record_poll("ghost", new_posts=3, now=T0, default_interval=DEFAULT)
    assert wl.get("ghost") is None
    assert wl.list() == []


# ------------------------------------------------------------- suggest


# Relative to now: suggest_interval measures *current* cadence and ignores
# anything older than its window, so a fixed past date would measure nothing.
BASE = datetime.now(timezone.utc)


def seed(ds: Dataset, handle: str, count: int, gap_minutes: int) -> None:
    """`count` posts, newest first, `gap_minutes` apart."""
    for i in range(count):
        when = BASE - timedelta(minutes=i * gap_minutes)
        ds.upsert_post(
            Post(
                id=f"{handle}-{i}",
                screen_name=handle,
                text=f"post {i}",
                created_at_utc=when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )


def test_suggest_interval_from_posting_rhythm(settings: Settings, wl: Watchlist) -> None:
    ds = Dataset(settings)
    try:
        seed(ds, "steady", 8, 10)  # one post every 10 minutes
        seed(ds, "sparse", 4, 10)  # not enough history to measure
        seed(ds, "slow", 8, 600)  # one post every 10 hours

        wl.add("steady")
        wl.add("sparse")
        wl.add("slow")

        # 600 s median gap * 5 ids per page * 0.5 = poll twice as often as needed.
        assert wl.suggest_interval("@Steady", ds) == pytest.approx(1500.0)
        assert wl.suggest_interval("sparse", ds) is None
        assert wl.suggest_interval("slow", ds) == pytest.approx(3600.0)  # ceiling
        assert wl.suggest_interval("steady", ds, ceiling=900.0) == pytest.approx(900.0)
        assert wl.suggest_interval("steady", ds, ids_per_page=1) == pytest.approx(300.0)
        assert wl.suggest_interval("unknown", ds) is None

        # A `--deep` backfill drops years of archive into the dataset. Measured
        # against @AFP that dragged the median gap from minutes to months and
        # pinned the suggestion at the ceiling for an account posting every few
        # minutes. Old posts must not influence current cadence.
        for i in range(40):
            old = BASE - timedelta(days=30 * (i + 1))
            ds.upsert_post(
                Post(
                    id=f"steady-archive-{i}",
                    screen_name="steady",
                    text="archive",
                    created_at_utc=old.strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
            )
        assert wl.suggest_interval("steady", ds) == pytest.approx(1500.0)
    finally:
        ds.close()


# ---------------------------------------------------------------- list


def test_list_filters_by_tag_exactly_and_by_enabled(wl: Watchlist) -> None:
    wl.add("alpha", tags=["news", "ops"])
    wl.add("beta", tags=["newsroom"])
    wl.add("gamma", tags=["ops"], enabled=False)

    assert [e.handle for e in wl.list(tag="news")] == ["alpha"]
    assert [e.handle for e in wl.list(tag="#NEWS")] == ["alpha"]
    assert [e.handle for e in wl.list(tag="newsroom")] == ["beta"]
    assert [e.handle for e in wl.list(tag="ops")] == ["alpha", "gamma"]
    assert [e.handle for e in wl.list(tag="ops", enabled_only=True)] == ["alpha"]
    assert [e.handle for e in wl.list(enabled_only=True)] == ["alpha", "beta"]
    assert [e.handle for e in wl.list()] == ["alpha", "beta", "gamma"]


def test_stats_summarises_the_watchlist(wl: Watchlist) -> None:
    wl.add("alpha", tags=["news"])
    wl.add("beta", enabled=False)
    wl.record_poll("alpha", new_posts=2, rollover=True, now=T0, default_interval=DEFAULT)

    stats = wl.stats()
    assert stats["total"] == 2
    assert stats["enabled"] == 1
    assert stats["disabled"] == 1
    assert stats["polls"] == 1
    assert stats["new_posts"] == 2
    assert stats["rollovers"] == 1
    assert stats["tags"] == {"news": 1}
    assert stats["erroring"] == 0
