"""Extraction pipeline: discover ids -> hydrate -> upsert into the dataset.

This is the data path, not the evidence path. Nothing here writes capture
packages, hashes files, renders documents or contacts a timestamp authority.

Only one timeline source exists without a login: the server-rendered profile
page at ``x.com/<handle>``, which carries the pinned post plus the 4-5 most
recent ones. Everything else logged-out (search, with_replies, hashtag) returns
an empty app shell. Coverage is therefore a function of poll cadence: to follow
an account posting faster than ~5 posts per interval you must poll faster, and
`watch` reports when a poll came back entirely new, which is the signal that
you are losing posts between polls.
"""

from __future__ import annotations

import asyncio
import dataclasses
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Sequence

import httpx

from . import xapi
from .collect import conversation_ids, profile_ids, syndication_timeline, user_profile
from .config import Settings, load_settings
from .ids import parse_inputs
from .models import Model, Post, TweetRef
from .normalize import merge, parse_sources
from .sources import fetch_sources

__all__ = [
    "PullReport",
    "hydrate",
    "pull",
    "pull_handles",
    "pull_ids",
    "expand_thread",
    "watch",
    "monitor",
]


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class PullReport(Model):
    handles: list[str] = field(default_factory=list)
    discovered: int = 0
    already_known: int = 0
    hydrated: int = 0
    new_posts: int = 0
    updated_posts: int = 0
    users: int = 0
    errors: list[str] = field(default_factory=list)
    elapsed_ms: int = 0

    def merge_in(self, other: "PullReport") -> "PullReport":
        self.handles += [h for h in other.handles if h not in self.handles]
        self.discovered += other.discovered
        self.already_known += other.already_known
        self.hydrated += other.hydrated
        self.new_posts += other.new_posts
        self.updated_posts += other.updated_posts
        self.users += other.users
        self.errors += other.errors
        return self


def _lean(settings: Settings) -> Settings:
    """Hydration settings: JSON sources only, no media, no render, no TSA."""
    return dataclasses.replace(
        settings,
        sources=list(settings.pull_sources),
        download_media=False,
        ocr=False,
        render=False,
        tsa=False,
    )


def client_for(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=settings.http_timeout,
        follow_redirects=True,
        proxy=settings.proxy,
        headers={"Accept-Language": "en-US,en;q=0.9"},
        limits=httpx.Limits(
            max_connections=max(4, settings.pull_concurrency * 2),
            max_keepalive_connections=max(4, settings.pull_concurrency),
        ),
    )


async def hydrate(
    client: httpx.AsyncClient,
    ids: Sequence[str],
    settings: Settings,
    *,
    handles: dict[str, str] | None = None,
) -> tuple[list[Post], list[str]]:
    """Fetch and merge the JSON sources for each id. Returns (posts, errors)."""
    lean = _lean(settings)
    semaphore = asyncio.Semaphore(max(1, settings.pull_concurrency))
    posts: list[Post] = []
    errors: list[str] = []

    async def one(tweet_id: str) -> None:
        ref = TweetRef(
            input=tweet_id,
            tweet_id=tweet_id,
            screen_name=(handles or {}).get(tweet_id),
        )
        async with semaphore:
            if settings.pull_delay:
                await asyncio.sleep(settings.pull_delay)
            try:
                raws = await fetch_sources(client, ref, lean)
            except Exception as exc:  # pragma: no cover - transport blew up
                errors.append(f"{tweet_id}: fetch_failed: {exc!r}")
                return
        try:
            parsed = parse_sources(raws)
            post = merge(ref, parsed)
        except Exception as exc:  # pragma: no cover - parser blew up
            errors.append(f"{tweet_id}: parse_failed: {exc!r}")
            return
        if not post.available_sources:
            reasons = {r.name: (r.error or f"HTTP {r.http_status}") for r in raws if not r.ok}
            errors.append(f"{tweet_id}: unavailable ({reasons or 'no source answered'})")
            return
        posts.append(post)

    await asyncio.gather(*(one(str(i)) for i in ids))
    return posts, errors


def _store(
    dataset: Dataset,
    posts: Iterable[Post],
    report: PullReport,
    new_out: list[Post] | None = None,
) -> None:
    """Upsert posts; `new_out` collects the ones seen for the first time.

    Monitoring needs the actual new posts, not just a count, so sinks can be
    handed the objects without re-querying.
    """
    for post in posts:
        try:
            if dataset.upsert_post(post):
                report.new_posts += 1
                if new_out is not None:
                    new_out.append(post)
            else:
                report.updated_posts += 1
            if post.profile and post.profile.screen_name:
                if dataset.upsert_user(post.profile):
                    report.users += 1
        except Exception as exc:  # pragma: no cover - sqlite failure
            report.errors.append(f"{post.id}: store_failed: {exc!r}")


async def pull_ids(
    ids: Sequence[str],
    settings: Settings,
    dataset: Dataset,
    *,
    client: httpx.AsyncClient | None = None,
    refresh: bool = True,
    handles: dict[str, str] | None = None,
    new_out: list[Post] | None = None,
) -> PullReport:
    """Hydrate specific ids. `refresh=False` skips ids already in the dataset."""
    started = time.perf_counter()
    report = PullReport(discovered=len(ids))
    wanted = [str(i) for i in ids]

    if not refresh and wanted:
        known = dataset.known(wanted)
        report.already_known = len(known)
        wanted = [i for i in wanted if i not in known]

    if wanted:
        owns = client is None
        client = client or client_for(settings)
        try:
            posts, errors = await hydrate(client, wanted, settings, handles=handles)
        finally:
            if owns:
                await client.aclose()
        report.hydrated = len(posts)
        report.errors += errors
        _store(dataset, posts, report, new_out)

    report.elapsed_ms = int((time.perf_counter() - started) * 1000)
    return report


async def _xapi_timeline(
    client: httpx.AsyncClient,
    session,
    handle: str,
    settings: Settings,
) -> tuple[list[Post], int, str | None]:
    """Walk a handle's guest-API timeline. Returns (posts, pages, error).

    `next_cursor` is routinely ``None`` on this endpoint, so a missing cursor
    is a clean stop, not a failure. `MAGPIE_XAPI_MAX_PAGES` bounds the rest.
    """
    posts: list[Post] = []
    seen: set[str] = set()
    cursor: str | None = None
    pages = 0

    for _ in range(max(1, settings.xapi_max_pages)):
        batch, cursor, error = await xapi.user_tweets(
            client,
            session,
            handle=handle,
            settings=settings,
            count=settings.xapi_page_size,
            cursor=cursor,
        )
        pages += 1
        if error:
            return posts, pages, error
        new = [p for p in batch if p.id and p.id not in seen]
        seen.update(p.id for p in new)
        posts += new
        if not cursor or not new:
            break
        if settings.pull_delay:
            await asyncio.sleep(settings.pull_delay)

    return posts, pages, None


async def pull_handles(
    handles: Sequence[str],
    settings: Settings,
    dataset: Dataset,
    *,
    client: httpx.AsyncClient | None = None,
    refresh: bool = False,
    deep: bool = False,
    with_profile: bool = True,
    new_out: list[Post] | None = None,
    on_handle=None,
) -> PullReport:
    """Collect each account's posts: guest GraphQL timeline first, SSR as fallback.

    Handles are polled concurrently: a profile fetch is ~1-2 s of pure latency
    and a watchlist is mostly waiting. Sequential polling of 12 accounts took
    23 s; the work inside a handle is already concurrent.
    """
    started = time.perf_counter()
    report = PullReport()
    owns = client is None
    client = client or client_for(settings)
    gate = asyncio.Semaphore(max(1, settings.pull_concurrency))
    # One guest token for the whole run: activation is shared and refreshed
    # under a lock, so N concurrent handles do not each mint their own.
    session = xapi.GuestSession(settings)

    names = [h.lstrip("@").strip() for h in handles]
    names = [h for h in names if h]
    report.handles = names

    async def one(handle: str) -> PullReport:
        sub = PullReport()
        found: list[str] = []
        discovery_error: str | None = None

        async with gate:
            # Freshness path. The SSR profile page is the ONLY surface that
            # returns what an account posted in the last minutes/hours.
            discovery = await profile_ids(client, handle, settings)
            found = list(discovery.ids)
            discovery_error = discovery.error
            if discovery.error:
                sub.errors.append(f"@{handle}: {discovery.error}")

            if settings.discover_timeline:
                extra = await syndication_timeline(client, handle, settings)
                found += [i for i in extra.ids if i not in found]

            hydrated = await pull_ids(
                found,
                settings,
                dataset,
                client=client,
                refresh=refresh,
                new_out=new_out,
                handles={i: handle for i in found},
            )
            sub.merge_in(hydrated)

            # Backfill path. The guest GraphQL timeline returns ~100 posts in one
            # call, but measured against @AFP and @Reuters it is the account's
            # high-engagement ARCHIVE, not its recent posts: newest entry was a
            # year old and the average post had ~8.3k likes while that day's real
            # posts had 3-30. It is breadth, never freshness, so it runs only on
            # request (`deep=True`) and never replaces the SSR poll.
            if deep and settings.xapi:
                posts, _pages, err = await _xapi_timeline(client, session, handle, settings)
                if err:
                    sub.errors.append(f"@{handle}: xapi: {err}")
                if posts:
                    ids = [p.id for p in posts if p.id]
                    keep = posts
                    if not refresh:
                        known = dataset.known(ids)
                        sub.already_known += len(known)
                        keep = [p for p in posts if p.id not in known]
                    sub.discovered += len(ids)
                    sub.hydrated += len(keep)
                    _store(dataset, keep, sub, new_out)

            if with_profile:
                profile, err = await user_profile(client, handle, settings)
                if profile and dataset.upsert_user(profile):
                    sub.users += 1
                elif err:
                    sub.errors.append(f"@{handle}: profile: {err}")

        sub.handles = []

        cursor = dataset.get_cursor(handle)
        dataset.set_cursor(
            handle,
            last_seen_id=found[0] if found else cursor.get("last_seen_id"),
            last_poll_utc=_now(),
            new_count=sub.new_posts,
            polls=(cursor.get("polls") or 0) + 1,
            last_error=discovery_error or (sub.errors[0] if sub.errors else None),
        )
        if on_handle is not None:
            try:
                on_handle(handle, sub)
            except Exception as exc:  # a bad callback must not kill the poll
                sub.errors.append(f"@{handle}: on_handle failed: {exc!r}")
        return sub

    try:
        subs = await asyncio.gather(*(one(h) for h in names), return_exceptions=True)
    finally:
        if owns:
            await client.aclose()

    for handle, sub in zip(names, subs):
        if isinstance(sub, BaseException):
            report.errors.append(f"@{handle}: poll_failed: {sub!r}")
        else:
            report.merge_in(sub)

    report.handles = names
    report.elapsed_ms = int((time.perf_counter() - started) * 1000)
    return report


async def expand_thread(
    root_id: str,
    settings: Settings,
    dataset: Dataset,
    *,
    client: httpx.AsyncClient | None = None,
    depth: int | None = None,
    max_posts: int | None = None,
) -> PullReport:
    """Breadth-first crawl outward from one post.

    Two edge kinds are followed: ids scraped off the status page (replies and
    related posts, ~2 per page) and the `reply_to_id`/`quoted_id` links the
    hydrated post itself carries, which walk reliably *up* a thread.
    """
    started = time.perf_counter()
    depth = settings.thread_depth if depth is None else depth
    budget = settings.thread_max_posts if max_posts is None else max_posts
    report = PullReport()

    owns = client is None
    client = client or client_for(settings)
    seen: set[str] = set()
    session = xapi.GuestSession(settings)
    frontier: list[tuple[str, str | None]] = [(str(root_id), None)]

    try:
        for level in range(depth + 1):
            frontier = [(i, h) for i, h in frontier if i not in seen][:budget]
            if not frontier:
                break
            ids = [i for i, _ in frontier]
            seen.update(ids)
            report.discovered += len(ids)

            posts, errors = await hydrate(
                client, ids, settings, handles={i: h for i, h in frontier if h}
            )
            report.hydrated += len(posts)
            report.errors += errors
            _store(dataset, posts, report)

            if level == depth:
                break

            nxt: dict[str, str | None] = {}
            for post in posts:
                for linked, who in (
                    (post.reply_to_id, post.reply_to_screen_name),
                    (post.quoted_id, post.quoted_screen_name),
                ):
                    if linked and linked not in seen:
                        nxt.setdefault(str(linked), who)

                # Guest TweetDetail returns the whole threaded conversation in
                # one call; the SSR status page yields ~2 ids. Fall back to SSR
                # when the API is disabled or its query ids have rotated.
                grew = False
                if settings.xapi:
                    convo, err = await xapi.tweet_detail(client, session, post.id, settings)
                    if err:
                        report.errors.append(f"{post.id}: xapi: {err}")
                    elif convo:
                        grew = True
                        report.discovered += len(convo)
                        fresh = [p for p in convo if p.id and p.id not in seen]
                        seen.update(p.id for p in fresh)
                        report.hydrated += len(fresh)
                        _store(dataset, fresh, report)
                if not grew:
                    page = await conversation_ids(client, post.id, post.screen_name, settings)
                    if page.error:
                        report.errors.append(f"{post.id}: {page.error}")
                    for linked in page.ids:
                        if linked not in seen:
                            nxt.setdefault(linked, None)
                budget -= 1
                if budget <= 0:
                    break
            frontier = list(nxt.items())
    finally:
        if owns:
            await client.aclose()

    report.elapsed_ms = int((time.perf_counter() - started) * 1000)
    return report


async def pull(
    inputs: str | Sequence[str],
    settings: Settings | None = None,
    dataset: Dataset | None = None,
    *,
    refresh: bool = False,
    deep: bool = False,
) -> PullReport:
    """Mixed entry point: `@handle` polls a profile, a URL or id hydrates directly."""
    settings = settings or load_settings()
    settings.ensure_dirs()
    dataset = dataset or Dataset(settings)

    items = [inputs] if isinstance(inputs, str) else list(inputs)
    tokens = [t for chunk in items for t in str(chunk).split() if t.strip()]

    handles = [t for t in tokens if t.startswith("@") or _looks_like_handle(t)]
    rest = [t for t in tokens if t not in handles]

    report = PullReport()
    started = time.perf_counter()
    async with client_for(settings) as client:
        if handles:
            report.merge_in(
                await pull_handles(
                    handles, settings, dataset, client=client, refresh=refresh, deep=deep
                )
            )
        if rest:
            refs = parse_inputs(rest, max_items=max(len(rest), 1))
            ids = [r.tweet_id for r in refs if r.ok and r.tweet_id]
            report.errors += [f"{r.input}: {r.error}" for r in refs if not r.ok]
            known_handles = {
                r.tweet_id: r.screen_name for r in refs if r.ok and r.tweet_id and r.screen_name
            }
            if ids:
                report.merge_in(
                    await pull_ids(
                        ids,
                        settings,
                        dataset,
                        client=client,
                        refresh=True,
                        handles=known_handles,
                    )
                )
    report.elapsed_ms = int((time.perf_counter() - started) * 1000)
    return report


def _looks_like_handle(token: str) -> bool:
    bare = token.lstrip("@")
    return (
        bare.isascii()
        and bare.replace("_", "").isalnum()
        and not bare.isdigit()
        and 1 <= len(bare) <= 15
        and "/" not in token
    )


async def watch(
    handles: Sequence[str],
    settings: Settings,
    dataset: Dataset,
    *,
    interval: float | None = None,
    rounds: int | None = None,
    on_round=None,
) -> PullReport:
    """Poll a watchlist forever (or `rounds` times), reporting each cycle.

    A round where every discovered id was new means the profile page rolled
    over entirely between polls: posts were missed and the interval is too
    long for that account. That is surfaced, not swallowed.
    """
    interval = settings.watch_interval if interval is None else interval
    total = PullReport()
    round_no = 0

    async with client_for(settings) as client:
        while rounds is None or round_no < rounds:
            round_no += 1
            report = await pull_handles(handles, settings, dataset, client=client, refresh=False)
            if report.discovered and report.new_posts == report.discovered:
                report.errors.append(
                    "every discovered id was new: the timeline rolled over between polls, "
                    "posts were probably missed - shorten the interval"
                )
            total.merge_in(report)
            if on_round:
                on_round(round_no, report)
            if rounds is not None and round_no >= rounds:
                break
            await asyncio.sleep(interval)

    return total


async def monitor(
    watchlist,
    settings: Settings,
    dataset: Dataset,
    *,
    notifier=None,
    rounds: int | None = None,
    tick: float = 5.0,
    on_round=None,
) -> PullReport:
    """Poll a persisted watchlist on per-account cadence until stopped.

    Unlike `watch()`, which polls a fixed list on one fixed interval, this
    asks the watchlist which accounts are *due*. That matters because posting
    rates differ by an order of magnitude: measured over two days, @Reuters
    averaged a post every 364 s (so its 5-id page rolls over in ~30 min) while
    @CNN averaged one every 3092 s. One global interval either wastes requests
    on quiet accounts or loses posts on busy ones.

    Each poll reports back to the watchlist, which halves an account's
    interval whenever a round comes back entirely new (proof the page rolled
    over and posts were missed) and relaxes it again after clean polls.
    """
    total = PullReport()
    round_no = 0

    async with client_for(settings) as client:
        while rounds is None or round_no < rounds:
            now = time.time()
            due = watchlist.due(now, default_interval=settings.watch_interval)
            if not due:
                if rounds is not None:
                    break
                await asyncio.sleep(tick)
                continue

            round_no += 1
            new_posts: list[Post] = []
            outcomes: dict[str, PullReport] = {}

            report = await pull_handles(
                [w.handle for w in due],
                settings,
                dataset,
                client=client,
                refresh=False,
                new_out=new_posts,
                on_handle=lambda h, sub: outcomes.__setitem__(h, sub),
            )

            polled = time.time()
            for entry in due:
                sub = outcomes.get(entry.handle)
                # An account's first poll is all-new by definition; only treat
                # a full page of new ids as a rollover once there is a baseline
                # to compare against, or every new watchlist entry would
                # immediately halve its own interval.
                rollover = bool(
                    sub and entry.polls and sub.discovered and sub.new_posts == sub.discovered
                )
                watchlist.record_poll(
                    entry.handle,
                    new_posts=sub.new_posts if sub else 0,
                    rollover=rollover,
                    error=(sub.errors[0] if sub and sub.errors else None),
                    now=polled,
                    default_interval=settings.watch_interval,
                )
                if rollover:
                    report.errors.append(
                        f"@{entry.handle}: every discovered id was new - the page rolled over "
                        f"between polls, posts were missed; interval halved"
                    )

            if notifier is not None and new_posts:
                try:
                    deliveries = await notifier.dispatch(
                        new_posts,
                        {
                            "round": round_no,
                            "handles": [w.handle for w in due],
                            "polled_utc": _now(),
                        },
                    )
                    report.errors += [
                        f"sink {d.sink}: {d.error}" for d in deliveries if not d.ok and d.error
                    ]
                except Exception as exc:  # pragma: no cover - Notifier already guards
                    report.errors.append(f"notify_failed: {exc!r}")

            total.merge_in(report)
            if on_round:
                on_round(round_no, report, new_posts)
            if rounds is not None and round_no >= rounds:
                break
            await asyncio.sleep(min(tick, settings.watch_interval))

    return total
