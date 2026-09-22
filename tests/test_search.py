"""Offline tests for keyword search.

Only the failures that would silently break a search crawl are covered: the
operator syntax `build_query` emits (a wrong operator returns the wrong
corpus, quietly), the cross-page dedupe, the repeated-cursor stop that is the
difference between finishing and spinning until rate-limited, the mid-page
`limit` cut, and the partial-result-plus-error contract that keeps page one
when page two is rejected.

The session is a double: search takes whatever object exposes `graphql`, so
these tests exercise the pagination logic without a credential, a token or a
live query id.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import httpx

from magpie import search as search_mod
from magpie import xapi
from magpie.config import Settings
from magpie.search import (
    SEARCH_OPERATION,
    SearchPage,
    build_query,
    search,
    search_page,
)

SETTINGS = Settings(http_retries=0, http_timeout=1.0, pull_delay=0.0)


def _run(coro):
    return asyncio.run(coro)


def _client() -> httpx.AsyncClient:
    # Search never touches the transport itself -- the session owns the wire --
    # but the client is a real one so the call signature stays honest.
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError(f"unexpected request: {request.url}")

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --------------------------------------------------------------------------
# payload fixtures: the shape SearchTimeline actually serves
# --------------------------------------------------------------------------


def _tweet(tweet_id: str) -> dict[str, Any]:
    return {
        "__typename": "Tweet",
        "rest_id": tweet_id,
        "core": {
            "user_results": {
                "result": {
                    "__typename": "User",
                    "rest_id": "99",
                    "legacy": {"screen_name": "jack", "name": "jack"},
                }
            }
        },
        "legacy": {
            "id_str": tweet_id,
            "full_text": f"post {tweet_id}",
            "created_at": "Wed Sep 11 19:31:57 +0000 2024",
            "lang": "en",
            "favorite_count": 3,
        },
    }


def _payload(ids: list[str], cursor: str | None = None) -> dict[str, Any]:
    entries: list[dict[str, Any]] = [
        {
            "entryId": f"tweet-{tweet_id}",
            "content": {
                "entryType": "TimelineTimelineItem",
                "itemContent": {
                    "itemType": "TimelineTweet",
                    "tweet_results": {"result": _tweet(tweet_id)},
                },
            },
        }
        for tweet_id in ids
    ]
    if cursor is not None:
        entries.append(
            {
                "entryId": "cursor-bottom-0",
                "content": {
                    "entryType": "TimelineTimelineCursor",
                    "cursorType": "Bottom",
                    "value": cursor,
                },
            }
        )
    return {
        "data": {
            "search_by_raw_query": {
                "search_timeline": {
                    "timeline": {
                        "instructions": [{"type": "TimelineAddEntries", "entries": entries}]
                    }
                }
            }
        }
    }


class FakeSession:
    """Anything with this `graphql` is a session as far as search cares."""

    def __init__(self, replies: list[tuple[int | None, dict | None, str | None]]) -> None:
        self.replies = replies
        self.calls: list[dict[str, Any]] = []

    async def graphql(
        self,
        client: httpx.AsyncClient,
        operation: str,
        variables: dict[str, Any],
        *,
        features: dict[str, Any] | None = None,
    ) -> tuple[int | None, dict | None, str | None]:
        self.calls.append({"operation": operation, "variables": dict(variables)})
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        return self.replies[index]


def _ok(ids: list[str], cursor: str | None = None):
    return (200, _payload(ids, cursor), None)


# --------------------------------------------------------------------------
# build_query
# --------------------------------------------------------------------------


def test_build_query_composes_every_operator() -> None:
    query = build_query(
        "signal OR telegram -spam",
        since=date(2024, 1, 1),
        until="2024-02-01",
        from_user="@jack",
        to_user="elonmusk",
        lang="EN",
        has_media=True,
        min_likes=50,
        replies=False,
    )
    assert query == (
        "signal OR telegram -spam from:jack to:elonmusk "
        "since:2024-01-01 until:2024-02-01 lang:en "
        "filter:media min_faves:50 -filter:replies"
    )


def test_build_query_leaves_free_text_alone_and_omits_unset_operators() -> None:
    # No quoting, no escaping: the caller's own operators must survive.
    assert build_query('"exact phrase" (a OR b)') == '"exact phrase" (a OR b)'
    assert build_query("x", replies=True) == "x"
    assert build_query("x", has_media=False, min_likes=None) == "x"
    # A bare operator query with no free text is legal.
    assert build_query("", from_user="jack") == "from:jack"


# --------------------------------------------------------------------------
# the wire contract
# --------------------------------------------------------------------------


def test_search_page_sends_the_searchtimeline_contract() -> None:
    session = FakeSession([_ok(["1"], "CUR")])

    async def go() -> SearchPage:
        async with _client() as client:
            return await search_page(
                client, session, "cats", SETTINGS, product="Latest", count=20
            )

    page = _run(go())

    assert page.error is None
    assert [post.id for post in page.posts] == ["1"]
    assert page.cursor == "CUR"
    assert session.calls[0]["operation"] == SEARCH_OPERATION
    assert session.calls[0]["variables"] == {
        "rawQuery": "cats",
        "count": 20,
        "querySource": "typed_query",
        "product": "Latest",
    }
    # The session resolves ids out of this table; search must have taught it
    # the operation, which xapi ships without.
    assert xapi.QUERY_IDS[SEARCH_OPERATION][0] == search_mod.resolve_query_id(
        SEARCH_OPERATION, SETTINGS
    )


def test_search_page_rejects_an_unknown_product_without_calling_x() -> None:
    session = FakeSession([_ok(["1"])])

    async def go() -> SearchPage:
        async with _client() as client:
            return await search_page(client, session, "cats", SETTINGS, product="Newest")

    page = _run(go())

    assert session.calls == []
    assert page.error is not None
    assert "Newest" in page.error
    assert page.posts == []


# --------------------------------------------------------------------------
# pagination
# --------------------------------------------------------------------------


def test_search_paginates_and_dedupes_across_pages() -> None:
    session = FakeSession(
        [
            _ok(["1", "2"], "c1"),
            _ok(["2", "3"], "c2"),  # id 2 repeats across the page boundary
            _ok(["4"], None),
        ]
    )
    seen_pages: list[tuple[int, int]] = []

    async def go():
        async with _client() as client:
            return await search(
                client,
                session,
                "cats",
                SETTINGS,
                limit=100,
                on_page=lambda n, page: seen_pages.append((n, len(page.posts))),
            )

    posts, error = _run(go())

    assert error is None
    assert [post.id for post in posts] == ["1", "2", "3", "4"]
    assert len(session.calls) == 3
    assert "cursor" not in session.calls[0]["variables"]
    assert session.calls[1]["variables"]["cursor"] == "c1"
    assert session.calls[2]["variables"]["cursor"] == "c2"
    assert seen_pages == [(1, 2), (2, 2), (3, 1)]


def test_repeated_cursor_stops_the_walk() -> None:
    # X hands back the same cursor forever at the end of a result set; without
    # the guard this walks until max_pages (or a rate limit).
    session = FakeSession([_ok(["1", "2"], "SAME"), _ok(["3", "4"], "SAME")])

    async def go():
        async with _client() as client:
            return await search(client, session, "cats", SETTINGS, limit=100, max_pages=25)

    posts, error = _run(go())

    assert error is None
    assert len(session.calls) == 2
    assert [post.id for post in posts] == ["1", "2", "3", "4"]


def test_limit_is_respected_mid_page() -> None:
    session = FakeSession(
        [
            _ok(["1", "2", "3", "4"], "c1"),
            _ok(["5", "6", "7", "8"], "c2"),
            _ok(["9"], "c3"),
        ]
    )

    async def go():
        async with _client() as client:
            return await search(client, session, "cats", SETTINGS, limit=5)

    posts, error = _run(go())

    assert error is None
    assert len(session.calls) == 2
    assert [post.id for post in posts] == ["1", "2", "3", "4", "5"]


def test_error_on_page_two_keeps_page_one() -> None:
    session = FakeSession([_ok(["1", "2"], "c1"), (429, None, "HTTP 429")])

    async def go():
        async with _client() as client:
            return await search(client, session, "cats", SETTINGS, limit=100)

    posts, error = _run(go())

    assert error == "HTTP 429"
    assert [post.id for post in posts] == ["1", "2"]
    assert len(session.calls) == 2


def test_missing_cursor_stops_cleanly() -> None:
    session = FakeSession([_ok(["1", "2"], None)])

    async def go():
        async with _client() as client:
            page = await search_page(client, session, "cats", SETTINGS)
            posts, error = await search(client, session, "cats", SETTINGS, limit=100)
            return page, posts, error

    page, posts, error = _run(go())

    assert page.cursor is None
    assert page.error is None
    assert error is None
    assert [post.id for post in posts] == ["1", "2"]
    assert len(session.calls) == 2  # one for search_page, one for the walk


def test_transport_failure_becomes_an_error_not_an_exception() -> None:
    class Boom:
        async def graphql(self, *args: Any, **kwargs: Any):
            raise httpx.ConnectError("no route to host")

    async def go():
        async with _client() as client:
            return await search(client, Boom(), "cats", SETTINGS, limit=10)

    posts, error = _run(go())

    assert posts == []
    assert error is not None and "ConnectError" in error
