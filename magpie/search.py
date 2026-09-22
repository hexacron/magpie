"""Keyword search over X's ``SearchTimeline`` GraphQL operation.

Search is the one high-value operation a guest token cannot reach: with the
current query id and nothing but a guest token, ``SearchTimeline`` answers
``404`` with an empty body while ``UserTweets`` answers ``200``. That 404 is
X saying *this credential may not call this operation* -- it is **not** the
signature of a stale id, and rotating the id will not fix it. Search needs a
real account cookie, so this module is written against whatever session object
it is handed and knows nothing about where the credential came from.

Design notes:

* **Session-agnostic.** Anything exposing
  ``async graphql(client, operation, variables, *, features=None)`` works --
  :class:`magpie.xapi.GuestSession`, an authenticated session, or a test
  double. This module never imports an account store and holds no credential.
* **Query id resolution.** :data:`magpie.xapi.QUERY_IDS` is the registry
  the session walks, and it ships without a ``SearchTimeline`` entry, so this
  module registers one. Every call re-asks
  :func:`magpie.queryids.query_id`, whose freshly scraped cache wins over
  ``MAGPIE_QUERY_ID_SEARCHTIMELINE``, which wins over the verified-current
  constant seeded at import. The constant is the floor, never the only
  source, and it costs no network to have it.
* **The repeated-cursor stop.** At the end of a result set X does not drop the
  cursor -- it hands back *the same cursor forever*. A pager that only stops
  on a missing cursor therefore spins until it is rate limited. Every cursor
  handed out is remembered and a repeat ends the walk.

Nothing here raises. Transport failures, malformed payloads and rejected
operations all come back as an ``error`` string on a :class:`SearchPage`.
"""

from __future__ import annotations

import asyncio
import inspect
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import httpx

from . import xapi
from .config import Settings
from .models import Model, Post
from .normalize import _dict, _text

__all__ = [
    "SEARCH_OPERATION",
    "SEARCH_PRODUCTS",
    "SearchPage",
    "build_query",
    "search",
    "search_page",
]

SEARCH_OPERATION = "SearchTimeline"

# Verified current against x.com's public logged-out bundle. Kept only as the
# floor under `queryids` and the env override -- see `resolve_query_id`.
FALLBACK_QUERY_ID = "auLkqtmHqYEpRvflfvLhyQ"

# `Latest` is reverse-chronological and complete; `Top` is ranked and lossy.
SEARCH_PRODUCTS = ("Latest", "Top", "Media", "People")
_PRODUCTS_BY_KEY = {product.lower(): product for product in SEARCH_PRODUCTS}

DEFAULT_PAGE_SIZE = 20

# The free-text operators `build_query` composes. Anything else the caller
# wants goes straight through in `text`, unquoted and untouched.
_DATE_FORMAT = "%Y-%m-%d"


# --------------------------------------------------------------------------
# query id
# --------------------------------------------------------------------------


def _env_query_id() -> str | None:
    raw = os.environ.get(f"MAGPIE_QUERY_ID_{SEARCH_OPERATION.upper()}") or ""
    for part in raw.split(","):
        candidate = part.strip()
        if candidate:
            return candidate
    return None


def register_query_id(operation: str, query_id: str | None) -> None:
    """Put ``query_id`` at the front of the registry the session walks.

    ``session.graphql`` takes no id argument by design -- it owns rotation --
    so :data:`xapi.QUERY_IDS` is the single channel for teaching it a new
    operation, and ``xapi`` ships no ``SearchTimeline`` entry. Previously
    known ids stay behind the new one as fallbacks.
    """
    if not query_id:
        return
    known = tuple(xapi.QUERY_IDS.get(operation, ()))
    if known[:1] == (query_id,):
        return
    xapi.QUERY_IDS[operation] = (query_id, *[qid for qid in known if qid != query_id])


# Seeded at import (pure dict work, no network) so the operation is callable
# before anything scrapes, and so `queryids.query_id`'s last precedence step
# -- `xapi.QUERY_IDS[operation][0]` -- always has something to return.
register_query_id(SEARCH_OPERATION, _env_query_id() or FALLBACK_QUERY_ID)


def _ask_queryids(operation: str, settings: Settings) -> str | None:
    """``queryids.query_id(operation, settings)`` if that module is installed.

    Imported lazily and duck-typed: the id scraper is an optional collaborator
    whose job is to notice a rotation, and search must keep working when it is
    absent, older, or broken.
    """
    try:
        from . import queryids  # noqa: PLC0415 - optional collaborator
    except Exception:
        return None
    getter = getattr(queryids, "query_id", None)
    if not callable(getter):
        return None
    try:
        value = getter(operation, settings)
    except TypeError:
        try:
            value = getter(operation)
        except Exception:
            return None
    except Exception:
        return None
    return _text(value)


def resolve_query_id(operation: str, settings: Settings) -> str | None:
    """The best known id: the scraper's cache, then env, then what is known.

    ``queryids`` already checks the env var and falls back to
    :data:`xapi.QUERY_IDS`, so this is a plain chain, not a duplicate policy:
    the extra steps only matter when that module is missing.
    """
    resolved = _ask_queryids(operation, settings) or _env_query_id()
    if resolved is not None:
        return resolved
    known = xapi.QUERY_IDS.get(operation, ())
    return known[0] if known else None


# --------------------------------------------------------------------------
# query building
# --------------------------------------------------------------------------


def _handle(value: str | None) -> str | None:
    """A bare screen name: no leading ``@``, no slashes, no spaces."""
    cleaned = _text(value)
    if cleaned is None:
        return None
    cleaned = cleaned.lstrip("@").strip("/").strip()
    return cleaned or None


def _day(value: str | date | datetime | None) -> str | None:
    """``YYYY-MM-DD`` for X's ``since:``/``until:`` operators."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().strftime(_DATE_FORMAT)
    if isinstance(value, date):
        return value.strftime(_DATE_FORMAT)
    return _text(value)


def build_query(
    text: str,
    *,
    since: str | date | datetime | None = None,
    until: str | date | datetime | None = None,
    from_user: str | None = None,
    to_user: str | None = None,
    lang: str | None = None,
    has_media: bool = False,
    min_likes: int | None = None,
    replies: bool = True,
) -> str:
    """Compose X's search operator syntax around free text.

    ``text`` is passed through verbatim and unquoted so a caller who already
    speaks the operator language ("a OR b -c") keeps that power. Every other
    argument is a convenience over the operators X documents.
    """
    parts: list[str] = []

    free = (text or "").strip()
    if free:
        parts.append(free)

    author = _handle(from_user)
    if author:
        parts.append(f"from:{author}")

    recipient = _handle(to_user)
    if recipient:
        parts.append(f"to:{recipient}")

    start = _day(since)
    if start:
        parts.append(f"since:{start}")

    end = _day(until)
    if end:
        parts.append(f"until:{end}")

    language = _text(lang)
    if language:
        parts.append(f"lang:{language.lower()}")

    if has_media:
        parts.append("filter:media")

    if min_likes is not None:
        try:
            floor = int(min_likes)
        except (TypeError, ValueError):
            floor = -1
        if floor >= 0:
            parts.append(f"min_faves:{floor}")

    if not replies:
        parts.append("-filter:replies")

    return " ".join(parts)


# --------------------------------------------------------------------------
# one page
# --------------------------------------------------------------------------


@dataclass
class SearchPage(Model):
    """One ``SearchTimeline`` response: posts, the next cursor, or an error."""

    posts: list[Post] = field(default_factory=list)
    cursor: str | None = None
    error: str | None = None
    query: str = ""
    product: str = SEARCH_PRODUCTS[0]


def _page_size(count: int, settings: Settings) -> int:
    try:
        size = int(count)
    except (TypeError, ValueError):
        size = 0
    if size < 1:
        try:
            size = int(getattr(settings, "xapi_page_size", DEFAULT_PAGE_SIZE))
        except (TypeError, ValueError):
            size = DEFAULT_PAGE_SIZE
    return max(1, min(size, 100))


async def search_page(
    client: httpx.AsyncClient,
    session: Any,
    query: str,
    settings: Settings,
    *,
    product: str = "Latest",
    count: int = DEFAULT_PAGE_SIZE,
    cursor: str | None = None,
) -> SearchPage:
    """One page of search results. Never raises; failures land in ``error``.

    An empty page with no cursor is not an error -- that is what a query with
    no matches looks like.
    """
    raw_query = (query or "").strip()
    wanted = _text(product) or SEARCH_PRODUCTS[0]
    resolved_product = _PRODUCTS_BY_KEY.get(wanted.lower())

    if not raw_query:
        return SearchPage(error="empty search query", query="", product=wanted)
    if resolved_product is None:
        return SearchPage(
            error=f"unsupported search product {wanted!r}; expected one of "
            + ", ".join(SEARCH_PRODUCTS),
            query=raw_query,
            product=wanted,
        )

    page = SearchPage(query=raw_query, product=resolved_product)

    variables: dict[str, Any] = {
        "rawQuery": raw_query,
        "count": _page_size(count, settings),
        "querySource": "typed_query",
        "product": resolved_product,
    }
    if _text(cursor) is not None:
        variables["cursor"] = cursor

    try:
        register_query_id(SEARCH_OPERATION, resolve_query_id(SEARCH_OPERATION, settings))
        _status, payload, error = await session.graphql(client, SEARCH_OPERATION, variables)
    except Exception as exc:  # transport or session blew up in a new way
        page.error = f"{type(exc).__name__}: {exc}"
        return page

    if error is not None:
        page.error = str(error)
        return page

    try:
        posts, next_cursor = xapi.parse_timeline(_dict(payload))
    except Exception as exc:  # pragma: no cover - parse_timeline is defensive
        page.error = f"unreadable search payload: {type(exc).__name__}: {exc}"
        return page

    page.posts = posts
    page.cursor = next_cursor
    return page


# --------------------------------------------------------------------------
# pagination
# --------------------------------------------------------------------------


async def _notify(
    on_page: Callable[[int, SearchPage], Any] | None, page_no: int, page: SearchPage
) -> None:
    """Fire the caller's per-page hook. A bad hook must not kill the crawl."""
    if on_page is None:
        return
    try:
        result = on_page(page_no, page)
        if inspect.isawaitable(result):
            await result
    except Exception:
        return


async def search(
    client: httpx.AsyncClient,
    session: Any,
    query: str,
    settings: Settings,
    *,
    product: str = "Latest",
    limit: int = 100,
    max_pages: int = 25,
    on_page: Callable[[int, SearchPage], Any] | None = None,
) -> tuple[list[Post], str | None]:
    """Walk search results to ``limit`` posts. Returns ``(posts, error)``.

    The walk stops on the first of: ``limit`` posts, ``max_pages`` requests,
    a page carrying an error, a page with no new posts, a missing cursor, or a
    cursor already handed out. Posts already collected survive a later error,
    so a partial crawl still yields its results alongside the reason it ended.
    """
    try:
        wanted = int(limit)
    except (TypeError, ValueError):
        wanted = 0
    try:
        pages_allowed = int(max_pages)
    except (TypeError, ValueError):
        pages_allowed = 1

    posts: list[Post] = []
    seen_ids: set[str] = set()
    if wanted < 1 or pages_allowed < 1:
        return posts, None

    seen_cursors: set[str] = set()
    cursor: str | None = None

    for page_no in range(1, pages_allowed + 1):
        page = await search_page(
            client,
            session,
            query,
            settings,
            product=product,
            count=min(DEFAULT_PAGE_SIZE, max(1, wanted - len(posts))),
            cursor=cursor,
        )
        await _notify(on_page, page_no, page)

        if page.error is not None:
            return posts, page.error

        fresh = [post for post in page.posts if post.id and post.id not in seen_ids]
        seen_ids.update(post.id for post in fresh)
        posts.extend(fresh)

        if len(posts) >= wanted:
            return posts[:wanted], None
        if not fresh:
            break

        next_cursor = _text(page.cursor)
        # X repeats the tail cursor forever instead of dropping it; treating a
        # repeat as "more results" is the infinite loop this guards.
        if next_cursor is None or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

        if settings.pull_delay:
            await asyncio.sleep(settings.pull_delay)

    return posts[:wanted], None
