"""Self-healing GraphQL query-id discovery, scraped from X's public bundles.

Every GraphQL call X's web client makes is addressed by an opaque ``queryId``
that rotates without notice. Hardcoding them guarantees a slow decay; the ids
are, however, **recoverable from the logged-out site**. ``GET
https://x.com/i/flow/login`` references a handful of
``https://abs.twimg.com/responsive-web/client-web/*.js`` bundles, and
``main.<hash>.js`` carries the whole table inline as
``queryId:"..",operationName:".."`` pairs -- 108 operations in one file, as
measured. No account, no cookie, no token: this module never touches a
credential and must never be given one.

Verified live from that scrape::

    SearchTimeline    auLkqtmHqYEpRvflfvLhyQ
    UserTweets        jeAA-59Y9FL7FmjgBNIVPw
    TweetDetail       zoF7_t363wZyzylk-BLfZQ
    UserByScreenName  KybxDj9RrADIITXlGG8kpw
    Followers         fVGYs5W9kNUuoUrZwYZQpQ
    Following         -Mn4uN7C-vxXBwUKtSwS6A
    HomeTimeline      og4a4SdSF3WiQkkwaPCdPg
    Likes             XHn_Tw60c6pi0n3DGhpwiA

**The one semantic that matters for callers.** A ``404`` with an *empty body*
in response to a *freshly scraped* id means the credential is not authorised
for that operation -- it does **not** mean the id is stale. Measured: holding
only a guest token, ``SearchTimeline``, ``TweetDetail`` and ``Followers`` all
answered 404 with the current ids while ``UserTweets`` answered 200 with 479
KB. Rotating an id that came out of a fresh bundle is therefore wasted work;
the fix for that 404 is a better credential, not another id.

Ids are cached at ``<data_dir>/query_ids.json`` and re-discovered once the
cache ages past ``settings.query_id_ttl`` (default 6 h). When discovery fails
the *stale* cache is returned rather than nothing: a stale id beats no id.

Nothing here raises. Transport failures, dead pages and unparseable bundles
all degrade into an error string or an empty mapping.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from .config import Settings

__all__ = [
    "BUNDLE_PAGES",
    "CACHE_NAME",
    "DEFAULT_TTL",
    "cache_path",
    "discover",
    "ensure",
    "extract_ids",
    "load_cache",
    "query_id",
    "save_cache",
]

# The login flow is the richest entry point (it ships the full client), with
# the bare root as the fallback for the day that URL changes shape.
BUNDLE_PAGES: tuple[str, ...] = ("https://x.com/i/flow/login", "https://x.com/")

BUNDLE_PREFIX = "https://abs.twimg.com/responsive-web/client-web/"
CACHE_NAME = "query_ids.json"
DEFAULT_TTL = 21_600.0  # 6 h
BUNDLE_CONCURRENCY = 8

_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)

# Greedy up to the last ``.js`` that is not part of a longer extension, so a
# hashed name such as ``main.9f3c1a2b.js`` survives intact.
_BUNDLE_RE = re.compile(
    re.escape(BUNDLE_PREFIX) + r"[A-Za-z0-9_.~/-]+\.js(?![A-Za-z0-9_])",
)

_QID = r"[A-Za-z0-9_-]{8,64}"
_OP = r"[A-Za-z][A-Za-z0-9_]{1,63}"

# X's minifier emits both key orders; missing one halves the yield.
_PAIR_RES: tuple[tuple[re.Pattern[str], int, int], ...] = (
    (
        re.compile(
            rf"""queryId\s*:\s*["']({_QID})["']\s*,\s*operationName\s*:\s*["']({_OP})["']"""
        ),
        2,  # operation group
        1,  # query id group
    ),
    (
        re.compile(
            rf"""operationName\s*:\s*["']({_OP})["']\s*,\s*queryId\s*:\s*["']({_QID})["']"""
        ),
        1,
        2,
    ),
)


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


def _headers(settings: Settings, *, accept: str) -> dict[str, str]:
    return {
        "User-Agent": settings.ua("x_page"),
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
    }


async def _fetch(
    client: httpx.AsyncClient,
    url: str,
    settings: Settings,
    *,
    accept: str,
) -> tuple[str, str | None]:
    """``(text, error)``. A non-200 is an error here: there is nothing to parse."""
    attempts = max(1, int(getattr(settings, "http_retries", 2)) + 1)
    timeout = float(getattr(settings, "http_timeout", 20.0))
    last_error: str | None = None
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            response = await client.get(
                url, headers=_headers(settings, accept=accept), timeout=timeout
            )
        except _RETRYABLE as exc:
            last_error = f"{type(exc).__name__}: {exc}".rstrip(": ")
            continue
        except Exception as exc:  # transport contract violation
            return "", f"{type(exc).__name__}: {exc}".rstrip(": ")
        if response.status_code >= 500 and attempt < attempts - 1:
            last_error = f"HTTP {response.status_code}"
            continue
        if response.status_code != 200:
            return "", f"HTTP {response.status_code}"
        try:
            return response.text, None
        except Exception:  # pragma: no cover - undecodable body
            return "", "undecodable body"
    return "", last_error or "request failed"


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------


def bundle_urls(text: str) -> list[str]:
    """Distinct client-web bundle URLs referenced by a page, in page order.

    ``main.*.js`` is hoisted first: it is the one that carries the table, so
    a truncated or partly failing run still gets the useful file.
    """
    found: list[str] = []
    seen: set[str] = set()
    # Inline JSON escapes the slashes; the same URL then looks different.
    for match in _BUNDLE_RE.finditer(text.replace("\\/", "/")):
        url = match.group(0)
        if url not in seen:
            seen.add(url)
            found.append(url)
    found.sort(key=lambda u: 0 if u[len(BUNDLE_PREFIX) :].startswith("main.") else 1)
    return found


def extract_ids(text: str) -> dict[str, str]:
    """``{operation: query_id}`` for every pair in a bundle, either ordering."""
    ids: dict[str, str] = {}
    for pattern, op_group, qid_group in _PAIR_RES:
        for match in pattern.finditer(text):
            ids.setdefault(match.group(op_group), match.group(qid_group))
    return ids


# --------------------------------------------------------------------------
# discovery
# --------------------------------------------------------------------------


async def discover(
    client: httpx.AsyncClient, settings: Settings
) -> tuple[dict[str, str], str | None]:
    """Scrape ``{operation: query_id}`` off the public logged-out bundles.

    ``error`` is non-``None`` only when nothing at all was recovered; a run
    that loses some bundles but parses others is a success, because a single
    bundle (``main.*.js``) already carries the whole table.
    """
    problems: list[str] = []
    bundles: list[str] = []
    seen: set[str] = set()
    for page in BUNDLE_PAGES:
        text, error = await _fetch(client, page, settings, accept="text/html,*/*")
        if error:
            problems.append(f"{page}: {error}")
            continue
        for url in bundle_urls(text):
            if url not in seen:
                seen.add(url)
                bundles.append(url)
    if not bundles:
        return {}, "; ".join(problems) or "no client-web bundles referenced"

    semaphore = asyncio.Semaphore(BUNDLE_CONCURRENCY)

    async def one(url: str) -> tuple[str, str, str | None]:
        async with semaphore:
            text, error = await _fetch(client, url, settings, accept="*/*")
            return url, text, error

    ids: dict[str, str] = {}
    for url, text, error in await asyncio.gather(*(one(u) for u in bundles)):
        if error:
            problems.append(f"{url}: {error}")
            continue
        for operation, qid in extract_ids(text).items():
            ids.setdefault(operation, qid)
    if not ids:
        return {}, "; ".join(problems) or "no query ids in any bundle"
    return ids, None


# --------------------------------------------------------------------------
# cache
# --------------------------------------------------------------------------


def cache_path(settings: Settings) -> Path:
    return Path(getattr(settings, "data_dir", ".")) / CACHE_NAME


def _now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _epoch(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        stamp = datetime.fromisoformat(text)
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.timestamp()


def load_cache(settings: Settings) -> dict[str, Any]:
    """The cache file, or ``{}`` when absent, unreadable or malformed."""
    try:
        raw = json.loads(cache_path(settings).read_text("utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    ids: dict[str, str] = {}
    entries = raw.get("ids")
    if isinstance(entries, dict):
        for operation, qid in entries.items():
            if isinstance(operation, str) and isinstance(qid, str) and operation and qid:
                ids[operation] = qid
    if not ids:
        return {}
    return {
        "fetched_at_utc": str(raw.get("fetched_at_utc") or ""),
        "source": str(raw.get("source") or ""),
        "ids": ids,
    }


def save_cache(settings: Settings, ids: dict[str, str]) -> None:
    """Write the cache atomically. A failure is not worth a crash."""
    if not ids:
        return
    path = cache_path(settings)
    payload = {
        "fetched_at_utc": _now_utc(),
        "source": BUNDLE_PREFIX + "*.js",
        "ids": dict(sorted(ids.items())),
    }
    tmp = path.with_name(f"{path.name}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", "utf-8")
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink()
        except Exception:
            pass


def _ttl(settings: Settings) -> float:
    try:
        return max(0.0, float(getattr(settings, "query_id_ttl", DEFAULT_TTL)))
    except (TypeError, ValueError):
        return DEFAULT_TTL


async def ensure(
    client: httpx.AsyncClient, settings: Settings, *, refresh: bool = False
) -> dict[str, str]:
    """Fresh-enough ids, re-scraping only when the cache has aged out.

    On a discovery failure the stale cache is returned unchanged -- a stale
    id still answers most operations, whereas an empty table answers none.
    """
    cached = load_cache(settings)
    stale: dict[str, str] = dict(cached.get("ids") or {})
    if not refresh and stale:
        age = _epoch(cached.get("fetched_at_utc"))
        if age is not None and (time.time() - age) < _ttl(settings):
            return stale
    fresh, _error = await discover(client, settings)
    if fresh:
        save_cache(settings, fresh)
        return fresh
    return stale


def query_id(
    operation: str, settings: Settings, cached: dict[str, str] | None = None
) -> str | None:
    """The id to use for ``operation``: env override, then cache, then code.

    ``cached`` is the already-loaded ``ids`` mapping (from :func:`ensure`);
    omit it and the cache file is read.
    """
    raw = os.environ.get(f"MAGPIE_QUERY_ID_{operation.upper()}") or ""
    for part in raw.split(","):
        part = part.strip()
        if part:
            return part

    table = cached if cached is not None else dict(load_cache(settings).get("ids") or {})
    qid = table.get(operation)
    if qid:
        return qid

    # Lazy: xapi may import this module, and the hardcoded table is only ever
    # the last resort.
    try:
        from . import xapi
    except Exception:  # pragma: no cover - import cycle or broken module
        return None
    fallback = xapi.QUERY_IDS.get(operation) or ()
    return fallback[0] if fallback else None
