"""Discovery: turning a handle or a known id into *more* ids, plus user profiles.

This module is the data-extraction counterpart to :mod:`sources` (which
hydrates one known id). It writes nothing, hashes nothing and renders nothing.

What actually works logged out, measured live:

* ``x.com/<handle>`` is server-rendered and carries 5-6 ``/status/<id>`` links
  (pinned post + most recent posts). It is the only unauthenticated timeline.
* ``x.com/<handle>/status/<id>`` carries the subject plus ~2 related ids.
* ``syndication.twitter.com/srv/timeline-profile`` answers 429 from most
  egress IPs. It stays here as an opt-in extra that degrades to an error
  string, never as something the crawler may depend on.
* ``with_replies``/``/media``/``/search``/``/hashtag`` serve the app shell with
  zero ids logged out, so they are deliberately absent.

Every parser is defensive: a markup change upstream must surface as
``error="no_ids_in_ssr"`` (the canary), never as an exception and never as a
silent empty result.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

from .config import Settings
from .models import Model, Profile

__all__ = [
    "Discovery",
    "conversation_ids",
    "profile_ids",
    "syndication_timeline",
    "user_profile",
]

_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
_JSON_ACCEPT = "application/json, text/plain, */*"
_HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

_ID_RE = re.compile(r"/status/(\d{10,25})")
_NEXT_DATA_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.IGNORECASE | re.DOTALL
)

# Marker -> error code. Probed against the lowercased page with curly
# apostrophes folded to ASCII, because X emits U+2019.
_PAGE_MARKERS: tuple[tuple[str, str], ...] = (
    ("account suspended", "account_suspended"),
    ("this account doesn't exist", "account_not_found"),
    ("page doesn't exist", "account_not_found"),
    ("these posts are protected", "account_protected"),
    ("these tweets are protected", "account_protected"),
)


@dataclass
class Discovery(Model):
    """One discovery attempt against one page. Never an exception, always a row."""

    source: str  # "profile_ssr" | "status_ssr" | "syndication_timeline"
    handle: str | None
    subject_id: str | None
    ids: list[str]
    http_status: int | None
    error: str | None
    fetched_at_utc: str
    bytes: int = 0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _clean_handle(handle: str | None) -> str:
    return (handle or "").strip().lstrip("@").strip("/")


def _headers(source_name: str, settings: Settings) -> dict[str, str]:
    headers = {
        "User-Agent": settings.ua(source_name),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": _HTML_ACCEPT if source_name in ("x_page", "syndication") else _JSON_ACCEPT,
    }
    if source_name == "syndication":
        headers["Referer"] = "https://platform.twitter.com/"
    if source_name == "x_page":
        headers["Upgrade-Insecure-Requests"] = "1"
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "none"
    return headers


async def _get(
    client: httpx.AsyncClient, url: str, source_name: str, settings: Settings
) -> tuple[int | None, str, str | None]:
    """GET with bounded retries (timeouts, network errors and 5xx only).

    Returns ``(status, text, error)``. ``error`` is set only for transport
    failures; an HTTP status the caller must interpret is left to the caller.
    """
    attempts = max(1, int(settings.http_retries) + 1)
    last_error: str | None = None
    headers = _headers(source_name, settings)
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            response = await client.get(
                url, headers=headers, timeout=settings.http_timeout, follow_redirects=True
            )
        except _RETRYABLE as exc:
            last_error = f"{type(exc).__name__}: {exc}".rstrip(": ")
            continue
        except httpx.HTTPError as exc:
            return None, "", f"{type(exc).__name__}: {exc}".rstrip(": ")
        except Exception as exc:  # pragma: no cover - transport contract violation
            return None, "", f"{type(exc).__name__}: {exc}".rstrip(": ")
        if response.status_code >= 500 and attempt < attempts - 1:
            last_error = f"HTTP {response.status_code}"
            continue
        try:
            text = response.text
        except Exception:  # pragma: no cover - undecodable body
            text = ""
        return response.status_code, text, None
    return None, "", last_error or "request failed"


def _extract_ids(html: str, handle: str | None = None) -> list[str]:
    """Status ids in first-appearance order, deduped.

    With a handle we take that handle's own ``/<handle>/status/<id>`` links, so
    a quoted or recommended post by somebody else is not mistaken for the
    subject's. The generic ``/status/<id>`` pass runs only as a fallback when
    the scoped pass finds nothing -- that is the safety net for X renaming the
    path, not a way to smuggle in foreign ids.
    """
    if not isinstance(html, str) or not html:
        return []
    seen: set[str] = set()
    ids: list[str] = []
    name = _clean_handle(handle)
    if name:
        try:
            scoped = re.compile(rf"/{re.escape(name)}/status/(\d{{10,25}})", re.IGNORECASE)
        except re.error:  # pragma: no cover - re.escape makes this unreachable
            scoped = None
        if scoped is not None:
            for match in scoped.finditer(html):
                tid = match.group(1)
                if tid not in seen:
                    seen.add(tid)
                    ids.append(tid)
    if ids:
        return ids
    for match in _ID_RE.finditer(html):
        tid = match.group(1)
        if tid not in seen:
            seen.add(tid)
            ids.append(tid)
    return ids


def _page_error(html: str, status: int | None) -> str | None:
    probe = html.lower().replace("\u2019", "'") if html else ""
    for marker, code in _PAGE_MARKERS:
        if marker in probe:
            return code
    if status != 200:
        return f"HTTP {status}" if status is not None else "request failed"
    return None


async def profile_ids(client: httpx.AsyncClient, handle: str, settings: Settings) -> Discovery:
    """The 5-6 ids X server-renders on a profile page (pinned + most recent)."""
    name = _clean_handle(handle)
    status, text, error = await _get(client, f"https://x.com/{name}", "x_page", settings)
    ids = _extract_ids(text, name)
    if error is None:
        error = _page_error(text, status)
        if error is None and not ids:
            # Canary: a healthy 200 with no status links means X changed its
            # markup. It must be loud, because every crawl depends on it.
            error = "no_ids_in_ssr"
    return Discovery(
        source="profile_ssr",
        handle=name or None,
        subject_id=None,
        ids=ids,
        http_status=status,
        error=error,
        fetched_at_utc=_now(),
        bytes=len(text.encode("utf-8", "replace")),
    )


async def conversation_ids(
    client: httpx.AsyncClient, tweet_id: str, handle: str | None, settings: Settings
) -> Discovery:
    """Related ids around one post (~2 in practice). The subject is excluded.

    Unlike a profile page this pass is *not* scoped to the handle: the whole
    point of a status page is the ids it points at that the crawler does not
    have yet, and replies/quotes are by definition other people's posts.
    """
    name = _clean_handle(handle)
    subject = str(tweet_id or "").strip()
    url = f"https://x.com/{name or 'i'}/status/{subject}"
    status, text, error = await _get(client, url, "x_page", settings)
    found = _extract_ids(text, None)
    ids = [tid for tid in found if tid != subject]
    if error is None:
        error = _page_error(text, status)
        if error is None and not found:
            # The subject's own id is always rendered; an empty page is markup
            # drift, whereas "no related ids" is a legitimate quiet thread.
            error = "no_ids_in_ssr"
    return Discovery(
        source="status_ssr",
        handle=name or None,
        subject_id=subject or None,
        ids=ids,
        http_status=status,
        error=error,
        fetched_at_utc=_now(),
        bytes=len(text.encode("utf-8", "replace")),
    )


def _next_data_ids(html: str) -> list[str]:
    match = _NEXT_DATA_RE.search(html or "")
    if match is None:
        return []
    try:
        payload = json.loads(match.group(1))
    except (ValueError, TypeError):
        return []
    node: Any = payload
    for key in ("props", "pageProps", "timeline", "entries"):
        node = node.get(key) if isinstance(node, dict) else None
        if node is None:
            return []
    if not isinstance(node, list):
        return []
    seen: set[str] = set()
    ids: list[str] = []
    for entry in node:
        content = entry.get("content") if isinstance(entry, dict) else None
        tweet = content.get("tweet") if isinstance(content, dict) else None
        raw = tweet.get("id_str") or tweet.get("id") if isinstance(tweet, dict) else None
        tid = str(raw).strip() if raw is not None else ""
        if tid.isdigit() and tid not in seen:
            seen.add(tid)
            ids.append(tid)
    return ids


async def syndication_timeline(
    client: httpx.AsyncClient, handle: str, settings: Settings
) -> Discovery:
    """Optional extra source. IP-throttled: 429 is the normal outcome here."""
    name = _clean_handle(handle)
    url = (
        "https://syndication.twitter.com/srv/timeline-profile/screen-name/"
        f"{name}?showReplies=true&lang=en"
    )
    status, text, error = await _get(client, url, "syndication", settings)
    ids = _next_data_ids(text) or _extract_ids(text, None)
    if error is None:
        if status == 429:
            ids = []
            error = "HTTP 429 (rate limited; this endpoint is IP-throttled)"
        elif status != 200:
            error = f"HTTP {status}"
        elif not ids:
            error = "no_ids_in_ssr"
    return Discovery(
        source="syndication_timeline",
        handle=name or None,
        subject_id=None,
        ids=ids,
        http_status=status,
        error=error,
        fetched_at_utc=_now(),
        bytes=len(text.encode("utf-8", "replace")),
    )


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _first_int(payload: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        found = _int(payload.get(key))
        if found is not None:
            return found
    return None


def _first_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        found = _text(payload.get(key))
        if found is not None:
            return found
    return None


def _profile_from_fx(user: dict[str, Any]) -> Profile:
    website = user.get("website")
    verification = user.get("verification")
    return Profile(
        screen_name=_text(user.get("screen_name")),
        name=_text(user.get("name")),
        description=_text(user.get("description")),
        location=_text(user.get("location")),
        website=_text(website.get("url")) if isinstance(website, dict) else _text(website),
        joined=_text(user.get("joined")),
        followers=_int(user.get("followers")),
        following=_int(user.get("following")),
        tweets=_first_int(user, "tweets", "statuses"),
        media_count=_int(user.get("media_count")),
        likes=_int(user.get("likes")),
        protected=_bool(user.get("protected")),
        verified=(
            _bool(verification.get("verified"))
            if isinstance(verification, dict)
            else _bool(user.get("verified"))
        ),
        avatar_url=_text(user.get("avatar_url")),
        banner_url=_text(user.get("banner_url")),
    )


def _profile_from_vx(payload: dict[str, Any]) -> Profile:
    return Profile(
        screen_name=_first_text(payload, "user_screen_name", "screen_name"),
        name=_first_text(payload, "user_name", "name"),
        description=_text(payload.get("description")),
        location=_text(payload.get("location")),
        website=_first_text(payload, "url", "website"),
        joined=_first_text(payload, "created_at", "joined"),
        followers=_first_int(payload, "followers_count", "followers"),
        following=_first_int(payload, "following_count", "friends_count", "following"),
        tweets=_first_int(payload, "tweet_count", "tweets_count", "statuses_count", "tweets"),
        media_count=_first_int(payload, "media_count"),
        likes=_first_int(payload, "likes_count", "favourites_count", "likes"),
        protected=_bool(payload.get("protected")),
        verified=_bool(payload.get("verified")),
        avatar_url=_first_text(
            payload, "profile_image_url", "user_profile_image_url", "avatar_url"
        ),
        banner_url=_first_text(payload, "profile_banner_url", "banner_url"),
    )


def _loads(text: str) -> Any:
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return None


async def user_profile(
    client: httpx.AsyncClient, handle: str, settings: Settings
) -> tuple[Profile | None, str | None]:
    """Account-level stats. fxtwitter first (richer), vxtwitter as the fallback."""
    name = _clean_handle(handle)
    problems: list[str] = []

    fx_url = f"https://api.fxtwitter.com/{name}"
    status, text, error = await _get(client, fx_url, "fxtwitter", settings)
    payload = _loads(text) if error is None else None
    user = payload.get("user") if isinstance(payload, dict) else None
    if status == 200 and isinstance(user, dict):
        return _profile_from_fx(user), None
    message = _text(payload.get("message")) if isinstance(payload, dict) else None
    problems.append(f"fxtwitter: {error or message or f'HTTP {status}'}")

    vx_url = f"https://api.vxtwitter.com/{name}"
    status, text, error = await _get(client, vx_url, "vxtwitter", settings)
    payload = _loads(text) if error is None else None
    handles = ("screen_name", "user_screen_name")
    named = isinstance(payload, dict) and any(_text(payload.get(k)) for k in handles)
    if status == 200 and named:
        return _profile_from_vx(payload), None  # type: ignore[arg-type]
    message = _text(payload.get("error")) if isinstance(payload, dict) else None
    problems.append(f"vxtwitter: {error or message or f'HTTP {status}'}")

    return None, "; ".join(problems)
