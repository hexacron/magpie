"""Anonymous guest-token access to X's public GraphQL API.

This is the high-yield counterpart to :mod:`collect`. The server-rendered
profile page exposes 5-6 status ids; one ``UserTweets`` call under a guest
token returned **136 distinct ids for @jack** in a single 479 KB response.
No account, no login, no cookie jar: ``POST /1.1/guest/activate.json`` with
the bearer x.com ships to logged-out browsers hands out an anonymous token,
and that token plus the bearer is the entire credential set.

Measured live, logged out:

* ``UserByScreenName`` -> 200, full user object (handle -> numeric rest id).
* ``UserTweets``       -> 200, 136 tweet ids in one call.
* ``TweetDetail``      -> 200, the whole threaded conversation.
* ``SearchTimeline``   -> 404 for every query id tried, as does
  ``2/search/adaptive.json`` and every ``1.1/statuses/*`` route. Search needs
  a real account, so search is deliberately absent from this module.

Two operational facts drive the design:

* **Query ids rotate.** A stale id answers ``404`` with an *empty body* --
  that is the signature of a dead id, not of an auth failure. Every operation
  therefore carries several candidate ids, tried in order; the winner is
  remembered for the process lifetime so later calls cost one request.
  ``MAGPIE_QUERY_ID_<OPERATION>`` overrides let a rotation be fixed without a
  code change.
* **Feature flags rotate too.** A missing flag answers ``422`` naming the
  flag it wants. That message is surfaced verbatim as the error string: it is
  the maintenance signal telling you exactly what to add to :data:`FEATURES`.

Nothing here raises on a changed payload. Every failure becomes an error
string, exactly as in :mod:`collect`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import Settings
from .models import Post, Profile
from .normalize import (
    _bool,
    _dict,
    _from_string,
    _int,
    _list,
    _syndication_media,
    _syndication_text,
    _text,
)

__all__ = [
    "FEATURES",
    "QUERY_IDS",
    "SOURCE_NAME",
    "WEB_BEARER",
    "GuestSession",
    "parse_timeline",
    "tweet_detail",
    "user_by_screen_name",
    "user_tweets",
]

# The bearer x.com serves to logged-out browsers. Public, static, and not a
# credential belonging to any account -- it is shipped in x.com's own JS
# bundle. The %3D is part of the literal token value; it is not escaping.
WEB_BEARER = (
    "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D"
    "1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"
)

SOURCE_NAME = "xapi_guest"

API_BASE = "https://api.x.com"
ACTIVATE_URL = f"{API_BASE}/1.1/guest/activate.json"

# Newest-first. The leading id of each tuple is the one verified working under
# a guest token; the rest are previous rotations kept as fallbacks.
_DEFAULT_QUERY_IDS: dict[str, tuple[str, ...]] = {
    "UserTweets": (
        "V7H0Ap3_Hh2FyS75OCDO3Q",
        "E3opETHurmVJflFsUBVuUQ",
        "HuTx74BxAnezK1gWvYY7zg",
    ),
    "UserByScreenName": (
        "G3KGOASz96M-Qu0nwmGXNg",
        "sLVLhk0bGj3MVFEKTdax1w",
    ),
    "TweetDetail": (
        "_8aYOgEDz35BrBcBal1-_w",
        "QuBlQ6SxNAQCt6-kBiCXCQ",
        "VWFGPVAGkZMGRKGe3GFFnA",
    ),
}


def _query_ids() -> dict[str, tuple[str, ...]]:
    """Defaults with ``MAGPIE_QUERY_ID_<OPERATION>`` prepended, deduped."""
    table: dict[str, tuple[str, ...]] = {}
    for operation, ids in _DEFAULT_QUERY_IDS.items():
        raw = os.environ.get(f"MAGPIE_QUERY_ID_{operation.upper()}") or ""
        override = [part.strip() for part in raw.split(",") if part.strip()]
        ordered: list[str] = []
        for qid in [*override, *ids]:
            if qid not in ordered:
                ordered.append(qid)
        table[operation] = tuple(ordered)
    return table


QUERY_IDS: dict[str, tuple[str, ...]] = _query_ids()

# The flat boolean map that answered 200. Unknown flags are ignored by the
# server; a *missing* one is a 422 naming itself, so erring wide is correct.
FEATURES: dict[str, bool] = {
    "articles_preview_enabled": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "communities_web_enable_tweet_community_results_fetch": True,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "hidden_profile_likes_enabled": True,
    "hidden_profile_subscriptions_enabled": True,
    "highlights_tweets_tab_ui_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "payments_enabled": False,
    "premium_content_api_read_enabled": False,
    "profile_label_improvements_pcf_label_in_post_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_grok_analysis_button_from_backend": False,
    "responsive_web_grok_analyze_button_fetch_trends_enabled": False,
    "responsive_web_grok_analyze_post_followups_enabled": False,
    "responsive_web_grok_community_note_auto_translation_is_enabled": False,
    "responsive_web_grok_image_annotation_enabled": False,
    "responsive_web_grok_share_attachment_enabled": False,
    "responsive_web_grok_show_grok_translated_post": False,
    "responsive_web_jetfuel_frame": False,
    "responsive_web_media_download_video_enabled": False,
    "responsive_web_twitter_article_notes_tab_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "rweb_video_screen_enabled": False,
    "rweb_video_timestamps_enabled": True,
    "standardized_nudges_misinfo": True,
    "subscriptions_feature_can_gift_premium": True,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "tweetypie_unmention_optimization_enabled": True,
    "verified_phone_label_enabled": False,
    "view_counts_everywhere_api_enabled": True,
}

# Per-operation fieldToggles. Omitting a required toggle is a hard 400, so the
# two operations that demand them carry them explicitly.
FIELD_TOGGLES: dict[str, dict[str, bool]] = {
    "UserByScreenName": {"withAuxiliaryUserLabels": False},
    "TweetDetail": {
        "withArticleRichContentState": True,
        "withArticlePlainText": False,
        "withGrokAnalyze": False,
        "withDisallowedReplyControls": False,
    },
}

_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)

# Process-lifetime memory so a rotation costs one wasted request, not one per
# call. `_WORKING_QID` is the winner; `_DEAD_QIDS` are ids that answered
# 404-with-empty-body and are skipped outright afterwards.
_WORKING_QID: dict[str, str] = {}
_DEAD_QIDS: dict[str, set[str]] = {}
_USER_ID_CACHE: dict[str, str] = {}

_COUNT_KEYS = ("likes", "retweets", "replies", "quotes", "views", "bookmarks")


# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------


def _headers(settings: Settings, guest_token: str | None) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {WEB_BEARER}",
        "User-Agent": settings.ua("x_page"),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "Origin": "https://x.com",
        "Referer": "https://x.com/",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
    }
    if guest_token:
        headers["x-guest-token"] = guest_token
    return headers


async def _send(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    settings: Settings,
    headers: dict[str, str],
    params: dict[str, str] | None = None,
) -> tuple[int | None, str, str | None]:
    """One request with bounded retries (timeouts, network errors and 5xx).

    Returns ``(status, text, error)``. ``error`` is transport-level only; an
    HTTP status the caller must interpret is handed back untouched.
    """
    attempts = max(1, int(getattr(settings, "http_retries", 2)) + 1)
    timeout = float(getattr(settings, "http_timeout", 20.0))
    last_error: str | None = None
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            response = await client.request(
                method, url, headers=headers, params=params, timeout=timeout
            )
        except _RETRYABLE as exc:
            last_error = f"{type(exc).__name__}: {exc}".rstrip(": ")
            continue
        except Exception as exc:  # transport contract violation
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


def _loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _payload_error(payload: Any) -> str | None:
    """Join the GraphQL ``errors[].message`` list, if any."""
    messages: list[str] = []
    for entry in _list(_dict(payload).get("errors")):
        message = _text(_dict(entry).get("message"))
        if message:
            messages.append(message)
    return "; ".join(messages) or None


# --------------------------------------------------------------------------
# guest token
# --------------------------------------------------------------------------


class GuestSession:
    """An anonymous guest token plus the GraphQL call that uses it.

    The token is cached in memory with its issue time and re-activated when it
    ages past ``settings.guest_token_ttl`` or when a caller forces it after a
    403/429. Activation is serialised by a lock: a burst of concurrent callers
    on a cold session performs exactly one ``guest/activate.json`` POST.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.error: str | None = None
        self.activations = 0
        self._token: str | None = None
        self._issued = 0.0
        self._lock = asyncio.Lock()

    @property
    def ttl(self) -> float:
        # getattr so the module keeps working against an older Settings.
        try:
            return max(0.0, float(getattr(self.settings, "guest_token_ttl", 900.0)))
        except (TypeError, ValueError):
            return 900.0

    def _fresh(self) -> bool:
        return self._token is not None and (time.monotonic() - self._issued) < self.ttl

    async def token(self, client: httpx.AsyncClient, *, force: bool = False) -> str | None:
        """The cached token, activating or re-activating only when needed."""
        seen = self._token
        if not force and self._fresh():
            return self._token
        async with self._lock:
            if force:
                # Another coroutine already rotated it out from under us --
                # one 403 must not cause N activations.
                if self._token is not None and self._token != seen:
                    return self._token
            elif self._fresh():
                return self._token
            return await self._activate(client)

    async def _activate(self, client: httpx.AsyncClient) -> str | None:
        self.activations += 1
        status, text, error = await _send(
            client,
            "POST",
            ACTIVATE_URL,
            settings=self.settings,
            headers=_headers(self.settings, None),
        )
        if error is not None:
            self.error = error
            return None
        if status != 200:
            self.error = f"guest activate HTTP {status}"
            return None
        token = _text(_dict(_loads(text)).get("guest_token"))
        if token is None:
            self.error = "guest activate returned no token"
            return None
        self._token = token
        self._issued = time.monotonic()
        self.error = None
        return token

    def _candidates(self, operation: str) -> list[str]:
        known = QUERY_IDS.get(operation, ())
        dead = _DEAD_QIDS.get(operation, set())
        winner = _WORKING_QID.get(operation)
        ordered = [qid for qid in known if qid not in dead]
        if winner is not None:
            ordered = [winner, *[qid for qid in ordered if qid != winner]]
        return ordered

    async def graphql(
        self,
        client: httpx.AsyncClient,
        operation: str,
        variables: dict[str, Any],
        *,
        features: dict[str, Any] | None = None,
    ) -> tuple[int | None, dict[str, Any] | None, str | None]:
        """Call one GraphQL operation, walking the candidate query ids.

        Returns ``(status, payload, error)``. 404-with-empty-body burns the id
        and moves to the next; 403/429 refreshes the guest token once and
        retries the same id; 422 returns the validation message naming the
        feature flag X now wants.
        """
        candidates = self._candidates(operation)
        if not candidates:
            return None, None, f"no query id configured for {operation}"

        token = await self.token(client)
        if token is None:
            return None, None, self.error or "no guest token"

        params_base = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(
                features if features is not None else FEATURES, separators=(",", ":")
            ),
        }
        toggles = FIELD_TOGGLES.get(operation)
        if toggles:
            params_base["fieldToggles"] = json.dumps(toggles, separators=(",", ":"))

        last_status: int | None = None
        for qid in candidates:
            url = f"{API_BASE}/graphql/{qid}/{operation}"
            refreshed = False
            while True:
                status, text, error = await _send(
                    client,
                    "GET",
                    url,
                    settings=self.settings,
                    headers=_headers(self.settings, token),
                    params=dict(params_base),
                )
                if error is not None:
                    return status, None, error
                last_status = status

                if status in (403, 429) and not refreshed:
                    refreshed = True
                    token = await self.token(client, force=True)
                    if token is None:
                        return status, None, self.error or "guest token refresh failed"
                    continue
                break

            if status == 404 and not text.strip():
                # The rotation signature: dead id, not an auth problem.
                _DEAD_QIDS.setdefault(operation, set()).add(qid)
                if _WORKING_QID.get(operation) == qid:
                    _WORKING_QID.pop(operation, None)
                continue

            payload = _loads(text)
            message = _payload_error(payload)

            if status == 422:
                return status, None, message or "GRAPHQL_VALIDATION_FAILED"
            if status != 200:
                return status, None, message or f"HTTP {status}"
            if not isinstance(payload, dict):
                return status, None, f"non-JSON response for {operation}"
            if message and not _dict(payload.get("data")):
                return status, payload, message

            _WORKING_QID[operation] = qid
            return status, payload, None

        return (
            last_status,
            None,
            f"no working query id for {operation} (ids rotate; update QUERY_IDS)",
        )


# --------------------------------------------------------------------------
# timeline walking
# --------------------------------------------------------------------------


@dataclass
class _Walk:
    posts: list[Post] = field(default_factory=list)
    cursor: str | None = None
    tombstones: int = 0
    seen: set[str] = field(default_factory=set)

    def add(self, posts: list[Post]) -> None:
        for post in posts:
            if post.id in self.seen:
                continue
            self.seen.add(post.id)
            self.posts.append(post)


def _instructions(payload: Any, depth: int = 0) -> list[Any]:
    """First ``instructions`` list anywhere in the payload.

    UserTweets hides it under ``data.user.result.timeline_v2.timeline`` while
    TweetDetail puts it under ``threaded_conversation_with_injections_v2``;
    both move between query ids. A bounded search beats hard-coded paths.
    """
    if depth > 8:
        return []
    if isinstance(payload, dict):
        found = payload.get("instructions")
        if isinstance(found, list):
            return found
        for value in payload.values():
            if isinstance(value, (dict, list)):
                nested = _instructions(value, depth + 1)
                if nested:
                    return nested
    elif isinstance(payload, list):
        for value in payload:
            if isinstance(value, (dict, list)):
                nested = _instructions(value, depth + 1)
                if nested:
                    return nested
    return []


def _cursor_value(entry_id: str | None, node: dict[str, Any]) -> str | None:
    """Bottom-cursor value out of an entry content or itemContent node."""
    kind = _text(node.get("cursorType"))
    value = _text(node.get("value"))
    if value is None:
        return None
    if kind == "Bottom":
        return value
    if kind is None and (entry_id or "").startswith("cursor-bottom"):
        return value
    return None


def _walk_item_content(
    walk: _Walk, entry_id: str | None, item_content: dict[str, Any]
) -> None:
    cursor = _cursor_value(entry_id, item_content)
    if cursor is not None:
        walk.cursor = cursor
        return
    results = _dict(item_content.get("tweet_results"))
    if not results:
        return
    walk.add(_posts_from_result(_dict(results.get("result")), walk))


def _walk_entry(walk: _Walk, entry: Any) -> None:
    entry_dict = _dict(entry)
    entry_id = _text(entry_dict.get("entryId"))
    content = _dict(entry_dict.get("content"))
    if not content:
        # TimelineAddToModule hands back `{entryId, item: {itemContent}}`.
        item = _dict(entry_dict.get("item"))
        if item:
            _walk_item_content(walk, entry_id, _dict(item.get("itemContent")))
        return

    kind = _text(content.get("entryType")) or _text(content.get("__typename")) or ""

    cursor = _cursor_value(entry_id, content)
    if cursor is not None:
        walk.cursor = cursor
        return

    if kind == "TimelineTimelineModule" or content.get("items") is not None:
        for module_item in _list(content.get("items")):
            item = _dict(_dict(module_item).get("item"))
            module_entry_id = _text(_dict(module_item).get("entryId")) or entry_id
            _walk_item_content(walk, module_entry_id, _dict(item.get("itemContent")))
        return

    _walk_item_content(walk, entry_id, _dict(content.get("itemContent")))


def _walk(payload: Any) -> _Walk:
    walk = _Walk()
    for instruction in _instructions(payload):
        node = _dict(instruction)
        kind = _text(node.get("type")) or _text(node.get("__typename")) or ""
        if kind == "TimelineAddEntries":
            entries = _list(node.get("entries"))
        elif kind == "TimelinePinEntry":
            entries = [node.get("entry")]
        elif kind == "TimelineAddToModule":
            entries = _list(node.get("moduleItems"))
        else:
            continue
        for entry in entries:
            _walk_entry(walk, entry)
    return walk


def parse_timeline(payload: dict[str, Any]) -> tuple[list[Post], str | None]:
    """Posts plus the bottom cursor out of any X timeline payload.

    The cursor is ``None`` when the payload carries none -- which is the
    measured live case for a guest ``UserTweets`` response, so callers must
    treat "no cursor" as a normal stop, never as an error.
    """
    walk = _walk(payload)
    return walk.posts, walk.cursor


# --------------------------------------------------------------------------
# tweet mapping
# --------------------------------------------------------------------------


def _unwrap(result: dict[str, Any]) -> dict[str, Any]:
    """``TweetWithVisibilityResults`` wraps the real tweet one level down."""
    for _ in range(3):
        typename = _text(result.get("__typename")) or ""
        inner = _dict(result.get("tweet"))
        if typename == "TweetWithVisibilityResults" and inner:
            result = inner
            continue
        if not result.get("legacy") and inner:
            result = inner
            continue
        break
    return result


def _user_result(result: dict[str, Any]) -> dict[str, Any]:
    return _dict(_dict(_dict(result.get("core")).get("user_results")).get("result"))


def _profile_from_user(user: dict[str, Any]) -> tuple[Profile, str | None]:
    """``Profile`` plus the screen name, from either user shape X serves."""
    legacy = _dict(user.get("legacy"))
    core = _dict(user.get("core"))
    screen_name = _text(legacy.get("screen_name")) or _text(core.get("screen_name"))
    name = _text(legacy.get("name")) or _text(core.get("name"))
    website = None
    for url_entry in _list(_dict(_dict(legacy.get("entities")).get("url")).get("urls")):
        website = _text(_dict(url_entry).get("expanded_url"))
        if website:
            break
    profile = Profile(
        screen_name=screen_name,
        name=name,
        description=_text(legacy.get("description")),
        location=_text(legacy.get("location"))
        or _text(_dict(user.get("location")).get("location")),
        website=website or _text(legacy.get("url")),
        joined=_from_string(legacy.get("created_at") or core.get("created_at"))[0],
        followers=_int(legacy.get("followers_count")),
        following=_int(legacy.get("friends_count")),
        tweets=_int(legacy.get("statuses_count")),
        media_count=_int(legacy.get("media_count")),
        likes=_int(legacy.get("favourites_count")),
        protected=_bool(legacy.get("protected")) or _bool(user.get("protected")),
        verified=_bool(legacy.get("verified")) or _bool(user.get("is_blue_verified")),
        avatar_url=_text(legacy.get("profile_image_url_https"))
        or _text(_dict(user.get("avatar")).get("image_url")),
        banner_url=_text(legacy.get("profile_banner_url")),
    )
    return profile, screen_name


def _counts(result: dict[str, Any], legacy: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for metric, key in (
        ("likes", "favorite_count"),
        ("retweets", "retweet_count"),
        ("replies", "reply_count"),
        ("quotes", "quote_count"),
        ("bookmarks", "bookmark_count"),
    ):
        value = _int(legacy.get(key))
        if value is not None:
            counts[metric] = value
    # views.count is a *string* in every response measured.
    views = _int(_dict(result.get("views")).get("count"))
    if views is not None:
        counts["views"] = views
    return counts


def _quoted(result: dict[str, Any], legacy: dict[str, Any]) -> tuple[str | None, str | None]:
    quoted_id = _text(legacy.get("quoted_status_id_str"))
    quoted_result = _unwrap(
        _dict(_dict(result.get("quoted_status_result")).get("result"))
    )
    if quoted_id is None:
        quoted_id = _text(quoted_result.get("rest_id"))
    screen_name = _profile_from_user(_user_result(quoted_result))[1]
    if screen_name is None:
        # The permalink is the only handle carried when the quote is not
        # hydrated: https://x.com/<handle>/status/<id>
        expanded = _text(_dict(legacy.get("quoted_status_permalink")).get("expanded")) or ""
        parts = expanded.split("/")
        if len(parts) >= 5 and parts[2].endswith(("x.com", "twitter.com")):
            screen_name = parts[3] or None
    return quoted_id, screen_name


def _post_from_result(result: dict[str, Any]) -> Post | None:
    legacy = _dict(result.get("legacy"))
    post_id = _text(result.get("rest_id")) or _text(legacy.get("id_str"))
    if post_id is None:
        return None

    user = _user_result(result)
    profile, screen_name = _profile_from_user(user)

    # The note_tweet / display_text_range handling is identical to the
    # syndication payload, so it is reused rather than re-derived.
    text, _truncated = _syndication_text(
        {
            "note_tweet": result.get("note_tweet"),
            "text": legacy.get("full_text"),
            "display_text_range": legacy.get("display_text_range"),
        }
    )
    created_utc, created_display = _from_string(legacy.get("created_at"))
    counts = _counts(result, legacy)
    quoted_id, quoted_screen_name = _quoted(result, legacy)

    post = Post(
        id=post_id,
        screen_name=screen_name,
        name=profile.name,
        text=text,
        text_source=SOURCE_NAME,
        created_at_utc=created_utc,
        created_at_display=created_display,
        lang=_text(legacy.get("lang")),
        possibly_sensitive=_bool(legacy.get("possibly_sensitive")),
        blue_verified=_bool(user.get("is_blue_verified")),
        avatar_url=profile.avatar_url,
        banner_url=profile.banner_url,
        counts=counts,
        count_sources={metric: SOURCE_NAME for metric in counts},
        count_agreement={metric: None for metric in counts},
        media=_syndication_media(
            {"mediaDetails": _list(_dict(legacy.get("extended_entities")).get("media"))}
        ),
        media_source=SOURCE_NAME,
        community_note=_text(
            _dict(_dict(result.get("birdwatch_pivot")).get("subtitle")).get("text")
        ),
        profile=profile,
        quoted_id=quoted_id,
        quoted_screen_name=quoted_screen_name,
        reply_to_id=_text(legacy.get("in_reply_to_status_id_str")),
        reply_to_screen_name=_text(legacy.get("in_reply_to_screen_name")),
        source_url=f"https://x.com/{screen_name or 'i'}/status/{post_id}",
        available_sources=[SOURCE_NAME],
    )
    if not post.media:
        post.media_source = None
    return post


def _posts_from_result(
    result: Any, walk: _Walk | None = None, depth: int = 0
) -> list[Post]:
    """One tweet result -> the posts it contains.

    A retweet yields two: the retweeting post as stored by the timeline *and*
    the underlying original, which is the actual content and would otherwise
    only ever exist as a truncated ``RT @x: ...`` string.
    """
    node = _unwrap(_dict(result))
    if not node or depth > 2:
        return []
    typename = _text(node.get("__typename")) or ""
    if typename == "TweetTombstone" or node.get("tombstone") is not None:
        if walk is not None:
            walk.tombstones += 1
        return []

    post = _post_from_result(node)
    if post is None:
        return []

    posts = [post]
    retweeted = _dict(_dict(node.get("legacy")).get("retweeted_status_result"))
    if retweeted:
        posts.extend(_posts_from_result(retweeted.get("result"), walk, depth + 1))
    return posts


# --------------------------------------------------------------------------
# operations
# --------------------------------------------------------------------------


def _clean_handle(handle: str | None) -> str:
    return (handle or "").strip().lstrip("@").strip("/")


async def user_by_screen_name(
    client: httpx.AsyncClient,
    session: GuestSession,
    handle: str,
    settings: Settings,
) -> tuple[Profile | None, str | None, str | None]:
    """``(profile, rest_id, error)`` for one handle."""
    cleaned = _clean_handle(handle)
    if not cleaned:
        return None, None, "empty handle"

    _status, payload, error = await session.graphql(
        client,
        "UserByScreenName",
        {
            "screen_name": cleaned,
            "withSafetyModeUserFields": True,
            "withGrokTranslatedBio": False,
        },
    )
    if error is not None:
        return None, None, error

    user = _dict(_dict(_dict(payload).get("data")).get("user"))
    result = _dict(user.get("result"))
    if not result:
        return None, None, f"no user in payload for @{cleaned}"

    typename = _text(result.get("__typename")) or ""
    if typename == "UserUnavailable":
        reason = _text(result.get("reason")) or "user_unavailable"
        return None, None, f"@{cleaned} unavailable ({reason.lower()})"

    profile, screen_name = _profile_from_user(result)
    rest_id = _text(result.get("rest_id")) or _text(_dict(result.get("legacy")).get("id_str"))
    if profile.screen_name is None:
        profile.screen_name = cleaned
    if rest_id is None:
        return profile, None, f"no rest_id for @{cleaned}"
    _USER_ID_CACHE[(screen_name or cleaned).lower()] = rest_id
    return profile, rest_id, None


async def user_tweets(
    client: httpx.AsyncClient,
    session: GuestSession,
    *,
    user_id: str | None = None,
    handle: str | None = None,
    settings: Settings,
    count: int = 40,
    cursor: str | None = None,
) -> tuple[list[Post], str | None, str | None]:
    """One page of an account's timeline: ``(posts, next_cursor, error)``.

    ~136 ids per call measured, against 5 from the server-rendered page. The
    live response carried no cursor at all, so ``next_cursor`` is routinely
    ``None`` and that is a clean stop, not a failure.
    """
    resolved = _text(user_id)
    cleaned = _clean_handle(handle)
    if resolved is None and cleaned:
        cached = _USER_ID_CACHE.get(cleaned.lower())
        if cached is not None:
            resolved = cached
        else:
            _profile, rest_id, error = await user_by_screen_name(
                client, session, cleaned, settings
            )
            if rest_id is None:
                return [], None, error or f"could not resolve @{cleaned}"
            resolved = rest_id
    if resolved is None:
        return [], None, "user_tweets needs user_id or handle"

    try:
        page = max(1, int(count))
    except (TypeError, ValueError):
        page = 40

    variables: dict[str, Any] = {
        "userId": resolved,
        "count": page,
        "includePromotedContent": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withVoice": True,
        "withV2Timeline": True,
    }
    if _text(cursor) is not None:
        variables["cursor"] = cursor

    _status, payload, error = await session.graphql(client, "UserTweets", variables)
    if error is not None:
        return [], None, error

    walk = _walk(payload)
    if not walk.posts:
        if walk.tombstones:
            return [], walk.cursor, f"{walk.tombstones} tombstoned post(s), nothing readable"
        # The canary: shape changed upstream and the crawl would silently
        # yield nothing forever.
        return [], walk.cursor, "no posts in timeline payload"
    return walk.posts, walk.cursor, None


async def tweet_detail(
    client: httpx.AsyncClient,
    session: GuestSession,
    tweet_id: str,
    settings: Settings,
) -> tuple[list[Post], str | None]:
    """The whole threaded conversation around one post."""
    focal = _text(tweet_id)
    if focal is None:
        return [], "empty tweet id"

    variables: dict[str, Any] = {
        "focalTweetId": focal,
        "with_rux_injections": False,
        "rankingMode": "Relevance",
        "includePromotedContent": True,
        "withCommunity": True,
        "withQuickPromoteEligibilityTweetFields": True,
        "withBirdwatchNotes": True,
        "withVoice": True,
        "withV2Timeline": True,
    }
    _status, payload, error = await session.graphql(client, "TweetDetail", variables)
    if error is not None:
        return [], error

    walk = _walk(payload)
    if not walk.posts:
        if walk.tombstones:
            return [], f"{walk.tombstones} tombstoned post(s), nothing readable"
        return [], f"no posts in conversation payload for {focal}"
    return walk.posts, None
