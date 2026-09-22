"""Offline tests for discovery and profile collection.

Only failures that would silently corrupt a crawl are covered: the markup
canary (X changes its SSR and every profile quietly yields nothing), handle
scoping (a recommended post by another account enters the dataset as the
subject's), subject leakage from a status page (a crawl re-hydrates the id it
just came from forever), the 429 that this machine's egress always gets from
syndication, and the two profile payload shapes.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from magpie.collect import (
    _extract_ids,
    conversation_ids,
    profile_ids,
    syndication_timeline,
    user_profile,
)
from magpie.config import Settings

SETTINGS = Settings(http_retries=0, http_timeout=1.0)

# Shape of the real profile SSR: absolute and relative links, canonical casing
# echoed back by X, plus a recommended post by an unrelated account.
PROFILE_HTML = """
<html><head><meta property="og:title" content="jack (@jack)"></head><body>
<a href="/Jack/status/1234567890123456789">pinned</a>
<link rel="canonical" href="https://x.com/jack/status/9876543210987654321"/>
<a href="/jack/status/1234567890123456789">same post again</a>
<a href="/elonmusk/status/1111111111111111111">recommended</a>
<a href="/jack/status/5555555555555555555">latest</a>
</body></html>
"""


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _run(coro):
    return asyncio.run(coro)


def _html_handler(body: str, status: int = 200):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text=body, headers={"content-type": "text/html"})

    return handler


# --------------------------------------------------------------------------
# id extraction
# --------------------------------------------------------------------------


def test_extract_ids_is_ordered_deduped_and_handle_scoped() -> None:
    ids = _extract_ids(PROFILE_HTML, "jack")
    assert ids == [
        "1234567890123456789",
        "9876543210987654321",
        "5555555555555555555",
    ]
    # The unrelated account's post must not enter the crawl as jack's.
    assert "1111111111111111111" not in ids


def test_extract_ids_without_handle_takes_every_status_link() -> None:
    assert "1111111111111111111" in _extract_ids(PROFILE_HTML, None)


def test_extract_ids_survives_garbage() -> None:
    assert _extract_ids("", "jack") == []
    assert _extract_ids("<a href='/jack/status/12'>too short</a>", "jack") == []


# --------------------------------------------------------------------------
# profile discovery
# --------------------------------------------------------------------------


def test_profile_ids_reports_the_markup_canary() -> None:
    shell = "<html><body><div id='react-root'></div></body></html>"
    result = _run(profile_ids(_client(_html_handler(shell)), "jack", SETTINGS))
    assert result.ids == []
    assert result.error == "no_ids_in_ssr"
    assert result.http_status == 200
    assert result.source == "profile_ssr"


def test_profile_ids_finds_the_timeline_ids() -> None:
    result = _run(profile_ids(_client(_html_handler(PROFILE_HTML)), "@jack", SETTINGS))
    assert result.error is None
    assert result.handle == "jack"
    assert len(result.ids) == 3
    assert result.bytes > 0


def test_profile_ids_classifies_account_states() -> None:
    cases = {
        "<html><body>Account suspended<a href='/jack/status/1234567890123456789'>p</a>"
        "</body></html>": "account_suspended",
        "<html><body>This account doesn\u2019t exist</body></html>": "account_not_found",
        "<html><body>These posts are protected</body></html>": "account_protected",
    }
    for html, expected in cases.items():
        result = _run(profile_ids(_client(_html_handler(html)), "jack", SETTINGS))
        assert result.error == expected, html
    # A suspended page still hands back whatever it rendered.
    suspended = next(h for h in cases if "suspended" in h)
    assert _run(profile_ids(_client(_html_handler(suspended)), "jack", SETTINGS)).ids == [
        "1234567890123456789"
    ]


def test_profile_ids_reports_http_failures() -> None:
    result = _run(profile_ids(_client(_html_handler("nope", status=404)), "jack", SETTINGS))
    assert result.error == "HTTP 404"

    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host")

    failed = _run(profile_ids(_client(boom), "jack", SETTINGS))
    assert failed.ids == []
    assert failed.error is not None and "ConnectError" in failed.error
    assert failed.http_status is None


# --------------------------------------------------------------------------
# conversation discovery
# --------------------------------------------------------------------------


def test_conversation_ids_excludes_the_subject() -> None:
    subject = "1234567890123456789"
    html = (
        f"<a href='/jack/status/{subject}'>subject</a>"
        "<a href='/someoneelse/status/2222222222222222222'>reply</a>"
        f"<a href='/jack/status/{subject}'>subject again</a>"
        "<a href='/jack/status/3333333333333333333'>self-reply</a>"
    )
    result = _run(conversation_ids(_client(_html_handler(html)), subject, "jack", SETTINGS))
    # Replies come from other accounts, so the status pass is deliberately
    # unscoped; only the subject itself is filtered out.
    assert result.ids == ["2222222222222222222", "3333333333333333333"]
    assert result.subject_id == subject
    assert result.source == "status_ssr"
    assert result.error is None


def test_conversation_ids_flags_an_empty_status_page() -> None:
    client = _client(_html_handler("<html></html>"))
    result = _run(conversation_ids(client, "1" * 19, "jack", SETTINGS))
    assert result.ids == []
    assert result.error == "no_ids_in_ssr"


# --------------------------------------------------------------------------
# syndication timeline (optional, IP-throttled)
# --------------------------------------------------------------------------


def test_syndication_timeline_degrades_on_429() -> None:
    client = _client(_html_handler("rate limited", 429))
    result = _run(syndication_timeline(client, "jack", SETTINGS))
    assert result.ids == []
    assert result.error == "HTTP 429 (rate limited; this endpoint is IP-throttled)"
    assert result.http_status == 429


def test_syndication_timeline_reads_next_data_entries() -> None:
    payload = {
        "props": {
            "pageProps": {
                "timeline": {
                    "entries": [
                        {"content": {"tweet": {"id_str": "1234567890123456789"}}},
                        {"content": {"tweet": {"id_str": "2222222222222222222"}}},
                        {"content": {}},
                        "junk",
                    ]
                }
            }
        }
    }
    html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
    result = _run(syndication_timeline(_client(_html_handler(html)), "jack", SETTINGS))
    assert result.ids == ["1234567890123456789", "2222222222222222222"]
    assert result.error is None


def test_syndication_timeline_survives_broken_next_data() -> None:
    html = '<script id="__NEXT_DATA__">{not json</script>'
    result = _run(syndication_timeline(_client(_html_handler(html)), "jack", SETTINGS))
    assert result.ids == []
    assert result.error == "no_ids_in_ssr"


# --------------------------------------------------------------------------
# user profiles
# --------------------------------------------------------------------------

FX_USER = {
    "code": 200,
    "message": "OK",
    "user": {
        "screen_name": "jack",
        "name": "jack",
        "description": "no state is the best state",
        "location": "",
        "website": {"url": "http://u.afp.com/socials", "display_url": "u.afp.com/socials"},
        "joined": "Tue Mar 21 20:50:14 +0000 2006",
        "followers": 12270635,
        "following": 3,
        "tweets": 30974,
        "likes": 40602,
        "media_count": 2974,
        "protected": False,
        "verification": {"verified": True, "type": "individual"},
        "avatar_url": "https://pbs.twimg.com/profile_images/1/a.jpg",
        "banner_url": "https://pbs.twimg.com/profile_banners/12/1742427520",
    },
}

VX_USER = {
    "created_at": "Tue Mar 21 20:50:14 +0000 2006",
    "description": "no state is the best state",
    "followers_count": 12161655,
    "following_count": 3,
    "id": 12,
    "location": "",
    "name": "jack",
    "profile_image_url": "https://pbs.twimg.com/profile_images/1/a.jpg",
    "protected": False,
    "screen_name": "jack",
    "tweet_count": 30966,
}


def _api_handler(responses: dict[str, tuple[int, object]]):
    def handler(request: httpx.Request) -> httpx.Response:
        for host, (status, body) in responses.items():
            if host in request.url.host:
                if isinstance(body, str):
                    return httpx.Response(status, text=body)
                return httpx.Response(status, json=body)
        return httpx.Response(404, text="unmapped")

    return handler


def test_user_profile_maps_the_fxtwitter_shape() -> None:
    client = _client(_api_handler({"fxtwitter": (200, FX_USER)}))
    profile, error = _run(user_profile(client, "jack", SETTINGS))
    assert error is None
    assert profile is not None
    assert (profile.screen_name, profile.followers, profile.following) == ("jack", 12270635, 3)
    assert (profile.tweets, profile.likes, profile.media_count) == (30974, 40602, 2974)
    assert profile.website == "http://u.afp.com/socials"  # dict, not a string, upstream
    assert profile.verified is True  # nested under "verification"
    assert profile.joined == "Tue Mar 21 20:50:14 +0000 2006"
    assert profile.location is None  # empty string is not a location


def test_user_profile_falls_through_to_vxtwitter() -> None:
    client = _client(
        _api_handler({"fxtwitter": (404, {"code": 404, "message": "User not found"}),
                      "vxtwitter": (200, VX_USER)})
    )
    profile, error = _run(user_profile(client, "jack", SETTINGS))
    assert error is None
    assert profile is not None
    assert (profile.screen_name, profile.followers, profile.tweets) == ("jack", 12161655, 30966)
    assert profile.joined == "Tue Mar 21 20:50:14 +0000 2006"
    assert profile.avatar_url == "https://pbs.twimg.com/profile_images/1/a.jpg"


def test_user_profile_reports_both_failures() -> None:
    client = _client(
        _api_handler({"fxtwitter": (404, {"code": 404, "message": "User not found"}),
                      "vxtwitter": (500, "upstream boom")})
    )
    profile, error = _run(user_profile(client, "ghost", SETTINGS))
    assert profile is None
    assert error is not None
    assert "fxtwitter: User not found" in error
    assert "vxtwitter" in error


def test_user_profile_survives_non_json_bodies() -> None:
    client = _client(_api_handler({"fxtwitter": (200, "<html>cloudflare</html>"),
                                   "vxtwitter": (200, "<html>cloudflare</html>")}))
    profile, error = _run(user_profile(client, "jack", SETTINGS))
    assert profile is None
    assert error is not None
