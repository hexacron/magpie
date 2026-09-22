"""Offline tests for the guest-token GraphQL collector.

Only the failures that would silently break a crawl are covered: guest-token
stampedes (N coroutines, N activations, instant rate-limit), the query-id
rotation X performs without notice (404 + empty body), the token refresh on a
403, the 422 that names a new feature flag, the four timeline entry shapes,
and the mapping details that quietly lose data -- long note_tweet text, the
highest-bitrate mp4, the string-typed view count, and a retweet whose real
content only exists in the nested original.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from magpie import xapi
from magpie.config import Settings
from magpie.xapi import (
    GuestSession,
    parse_timeline,
    tweet_detail,
    user_tweets,
)

SETTINGS = Settings(http_retries=0, http_timeout=1.0)

USER_TWEETS_IDS = xapi.QUERY_IDS["UserTweets"]


def _reset() -> None:
    xapi._WORKING_QID.clear()
    xapi._DEAD_QIDS.clear()
    xapi._USER_ID_CACHE.clear()


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _run(coro):
    return asyncio.run(coro)


def _activate(token: str = "guest-1") -> httpx.Response:
    return httpx.Response(200, json={"guest_token": token})


# --------------------------------------------------------------------------
# fixtures: the payload shapes X actually serves
# --------------------------------------------------------------------------

LONG_TEXT = "A note tweet body that runs well past the 280 character cut. " * 6


def _user_results(screen_name: str = "jack", name: str = "jack") -> dict:
    return {
        "result": {
            "__typename": "User",
            "rest_id": "12",
            "is_blue_verified": True,
            "legacy": {
                "screen_name": screen_name,
                "name": name,
                "description": "bio",
                "followers_count": 6_500_000,
                "friends_count": 4_200,
                "statuses_count": 28_000,
                "profile_image_url_https": "https://pbs.twimg.com/profile/jack.jpg",
                "profile_banner_url": "https://pbs.twimg.com/banner/jack",
            },
        }
    }


def _tweet(tweet_id: str, full_text: str = "hello world", **legacy_extra) -> dict:
    legacy = {
        "id_str": tweet_id,
        "full_text": full_text,
        "created_at": "Wed Sep 11 19:31:57 +0000 2024",
        "lang": "en",
        "favorite_count": 10,
        "retweet_count": 2,
        "reply_count": 1,
        "quote_count": 0,
        "bookmark_count": 3,
    }
    legacy.update(legacy_extra)
    return {
        "__typename": "Tweet",
        "rest_id": tweet_id,
        "core": {"user_results": _user_results()},
        "views": {"count": "123456", "state": "EnabledWithCount"},
        "legacy": legacy,
    }


def _tombstone() -> dict:
    return {
        "__typename": "TweetTombstone",
        "tombstone": {"text": {"text": "This Post is unavailable."}},
    }


def _item_entry(entry_id: str, result: dict) -> dict:
    return {
        "entryId": entry_id,
        "content": {
            "entryType": "TimelineTimelineItem",
            "__typename": "TimelineTimelineItem",
            "itemContent": {
                "itemType": "TimelineTweet",
                "__typename": "TimelineTweet",
                "tweet_results": {"result": result},
            },
        },
    }


def _module_entry(entry_id: str, results: list[dict]) -> dict:
    return {
        "entryId": entry_id,
        "content": {
            "entryType": "TimelineTimelineModule",
            "__typename": "TimelineTimelineModule",
            "items": [
                {
                    "entryId": f"{entry_id}-tweet-{index}",
                    "item": {
                        "itemContent": {
                            "itemType": "TimelineTweet",
                            "tweet_results": {"result": result},
                        }
                    },
                }
                for index, result in enumerate(results)
            ],
        },
    }


def _cursor_entry(value: str = "DAABCgABGc_cursor") -> dict:
    return {
        "entryId": "cursor-bottom-0",
        "content": {
            "entryType": "TimelineTimelineCursor",
            "__typename": "TimelineTimelineCursor",
            "value": value,
            "cursorType": "Bottom",
        },
    }


def _timeline(entries: list[dict]) -> dict:
    return {
        "data": {
            "user": {
                "result": {
                    "__typename": "User",
                    "timeline_v2": {
                        "timeline": {
                            "instructions": [
                                {"type": "TimelineAddEntries", "entries": entries}
                            ]
                        }
                    },
                }
            }
        }
    }


# --------------------------------------------------------------------------
# guest token
# --------------------------------------------------------------------------


def test_guest_token_activates_once_for_concurrent_callers_and_again_on_force() -> None:
    _reset()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        return _activate(f"guest-{len(calls)}")

    async def scenario() -> tuple[list[str | None], str | None]:
        async with _client(handler) as client:
            session = GuestSession(SETTINGS)
            tokens = await asyncio.gather(*(session.token(client) for _ in range(6)))
            forced = await session.token(client, force=True)
            return list(tokens), forced

    tokens, forced = _run(scenario())

    activations = [path for path in calls if path.endswith("/guest/activate.json")]
    assert len(activations) == 2, calls
    assert tokens == ["guest-1"] * 6
    assert forced == "guest-2"


def test_guest_token_activation_failure_is_an_error_string_not_an_exception() -> None:
    _reset()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream down")

    async def scenario():
        async with _client(handler) as client:
            session = GuestSession(SETTINGS)
            token = await session.token(client)
            status, payload, error = await session.graphql(client, "UserTweets", {})
            return token, session.error, status, payload, error

    token, session_error, status, payload, error = _run(scenario())
    assert token is None
    assert "503" in (session_error or "")
    assert payload is None
    assert error is not None


# --------------------------------------------------------------------------
# query id rotation
# --------------------------------------------------------------------------


def test_graphql_skips_a_dead_query_id_and_remembers_the_winner() -> None:
    _reset()
    dead, alive = USER_TWEETS_IDS[0], USER_TWEETS_IDS[1]
    graphql_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/guest/activate.json"):
            return _activate()
        graphql_calls.append(path)
        if f"/graphql/{dead}/" in path:
            # The rotation signature: 404 with a completely empty body.
            return httpx.Response(404, text="")
        return httpx.Response(200, json=_timeline([_item_entry("tweet-1", _tweet("1"))]))

    async def scenario():
        async with _client(handler) as client:
            session = GuestSession(SETTINGS)
            first = await session.graphql(client, "UserTweets", {})
            graphql_calls.clear()
            second = await session.graphql(client, "UserTweets", {})
            return first, second, list(graphql_calls)

    (status1, payload1, error1), (status2, payload2, error2), second_calls = _run(scenario())

    assert (status1, error1) == (200, None)
    assert payload1 is not None
    assert (status2, error2) == (200, None)
    assert payload2 is not None
    # The dead id is never asked again.
    assert len(second_calls) == 1
    assert f"/graphql/{alive}/UserTweets" in second_calls[0]
    assert xapi._WORKING_QID["UserTweets"] == alive


def test_graphql_reports_exhausted_query_ids_with_the_rotation_hint() -> None:
    _reset()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            return _activate()
        return httpx.Response(404, text="")

    async def scenario():
        async with _client(handler) as client:
            return await GuestSession(SETTINGS).graphql(client, "UserTweets", {})

    status, payload, error = _run(scenario())
    assert status == 404
    assert payload is None
    assert "no working query id for UserTweets" in (error or "")
    assert "update QUERY_IDS" in (error or "")


# --------------------------------------------------------------------------
# auth refresh and validation
# --------------------------------------------------------------------------


def test_graphql_refreshes_the_guest_token_once_on_403_and_retries() -> None:
    _reset()
    activations: list[int] = []
    seen_tokens: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            activations.append(1)
            return _activate(f"guest-{len(activations)}")
        seen_tokens.append(request.headers.get("x-guest-token"))
        if len(seen_tokens) == 1:
            return httpx.Response(403, json={"errors": [{"code": 200, "message": "Forbidden"}]})
        return httpx.Response(200, json=_timeline([_item_entry("tweet-1", _tweet("1"))]))

    async def scenario():
        async with _client(handler) as client:
            session = GuestSession(SETTINGS)
            result = await session.graphql(client, "UserTweets", {})
            return result, session.activations

    (status, payload, error), session_activations = _run(scenario())

    assert (status, error) == (200, None)
    assert payload is not None
    assert len(activations) == 2 and session_activations == 2
    assert seen_tokens == ["guest-1", "guest-2"]


def test_graphql_surfaces_the_422_missing_feature_flag_message() -> None:
    _reset()
    message = "The following features cannot be null: rweb_video_screen_enabled"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            return _activate()
        return httpx.Response(
            422,
            json={"errors": [{"message": message, "name": "GRAPHQL_VALIDATION_FAILED"}]},
        )

    async def scenario():
        async with _client(handler) as client:
            return await GuestSession(SETTINGS).graphql(client, "UserTweets", {})

    status, payload, error = _run(scenario())
    assert status == 422
    assert payload is None
    assert error == message
    assert "rweb_video_screen_enabled" in (error or "")


def test_graphql_sends_the_bearer_guest_token_and_encoded_variables() -> None:
    _reset()
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            return _activate("guest-abc")
        captured["auth"] = request.headers.get("authorization")
        captured["token"] = request.headers.get("x-guest-token")
        captured["ua"] = request.headers.get("user-agent")
        captured["variables"] = json.loads(request.url.params["variables"])
        captured["features"] = json.loads(request.url.params["features"])
        return httpx.Response(200, json=_timeline([_item_entry("tweet-1", _tweet("1"))]))

    async def scenario():
        async with _client(handler) as client:
            await GuestSession(SETTINGS).graphql(client, "UserTweets", {"userId": "12"})

    _run(scenario())

    assert captured["auth"] == f"Bearer {xapi.WEB_BEARER}"
    assert captured["token"] == "guest-abc"
    assert captured["ua"] == SETTINGS.ua("x_page")
    assert captured["variables"] == {"userId": "12"}
    assert captured["features"]["view_counts_everywhere_api_enabled"] is True


# --------------------------------------------------------------------------
# timeline walking
# --------------------------------------------------------------------------


def test_parse_timeline_handles_items_modules_tombstones_and_the_bottom_cursor() -> None:
    payload = _timeline(
        [
            _item_entry("tweet-1", _tweet("1")),
            _module_entry("conversation-9", [_tweet("2"), _tweet("3")]),
            _item_entry("tweet-4", _tombstone()),
            _cursor_entry("DAABCgABGc_cursor"),
        ]
    )
    posts, cursor = parse_timeline(payload)

    assert [post.id for post in posts] == ["1", "2", "3"]
    assert cursor == "DAABCgABGc_cursor"


def test_parse_timeline_returns_no_cursor_when_the_payload_carries_none() -> None:
    # The measured live case: a guest UserTweets response had zero cursors.
    posts, cursor = parse_timeline(_timeline([_item_entry("tweet-1", _tweet("1"))]))
    assert len(posts) == 1
    assert cursor is None


def test_parse_timeline_unwraps_visibility_results_and_add_to_module() -> None:
    payload = {
        "data": {
            "threaded_conversation_with_injections_v2": {
                "instructions": [
                    {
                        "type": "TimelineAddToModule",
                        "moduleItems": [
                            {
                                "entryId": "conversation-1-tweet-0",
                                "item": {
                                    "itemContent": {
                                        "itemType": "TimelineTweet",
                                        "tweet_results": {
                                            "result": {
                                                "__typename": "TweetWithVisibilityResults",
                                                "tweet": _tweet("77"),
                                            }
                                        },
                                    }
                                },
                            }
                        ],
                    }
                ]
            }
        }
    }
    posts, cursor = parse_timeline(payload)
    assert [post.id for post in posts] == ["77"]
    assert cursor is None


def test_parse_timeline_never_raises_on_garbage() -> None:
    for payload in ({}, {"data": None}, {"data": {"user": []}}, {"instructions": "nope"}):
        assert parse_timeline(payload) == ([], None)


# --------------------------------------------------------------------------
# mapping fidelity
# --------------------------------------------------------------------------


def _rich_tweet() -> dict:
    truncated = "A note tweet body that runs well past the 280 https://t.co/abcd1234"
    result = _tweet(
        "555",
        truncated,
        display_text_range=[0, 45],
        in_reply_to_status_id_str="444",
        in_reply_to_screen_name="someone",
        extended_entities={
            "media": [
                {
                    "type": "video",
                    "media_url_https": "https://pbs.twimg.com/thumb.jpg",
                    "ext_alt_text": "a clip",
                    "original_info": {"width": 1280, "height": 720},
                    "video_info": {
                        "duration_millis": 30_000,
                        "variants": [
                            {
                                "bitrate": 256_000,
                                "content_type": "video/mp4",
                                "url": "https://video.x.com/low.mp4",
                            },
                            {
                                "content_type": "application/x-mpegURL",
                                "url": "https://video.x.com/playlist.m3u8",
                            },
                            {
                                "bitrate": 2_176_000,
                                "content_type": "video/mp4",
                                "url": "https://video.x.com/high.mp4",
                            },
                            {
                                "bitrate": 832_000,
                                "content_type": "video/mp4",
                                "url": "https://video.x.com/mid.mp4",
                            },
                        ],
                    },
                }
            ]
        },
    )
    result["note_tweet"] = {"note_tweet_results": {"result": {"text": LONG_TEXT}}}
    return result


def test_mapping_keeps_note_text_best_mp4_int_views_and_reply_parent() -> None:
    posts, _cursor = parse_timeline(_timeline([_item_entry("tweet-555", _rich_tweet())]))
    assert len(posts) == 1
    post = posts[0]

    assert post.text == LONG_TEXT.strip()
    assert post.counts["views"] == 123456 and isinstance(post.counts["views"], int)
    assert post.counts["likes"] == 10
    assert post.counts["bookmarks"] == 3
    assert post.reply_to_id == "444"
    assert post.reply_to_screen_name == "someone"

    assert len(post.media) == 1
    media = post.media[0]
    assert media.type == "video"
    assert media.best_url == "https://video.x.com/high.mp4"
    assert media.thumb_url == "https://pbs.twimg.com/thumb.jpg"
    assert media.alt == "a clip"
    assert media.duration_s == 30.0

    assert post.created_at_utc == "2024-09-11T19:31:57Z"
    assert post.screen_name == "jack"
    assert post.source_url == "https://x.com/jack/status/555"
    assert post.available_sources == ["xapi_guest"]
    assert post.text_source == "xapi_guest"
    assert post.media_source == "xapi_guest"
    assert post.count_sources["views"] == "xapi_guest"
    assert post.profile is not None and post.profile.followers == 6_500_000
    assert post.to_dict()["id"] == "555"


def test_mapping_trims_the_trailing_media_tco_when_there_is_no_note_tweet() -> None:
    body = "look at this"
    result = _tweet("9", f"{body} https://t.co/abcd1234", display_text_range=[0, len(body)])
    posts, _cursor = parse_timeline(_timeline([_item_entry("tweet-9", result)]))
    assert posts[0].text == body


def test_retweet_yields_both_the_retweeting_post_and_the_original() -> None:
    original = _tweet("100", "the original full body")
    original["core"] = {"user_results": _user_results("origauthor", "Orig Author")}
    retweet = _tweet("200", "RT @origauthor: the original full bo\u2026")
    retweet["legacy"]["retweeted_status_result"] = {"result": original}

    posts, _cursor = parse_timeline(_timeline([_item_entry("tweet-200", retweet)]))

    assert [post.id for post in posts] == ["200", "100"]
    assert posts[0].screen_name == "jack"
    assert posts[1].screen_name == "origauthor"
    assert posts[1].text == "the original full body"
    assert posts[1].source_url == "https://x.com/origauthor/status/100"


def test_mapping_survives_a_result_with_no_legacy_object() -> None:
    posts, cursor = parse_timeline(
        _timeline([_item_entry("tweet-1", {"__typename": "Tweet"})])
    )
    assert posts == [] and cursor is None


# --------------------------------------------------------------------------
# operations
# --------------------------------------------------------------------------


def test_user_tweets_resolves_a_handle_then_caches_the_rest_id() -> None:
    _reset()
    operations: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/guest/activate.json"):
            return _activate()
        operation = path.rsplit("/", 1)[-1]
        operations.append(operation)
        if operation == "UserByScreenName":
            return httpx.Response(
                200,
                json={
                    "data": {
                        "user": {
                            "result": {
                                "__typename": "User",
                                "rest_id": "12",
                                "legacy": {"screen_name": "jack", "name": "jack"},
                            }
                        }
                    }
                },
            )
        variables = json.loads(request.url.params["variables"])
        assert variables["userId"] == "12"
        assert variables["count"] == 40
        return httpx.Response(
            200,
            json=_timeline([_item_entry("tweet-1", _tweet("1")), _cursor_entry("NEXT")]),
        )

    async def scenario():
        async with _client(handler) as client:
            session = GuestSession(SETTINGS)
            first = await user_tweets(client, session, handle="@jack", settings=SETTINGS)
            second = await user_tweets(client, session, handle="jack", settings=SETTINGS)
            return first, second

    (posts1, cursor1, error1), (posts2, cursor2, error2) = _run(scenario())

    assert error1 is None and error2 is None
    assert [p.id for p in posts1] == ["1"] and [p.id for p in posts2] == ["1"]
    assert cursor1 == "NEXT" and cursor2 == "NEXT"
    # Resolved once; the second call goes straight to the timeline.
    assert operations.count("UserByScreenName") == 1
    assert operations.count("UserTweets") == 2


def test_user_tweets_reports_an_empty_payload_as_the_canary_not_as_success() -> None:
    _reset()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            return _activate()
        return httpx.Response(200, json={"data": {"user": {"result": {}}}})

    async def scenario():
        async with _client(handler) as client:
            return await user_tweets(
                client, GuestSession(SETTINGS), user_id="12", settings=SETTINGS
            )

    posts, cursor, error = _run(scenario())
    assert posts == []
    assert cursor is None
    assert error == "no posts in timeline payload"


def test_tweet_detail_parses_the_threaded_conversation() -> None:
    _reset()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/guest/activate.json"):
            return _activate()
        assert json.loads(request.url.params["variables"])["focalTweetId"] == "555"
        return httpx.Response(
            200,
            json={
                "data": {
                    "threaded_conversation_with_injections_v2": {
                        "instructions": [
                            {
                                "type": "TimelineAddEntries",
                                "entries": [
                                    _item_entry("tweet-555", _tweet("555")),
                                    _module_entry(
                                        "conversationthread-1",
                                        [_tweet("556"), _tweet("557")],
                                    ),
                                ],
                            }
                        ]
                    }
                }
            },
        )

    async def scenario():
        async with _client(handler) as client:
            return await tweet_detail(client, GuestSession(SETTINGS), "555", SETTINGS)

    posts, error = _run(scenario())
    assert error is None
    assert [post.id for post in posts] == ["555", "556", "557"]
