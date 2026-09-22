"""Offline tests for input parsing, token derivation, merging and cross-checking.

Only behaviours where a plausible bug produces a wrong *evidence package* are
covered: the syndication token (an off-by-one digit means every syndication
fetch is made with a wrong token), host rejection (defect 7 — the original
turned a TikTok URL into a tweet id), truncation-aware text comparison
(defect 3 — a flag that fires on every note tweet), and text precedence during
the merge (the record must show the untruncated body).
"""

from __future__ import annotations

import asyncio
import hashlib
import json

import httpx

from magpie.config import BROWSER_UA, DEFAULT_SOURCE_UA, PLAIN_UA, Settings
from magpie.crosscheck import crosscheck
from magpie.ids import parse_inputs, syndication_token
from magpie.models import SOURCE_NAMES, ParsedSource, RawSource, TweetRef
from magpie.normalize import merge, parse_sources
from magpie.sources import fetch_sources

# Measured against X's own embed widget.
# Both observed in live captures made by the reference implementation, whose
# tokens X's widget would also produce. "20" is the discriminating case: an
# exact base-36 expansion yields 6dq1a2xwd91cz35wmb5wn4rku9q2hi3k instead.
TOKEN_VECTORS = {
    "2100949925420835118": "53cbu6vuajtn61blw61or",
    "20": "6dq1a2xwd93",
    "266031293945503744": "n7rfhxzwhkcjmgsetvwe9u",
    "855430621442822148": "22nex9iuvokezy5lwjxg8pv",
    "1234567890123456789": "2zqic77uqyke67ncb9wqxgv",
}

LONG_TEXT = (
    "The archive holds every public post it captured, hashed at the moment of "
    "capture, together with the raw bytes each endpoint returned so a reader "
    "can recompute the digests themselves instead of taking our word for it. "
    "That is the whole point: an evidence package nobody can independently "
    "verify is a screenshot with extra steps, and a screenshot is not evidence."
)


def _parsed(name: str, **kwargs: object) -> ParsedSource:
    source = ParsedSource(name=name, available=True, reason="available")
    for key, value in kwargs.items():
        setattr(source, key, value)
    return source


def _raw(name: str, ok: bool = True, error: str | None = None) -> RawSource:
    return RawSource(name=name, url=f"https://example.invalid/{name}", ok=ok, error=error)


# --------------------------------------------------------------------------
# token derivation
# --------------------------------------------------------------------------


def test_syndication_token_matches_measured_vectors() -> None:
    for tweet_id, expected in TOKEN_VECTORS.items():
        assert syndication_token(tweet_id) == expected


def test_syndication_token_has_no_radix_point_or_zeroes() -> None:
    token = syndication_token("1234567890123456789")
    assert "." not in token
    assert "0" not in token


# --------------------------------------------------------------------------
# input parsing (defect 7)
# --------------------------------------------------------------------------


def test_parse_inputs_rejects_non_x_host() -> None:
    (ref,) = parse_inputs("https://www.tiktok.com/@someone/video/7412345678901234567")
    assert ref.tweet_id is None
    assert ref.error == "unsupported_host: www.tiktok.com"


def test_parse_inputs_accepts_supported_forms() -> None:
    refs = parse_inputs(
        "\n".join(
            [
                "https://x.com/jack/status/20",
                "https://twitter.com/Interior/statuses/463440424141459456",
                "1234567890123456789",
                "https://x.com/i/status/1861234567890123456",
                "https://mobile.twitter.com/someone/status/1111111111111111111?s=20",
            ]
        )
    )
    assert [(r.tweet_id, r.screen_name, r.error) for r in refs] == [
        ("20", "jack", None),
        ("463440424141459456", "Interior", None),
        ("1234567890123456789", None, None),
        ("1861234567890123456", None, None),
        ("1111111111111111111", "someone", None),
    ]


def test_parse_inputs_dedupes_and_caps() -> None:
    # Same post three ways (bare id, x.com URL, twitter.com URL) collapses to one.
    refs = parse_inputs(
        "1861234567890123456, 1861234567890123456,"
        " https://x.com/jack/status/1861234567890123456,"
        " 1861234567890123457, 1861234567890123458",
        max_items=2,
    )
    assert [r.tweet_id for r in refs] == [
        "1861234567890123456",
        "1861234567890123457",
        "1861234567890123458",
    ]
    assert [r.error for r in refs] == [None, None, "too_many_inputs"]


def test_parse_inputs_flags_garbage() -> None:
    (ref,) = parse_inputs("gibberish-not-a-url")
    assert ref.error == "unrecognised_input"


def test_parse_inputs_rejects_short_bare_number() -> None:
    # A stray "42" in pasted text must not become a capture target.
    (ref,) = parse_inputs("42")
    assert ref.tweet_id is None
    assert ref.error == "unrecognised_input"


# --------------------------------------------------------------------------
# truncation-aware cross-check (defect 3)
# --------------------------------------------------------------------------


def test_truncated_syndication_text_is_not_a_discrepancy() -> None:
    truncated = LONG_TEXT[:280]
    parsed = {
        "syndication": _parsed("syndication", text=truncated, text_truncated=True),
        "fxtwitter": _parsed("fxtwitter", text=LONG_TEXT),
    }
    result = crosscheck(parsed, [_raw("syndication"), _raw("fxtwitter")])

    assert result.text["agree"] is True
    assert "source_truncated:syndication" in result.flags
    assert "text_differs_between_sources" not in result.flags
    assert result.text["truncated"] == ["syndication"]
    assert result.text["longest_source"] == "fxtwitter"
    assert set(result.text["values"]) == {"syndication", "fxtwitter"}


def test_ellipsis_and_tco_tail_do_not_break_prefix_detection() -> None:
    parsed = {
        "syndication": _parsed(
            "syndication", text=LONG_TEXT[:280] + "\u2026 https://t.co/abc123XY"
        ),
        "fxtwitter": _parsed("fxtwitter", text=LONG_TEXT),
    }
    result = crosscheck(parsed, [_raw("syndication"), _raw("fxtwitter")])
    assert result.text["agree"] is True
    assert "source_truncated:syndication" in result.flags


def test_genuinely_different_text_is_a_discrepancy() -> None:
    parsed = {
        "syndication": _parsed("syndication", text="the original wording of the post"),
        "fxtwitter": _parsed("fxtwitter", text="a completely different wording entirely"),
    }
    result = crosscheck(parsed, [_raw("syndication"), _raw("fxtwitter")])

    assert result.text["agree"] is False
    assert "text_differs_between_sources" in result.flags
    assert not result.text["truncated"]


def test_single_source_counts_are_not_reported_as_agreement() -> None:
    parsed = {
        "syndication": _parsed("syndication", text="hello", counts={"likes": 10}),
        "fxtwitter": _parsed(
            "fxtwitter", text="hello", counts={"likes": 11, "bookmarks": 3}
        ),
    }
    result = crosscheck(parsed, [_raw("syndication"), _raw("fxtwitter")])

    assert result.counts["bookmarks"]["agree"] is None
    assert result.counts["likes"]["agree"] is False
    assert "counts_differ:likes" in result.flags
    assert "counts_differ:bookmarks" not in result.flags


def test_deleted_post_is_unavailable_not_a_fetch_error() -> None:
    """A 404 means X says the post is gone; a 403 means our fetch was blocked.

    Collapsing both into "error" would let a tooling failure read as proof of
    deletion, which is exactly the claim an evidence package must not fudge.
    """
    parsed = {
        "syndication": ParsedSource(name="syndication", available=False, reason="error"),
        "vxtwitter": ParsedSource(name="vxtwitter", available=False, reason="error"),
    }
    raws = [
        _raw("syndication", ok=False, error="HTTP 404"),
        _raw("vxtwitter", ok=False, error="HTTP 403 (bot challenge — check MAGPIE_UA_VXTWITTER)"),
    ]
    raws[0].http_status = 404
    raws[1].http_status = 403
    result = crosscheck(parsed, raws)

    assert result.availability["syndication"] == "unavailable"
    assert result.availability["vxtwitter"] == "error"
    assert result.availability["fxtwitter"] == "skipped"
    assert result.availability["x_page"] == "skipped"
    assert "source_unavailable:syndication" in result.flags
    assert "source_error:vxtwitter" in result.flags
    assert "source_error:syndication" not in result.flags
    assert "post_unavailable_everywhere" in result.flags
    assert result.flags == sorted(result.flags)


def test_created_at_skew_within_two_seconds_agrees() -> None:
    parsed = {
        "syndication": _parsed(
            "syndication", text="hello", created_at_utc="2024-05-01T12:00:00Z"
        ),
        "fxtwitter": _parsed(
            "fxtwitter", text="hello", created_at_utc="2024-05-01T12:00:01Z"
        ),
        "vxtwitter": _parsed(
            "vxtwitter", text="hello", created_at_utc="2024-05-01T12:05:00Z"
        ),
    }
    two_agree = crosscheck(
        {k: v for k, v in parsed.items() if k != "vxtwitter"},
        [_raw("syndication"), _raw("fxtwitter")],
    )
    assert two_agree.fields["created_at_utc"]["agree"] is True
    assert "created_at_differs_between_sources" not in two_agree.flags

    all_three = crosscheck(parsed, [_raw(n) for n in parsed])
    assert all_three.fields["created_at_utc"]["agree"] is False
    assert "created_at_differs_between_sources" in all_three.flags


# --------------------------------------------------------------------------
# merge precedence
# --------------------------------------------------------------------------


def test_merge_prefers_untruncated_text_and_records_the_source() -> None:
    parsed = {
        "syndication": _parsed(
            "syndication",
            text=LONG_TEXT[:280],
            text_truncated=True,
            screen_name="archivist",
            counts={"likes": 10, "replies": 2},
        ),
        "fxtwitter": _parsed(
            "fxtwitter",
            text=LONG_TEXT,
            screen_name="archivist",
            counts={"likes": 10, "views": 900},
        ),
    }
    post = merge(TweetRef(input="20", tweet_id="20", screen_name="archivist"), parsed)

    assert post.text == LONG_TEXT
    assert post.text_source == "fxtwitter"
    assert post.counts == {"likes": 10, "replies": 2, "views": 900}
    assert post.count_sources["likes"] == "syndication"
    assert post.count_sources["views"] == "fxtwitter"
    assert post.count_agreement["likes"] is True
    assert post.count_agreement["views"] is None
    assert post.available_sources == ["syndication", "fxtwitter"]
    assert post.source_url == "https://x.com/archivist/status/20"


def test_merge_does_not_duplicate_a_link_the_longer_source_repeats() -> None:
    """Reuters' article link and its media entity share one t.co shortlink.

    fxtwitter's raw_text therefore ends with the same URL twice. Adopting the
    longer string stored "... sources say https://t.co/X https://t.co/X" for
    every such post, which was visible in the UI.
    """
    body = "Traders push for discounts on oil, sources say https://t.co/s9KHztYePQ"
    parsed = {
        "syndication": _parsed("syndication", text=body, screen_name="Reuters"),
        "fxtwitter": _parsed(
            "fxtwitter", text=f"{body} https://t.co/s9KHztYePQ", screen_name="Reuters"
        ),
    }
    post = merge(TweetRef(input="1", tweet_id="1", screen_name="Reuters"), parsed)

    assert post.text == body
    assert post.text.count("https://t.co/s9KHztYePQ") == 1


def test_merge_still_recovers_a_genuinely_dropped_tail() -> None:
    """The duplicate guard must not break the case it was added for."""
    short = "La portada del 22 de septiembre."
    full = f"{short} Noticia completa https://t.co/5GRFUfszbj"
    parsed = {
        "syndication": _parsed("syndication", text=short, screen_name="okdiario"),
        "fxtwitter": _parsed("fxtwitter", text=full, screen_name="okdiario"),
    }
    post = merge(TweetRef(input="2", tweet_id="2", screen_name="okdiario"), parsed)

    assert post.text == full


def test_merge_falls_back_to_truncated_text_when_that_is_all_there_is() -> None:
    parsed = {
        "syndication": _parsed(
            "syndication", text=LONG_TEXT[:280], text_truncated=True
        ),
    }
    post = merge(TweetRef(input="20", tweet_id="20"), parsed)

    assert post.text == LONG_TEXT[:280]
    assert post.text_source == "syndication"
    assert post.source_url == "https://x.com/i/status/20"


def test_merge_ignores_a_sources_false_claim_that_it_is_untruncated() -> None:
    """Measured over 12 consecutive live calls, api.fxtwitter.com returned the
    full 500-char note tweet 4 times and a silently truncated 279-char copy 8
    times — the short ones even carried ``is_note_tweet: false``.

    So a source claiming it is untruncated proves nothing. Being a strict
    prefix of another source's text does.
    """
    parsed = {
        # Highest text precedence, claims to be complete, but is a prefix.
        "fxtwitter": _parsed("fxtwitter", text=LONG_TEXT[:279], text_truncated=False),
        "vxtwitter": _parsed("vxtwitter", text=LONG_TEXT, text_truncated=False),
        "syndication": _parsed("syndication", text=LONG_TEXT[:279], text_truncated=True),
    }
    post = merge(TweetRef(input="20", tweet_id="20"), parsed)

    assert post.text == LONG_TEXT
    assert post.text_source == "vxtwitter"


def test_merge_keeps_source_precedence_when_texts_match() -> None:
    """Equal texts are not prefixes of each other, so precedence still wins."""
    parsed = {
        "fxtwitter": _parsed("fxtwitter", text=LONG_TEXT),
        "vxtwitter": _parsed("vxtwitter", text=LONG_TEXT),
    }
    post = merge(TweetRef(input="20", tweet_id="20"), parsed)

    assert post.text_source == "fxtwitter"


# --------------------------------------------------------------------------
# payload shapes measured against the live endpoints
# --------------------------------------------------------------------------


def _json_raw(name: str, payload: object) -> RawSource:
    return RawSource(
        name=name,
        url=f"https://example.invalid/{name}",
        ok=True,
        http_status=200,
        body=json.dumps(payload).encode(),
    )


def test_syndication_truncation_keys_on_note_tweet_not_text_length() -> None:
    """Live shapes: a 279-char *complete* post has no note_tweet, and a
    *truncated* note tweet came back at 268 chars with `note_tweet: {"id": ...}`
    and no text in it. Keying truncation on length misreads both.
    """
    complete = _json_raw(
        "syndication",
        {"text": "x" * 279 + " https://t.co/90hvFsx93x", "display_text_range": [0, 279]},
    )
    note = _json_raw(
        "syndication",
        {"text": "y" * 268, "display_text_range": [0, 268], "note_tweet": {"id": "1"}},
    )

    assert parse_sources([complete])["syndication"].text_truncated is False
    assert parse_sources([note])["syndication"].text_truncated is True


def test_syndication_drops_the_auto_appended_media_shortlink() -> None:
    parsed = parse_sources(
        [
            _json_raw(
                "syndication",
                {"text": "look at this https://t.co/90hvFsx93x", "display_text_range": [0, 12]},
            )
        ]
    )["syndication"]
    assert parsed.text == "look at this"


def test_fxtwitter_keeps_authored_links_but_drops_the_media_shortlink() -> None:
    """`raw_text` carries X's auto-appended media t.co; `text` does not.

    A link the author actually typed appears in both, so it must survive.
    """
    appended = parse_sources(
        [
            _json_raw(
                "fxtwitter",
                {
                    "code": 200,
                    "tweet": {
                        "text": "story here @Tagesspiegel",
                        "raw_text": {"text": "story here @Tagesspiegel https://t.co/90hvFsx93x"},
                    },
                },
            )
        ]
    )["fxtwitter"]
    assert appended.text == "story here @Tagesspiegel"

    authored = parse_sources(
        [
            _json_raw(
                "fxtwitter",
                {
                    "code": 200,
                    "tweet": {
                        "text": "read this https://t.co/abc123XY",
                        "raw_text": {"text": "read this https://t.co/abc123XY"},
                    },
                },
            )
        ]
    )["fxtwitter"]
    assert authored.text == "read this https://t.co/abc123XY"


def test_x_page_html_participates_in_the_crosscheck() -> None:
    """Defect 5: the original saved x_page.html and never parsed it."""
    html = (
        "<!DOCTYPE html><html><head>"
        '<meta property="og:title" content="The Archivist (@archivist) on X">'
        '<meta property="og:description" content="&#8220;' + LONG_TEXT[:150] + "&#8230;&#8221;\">"
        '<link rel="canonical" href="https://x.com/archivist/status/20">'
        "</head><body></body></html>"
    )
    parsed = parse_sources(
        [RawSource(name="x_page", url="u", ok=True, http_status=200, body=html.encode())]
    )["x_page"]

    assert parsed.available is True
    assert parsed.screen_name == "archivist"
    assert parsed.name_display == "The Archivist"
    # The served ellipsis is kept verbatim -- truncation is reported by the
    # flag, never by silently editing what the server sent.
    assert parsed.text == LONG_TEXT[:150] + "\u2026"
    assert parsed.text_truncated is True

    # ...and that parse actually reaches the cross-check as a truncated source.
    result = crosscheck(
        {"x_page": parsed, "fxtwitter": _parsed("fxtwitter", text=LONG_TEXT)},
        [_raw("x_page"), _raw("fxtwitter")],
    )
    assert result.text["agree"] is True
    assert "source_truncated:x_page" in result.flags


def test_a_broken_payload_degrades_only_its_own_source() -> None:
    raws = [
        RawSource(name="syndication", url="u", ok=True, http_status=200, body=b"<<<not json>>>"),
        _json_raw("fxtwitter", {"code": 200, "tweet": {"text": "still here", "likes": 3}}),
    ]
    parsed = parse_sources(raws)

    assert parsed["syndication"].available is False
    assert parsed["syndication"].reason == "error"
    assert parsed["fxtwitter"].available is True
    assert merge(TweetRef(input="20", tweet_id="20"), parsed).text == "still here"


# --------------------------------------------------------------------------
# fetch layer (offline, via httpx MockTransport)
# --------------------------------------------------------------------------


def _settings(*sources: str) -> Settings:
    """Settings pinned to the defaults under test, ignoring ambient MAGPIE_* env."""
    settings = Settings()
    settings.sources = list(sources) or list(SOURCE_NAMES)
    settings.user_agents = dict(DEFAULT_SOURCE_UA)
    settings.http_retries = 2
    return settings


def _fetch(handler, settings: Settings, ref: TweetRef) -> dict[str, RawSource]:
    async def run() -> list[RawSource]:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await fetch_sources(client, ref, settings)

    return {raw.name: raw for raw in asyncio.run(run())}


def test_a_dead_post_reads_as_gone_from_every_source_not_as_a_tool_failure() -> None:
    """Measured live: for a deleted post syndication/x.com answer 404,
    fxtwitter answers ``NOT_FOUND`` and vxtwitter answers **200 with an HTML
    page** reading "Failed to scan your link!". Reporting that last one as a
    parse error would claim our tooling broke when in fact X says the post
    is gone — the opposite evidentiary claim.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "vxtwitter" in url:
            return httpx.Response(
                200, text="<!DOCTYPE html><html><body>Failed to scan your link!</body></html>"
            )
        if "fxtwitter" in url:
            return httpx.Response(404, json={"code": 404, "message": "NOT_FOUND"})
        return httpx.Response(404, text="not found")

    raws = _fetch(handler, _settings(), TweetRef(input="x", tweet_id="1000000000000000001"))

    assert raws["vxtwitter"].ok is False
    assert "not found" in (raws["vxtwitter"].error or "")
    assert raws["fxtwitter"].error == "NOT_FOUND"

    result = crosscheck(parse_sources(list(raws.values())), list(raws.values()))
    assert set(result.availability.values()) == {"unavailable"}
    assert "post_unavailable_everywhere" in result.flags
    assert not [f for f in result.flags if f.startswith("source_error:")]


def test_vxtwitter_defaults_to_the_plain_ua_and_403_names_the_knob() -> None:
    """Defect 4: vxtwitter 403s a browser UA, so the plain client UA is the
    default, and a 403 must name the override knob instead of blaming
    "Cloudflare". A 403 is also a 4xx: exactly one request, no retries.
    """
    assert DEFAULT_SOURCE_UA["vxtwitter"] == PLAIN_UA
    assert DEFAULT_SOURCE_UA["x_page"] == BROWSER_UA

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["user-agent"])
        return httpx.Response(403, text="blocked")

    raws = _fetch(
        handler, _settings("vxtwitter"), TweetRef(input="x", tweet_id="1861234567890123456")
    )

    assert seen == [PLAIN_UA]
    assert "MAGPIE_UA_VXTWITTER" in (raws["vxtwitter"].error or "")


def test_syndication_empty_object_is_not_a_successful_capture() -> None:
    """The endpoint answers ``{}`` when the token is missing; 200 is not enough."""
    settings = _settings("syndication")
    raws = _fetch(
        lambda request: httpx.Response(200, json={}),
        settings,
        TweetRef(input="x", tweet_id="1861234567890123456"),
    )

    assert raws["syndication"].ok is False
    assert raws["syndication"].error == "empty_response"
    # Still saved: the empty body is itself evidence of what the endpoint said.
    assert raws["syndication"].saved_file == "syndication.json"


def test_transport_evidence_is_recorded_and_the_body_is_never_serialised() -> None:
    """Defect 2: the original stored no request/response headers at all."""
    settings = _settings("fxtwitter")
    payload = {"code": 200, "tweet": {"text": "hello"}}
    raws = _fetch(
        lambda request: httpx.Response(200, json=payload, headers={"ETag": 'W/"abc"'}),
        settings,
        TweetRef(input="x", tweet_id="1861234567890123456"),
    )
    raw = raws["fxtwitter"]

    assert raw.headers["etag"] == 'W/"abc"'
    assert raw.request_headers["user-agent"] == PLAIN_UA
    assert raw.body_sha256 == hashlib.sha256(raw.body or b"").hexdigest()
    assert raw.bytes == len(raw.body or b"")
    assert "body" not in raw.to_dict()


def test_retries_5xx_but_never_4xx() -> None:
    settings = _settings("fxtwitter")
    settings.http_retries = 3

    calls = {"n": 0}

    def flaky(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, text="try later")
        return httpx.Response(200, json={"code": 200, "tweet": {"text": "hello"}})

    assert _fetch(flaky, settings, TweetRef(input="x", tweet_id="1861234567890123456"))[
        "fxtwitter"
    ].ok
    assert calls["n"] == 3

    calls["n"] = 0

    def gone(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, json={"code": 404, "message": "NOT_FOUND"})

    assert not _fetch(gone, settings, TweetRef(input="x", tweet_id="1861234567890123456"))[
        "fxtwitter"
    ].ok
    assert calls["n"] == 1


def test_unparseable_input_fetches_nothing() -> None:
    def explode(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        raise AssertionError("a rejected input must never hit the network")

    raws = _fetch(
        explode, _settings(), TweetRef(input="tiktok", error="unsupported_host: www.tiktok.com")
    )
    assert [r.ok for r in raws.values()] == [False, False, False, False]
    assert {r.error for r in raws.values()} == {"unsupported_host: www.tiktok.com"}
