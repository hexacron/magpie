"""Offline tests for query-id discovery.

The failures that matter are the ones that would silently strand every
GraphQL call: only one of X's two minified key orderings being parsed, a
single dead bundle taking the whole scrape with it, the cache being ignored
(a page fetch on every call) or honoured forever (ids that never rotate),
discovery failure wiping a usable stale cache, and the override precedence
that lets a rotation be fixed from the environment.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from magpie import queryids, xapi
from magpie.config import Settings

MAIN_JS = "https://abs.twimg.com/responsive-web/client-web/main.9f3c1a2b.js"
OTHER_JS = "https://abs.twimg.com/responsive-web/client-web/bundle.Search.44d0.js"

# Both orderings, exactly as X's minifier emits them.
BUNDLE_BODY = (
    'e.exports={queryId:"jeAA-59Y9FL7FmjgBNIVPw",operationName:"UserTweets",'
    'operationType:"query"},'
    't.exports={operationName:"SearchTimeline",queryId:"auLkqtmHqYEpRvflfvLhyQ"},'
    'n.exports={queryId:"zoF7_t363wZyzylk-BLfZQ",operationName:"TweetDetail"}'
)
OTHER_BODY = 'r.exports={operationName:"Followers",queryId:"fVGYs5W9kNUuoUrZwYZQpQ"}'

LOGIN_HTML = (
    "<!DOCTYPE html><html><head>"
    f'<link rel="preload" href="{MAIN_JS}" as="script">'
    f'<script src="{OTHER_JS}" crossorigin></script>'
    "</head><body></body></html>"
)
# The root page escapes its slashes inside an inline JSON blob.
ROOT_HTML = '<script>window.__INITIAL_STATE__={"src":"%s"}</script>' % MAIN_JS.replace("/", "\\/")


def _settings(tmp_path, **kw) -> Settings:
    return Settings(data_dir=tmp_path, http_retries=0, http_timeout=1.0, **kw)


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _run(coro):
    return asyncio.run(coro)


def _handler(calls: list[str], *, bundles: dict[str, httpx.Response] | None = None):
    bodies = bundles if bundles is not None else {MAIN_JS: httpx.Response(200, text=BUNDLE_BODY)}

    def handle(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        calls.append(url)
        if url == "https://x.com/i/flow/login":
            return httpx.Response(200, text=LOGIN_HTML)
        if url == "https://x.com/":
            return httpx.Response(200, text=ROOT_HTML)
        if url in bodies:
            return bodies[url]
        return httpx.Response(404, text="")

    return handle


# --------------------------------------------------------------------------


def test_discover_walks_page_to_bundle_and_parses_both_orderings(tmp_path):
    calls: list[str] = []
    bundles = {
        MAIN_JS: httpx.Response(200, text=BUNDLE_BODY),
        OTHER_JS: httpx.Response(200, text=OTHER_BODY),
    }

    async def go():
        async with _client(_handler(calls, bundles=bundles)) as client:
            return await queryids.discover(client, _settings(tmp_path))

    ids, error = _run(go())

    assert error is None
    # queryId-first and operationName-first pairs both recovered.
    assert ids["UserTweets"] == "jeAA-59Y9FL7FmjgBNIVPw"
    assert ids["SearchTimeline"] == "auLkqtmHqYEpRvflfvLhyQ"
    assert ids["TweetDetail"] == "zoF7_t363wZyzylk-BLfZQ"
    assert ids["Followers"] == "fVGYs5W9kNUuoUrZwYZQpQ"
    # The escaped-slash reference on the root page is the same bundle, and is
    # fetched once, not twice.
    assert calls.count(MAIN_JS) == 1
    assert "https://x.com/i/flow/login" in calls and "https://x.com/" in calls


def test_discover_survives_a_dead_bundle(tmp_path):
    calls: list[str] = []
    bundles = {
        MAIN_JS: httpx.Response(500, text="upstream sad"),
        OTHER_JS: httpx.Response(200, text=OTHER_BODY),
    }

    async def go():
        async with _client(_handler(calls, bundles=bundles)) as client:
            return await queryids.discover(client, _settings(tmp_path))

    ids, error = _run(go())

    assert error is None
    assert ids == {"Followers": "fVGYs5W9kNUuoUrZwYZQpQ"}
    assert "UserTweets" not in ids


def test_discover_reports_instead_of_raising_when_every_page_dies(tmp_path):
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route to host", request=request)

    async def go():
        async with _client(handle) as client:
            return await queryids.discover(client, _settings(tmp_path))

    ids, error = _run(go())

    assert ids == {}
    assert error and "ConnectError" in error


def test_ensure_serves_a_fresh_cache_without_touching_the_network(tmp_path):
    settings = _settings(tmp_path)
    queryids.save_cache(settings, {"UserTweets": "cached-id"})
    calls: list[str] = []

    async def go():
        async with _client(_handler(calls)) as client:
            return await queryids.ensure(client, settings)

    assert _run(go()) == {"UserTweets": "cached-id"}
    assert calls == []


def test_ensure_rediscovers_once_the_cache_ages_past_the_ttl(tmp_path):
    settings = _settings(tmp_path)
    queryids.save_cache(settings, {"UserTweets": "cached-id"})
    settings.query_id_ttl = 0.0  # every cache is now stale
    calls: list[str] = []

    async def go():
        async with _client(_handler(calls)) as client:
            return await queryids.ensure(client, settings)

    ids = _run(go())

    assert ids["UserTweets"] == "jeAA-59Y9FL7FmjgBNIVPw"
    assert calls, "expected a re-discovery"
    # The refreshed table is what a later process will read.
    on_disk = json.loads(queryids.cache_path(settings).read_text("utf-8"))
    assert on_disk["ids"]["SearchTimeline"] == "auLkqtmHqYEpRvflfvLhyQ"
    assert on_disk["fetched_at_utc"]


def test_ensure_keeps_stale_ids_when_discovery_fails(tmp_path):
    settings = _settings(tmp_path)
    queryids.save_cache(settings, {"UserTweets": "stale-id", "TweetDetail": "stale-detail"})
    settings.query_id_ttl = 0.0

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="")

    async def go():
        async with _client(handle) as client:
            return await queryids.ensure(client, settings)

    ids = _run(go())

    assert ids == {"UserTweets": "stale-id", "TweetDetail": "stale-detail"}


def test_ensure_is_empty_only_when_there_is_no_cache_at_all(tmp_path):
    settings = _settings(tmp_path)

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="")

    async def go():
        async with _client(handle) as client:
            return await queryids.ensure(client, settings)

    assert _run(go()) == {}


def test_query_id_precedence_env_then_cache_then_xapi(tmp_path, monkeypatch):
    settings = _settings(tmp_path)
    monkeypatch.delenv("MAGPIE_QUERY_ID_USERTWEETS", raising=False)

    # 3. nothing anywhere -> the hardcoded table in xapi.
    assert queryids.query_id("UserTweets", settings, {}) == xapi.QUERY_IDS["UserTweets"][0]

    # 2. cache beats the hardcoded table.
    queryids.save_cache(settings, {"UserTweets": "from-cache"})
    assert queryids.query_id("UserTweets", settings) == "from-cache"
    assert queryids.query_id("UserTweets", settings, {"UserTweets": "passed-in"}) == "passed-in"

    # 1. env beats everything.
    monkeypatch.setenv("MAGPIE_QUERY_ID_USERTWEETS", "from-env")
    assert queryids.query_id("UserTweets", settings, {"UserTweets": "passed-in"}) == "from-env"

    # An operation nobody knows about stays None rather than guessing.
    assert queryids.query_id("NoSuchOperation", settings, {}) is None


def test_load_cache_ignores_a_corrupt_file(tmp_path):
    settings = _settings(tmp_path)
    queryids.cache_path(settings).parent.mkdir(parents=True, exist_ok=True)
    queryids.cache_path(settings).write_text("{not json", "utf-8")

    assert queryids.load_cache(settings) == {}
