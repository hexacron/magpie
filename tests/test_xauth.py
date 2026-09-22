"""Offline tests for the account-cookie collector and its credential store.

The live path cannot be tested (no credentials, and using one would suspend
it), so these carry the weight instead. They cover exactly the failures that
would otherwise be discovered in production: a cookie leaking into a repr or a
log line, a 429 burning the whole run instead of rotating, a suspended account
being retried forever, and an empty 404 being mistaken for a stale query id
and rotating a *working* one away.
"""

from __future__ import annotations

import asyncio
import json
import os
import time

import httpx

from magpie import accounts as accounts_mod
from magpie import xapi
from magpie.accounts import AccountStore
from magpie.config import Settings
from magpie.xauth import AuthSession

TOKEN_A = "authtokenaaaaaaaaaaaaaaaaaaaaaaaa1111"
CT0_A = "ct0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2222"
TOKEN_B = "authtokenbbbbbbbbbbbbbbbbbbbbbbbb3333"
CT0_B = "ct0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb4444"

ALL_SECRETS = (TOKEN_A, CT0_A, TOKEN_B, CT0_B)

OK_PAYLOAD = {"data": {"user": {"result": {"__typename": "User"}}}}


def _settings(tmp_path) -> Settings:
    return Settings(
        http_retries=0,
        http_timeout=1.0,
        data_dir=tmp_path,
        accounts_file=str(tmp_path / "accounts.json"),
    )


def _store(tmp_path, *, two: bool = False) -> AccountStore:
    store = AccountStore(_settings(tmp_path))
    store.add("acct-1", TOKEN_A, CT0_A)
    if two:
        store.add("acct-2", TOKEN_B, CT0_B)
    return store


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _run(coro):
    return asyncio.run(coro)


async def _call(session: AuthSession, handler, operation: str = "UserTweets"):
    async with _client(handler) as client:
        return await session.graphql(client, operation, {"userId": "12"})


def _state(store: AccountStore, label: str) -> str:
    account = store.get(label)
    assert account is not None
    return account.state


# --------------------------------------------------------------------------
# 1. the header set X actually checks
# --------------------------------------------------------------------------


def test_request_carries_cookie_csrf_pair_and_web_bearer(tmp_path):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=OK_PAYLOAD)

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)
    assert session.available is True

    status, payload, error = _run(_call(session, handler))

    assert (status, error) == (200, None)
    assert payload == OK_PAYLOAD
    assert len(seen) == 1
    headers = seen[0].headers
    # ct0 has to appear as both a cookie and the CSRF header: X compares them.
    assert headers["cookie"] == f"auth_token={TOKEN_A}; ct0={CT0_A}"
    assert headers["x-csrf-token"] == CT0_A
    assert headers["authorization"] == f"Bearer {xapi.WEB_BEARER}"
    assert headers["x-twitter-auth-type"] == "OAuth2Session"
    assert session.last_account == "acct-1"


# --------------------------------------------------------------------------
# 2. a rate-limited account cools and the call rotates
# --------------------------------------------------------------------------


def test_rate_limit_cools_first_account_and_succeeds_on_second(tmp_path):
    reset = int(time.time()) + 300
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cookie = request.headers["cookie"]
        calls.append(cookie)
        if TOKEN_A in cookie:
            return httpx.Response(
                429,
                headers={"x-rate-limit-remaining": "0", "x-rate-limit-reset": str(reset)},
                json={"errors": [{"message": "Rate limit exceeded", "code": 88}]},
            )
        return httpx.Response(200, json=OK_PAYLOAD)

    store = _store(tmp_path, two=True)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert (status, error) == (200, None)
    assert payload == OK_PAYLOAD
    assert len(calls) == 2
    assert _state(store, "acct-1") == "cooling"
    assert _state(store, "acct-2") == "active"
    cooling = store.get("acct-1")
    assert cooling is not None and cooling.cooldown_until_utc is not None
    assert session.last_account == "acct-2"


def test_rate_limit_with_no_account_left_reports_cooling_window(tmp_path):
    reset = int(time.time()) + 120

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"x-rate-limit-reset": str(reset)},
            json={"errors": [{"message": "Rate limit exceeded", "code": 88}]},
        )

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert payload is None
    assert status == 429
    assert error is not None
    assert "rate limited" in error and "acct-1" in error and "cooling until" in error
    assert _state(store, "acct-1") == "cooling"
    assert not any(secret in error for secret in ALL_SECRETS)


# --------------------------------------------------------------------------
# 3. a suspended account is retired, not retried forever
# --------------------------------------------------------------------------


def test_suspended_account_is_marked_dead_and_call_rotates(tmp_path):
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cookie = request.headers["cookie"]
        calls.append(cookie)
        if TOKEN_A in cookie:
            return httpx.Response(
                403, json={"errors": [{"message": "Your account is suspended", "code": 64}]}
            )
        return httpx.Response(200, json=OK_PAYLOAD)

    store = _store(tmp_path, two=True)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert (status, error) == (200, None)
    assert payload == OK_PAYLOAD
    assert len(calls) == 2
    assert _state(store, "acct-1") == "dead"
    dead = store.get("acct-1")
    assert dead is not None and "account_suspended" in (dead.note or "")
    assert not any(secret in (dead.note or "") for secret in ALL_SECRETS)


def test_suspended_everywhere_reports_the_named_condition_without_the_token(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403, json={"errors": [{"message": "Your account is suspended", "code": 64}]}
        )

    store = _store(tmp_path, two=True)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert payload is None
    assert error is not None and "account_suspended" in error
    assert not any(secret in error for secret in ALL_SECRETS)
    assert _state(store, "acct-1") == "dead"
    assert _state(store, "acct-2") == "dead"
    assert session.available is False


def test_401_without_codes_is_bad_credentials(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="")

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)

    _status, payload, error = _run(_call(session, handler))

    assert payload is None
    assert error is not None and "bad_credentials" in error
    assert _state(store, "acct-1") == "dead"


# --------------------------------------------------------------------------
# 4. an empty 404 is a capability answer, not a stale query id
# --------------------------------------------------------------------------


def test_empty_404_is_a_capability_error_and_does_not_rotate_the_query_id(tmp_path):
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(404, text="")

    store = _store(tmp_path, two=True)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert status == 404
    assert payload is None
    assert error == "operation UserTweets not available to this credential"
    # One call: no query-id walk, and no account rotation either.
    assert len(calls) == 1
    assert _state(store, "acct-1") == "active"
    assert store.get("acct-1").failures == 0


def test_422_returns_the_validation_message_naming_the_missing_flag(tmp_path):
    message = "The following features cannot be null: rweb_tipjar_consumption_enabled"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"errors": [{"message": message}]})

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert (status, payload, error) == (422, None, message)
    assert _state(store, "acct-1") == "active"


# --------------------------------------------------------------------------
# 5. the store on disk: round trip, 0600, and no cookie in any display path
# --------------------------------------------------------------------------


def test_store_round_trips_stays_0600_and_never_shows_a_full_cookie(tmp_path):
    settings = _settings(tmp_path)
    store = AccountStore(settings)
    store.add("acct-1", TOKEN_A, CT0_A, proxy="socks5://127.0.0.1:9050", note="burner")
    store.add("acct-2", TOKEN_B, CT0_B)

    path = tmp_path / "accounts.json"
    assert path.exists()
    assert os.stat(path).st_mode & 0o777 == 0o600

    reloaded = AccountStore(settings)
    assert [a.label for a in reloaded.list()] == ["acct-1", "acct-2"]
    first = reloaded.get("acct-1")
    assert first is not None
    assert (first.auth_token, first.ct0) == (TOKEN_A, CT0_A)
    assert first.proxy == "socks5://127.0.0.1:9050"

    # Every display path is redacted: repr, str, redacted(), and stats().
    displays = (
        repr(first),
        str(first),
        json.dumps(first.redacted()),
        json.dumps(reloaded.stats()),
    )
    for rendered in displays:
        assert TOKEN_A not in rendered
        assert CT0_A not in rendered
        assert "****1111" in rendered or "acct-1" in rendered
    assert "****1111" in repr(first) and "****2222" in repr(first)

    assert reloaded.remove("acct-2") is True
    assert reloaded.remove("acct-2") is False
    assert [a.label for a in AccountStore(settings).list()] == ["acct-1"]


def test_loose_file_mode_is_tightened_and_warned_about(tmp_path):
    settings = _settings(tmp_path)
    store = AccountStore(settings)
    store.add("acct-1", TOKEN_A, CT0_A)

    path = tmp_path / "accounts.json"
    os.chmod(path, 0o644)

    reopened = AccountStore(settings)
    assert os.stat(path).st_mode & 0o777 == 0o600
    assert reopened.warnings and "0644" in reopened.warnings[0]
    assert [a.label for a in reopened.list()] == ["acct-1"]


# --------------------------------------------------------------------------
# 6. rotation policy
# --------------------------------------------------------------------------


def test_acquire_skips_dead_and_cooling_until_the_window_passes(tmp_path, monkeypatch):
    clock = {"t": 1_700_000_000.0}
    monkeypatch.setattr(accounts_mod, "_now", lambda: clock["t"])

    store = _store(tmp_path, two=True)
    store.mark_dead("acct-1", "account_suspended (HTTP 403)")
    store.cooldown("acct-2", clock["t"] + 600, "rate limited (HTTP 429)")

    assert store.acquire() is None
    assert store.usable() == []

    clock["t"] += 601
    acquired = store.acquire()
    assert acquired is not None and acquired.label == "acct-2"
    assert _state(store, "acct-2") == "active"
    assert store.get("acct-2").cooldown_until_utc is None
    assert _state(store, "acct-1") == "dead"


def test_acquire_is_least_recently_used(tmp_path, monkeypatch):
    clock = {"t": 1_700_000_000.0}
    monkeypatch.setattr(accounts_mod, "_now", lambda: clock["t"])

    store = _store(tmp_path, two=True)
    first = store.acquire()
    clock["t"] += 10
    second = store.acquire()
    clock["t"] += 10
    third = store.acquire()

    assert first is not None and second is not None and third is not None
    assert {first.label, second.label} == {"acct-1", "acct-2"}
    assert third.label == first.label


def test_three_consecutive_failures_retire_the_account(tmp_path):
    store = _store(tmp_path)
    for _ in range(2):
        store.release("acct-1", ok=False, status=500, error="HTTP 500")
    assert _state(store, "acct-1") == "active"
    store.release("acct-1", ok=False, status=500, error="HTTP 500")
    assert _state(store, "acct-1") == "dead"
    assert "consecutive failures" in (store.get("acct-1").note or "")


def test_a_nearly_exhausted_window_cools_before_it_429s(tmp_path):
    reset = int(time.time()) + 240

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"x-rate-limit-remaining": "0", "x-rate-limit-reset": str(reset)},
            json=OK_PAYLOAD,
        )

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)

    status, _payload, error = _run(_call(session, handler))

    assert (status, error) == (200, None)
    assert _state(store, "acct-1") == "cooling"
    assert session.available is False


# --------------------------------------------------------------------------
# 7. an empty store degrades to an error, never an exception
# --------------------------------------------------------------------------


def test_empty_store_is_unavailable_and_graphql_returns_an_error(tmp_path):
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - must not fire
        calls.append(request)
        return httpx.Response(200, json=OK_PAYLOAD)

    store = AccountStore(_settings(tmp_path))
    session = AuthSession(_settings(tmp_path), store)

    assert session.available is False
    assert store.stats()["total"] == 0

    status, payload, error = _run(_call(session, handler))

    assert (status, payload) == (None, None)
    assert error is not None and "no usable authenticated account" in error
    assert calls == []


def test_transport_failure_becomes_an_error_string_without_the_cookie(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    store = _store(tmp_path)
    session = AuthSession(_settings(tmp_path), store)

    status, payload, error = _run(_call(session, handler))

    assert (status, payload) == (None, None)
    assert error is not None
    assert not any(secret in error for secret in ALL_SECRETS)
    assert store.get("acct-1").failures == 1
