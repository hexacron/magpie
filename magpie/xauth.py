"""Authenticated GraphQL access using account cookies.

This is the ToS-breaking path, and it is built on the assumption that the
accounts behind it *will* be rate limited and *will* be suspended. Every
failure mode X has is therefore a routing decision rather than an exception:

* ``429`` parks the account until ``x-rate-limit-reset`` and retries on the
  next one;
* ``401`` and the ``403`` codes 32/64/326 retire the account and retry on the
  next one;
* ``404`` with an empty body is *not* a stale query id. Measured with ids
  scraped fresh from x.com's own logged-out bundle, a guest token gets 404 on
  ``SearchTimeline``/``TweetDetail``/``Followers`` while ``UserTweets``
  answers 200 -- so an empty 404 means "this credential may not call this
  operation", and rotating the id would only burn a working one.
* ``422`` hands back X's validation message verbatim, because it names the
  feature flag that has to be added to :data:`xapi.FEATURES`.

:meth:`AuthSession.graphql` is signature-compatible with
:meth:`xapi.GuestSession.graphql`, so a caller can hold either and fall back
from one to the other on :attr:`AuthSession.available`.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from . import queryids, xapi
from .accounts import Account, AccountStore, redact
from .config import Settings
from .normalize import _dict, _list, _text

__all__ = ["AuthSession", "SOURCE_NAME"]

SOURCE_NAME = "xapi_auth"

# X's own error codes. 32 is a bad/expired token, 64 a suspended account, 326
# a locked one awaiting a captcha. All three mean "this cookie is finished".
DEAD_CODES: dict[int, str] = {
    32: "bad_credentials",
    64: "account_suspended",
    326: "account_locked",
}

_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)

# At most one rotation per call: two accounts is enough to tell "this account
# is finished" from "this operation is finished", and more would turn one bad
# request into a walk through the whole roster.
MAX_ACCOUNTS_PER_CALL = 2


def _headers(settings: Settings, account: Account) -> dict[str, str]:
    """The exact header set x.com's web client sends for a logged-in call.

    ``ct0`` appears twice on purpose: as a cookie and as ``x-csrf-token``. X
    compares them, and a request carrying only one of the two is a 403.
    """
    return {
        "authorization": f"Bearer {xapi.WEB_BEARER}",
        "cookie": f"auth_token={account.auth_token}; ct0={account.ct0}",
        "x-csrf-token": account.ct0,
        "x-twitter-auth-type": "OAuth2Session",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "user-agent": settings.ua("x_page"),
        "content-type": "application/json",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://x.com",
        "referer": "https://x.com/",
    }


async def _send(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    settings: Settings,
    headers: dict[str, str],
    params: dict[str, str] | None,
    secrets: tuple[str, ...],
) -> tuple[int | None, str, httpx.Headers, str | None]:
    """One request with bounded retries. Returns ``(status, text, headers, error)``.

    ``error`` is transport-level only and is scrubbed of ``secrets``: httpx
    puts the request headers into some exception reprs, and this is the one
    place a cookie could reach a log line.
    """
    attempts = max(1, int(getattr(settings, "http_retries", 2)) + 1)
    timeout = float(getattr(settings, "http_timeout", 20.0))
    last_error: str | None = None
    empty = httpx.Headers()
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        try:
            response = await client.request(
                method, url, headers=headers, params=params, timeout=timeout
            )
        except _RETRYABLE as exc:
            last_error = redact(f"{type(exc).__name__}: {exc}".rstrip(": "), *secrets)
            continue
        except Exception as exc:  # transport contract violation
            return None, "", empty, redact(f"{type(exc).__name__}: {exc}".rstrip(": "), *secrets)
        if response.status_code >= 500 and attempt < attempts - 1:
            last_error = f"HTTP {response.status_code}"
            continue
        try:
            text = response.text
        except Exception:  # pragma: no cover - undecodable body
            text = ""
        return response.status_code, text, response.headers, None
    return None, "", empty, last_error or "request failed"


def _loads(text: str) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _payload_error(payload: Any) -> str | None:
    messages: list[str] = []
    for entry in _list(_dict(payload).get("errors")):
        message = _text(_dict(entry).get("message"))
        if message:
            messages.append(message)
    return "; ".join(messages) or None


def _error_codes(payload: Any) -> set[int]:
    """Every ``code`` X put in the errors list, top level or in extensions."""
    codes: set[int] = set()
    for entry in _list(_dict(payload).get("errors")):
        node = _dict(entry)
        for candidate in (node.get("code"), _dict(node.get("extensions")).get("code")):
            try:
                codes.add(int(candidate))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                continue
    return codes


def _rate(headers: httpx.Headers) -> tuple[int | None, float | None]:
    def _num(name: str) -> float | None:
        raw = headers.get(name)
        if raw is None:
            return None
        try:
            return float(str(raw).strip())
        except (TypeError, ValueError):
            return None

    remaining = _num("x-rate-limit-remaining")
    reset = _num("x-rate-limit-reset")
    return (None if remaining is None else int(remaining)), reset


class AuthSession:
    """A rotating pool of account cookies behind one GraphQL call."""

    def __init__(self, settings: Settings, store: AccountStore) -> None:
        self.settings = settings
        self.store = store
        self.last_account: str | None = None
        self.last_proxy: str | None = None
        self.error: str | None = None
        self.calls = 0
        self.rotations = 0
        self.query_ids: dict[str, str] = {}

    # -- availability ------------------------------------------------------

    @property
    def available(self) -> bool:
        """False when nothing in the store is usable -- fall back to guest."""
        try:
            return bool(self.store.usable())
        except Exception:  # pragma: no cover - a broken store is not fatal
            return False

    async def refresh_query_ids(
        self, client: httpx.AsyncClient, *, refresh: bool = False
    ) -> dict[str, str]:
        """Scrape/ load the id table once so per-call lookups stay in memory."""
        try:
            self.query_ids = await queryids.ensure(client, self.settings, refresh=refresh)
        except Exception as exc:  # pragma: no cover - discovery is best effort
            self.error = f"query id discovery failed: {type(exc).__name__}: {exc}"
        return self.query_ids

    def _query_id(self, operation: str) -> str | None:
        try:
            qid = queryids.query_id(operation, self.settings, self.query_ids or None)
        except Exception:  # pragma: no cover - never let lookup break a call
            qid = None
        if qid:
            return qid
        known = xapi.QUERY_IDS.get(operation) or ()
        return known[0] if known else None

    # -- the call ----------------------------------------------------------

    async def graphql(
        self,
        client: httpx.AsyncClient,
        operation: str,
        variables: dict[str, Any],
        *,
        features: dict[str, Any] | None = None,
    ) -> tuple[int | None, dict[str, Any] | None, str | None]:
        """Call one operation with a live account, rotating once on account failure.

        Returns ``(status, payload, error)``, exactly like
        :meth:`xapi.GuestSession.graphql`. Nothing here raises: a transport
        blow-up, a malformed body and a suspended account all come back as an
        error string, and none of them can contain a cookie.
        """
        qid = self._query_id(operation)
        if qid is None:
            return self._fail(None, f"no query id configured for {operation}")

        params_base = {
            "variables": json.dumps(variables, separators=(",", ":")),
            "features": json.dumps(
                features if features is not None else xapi.FEATURES, separators=(",", ":")
            ),
        }
        toggles = xapi.FIELD_TOGGLES.get(operation)
        if toggles:
            params_base["fieldToggles"] = json.dumps(toggles, separators=(",", ":"))
        url = f"{xapi.API_BASE}/graphql/{qid}/{operation}"

        last_status: int | None = None
        last_error: str | None = None
        for attempt in range(MAX_ACCOUNTS_PER_CALL):
            account = self.store.acquire()
            if account is None:
                return self._fail(
                    last_status,
                    last_error
                    or "no usable authenticated account (all cooling, dead or none added)",
                )
            if attempt:
                self.rotations += 1
            self.last_account = account.label
            self.last_proxy = account.proxy
            self.calls += 1
            secrets = (account.auth_token, account.ct0)

            status, text, headers, error = await _send(
                client,
                "GET",
                url,
                settings=self.settings,
                headers=_headers(self.settings, account),
                params=dict(params_base),
                secrets=secrets,
            )
            last_status = status

            if error is not None:
                self.store.release(account.label, ok=False, status=status, error=error)
                return self._fail(status, error)

            payload = _loads(text)
            message = redact(_payload_error(payload), *secrets)
            remaining, reset = _rate(headers)

            # -- the account is finished ----------------------------------
            condition = self._dead_condition(status, payload)
            if condition is not None:
                self.store.mark_dead(account.label, f"{condition} (HTTP {status})")
                last_error = f"{condition} (account {account.label}, HTTP {status})"
                continue

            # -- the account is merely spent ------------------------------
            if status == 429:
                until = reset or (time.time() + 900.0)
                self.store.cooldown(account.label, until, "rate limited (HTTP 429)")
                cooling = self.store.get(account.label)
                until_text = (cooling.cooldown_until_utc if cooling else None) or "later"
                last_error = f"rate limited (account {account.label} cooling until {until_text})"
                continue

            # -- the operation is finished, not the account ---------------
            if status == 404 and not text.strip():
                # Deliberately no query-id rotation: the id came from x.com's
                # own bundle, and an empty 404 is a capability answer.
                self.store.release(account.label, ok=True, status=status)
                return self._fail(status, f"operation {operation} not available to this credential")

            if status == 422:
                self.store.release(account.label, ok=True, status=status)
                return self._fail(status, message or "GRAPHQL_VALIDATION_FAILED")

            if status != 200:
                self.store.release(
                    account.label, ok=False, status=status, error=message or f"HTTP {status}"
                )
                return self._fail(status, message or f"HTTP {status}")

            self.store.release(
                account.label,
                ok=True,
                status=status,
                rate_remaining=remaining,
                rate_reset=reset,
            )
            if not isinstance(payload, dict):
                return self._fail(status, f"non-JSON response for {operation}")
            if message and not _dict(payload.get("data")):
                return status, payload, self._note(message)
            self.error = None
            return status, payload, None

        return self._fail(last_status, last_error or f"{operation} failed on every account")

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _dead_condition(status: int | None, payload: Any) -> str | None:
        """The named credential failure behind a 401/403, if that is what it is."""
        if status == 401:
            codes = _error_codes(payload)
            for code, name in DEAD_CODES.items():
                if code in codes:
                    return name
            return "bad_credentials"
        if status == 403:
            for code in sorted(_error_codes(payload)):
                if code in DEAD_CODES:
                    return DEAD_CODES[code]
        return None

    def _note(self, error: str) -> str:
        self.error = error
        return error

    def _fail(self, status: int | None, error: str) -> tuple[int | None, None, str]:
        return status, None, self._note(error)
