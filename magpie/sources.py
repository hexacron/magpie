"""Fetching the four public, unauthenticated sources for one post.

Every source is fetched concurrently and *nothing* here raises: a failure is a
populated ``RawSource`` with ``ok=False`` and an ``error`` string.  The full
request headers, response headers and (best effort) TLS peer details are kept,
because a hash over a body nobody can attribute to a server is weak evidence.

Notes that cost real measurement time, so they are recorded here:

* ``api.vxtwitter.com`` answers 200 to a plain client UA and 403 to a Chrome
  UA.  The original tool's permanent "Cloudflare block" was self-inflicted.
* ``x.com/<handle>/status/<id>`` serves ~185 KB of real server-rendered HTML to
  an ordinary browser UA with no cookies; a Googlebot UA gets 403.
* ``cdn.syndication.twimg.com`` returns ``{}`` when the token is *missing* and
  data when it is merely wrong, so the token must be present.
* When the handle is unknown, ``i`` works as a placeholder on both x.com and
  api.fxtwitter.com.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from typing import Any

import httpx

from .config import Settings
from .ids import syndication_token
from .models import SOURCE_NAMES, RawSource, TweetRef

__all__ = ["fetch_sources", "source_filename", "source_url"]

_FILENAMES = {
    "syndication": "syndication.json",
    "fxtwitter": "fxtwitter.json",
    "vxtwitter": "vxtwitter.json",
    "x_page": "x_page.html",
}

_JSON_ACCEPT = "application/json, text/plain, */*"
_HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

# Retryable transport failures: timeouts and network-level errors only.
_RETRYABLE = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)


def source_filename(name: str) -> str:
    """On-disk name for a source's raw body inside the package."""
    return _FILENAMES.get(name, f"{name}.txt")


def source_url(name: str, ref: TweetRef) -> str:
    """Public URL fetched for ``name``. Handle falls back to X's own ``i``."""
    tweet_id = ref.tweet_id or ""
    handle = ref.screen_name or "i"
    if name == "syndication":
        try:
            token = syndication_token(tweet_id)
        except ValueError:
            token = ""
        return (
            "https://cdn.syndication.twimg.com/tweet-result"
            f"?id={tweet_id}&token={token}&lang=en"
        )
    if name == "fxtwitter":
        return f"https://api.fxtwitter.com/{handle}/status/{tweet_id}"
    if name == "vxtwitter":
        return f"https://api.vxtwitter.com/{handle}/status/{tweet_id}"
    if name == "x_page":
        return f"https://x.com/{handle}/status/{tweet_id}"
    return f"https://x.com/{handle}/status/{tweet_id}"


def _request_headers(name: str, settings: Settings) -> dict[str, str]:
    headers = {
        "User-Agent": settings.ua(name),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": _HTML_ACCEPT if name == "x_page" else _JSON_ACCEPT,
    }
    if name == "syndication":
        headers["Referer"] = "https://platform.twitter.com/"
        headers["Origin"] = "https://platform.twitter.com"
    if name == "x_page":
        headers["Upgrade-Insecure-Requests"] = "1"
        headers["Sec-Fetch-Dest"] = "document"
        headers["Sec-Fetch-Mode"] = "navigate"
        headers["Sec-Fetch-Site"] = "none"
    return headers


def _rdn_value(rdn: Any, key: str) -> str | None:
    """Pull one attribute out of ssl's nested subject/issuer tuple form."""
    if not rdn:
        return None
    try:
        for part in rdn:
            for attr, value in part:
                if attr == key:
                    return str(value)
    except (TypeError, ValueError):
        return None
    return None


def _tls_info(response: httpx.Response) -> dict[str, Any] | None:
    """Best-effort TLS peer details. Never raises, never blocks."""
    try:
        stream = response.extensions.get("network_stream")
        if stream is None:
            return None
        ssl_object = stream.get_extra_info("ssl_object")
        if ssl_object is None:
            return None
        cert = ssl_object.getpeercert() or {}
        cipher = ssl_object.cipher() or ()
        issuer = _rdn_value(cert.get("issuer"), "commonName") or _rdn_value(
            cert.get("issuer"), "organizationName"
        )
        return {
            "peer_cn": _rdn_value(cert.get("subject"), "commonName"),
            "issuer": issuer,
            "not_after": cert.get("notAfter"),
            "protocol": ssl_object.version(),
            "cipher": cipher[0] if cipher else None,
        }
    except Exception:  # noqa: BLE001 - evidence extras must never break a capture
        return None


def _headers_dict(headers: Any) -> dict[str, str]:
    try:
        return {str(k).lower(): str(v) for k, v in headers.items()}
    except Exception:  # noqa: BLE001
        return {}


async def _get(
    client: httpx.AsyncClient, url: str, headers: dict[str, str], settings: Settings
) -> tuple[httpx.Response | None, str | None, int]:
    """GET with bounded retries. Retries timeouts/network errors/5xx only."""
    attempts = max(1, int(settings.http_retries) + 1)
    last_error: str | None = None
    total_ms = 0
    for attempt in range(attempts):
        if attempt:
            await asyncio.sleep(0.5 * attempt)
        started = time.perf_counter()
        try:
            response = await client.get(
                url,
                headers=headers,
                timeout=settings.http_timeout,
                follow_redirects=True,
            )
        except _RETRYABLE as exc:
            total_ms += int((time.perf_counter() - started) * 1000)
            last_error = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
            continue
        except httpx.HTTPError as exc:
            total_ms += int((time.perf_counter() - started) * 1000)
            return None, f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__, total_ms

        total_ms += int((time.perf_counter() - started) * 1000)
        if response.status_code >= 500 and attempt < attempts - 1:
            last_error = f"HTTP {response.status_code}"
            continue
        return response, None, total_ms

    return None, last_error or "request failed", total_ms


def _loads(body: bytes) -> Any:
    return json.loads(body.decode("utf-8", "replace"))


def _classify_syndication(raw: RawSource, status: int, body: bytes) -> None:
    if status == 404:
        raw.ok = False
        raw.error = "HTTP 404"
        return
    if status != 200:
        raw.ok = False
        raw.error = f"HTTP {status}"
        return
    try:
        payload = _loads(body)
    except ValueError as exc:
        raw.ok = False
        raw.error = f"invalid JSON: {exc}"
        return
    if not isinstance(payload, dict) or not payload:
        # `{}` is what the endpoint returns when the token is absent.
        raw.ok = False
        raw.error = "empty_response"
        return
    raw.ok = True


def _classify_fxtwitter(raw: RawSource, status: int, body: bytes) -> None:
    payload: Any = None
    try:
        payload = _loads(body)
    except ValueError:
        payload = None
    message = None
    if isinstance(payload, dict):
        message = payload.get("message")
    if status == 200 and isinstance(payload, dict) and payload.get("code") == 200:
        raw.ok = True
        return
    raw.ok = False
    raw.error = (message if isinstance(message, str) and message else None) or f"HTTP {status}"


def _classify_vxtwitter(raw: RawSource, status: int, body: bytes) -> None:
    if status == 403:
        raw.ok = False
        raw.error = "HTTP 403 (bot challenge — check MAGPIE_UA_VXTWITTER)"
        return
    if status != 200:
        raw.ok = False
        raw.error = f"HTTP {status}"
        return
    try:
        payload = _loads(body)
    except ValueError as exc:
        # Measured: for a deleted/private/suspended post vxtwitter answers 200
        # with an HTML page reading "Failed to scan your link!". That is a
        # statement about the post, not about our request, so it must not be
        # recorded as a tooling failure.
        if b"Failed to scan your link" in body or body.lstrip()[:9].lower() == b"<!doctype":
            raw.ok = False
            raw.error = "not found (vxtwitter could not scan the link)"
            return
        raw.ok = False
        raw.error = f"invalid JSON: {exc}"
        return
    if not isinstance(payload, dict) or not payload:
        raw.ok = False
        raw.error = "empty_response"
        return
    raw.ok = True


def _classify_x_page(raw: RawSource, status: int, body: bytes) -> None:
    if status != 200:
        raw.ok = False
        raw.error = f"HTTP {status}"
        return
    # A 200 page is a successful fetch and is always saved, even when X served
    # a shell with no post in it -- normalize/crosscheck decide availability.
    raw.ok = True
    raw.error = None


_CLASSIFIERS = {
    "syndication": _classify_syndication,
    "fxtwitter": _classify_fxtwitter,
    "vxtwitter": _classify_vxtwitter,
    "x_page": _classify_x_page,
}


async def _fetch_one(
    client: httpx.AsyncClient, name: str, ref: TweetRef, settings: Settings
) -> RawSource:
    url = source_url(name, ref)
    headers = _request_headers(name, settings)
    raw = RawSource(name=name, url=url, request_headers=dict(headers))

    response, error, elapsed_ms = await _get(client, url, headers, settings)
    raw.elapsed_ms = elapsed_ms
    if response is None:
        raw.ok = False
        raw.error = error
        return raw

    body = response.content or b""
    raw.http_status = response.status_code
    raw.headers = _headers_dict(response.headers)
    raw.request_headers = _headers_dict(response.request.headers) or dict(headers)
    raw.body = body
    raw.bytes = len(body)
    raw.body_sha256 = hashlib.sha256(body).hexdigest()
    if settings.capture_tls_info:
        raw.tls = _tls_info(response)

    classifier = _CLASSIFIERS.get(name)
    if classifier is not None:
        classifier(raw, response.status_code, body)
    else:
        raw.ok = response.status_code == 200
        raw.error = None if raw.ok else f"HTTP {response.status_code}"

    if body:
        raw.saved_file = source_filename(name)
    return raw


async def fetch_sources(
    client: httpx.AsyncClient, ref: TweetRef, settings: Settings
) -> list[RawSource]:
    """Fetch every configured source for ``ref`` concurrently.

    Returns one ``RawSource`` per configured source, in configuration order.
    Never raises.
    """
    names = [n for n in settings.sources if n in SOURCE_NAMES]
    if not names:
        names = list(SOURCE_NAMES)

    if not ref.tweet_id:
        return [
            RawSource(
                name=name,
                url=source_url(name, ref),
                ok=False,
                error=ref.error or "no tweet id",
            )
            for name in names
        ]

    results = await asyncio.gather(
        *(_fetch_one(client, name, ref, settings) for name in names),
        return_exceptions=True,
    )

    out: list[RawSource] = []
    for name, result in zip(names, results):
        if isinstance(result, RawSource):
            out.append(result)
        else:
            out.append(
                RawSource(
                    name=name,
                    url=source_url(name, ref),
                    ok=False,
                    error=f"{type(result).__name__}: {result}",
                )
            )
    return out
