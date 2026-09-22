"""Delivery sinks for monitored posts.

The monitor loop is the thing that must survive: a webhook that 500s at 3am, a
notify command that was uninstalled, a log directory that got unmounted -- none
of those are allowed to stop polling. So every sink here obeys one rule:

    `deliver()` returns a `Delivery`, it never raises.

`Notifier.dispatch` enforces that a second time (gather + return_exceptions),
because a sink written later will eventually get it wrong.

Sinks are configured by spec strings so the watchlist can store them as text::

    webhook:https://hooks.example/x   POST the JSON envelope
    https://hooks.example/x           same thing, bare URL
    file:/var/log/magpie/posts.jsonl  append JSONL
    cmd:notify-send {count} new       run argv (no shell), posts JSON on stdin
    stdout                            human-readable lines

An unparseable spec is an error string, not an exception: one bad watchlist row
must not take the other sinks down with it.
"""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence, TextIO

import httpx

from .config import Settings
from .models import Model, Post

__all__ = [
    "Delivery",
    "Sink",
    "WebhookSink",
    "CommandSink",
    "FileSink",
    "StdoutSink",
    "Notifier",
    "build_sinks",
    "envelope",
    "WEBHOOK_CHUNK",
    "WEBHOOK_RETRIES",
    "COMMAND_TIMEOUT",
]

# A `--deep` backfill can hand the notifier thousands of posts at once. 100 per
# request keeps a body in the low hundreds of KB instead of tens of MB.
WEBHOOK_CHUNK = 100
WEBHOOK_RETRIES = 2
WEBHOOK_BACKOFF = 0.5
COMMAND_TIMEOUT = 30.0
STDOUT_TEXT_LIMIT = 140
_DETAIL_LIMIT = 300


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _trim(text: str, limit: int = _DETAIL_LIMIT) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1] + "\u2026"


@dataclass
class Delivery(Model):
    """One sink's answer for one batch. `count` is posts actually delivered."""

    sink: str
    ok: bool
    count: int
    error: str | None = None
    detail: str | None = None


def envelope(posts: Sequence[Post], context: dict[str, Any] | None = None) -> dict[str, Any]:
    """The wire format every structured sink emits."""
    return {
        "event": "posts",
        "count": len(posts),
        "context": dict(context or {}),
        "posts": [p.to_dict() for p in posts],
    }


def _handles(posts: Sequence[Post], context: dict[str, Any] | None) -> list[str]:
    ctx_handles = (context or {}).get("handles")
    if isinstance(ctx_handles, (list, tuple)):
        return [str(h) for h in ctx_handles]
    seen: list[str] = []
    for post in posts:
        name = post.screen_name
        if name and name not in seen:
            seen.append(name)
    return seen


class Sink:
    """Base sink. Subclasses implement `deliver` and must not raise out of it."""

    name: str = "sink"

    async def deliver(self, posts: Sequence[Post], context: dict[str, Any]) -> Delivery:
        raise NotImplementedError

    def _fail(self, exc: BaseException, count: int = 0) -> Delivery:
        return Delivery(
            sink=self.name,
            ok=False,
            count=count,
            error=f"{type(exc).__name__}: {_trim(str(exc))}",
        )


class WebhookSink(Sink):
    """POST the envelope as JSON, chunked, with a bounded retry on transient failures.

    4xx is a contract error -- the receiver rejected the shape, retrying just
    repeats it -- so only timeouts, connect failures and 5xx are retried.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 20.0,
        token: str | None = None,
        proxy: str | None = None,
        client: httpx.AsyncClient | None = None,
        chunk: int = WEBHOOK_CHUNK,
        retries: int = WEBHOOK_RETRIES,
        backoff: float = WEBHOOK_BACKOFF,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.token = token
        self.proxy = proxy
        self.chunk = max(1, chunk)
        self.retries = max(0, retries)
        self.backoff = max(0.0, backoff)
        self._client = client
        self.name = f"webhook:{url}"

    def headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    async def deliver(self, posts: Sequence[Post], context: dict[str, Any]) -> Delivery:
        if not posts:
            return Delivery(sink=self.name, ok=True, count=0)
        chunks = [list(posts[i : i + self.chunk]) for i in range(0, len(posts), self.chunk)]
        sent = 0
        errors: list[str] = []
        requests = 0
        try:
            client = self._client
            owned = client is None
            if client is None:
                client = httpx.AsyncClient(timeout=self.timeout, proxy=self.proxy)
            try:
                for batch in chunks:
                    ok, attempts, error = await self._send(client, batch, context)
                    requests += attempts
                    if ok:
                        sent += len(batch)
                    else:
                        errors.append(error or "unknown error")
            finally:
                if owned:
                    await client.aclose()
        except Exception as exc:  # client construction / close, never the loop's problem
            return self._fail(exc, sent)
        return Delivery(
            sink=self.name,
            ok=not errors,
            count=sent,
            error="; ".join(errors[:3]) or None,
            detail=f"{requests} request(s), {len(chunks)} chunk(s)",
        )

    async def _send(
        self,
        client: httpx.AsyncClient,
        batch: Sequence[Post],
        context: dict[str, Any],
    ) -> tuple[bool, int, str | None]:
        body = json.dumps(envelope(batch, context), ensure_ascii=False).encode("utf-8")
        headers = self.headers()
        attempts = 0
        error: str | None = None
        for attempt in range(1, self.retries + 2):
            attempts = attempt
            try:
                resp = await client.post(self.url, content=body, headers=headers)
            except httpx.TransportError as exc:  # timeouts, connect, protocol
                error = f"{type(exc).__name__}: {_trim(str(exc))}"
            except Exception as exc:  # malformed URL and friends: not retryable
                return False, attempts, f"{type(exc).__name__}: {_trim(str(exc))}"
            else:
                if resp.status_code < 400:
                    return True, attempts, None
                error = f"HTTP {resp.status_code}: {_trim(resp.text, 120)}"
                if resp.status_code < 500:
                    return False, attempts, error  # the receiver said no; asking again is rude
            if attempt <= self.retries and self.backoff:
                await asyncio.sleep(self.backoff * attempt)
        return False, attempts, error


class CommandSink(Sink):
    """Run argv once per batch with the JSON envelope on stdin.

    Never `shell=True`: post text is attacker-controlled and would otherwise be
    one backtick away from arbitrary command execution.
    """

    def __init__(self, command: str, *, timeout: float = COMMAND_TIMEOUT) -> None:
        self.command = command
        self.argv = shlex.split(command)
        if not self.argv:
            raise ValueError("empty command")
        self.timeout = timeout
        self.name = f"cmd:{command}"

    def _argv_for(self, posts: Sequence[Post], context: dict[str, Any]) -> list[str]:
        # str.replace, not str.format: an argv token may legitimately contain
        # braces (jq filters, JSON literals) and format() would explode on them.
        count = str(len(posts))
        handles = ",".join(_handles(posts, context))
        return [arg.replace("{count}", count).replace("{handles}", handles) for arg in self.argv]

    async def deliver(self, posts: Sequence[Post], context: dict[str, Any]) -> Delivery:
        if not posts:
            return Delivery(sink=self.name, ok=True, count=0)
        argv = self._argv_for(posts, context)
        payload = json.dumps(envelope(posts, context), ensure_ascii=False).encode("utf-8")
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await asyncio.wait_for(proc.communicate(payload), timeout=self.timeout)
        except asyncio.TimeoutError:
            if proc is not None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            return Delivery(
                sink=self.name,
                ok=False,
                count=0,
                error=f"timeout after {self.timeout:g}s",
            )
        except Exception as exc:
            return self._fail(exc)
        rc = proc.returncode or 0
        stdout = _trim(out.decode("utf-8", "replace"))
        stderr = _trim(err.decode("utf-8", "replace"))
        if rc != 0:
            return Delivery(
                sink=self.name,
                ok=False,
                count=0,
                error=f"exit {rc}: {stderr or stdout or 'no output'}",
            )
        return Delivery(sink=self.name, ok=True, count=len(posts), detail=stdout or None)


class FileSink(Sink):
    """Append one JSON object per post to a JSONL file.

    Opened in append mode per dispatch so `logrotate` (or a plain `mv`) works
    without the monitor holding a deleted inode open forever.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self.name = f"file:{self.path}"

    def _write(self, blob: str) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        data = blob.encode("utf-8")
        with self.path.open("ab") as fh:
            fh.write(data)
        return len(data)

    async def deliver(self, posts: Sequence[Post], context: dict[str, Any]) -> Delivery:
        if not posts:
            return Delivery(sink=self.name, ok=True, count=0)
        try:
            ts = _now()
            ctx = dict(context or {})
            blob = "".join(
                json.dumps({"ts": ts, "context": ctx, "post": p.to_dict()}, ensure_ascii=False)
                + "\n"
                for p in posts
            )
            written = await asyncio.to_thread(self._write, blob)
        except Exception as exc:
            return self._fail(exc)
        return Delivery(sink=self.name, ok=True, count=len(posts), detail=f"{written} bytes")


class StdoutSink(Sink):
    """One readable line per post. Text is truncated: this is a monitor log, not an archive."""

    name = "stdout"

    def __init__(self, stream: TextIO | None = None, *, limit: int = STDOUT_TEXT_LIMIT) -> None:
        self.stream = stream
        self.limit = limit

    def format(self, post: Post) -> str:
        handle = f"@{post.screen_name}" if post.screen_name else "@?"
        when = post.created_at_utc or ""
        text = _trim(post.text or "", self.limit)
        return f"[{when}] {handle} {post.id}: {text}".rstrip()

    async def deliver(self, posts: Sequence[Post], context: dict[str, Any]) -> Delivery:
        if not posts:
            return Delivery(sink=self.name, ok=True, count=0)
        try:
            stream = self.stream or sys.stdout
            stream.write("".join(self.format(p) + "\n" for p in posts))
            stream.flush()
        except Exception as exc:
            return self._fail(exc)
        return Delivery(sink=self.name, ok=True, count=len(posts))


def webhook_token(settings: Settings | None) -> str | None:
    """Shared secret for webhook sinks. Env-first so it can be rotated without a restart."""
    configured = getattr(settings, "webhook_token", None) if settings is not None else None
    return configured or os.environ.get("MAGPIE_WEBHOOK_TOKEN") or None


def _build_one(spec: str, settings: Settings | None) -> Sink | None:
    lowered = spec.lower()
    if lowered in ("stdout", "-", "stdout:"):
        return StdoutSink()
    if lowered.startswith(("http://", "https://")):
        return _webhook(spec, settings)

    scheme, sep, rest = spec.partition(":")
    scheme = scheme.strip().lower()
    rest = rest.strip()
    if not sep:
        return None
    if scheme == "webhook":
        if not rest.lower().startswith(("http://", "https://")):
            raise ValueError("webhook target must be an http(s) URL")
        return _webhook(rest, settings)
    if scheme == "file":
        if not rest:
            raise ValueError("file sink needs a path")
        return FileSink(rest)
    if scheme in ("cmd", "command", "exec"):
        if not rest:
            raise ValueError("command sink needs a command")
        return CommandSink(rest)
    if scheme == "stdout":
        return StdoutSink()
    return None


def _webhook(url: str, settings: Settings | None) -> WebhookSink:
    return WebhookSink(
        url,
        timeout=getattr(settings, "http_timeout", 20.0),
        proxy=getattr(settings, "proxy", None),
        token=webhook_token(settings),
    )


def build_sinks(
    specs: Sequence[str], settings: Settings | None = None
) -> tuple[list[Sink], list[str]]:
    """Parse spec strings into sinks. Bad specs come back as error strings."""
    sinks: list[Sink] = []
    errors: list[str] = []
    for raw in specs:
        spec = (raw or "").strip()
        if not spec:
            continue
        try:
            sink = _build_one(spec, settings)
        except Exception as exc:
            errors.append(f"{spec}: {type(exc).__name__}: {_trim(str(exc))}")
            continue
        if sink is None:
            errors.append(f"{spec}: unknown sink scheme (want webhook:/file:/cmd:/stdout or a URL)")
            continue
        sinks.append(sink)
    return sinks, errors


class Notifier:
    """Fan one batch of new posts out to every sink, concurrently and safely."""

    def __init__(self, sinks: Sequence[Sink]) -> None:
        self.sinks: list[Sink] = list(sinks)

    def __bool__(self) -> bool:
        return bool(self.sinks)

    async def dispatch(
        self, posts: Sequence[Post], context: dict[str, Any] | None = None
    ) -> list[Delivery]:
        if not posts or not self.sinks:
            return []
        ctx = dict(context or {})
        results = await asyncio.gather(
            *(self._guarded(sink, posts, ctx) for sink in self.sinks),
            return_exceptions=True,
        )
        deliveries: list[Delivery] = []
        for sink, result in zip(self.sinks, results):
            if isinstance(result, Delivery):
                deliveries.append(result)
            elif isinstance(result, BaseException):
                deliveries.append(
                    Delivery(
                        sink=getattr(sink, "name", type(sink).__name__),
                        ok=False,
                        count=0,
                        error=f"{type(result).__name__}: {_trim(str(result))}",
                    )
                )
            else:
                deliveries.append(
                    Delivery(
                        sink=getattr(sink, "name", type(sink).__name__),
                        ok=False,
                        count=0,
                        error=f"sink returned {type(result).__name__}, expected Delivery",
                    )
                )
        return deliveries

    @staticmethod
    async def _guarded(sink: Sink, posts: Sequence[Post], context: dict[str, Any]) -> Any:
        try:
            return await sink.deliver(posts, context)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return Delivery(
                sink=getattr(sink, "name", type(sink).__name__),
                ok=False,
                count=0,
                error=f"{type(exc).__name__}: {_trim(str(exc))}",
            )
