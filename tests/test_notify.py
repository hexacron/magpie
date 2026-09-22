"""Offline tests for the monitor's delivery sinks.

The invariants worth pinning are the ones that decide whether a monitor is
still running in the morning: spec parsing degrades to error strings, the
webhook retries what is worth retrying and *only* that, big batches are chunked
instead of posted as one enormous body, the file sink really appends, a command
that exits non-zero is reported rather than raised, and one broken sink cannot
take the dispatch (and therefore the poll loop) down with it.

No network, no shell, no sleeping: webhook sinks get an `httpx.MockTransport`
client and a zero backoff, command sinks run this interpreter.
"""

from __future__ import annotations

import asyncio
import json
import shlex
import sys
from io import StringIO
from typing import Any

import httpx
import pytest

from magpie.config import Settings
from magpie.models import Post
from magpie.notify import (
    CommandSink,
    Delivery,
    FileSink,
    Notifier,
    Sink,
    StdoutSink,
    WebhookSink,
    build_sinks,
)

SETTINGS = Settings(http_timeout=1.0)


def _run(coro):
    return asyncio.run(coro)


def _posts(n: int = 2, start: int = 1) -> list[Post]:
    return [
        Post(
            id=str(start + i),
            screen_name="jack",
            text=f"post {start + i}",
            created_at_utc="2024-09-11T19:31:57Z",
        )
        for i in range(n)
    ]


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# --------------------------------------------------------------------------
# spec parsing
# --------------------------------------------------------------------------


def test_build_sinks_parses_every_scheme_and_reports_unknown(tmp_path):
    specs = [
        "webhook:https://hooks.example/new",
        f"file:{tmp_path / 'posts.jsonl'}",
        "cmd:notify-send {count} new posts",
        "stdout",
        "https://bare.example/hook",
        "nope:foo",
    ]
    sinks, errors = build_sinks(specs, SETTINGS)

    assert [type(s).__name__ for s in sinks] == [
        "WebhookSink",
        "FileSink",
        "CommandSink",
        "StdoutSink",
        "WebhookSink",
    ]
    assert sinks[0].url == "https://hooks.example/new"
    assert sinks[4].url == "https://bare.example/hook"
    assert sinks[2].argv == ["notify-send", "{count}", "new", "posts"]
    # the bad row is a string, not a traceback out of the watchlist loader
    assert len(errors) == 1
    assert errors[0].startswith("nope:foo")


def test_build_sinks_rejects_broken_specs_without_raising():
    sinks, errors = build_sinks(["webhook:not-a-url", "cmd:", "file:", "cmd:'unbalanced"], SETTINGS)
    assert sinks == []
    assert len(errors) == 4


# --------------------------------------------------------------------------
# webhook
# --------------------------------------------------------------------------


def test_webhook_posts_documented_envelope_with_bearer_token(monkeypatch):
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setenv("MAGPIE_WEBHOOK_TOKEN", "s3cret")
    sinks, errors = build_sinks(["webhook:https://hooks.example/new"], SETTINGS)
    assert errors == []
    sink = sinks[0]
    assert isinstance(sink, WebhookSink)
    sink._client = _client(handler)

    delivery = _run(sink.deliver(_posts(2), {"round": 3, "handles": ["jack"]}))

    assert delivery.ok and delivery.count == 2
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["authorization"] == "Bearer s3cret"

    body = json.loads(request.content)
    assert body["event"] == "posts"
    assert body["count"] == 2
    assert body["context"] == {"round": 3, "handles": ["jack"]}
    assert [p["id"] for p in body["posts"]] == ["1", "2"]
    assert body["posts"][0]["text"] == "post 1"  # full object, not truncated


def test_webhook_omits_authorization_without_token(monkeypatch):
    monkeypatch.delenv("MAGPIE_WEBHOOK_TOKEN", raising=False)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    sinks, _ = build_sinks(["https://hooks.example/new"], SETTINGS)
    sinks[0]._client = _client(handler)
    delivery = _run(sinks[0].deliver(_posts(1), {}))

    assert delivery.ok
    assert "authorization" not in seen[0].headers


def test_webhook_retries_5xx_twice_then_succeeds():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(503, text="overloaded")
        return httpx.Response(200)

    sink = WebhookSink("https://hooks.example/new", client=_client(handler), backoff=0.0)
    delivery = _run(sink.deliver(_posts(1), {}))

    assert delivery.ok is True
    assert delivery.count == 1
    assert len(calls) == 3  # 1 attempt + 2 retries


def test_webhook_does_not_retry_4xx():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(400, text="bad shape")

    sink = WebhookSink("https://hooks.example/new", client=_client(handler), backoff=0.0)
    delivery = _run(sink.deliver(_posts(1), {}))

    assert delivery.ok is False
    assert delivery.count == 0
    assert "400" in (delivery.error or "")
    assert len(calls) == 1


def test_webhook_retries_transport_errors_then_gives_up():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise httpx.ConnectTimeout("timed out", request=request)

    sink = WebhookSink("https://hooks.example/new", client=_client(handler), backoff=0.0)
    delivery = _run(sink.deliver(_posts(1), {}))

    assert delivery.ok is False
    assert len(calls) == 3
    assert "ConnectTimeout" in (delivery.error or "")


def test_webhook_chunks_large_batches_at_100():
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(200)

    sink = WebhookSink("https://hooks.example/new", client=_client(handler), backoff=0.0)
    delivery = _run(sink.deliver(_posts(250), {}))

    assert len(bodies) == 3
    assert [b["count"] for b in bodies] == [100, 100, 50]
    assert delivery.ok and delivery.count == 250


# --------------------------------------------------------------------------
# file
# --------------------------------------------------------------------------


def test_file_sink_appends_jsonl_across_dispatches(tmp_path):
    path = tmp_path / "nested" / "posts.jsonl"
    sink = FileSink(path)

    first = _run(sink.deliver(_posts(2, start=1), {"round": 1}))
    second = _run(sink.deliver(_posts(2, start=3), {"round": 2}))

    assert first.ok and second.ok
    assert first.detail and first.detail.endswith("bytes")

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    records = [json.loads(line) for line in lines]
    assert [r["post"]["id"] for r in records] == ["1", "2", "3", "4"]
    assert [r["context"]["round"] for r in records] == [1, 1, 2, 2]


def test_file_sink_reports_unwritable_path_without_raising(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    sink = FileSink(blocker / "posts.jsonl")

    delivery = _run(sink.deliver(_posts(1), {}))
    assert delivery.ok is False
    assert delivery.error


# --------------------------------------------------------------------------
# command
# --------------------------------------------------------------------------


def _python_cmd(script: str, *args: str) -> str:
    parts = [shlex.quote(sys.executable), "-c", shlex.quote(script), *args]
    return " ".join(parts)


def test_command_sink_substitutes_placeholders_and_feeds_stdin(tmp_path):
    out = tmp_path / "seen.json"
    script = (
        "import json,sys;"
        "data=json.load(sys.stdin);"
        "open(sys.argv[1],'w').write("
        "json.dumps({'count':data['count'],'ids':[p['id'] for p in data['posts']],"
        "'argv_count':sys.argv[2],'argv_handles':sys.argv[3]}))"
    )
    command = _python_cmd(script, shlex.quote(str(out)), "{count}", "{handles}")
    sink = CommandSink(command)

    delivery = _run(sink.deliver(_posts(2), {"handles": ["jack", "kim"]}))

    assert delivery.ok is True, delivery.error
    assert delivery.count == 2
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["count"] == 2
    assert payload["ids"] == ["1", "2"]
    assert payload["argv_count"] == "2"
    assert payload["argv_handles"] == "jack,kim"


def test_command_sink_reports_failure_without_raising():
    sink = CommandSink(_python_cmd("import sys; sys.stderr.write('boom'); sys.exit(3)"))
    delivery = _run(sink.deliver(_posts(1), {}))

    assert delivery.ok is False
    assert delivery.count == 0
    assert "exit 3" in (delivery.error or "")
    assert "boom" in (delivery.error or "")


def test_command_sink_missing_binary_is_an_error_string():
    sink = CommandSink("magpie-no-such-binary-xyz {count}")
    delivery = _run(sink.deliver(_posts(1), {}))
    assert delivery.ok is False
    assert delivery.error


def test_command_sink_never_uses_a_shell(tmp_path):
    # Shell metacharacters must reach the program as literal argv, not be
    # interpreted: post text is attacker-controlled.
    marker = tmp_path / "pwned"
    script = "import sys; open(sys.argv[2],'w').write(sys.argv[1])"
    seen = tmp_path / "argv.txt"
    command = [
        shlex.quote(sys.executable),
        "-c",
        shlex.quote(script),
        shlex.quote(f"; touch {marker}"),
        shlex.quote(str(seen)),
    ]
    sink = CommandSink(" ".join(command))

    delivery = _run(sink.deliver(_posts(1), {}))

    assert delivery.ok is True, delivery.error
    assert seen.read_text(encoding="utf-8") == f"; touch {marker}"
    assert not marker.exists()


# --------------------------------------------------------------------------
# stdout
# --------------------------------------------------------------------------


def test_stdout_sink_truncates_post_text():
    stream = StringIO()
    sink = StdoutSink(stream, limit=20)
    post = Post(id="7", screen_name="jack", text="x" * 500)

    delivery = _run(sink.deliver([post], {}))

    line = stream.getvalue().strip()
    assert delivery.ok and delivery.count == 1
    assert "@jack" in line and "7:" in line
    assert len(line) < 80
    assert line.endswith("\u2026")


# --------------------------------------------------------------------------
# notifier
# --------------------------------------------------------------------------


class _BoomSink(Sink):
    name = "boom"

    async def deliver(self, posts, context) -> Delivery:
        raise RuntimeError("sink exploded")


class _SyncBoomSink(Sink):
    """Raises before returning an awaitable at all -- the nastier failure mode."""

    name = "sync-boom"

    def deliver(self, posts, context):  # type: ignore[override]
        raise ValueError("not even a coroutine")


def test_dispatch_isolates_a_broken_sink():
    stream = StringIO()
    notifier = Notifier([StdoutSink(stream), _BoomSink()])

    deliveries = _run(notifier.dispatch(_posts(2), {"round": 1}))

    assert len(deliveries) == 2
    ok = [d for d in deliveries if d.ok]
    bad = [d for d in deliveries if not d.ok]
    assert len(ok) == 1 and ok[0].count == 2
    assert len(bad) == 1
    assert bad[0].sink == "boom"
    assert "RuntimeError" in (bad[0].error or "")
    assert stream.getvalue().count("\n") == 2  # healthy sink still ran


def test_dispatch_survives_a_sink_that_is_not_even_async():
    notifier = Notifier([_SyncBoomSink()])
    deliveries = _run(notifier.dispatch(_posts(1), {}))
    assert len(deliveries) == 1
    assert deliveries[0].ok is False
    assert deliveries[0].error


def test_dispatch_is_a_noop_for_an_empty_batch(tmp_path):
    path = tmp_path / "posts.jsonl"
    notifier = Notifier([FileSink(path), _BoomSink()])
    assert _run(notifier.dispatch([], {"round": 1})) == []
    assert not path.exists()


def test_dispatch_with_no_sinks_returns_empty():
    assert _run(Notifier([]).dispatch(_posts(3), {})) == []


def test_delivery_serialises_like_every_other_model():
    d = Delivery(sink="stdout", ok=True, count=2, detail="ok")
    assert d.to_dict() == {
        "sink": "stdout",
        "ok": True,
        "count": 2,
        "error": None,
        "detail": "ok",
    }


@pytest.mark.parametrize("spec", ["stdout", "STDOUT", "-"])
def test_stdout_aliases(spec):
    sinks, errors = build_sinks([spec], SETTINGS)
    assert errors == []
    assert isinstance(sinks[0], StdoutSink)
