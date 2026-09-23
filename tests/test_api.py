"""`/api/v1` tests: auth, dataset reads, watchlist edits, jobs, monitor, docs.

Hermetic: nothing here touches the network. Collection endpoints are covered
through the job registry with stub coroutines rather than by calling X.

Driven with httpx's ASGI transport rather than TestClient because the job tests
create asyncio tasks: TestClient runs the app in its own private event loop, and
a task created in the test's loop cannot be awaited from that one.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from magpie.api import build_api_router
from magpie.config import Settings
from magpie.dataset import Dataset
from magpie.jobs import Job, JobCapacityError, JobRegistry
from magpie.models import Post
from magpie.store import Store
from magpie.watchlist import Watchlist
from magpie.web import create_app

TOKEN = "s3cret"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def client_for(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api", headers=AUTH
    )


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path, auth_token=TOKEN)


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


@pytest.fixture
def ds(settings: Settings) -> Dataset:
    dataset = Dataset(settings)
    yield dataset
    dataset.close()


def make_post(tweet_id: str, handle: str = "alpha", created: str = "2026-03-01T10:00:00Z") -> Post:
    return Post(
        id=tweet_id,
        screen_name=handle,
        name=handle.title(),
        text=f"post {tweet_id}",
        created_at_utc=created,
        lang="en",
        available_sources=["syndication"],
    )


# ------------------------------------------------------------------- auth


async def test_reads_require_the_token(app: FastAPI) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://api"
    ) as anon:
        denied = await anon.get("/api/v1/posts")
        assert denied.status_code == 401
        assert denied.headers.get("www-authenticate", "").lower().startswith("bearer")

        # The token gate is the API's, not the whole app's: liveness stays open.
        assert (await anon.get("/healthz")).status_code == 200

        allowed = await anon.get("/api/v1/posts", headers=AUTH)
        assert allowed.status_code == 200


# ---------------------------------------------------------------- dataset


async def test_posts_filter_and_paginate(app: FastAPI, ds: Dataset) -> None:
    ds.upsert_post(make_post("1", created="2026-03-01T10:00:00Z"))
    ds.upsert_post(make_post("2", created="2026-03-01T11:00:00Z"))
    ds.upsert_post(make_post("3", handle="beta"))

    async with client_for(app) as api:
        body = (await api.get("/api/v1/posts", params={"user": "alpha", "limit": 1})).json()

    assert body["total"] == 2  # total counts the filter, not the page
    assert body["limit"] == 1 and body["offset"] == 0
    assert [row["id"] for row in body["rows"]] == ["2"]  # newest first
    assert body["rows"][0]["screen_name"] == "alpha"


async def test_post_detail_and_missing_post(app: FastAPI, ds: Dataset) -> None:
    ds.upsert_post(make_post("1"))
    async with client_for(app) as api:
        found = await api.get("/api/v1/posts/1")
        missing = await api.get("/api/v1/posts/404404")

    assert found.status_code == 200
    assert found.json()["media"] == []
    assert missing.status_code == 404
    assert missing.json() == {"detail": "unknown post"}


async def test_query_rejects_writes(app: FastAPI, ds: Dataset) -> None:
    ds.upsert_post(make_post("1"))

    async with client_for(app) as api:
        denied = await api.post("/api/v1/query", json={"sql": "DELETE FROM posts"})
        allowed = await api.post(
            "/api/v1/query",
            json={"sql": "SELECT id FROM posts WHERE screen_name = ?", "params": ["alpha"]},
        )

    assert denied.status_code == 400
    assert "SELECT" in denied.json()["detail"]
    assert allowed.json() == {"rows": [{"id": "1"}], "count": 1, "truncated": False}
    # The rejection was a rejection, not a silent success.
    assert ds.post("1") is not None


async def test_export_formats(app: FastAPI, ds: Dataset) -> None:
    ds.upsert_post(make_post("1"))

    async with client_for(app) as api:
        jsonl = await api.get("/api/v1/export/jsonl")
        csv_out = await api.get("/api/v1/export/csv")
        bogus = await api.get("/api/v1/export/xlsx")

    assert jsonl.status_code == 200
    assert jsonl.headers["content-type"].startswith("application/x-ndjson")
    assert 'attachment; filename="magpie-posts-' in jsonl.headers["content-disposition"]
    assert jsonl.text.strip().count("\n") == 0 and '"id": "1"' in jsonl.text
    assert csv_out.status_code == 200 and "alpha" in csv_out.text
    assert bogus.status_code == 404


async def test_query_caps_the_result_set(settings: Settings, ds: Dataset) -> None:
    """Read-only does not mean cheap: a self-join must not define the memory ceiling."""
    settings.api_query_rows = 2
    for n in range(5):
        ds.upsert_post(make_post(str(n)))

    async with client_for(create_app(settings)) as api:
        body = (await api.post("/api/v1/query", json={"sql": "SELECT id FROM posts"})).json()

    assert len(body["rows"]) == 2
    assert body["count"] == 2 and body["truncated"] is True


async def test_query_error_text_stays_in_the_log(app: FastAPI) -> None:
    """sqlite messages carry database paths; the caller gets the class only."""
    async with client_for(app) as api:
        broken = await api.post("/api/v1/query", json={"sql": "SELECT nope FROM posts"})

    assert broken.status_code == 400
    assert broken.json()["detail"] == "query failed: OperationalError"


# -------------------------------------------------------------- watchlist


async def test_watchlist_lifecycle(app: FastAPI) -> None:
    async with client_for(app) as api:
        added = await api.post("/api/v1/watchlist", json={"handles": ["@alpha"], "tags": ["news"]})
        assert added.status_code == 201
        assert added.json()["added"][0]["handle"] == "alpha"
        # No explicit interval: the row inherits the global default.
        assert added.json()["added"][0]["interval"] is None
        assert added.json()["added"][0]["effective_interval"] == 300.0

        listed = (await api.get("/api/v1/watchlist")).json()["rows"]
        assert [row["handle"] for row in listed] == ["alpha"]

        patched = await api.patch("/api/v1/watchlist/alpha", json={"interval": 120})
        assert patched.status_code == 200
        assert patched.json()["interval"] == 120.0
        assert patched.json()["effective_interval"] == 120.0

        disabled = await api.patch("/api/v1/watchlist/alpha", json={"enabled": False})
        assert disabled.json()["enabled"] is False
        assert (await api.get("/api/v1/watchlist", params={"enabled": True})).json()["rows"] == []

        assert (await api.patch("/api/v1/watchlist/ghost", json={"interval": 60})).status_code == 404

        assert (await api.delete("/api/v1/watchlist/alpha")).json() == {"removed": True}
        assert (await api.delete("/api/v1/watchlist/alpha")).json() == {"removed": False}


async def test_watchlist_add_rejects_empty_handles(app: FastAPI) -> None:
    async with client_for(app) as api:
        assert (await api.post("/api/v1/watchlist", json={"handles": []})).status_code == 400
        assert (await api.post("/api/v1/watchlist", json={})).status_code == 400


async def test_watchlist_rejects_junk_input(app: FastAPI) -> None:
    """Coercion here would report success while doing the opposite of the request."""
    async with client_for(app) as api:
        await api.post("/api/v1/watchlist", json={"handles": ["@alpha"], "interval": 120})

        # An unparseable interval must not read as "clear the override".
        bad = await api.patch("/api/v1/watchlist/alpha", json={"interval": "soon"})
        assert bad.status_code == 400
        assert (await api.get("/api/v1/watchlist")).json()["rows"][0]["interval"] == 120.0

        # `0` is the numeric false shells and jq emit; it must not enable.
        assert (await api.patch("/api/v1/watchlist/alpha", json={"enabled": 0})).status_code == 400
        assert (await api.get("/api/v1/watchlist")).json()["rows"][0]["enabled"] is True

        # Explicit null still clears it.
        cleared = await api.patch("/api/v1/watchlist/alpha", json={"interval": None})
        assert cleared.json()["interval"] is None

        # Anything non-empty used to become a watched account the monitor polls forever.
        assert (await api.post("/api/v1/watchlist", json={"handles": ["../../etc"]})).status_code == 400
        assert (await api.post("/api/v1/watchlist", json={"handles": ["@a"] * 99})).status_code == 400


async def test_tune_never_slows_an_account_that_lost_posts(
    settings: Settings, ds: Dataset
) -> None:
    """A rollover is measured evidence of loss; the suggestion is a sparse estimate."""
    now = datetime.now(timezone.utc)
    for n in range(8):
        stamp = (now - timedelta(minutes=30 * n)).strftime("%Y-%m-%dT%H:%M:%SZ")
        ds.upsert_post(make_post(str(n), created=stamp))

    watchlist = Watchlist(settings)
    watchlist.add("@alpha", interval=60.0)
    watchlist.record_poll("alpha", rollover=True, default_interval=settings.watch_interval)
    watchlist.close()

    async with client_for(create_app(settings)) as api:
        tuned = (await api.post("/api/v1/watchlist/alpha/tune")).json()
        rows = (await api.get("/api/v1/watchlist")).json()["rows"]

    # suggest_interval measures ~30-minute gaps and would slow this to the
    # 300s ceiling; the rollover clamp keeps the faster learned cadence.
    assert tuned["interval"] <= 60.0
    assert "rollover" in tuned["reason"]
    assert rows[0]["effective_interval"] == tuned["interval"]


async def test_tune_needs_history(app: FastAPI) -> None:
    async with client_for(app) as api:
        await api.post("/api/v1/watchlist", json={"handles": ["@alpha"]})
        tuned = (await api.post("/api/v1/watchlist/alpha/tune")).json()
        assert tuned == {"handle": "alpha", "interval": None, "reason": "not enough recent history"}
        assert (await api.post("/api/v1/watchlist/ghost/tune")).status_code == 404


# ---------------------------------------------------------------- monitor


async def test_monitor_flags_never_polled_account(app: FastAPI) -> None:
    async with client_for(app) as api:
        empty = (await api.get("/api/v1/monitor")).json()
        assert empty["ok"] is True  # waiting for accounts is a valid state

        await api.post("/api/v1/watchlist", json={"handles": ["@alpha"]})
        body = (await api.get("/api/v1/monitor")).json()

    assert body["ok"] is False
    assert body["stale"] == ["alpha"]
    entry = body["accounts"][0]
    assert entry["stale"] is True
    assert entry["last_poll_utc"] is None and entry["age_s"] is None
    assert entry["interval"] == 300.0


# ------------------------------------------------------------------- jobs


@pytest.fixture
def jobs(settings: Settings) -> JobRegistry:
    return JobRegistry(settings)


@pytest.fixture
def job_app(settings: Settings, jobs: JobRegistry) -> FastAPI:
    """Just the router, with the registry the test also holds a handle to."""
    application = FastAPI()
    application.include_router(
        build_api_router(
            settings=settings,
            store=Store(settings),
            dataset=Dataset(settings),
            watchlist=Watchlist(settings),
            jobs=jobs,
            write=[],
        )
    )
    return application


async def _settled(jobs: JobRegistry, job_id: str, timeout: float = 5.0) -> Job:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = jobs.get(job_id)
        assert job is not None
        if job.done:
            return job
        await asyncio.sleep(0.01)
    raise AssertionError(f"job {job_id} never settled")


async def test_job_completes_and_reports_its_result(
    job_app: FastAPI, jobs: JobRegistry
) -> None:
    async def run(job: Job) -> dict:
        job.progress = {"page": 1}
        return {"new_posts": 3}

    submitted = jobs.submit("pull", {"targets": ["@alpha"]}, run)
    assert submitted.status == "queued"

    settled = await _settled(jobs, submitted.id)
    assert settled.status == "completed"
    assert settled.result == {"new_posts": 3}
    assert settled.progress == {"page": 1}
    assert settled.started_utc and settled.finished_utc

    async with client_for(job_app) as api:
        body = (await api.get(f"/api/v1/jobs/{submitted.id}")).json()
        assert body["status"] == "completed" and body["result"] == {"new_posts": 3}

        rows = (await api.get("/api/v1/jobs", params={"kind": "pull"})).json()["rows"]
        assert [row["id"] for row in rows] == [submitted.id]
        assert (await api.get("/api/v1/jobs", params={"kind": "search"})).json()["rows"] == []

        # Finished work cannot be cancelled, and saying so is not an error.
        assert (await api.delete(f"/api/v1/jobs/{submitted.id}")).json() == {"cancelled": False}
        assert (await api.get("/api/v1/jobs/nosuchjob")).status_code == 404


async def test_job_failure_is_captured_not_raised(job_app: FastAPI, jobs: JobRegistry) -> None:
    async def boom(job: Job) -> dict:
        raise RuntimeError("search requires an account")

    submitted = jobs.submit("search", {"query": "x"}, boom)
    settled = await _settled(jobs, submitted.id)

    assert settled.status == "failed"
    assert settled.error == "RuntimeError: search requires an account"
    assert settled.result is None
    assert "Traceback" not in (settled.error or "")

    async with client_for(job_app) as api:
        body = (await api.get(f"/api/v1/jobs/{submitted.id}")).json()
    assert body["error"] == "RuntimeError: search requires an account"


async def test_running_job_can_be_cancelled(jobs: JobRegistry) -> None:
    started = asyncio.Event()

    async def forever(job: Job) -> dict:
        started.set()
        await asyncio.sleep(3600)
        return {}

    submitted = jobs.submit("thread", {"tweet_id": "1"}, forever)
    await asyncio.wait_for(started.wait(), timeout=5)

    assert await jobs.cancel(submitted.id) is True
    assert jobs.get(submitted.id).status == "cancelled"
    assert await jobs.cancel(submitted.id) is False
    assert await jobs.cancel("nosuchjob") is False


async def test_concurrency_gate_queues_the_overflow(settings: Settings) -> None:
    settings.api_job_concurrency = 1
    registry = JobRegistry(settings)
    release = asyncio.Event()

    async def hold(job: Job) -> dict:
        await release.wait()
        return {}

    first = registry.submit("pull", {}, hold)
    second = registry.submit("pull", {}, hold)
    await asyncio.sleep(0.05)

    assert first.status == "running"
    assert second.status == "queued"  # the gate is real, not decorative

    release.set()
    await _settled(registry, second.id)
    assert first.status == "completed" and second.status == "completed"
    await registry.shutdown()


async def test_queue_depth_is_bounded(settings: Settings) -> None:
    settings.api_job_concurrency = 1
    registry = JobRegistry(settings)
    release = asyncio.Event()

    async def hold(job: Job) -> dict:
        await release.wait()
        return {}

    # Nothing has run yet: every submit is still queued, and the ceiling is
    # concurrency * 10.
    for _ in range(10):
        registry.submit("pull", {}, hold)

    with pytest.raises(JobCapacityError):
        registry.submit("pull", {}, hold)

    release.set()
    await registry.shutdown()


async def test_pull_job_validates_its_targets(job_app: FastAPI, settings: Settings) -> None:
    async with client_for(job_app) as api:
        assert (await api.post("/api/v1/jobs/pull", json={"targets": []})).status_code == 400
        too_many = await api.post(
            "/api/v1/jobs/pull",
            json={"targets": [f"@a{n}" for n in range(settings.max_batch + 1)]},
        )
    assert too_many.status_code == 400
    assert str(settings.max_batch) in too_many.json()["detail"]
    # `pull()` re-splits on whitespace, so the cap has to count tokens: one
    # string used to smuggle an unbounded crawl past max_batch.
    async with client_for(job_app) as api:
        smuggled = await api.post(
            "/api/v1/jobs/pull",
            json={"targets": [" ".join(f"@a{n}" for n in range(settings.max_batch + 1))]},
        )
    assert smuggled.status_code == 400


async def test_thread_job_bounds_the_crawl(job_app: FastAPI) -> None:
    """depth/max_posts are outbound request counts against X, not preferences."""
    async with client_for(job_app) as api:
        deep = await api.post("/api/v1/jobs/thread", json={"tweet_id": "1", "depth": 100000})
        wide = await api.post(
            "/api/v1/jobs/thread", json={"tweet_id": "1", "max_posts": 100000}
        )
        assert (await api.post("/api/v1/jobs/thread", json={})).status_code == 400
    assert deep.status_code == 400 and wide.status_code == 400


async def test_jobs_filters_reject_unknown_values(job_app: FastAPI) -> None:
    """A typo must not read as 'no such jobs'."""
    async with client_for(job_app) as api:
        assert (await api.get("/api/v1/jobs", params={"kind": "pul"})).status_code == 422
        assert (await api.get("/api/v1/jobs", params={"status": "runnin"})).status_code == 422
        assert (await api.get("/api/v1/jobs", params={"kind": "pull"})).status_code == 200


async def test_search_job_rejects_unknown_product(job_app: FastAPI) -> None:
    async with client_for(job_app) as api:
        bad = await api.post("/api/v1/jobs/search", json={"query": "x", "product": "Trending"})
    assert bad.status_code == 400
    assert "Trending" in bad.json()["detail"]


# ------------------------------------------------------------------- docs


async def test_openapi_and_swagger_are_served(app: FastAPI) -> None:
    async with client_for(app) as api:
        document = await api.get("/api/v1/openapi.json")
        docs = await api.get("/api/v1/docs")

    assert document.status_code == 200
    paths = document.json()["paths"]
    assert "/api/v1/posts" in paths
    assert "/api/v1/jobs/pull" in paths
    assert docs.status_code == 200 and "swagger-ui" in docs.text


async def test_docs_can_be_switched_off(settings: Settings) -> None:
    settings.api_docs = False
    async with client_for(create_app(settings)) as api:
        assert (await api.get("/api/v1/docs")).status_code == 404


# --------------------------------------------------------------- captures


async def test_capture_routes_reject_unservable_folders(app: FastAPI) -> None:
    async with client_for(app) as api:
        listed = await api.get("/api/v1/captures")
        missing = await api.get("/api/v1/captures/20260101T000000Z_a_1")
        traversal = await api.get("/api/v1/captures/..%2F..%2Fetc")
        verify = await api.get("/api/v1/captures/20260101T000000Z_a_1/verify")

    assert listed.json() == {"total": 0, "limit": 50, "offset": 0, "rows": []}
    assert missing.status_code == 404 and missing.json() == {"detail": "unknown capture"}
    assert traversal.status_code == 404
    assert verify.status_code == 404


# ------------------------------------------------------------------- auth


async def test_api_refuses_to_serve_without_a_token(tmp_path: Path) -> None:
    """`require_write` is a no-op with no token; the API must not inherit that."""
    open_app = create_app(Settings(data_dir=tmp_path, auth_token=None))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=open_app), base_url="http://api"
    ) as api:
        refused = await api.get("/api/v1/posts")
        assert refused.status_code == 503
        assert "MAGPIE_AUTH_TOKEN" in refused.json()["detail"]
        assert (await api.post("/api/v1/query", json={"sql": "SELECT 1"})).status_code == 503
        # The HTML UI keeps its open-by-default behaviour.
        assert (await api.get("/healthz")).status_code == 200
