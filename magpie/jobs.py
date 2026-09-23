"""In-process registry for the API's background work.

Collection is slow: one pull walks a timeline and hydrates every id it finds, a
capture renders a PDF and may contact a timestamp authority. None of that fits
in a request, so `/api/v1/jobs/*` submits here and the caller polls.

Deliberately in-process and volatile. A restart forgets every job, which is the
right trade: what a job *produces* is already durable in sqlite, and the
alternative - a jobs table shared with the monitor container - buys
cross-process visibility nobody has asked for at the cost of a second write
path into the dataset.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .config import Settings
from .models import Model

log = logging.getLogger("magpie.jobs")

__all__ = ["JOB_KINDS", "STATUSES", "Job", "JobCapacityError", "JobRegistry"]

JOB_KINDS = ("pull", "search", "thread", "capture")
STATUSES = ("queued", "running", "completed", "failed", "cancelled")

#: Finished, in any sense. These are the ones retention may drop.
_TERMINAL = ("completed", "failed", "cancelled")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class JobCapacityError(RuntimeError):
    """The queue is already long enough that another job would just rot in it."""


@dataclass
class Job(Model):
    """One unit of background work and everything the API reports about it."""

    id: str
    kind: str
    status: str = "queued"
    params: dict[str, Any] = field(default_factory=dict)
    created_utc: str = ""
    started_utc: str | None = None
    finished_utc: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)

    @property
    def done(self) -> bool:
        return self.status in _TERMINAL


class JobRegistry:
    """Submit, poll and cancel background jobs. One per web process."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._jobs: dict[str, Job] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        # Created lazily: a registry is built in create_app(), which may run
        # outside a running loop, and a Semaphore must bind to the loop that
        # awaits it.
        self._gate: asyncio.Semaphore | None = None
        self._finished_at: dict[str, float] = {}

    # ------------------------------------------------------------ internals

    def _semaphore(self) -> asyncio.Semaphore:
        if self._gate is None:
            self._gate = asyncio.Semaphore(max(1, int(self.settings.api_job_concurrency)))
        return self._gate

    def _queued(self) -> int:
        return sum(1 for job in self._jobs.values() if job.status == "queued")

    def _prune(self) -> None:
        """Drop finished jobs past the TTL, then past the count cap.

        Running and queued jobs are never dropped: the registry is the only
        handle their caller has.
        """
        ttl = float(self.settings.api_job_ttl or 0.0)
        now = time.monotonic()
        if ttl > 0:
            for job_id, stamp in list(self._finished_at.items()):
                if now - stamp > ttl:
                    self._forget(job_id)

        cap = int(self.settings.api_job_max or 0)
        if cap > 0 and len(self._jobs) > cap:
            # Oldest completion first; a job still finishing outlives them all.
            oldest = sorted(self._finished_at.items(), key=lambda kv: kv[1])
            for job_id, _stamp in oldest:
                if len(self._jobs) <= cap:
                    break
                self._forget(job_id)

    def _forget(self, job_id: str) -> None:
        self._jobs.pop(job_id, None)
        self._tasks.pop(job_id, None)
        self._finished_at.pop(job_id, None)

    def _settle(self, job: Job, status: str) -> None:
        job.status = status
        job.finished_utc = _now()
        self._finished_at[job.id] = time.monotonic()

    async def _run(self, job: Job, run: Callable[[Job], Awaitable[dict[str, Any]]]) -> None:
        try:
            async with self._semaphore():
                job.status = "running"
                job.started_utc = _now()
                result = await run(job)
            job.result = result if isinstance(result, dict) else {"result": result}
            self._settle(job, "completed")
        except asyncio.CancelledError:
            self._settle(job, "cancelled")
            raise
        except Exception as exc:
            # A traceback in an API response is a leak; the class and message
            # are what a caller can act on. The full one goes to the log.
            job.error = f"{type(exc).__name__}: {exc}"
            self._settle(job, "failed")
            log.exception("job %s (%s) failed", job.id, job.kind)
        finally:
            self._tasks.pop(job.id, None)
            self._prune()

    # --------------------------------------------------------------- public

    def submit(
        self,
        kind: str,
        params: dict[str, Any],
        run: Callable[[Job], Awaitable[dict[str, Any]]],
    ) -> Job:
        """Queue `run` and return its job immediately.

        `run` is handed its own Job so a long crawl can publish progress into
        `job.progress` while it works.
        """
        backlog = max(1, int(self.settings.api_job_concurrency)) * 10
        if self._queued() >= backlog:
            raise JobCapacityError(f"{self._queued()} jobs already queued")

        job = Job(
            id=uuid.uuid4().hex[:12],
            kind=kind,
            params=dict(params or {}),
            created_utc=_now(),
        )
        self._jobs[job.id] = job
        self._tasks[job.id] = asyncio.create_task(self._run(job, run), name=f"magpie-job-{job.id}")
        return job

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(str(job_id or "").strip())

    def list(
        self,
        *,
        status: str | None = None,
        kind: str | None = None,
        limit: int = 50,
    ) -> list[Job]:
        """Newest first. `created_utc` ties are broken by insertion order."""
        jobs = list(self._jobs.values())
        if status:
            jobs = [j for j in jobs if j.status == status]
        if kind:
            jobs = [j for j in jobs if j.kind == kind]
        jobs.reverse()  # dicts preserve insertion order; newest submitted first
        jobs.sort(key=lambda j: j.created_utc, reverse=True)
        return jobs[: max(0, int(limit))]

    async def cancel(self, job_id: str) -> bool:
        """True only when a live job was actually stopped."""
        job = self.get(job_id)
        if job is None or job.done:
            return False
        task = self._tasks.get(job.id)
        if task is None or task.done():
            return False
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        if not job.done:  # pragma: no cover - _run always settles
            self._settle(job, "cancelled")
        return job.status == "cancelled"

    async def shutdown(self) -> None:
        """Cancel everything still running and wait for it. Never raises."""
        tasks = [t for t in self._tasks.values() if not t.done()]
        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
