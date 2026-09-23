"""Versioned JSON API: the CLI's capabilities over HTTP.

Everything `magpie` can do from a shell is reachable here - read the dataset,
drive collection, edit the watchlist, check the monitor - except that slow work
does not block a request. Collection endpoints return a job id immediately and
the caller polls ``/jobs/{id}``.

The whole router sits behind ``require_write``: reads included. An API token is
a machine credential, and a dataset of who-said-what is not something to hand
out because ``MAGPIE_PUBLIC_READ`` was left on for the HTML browser.

Request bodies are parsed by hand rather than declared as pydantic models, for
the same reason the rest of this app does: the error strings are part of the
contract and are easier to keep exact this way.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response, StreamingResponse

from .capture import capture_many
from .config import Settings
from .dataset import CSV_HEADER, Dataset, handle_key
from .jobs import JOB_KINDS, STATUSES, Job, JobCapacityError, JobRegistry
from .models import TOOL_VERSION
from .pull import PullReport, expand_thread, pull
from .search import SEARCH_PRODUCTS
from .store import FOLDER_RE, Store
from .watchlist import Watchlist

log = logging.getLogger("magpie.api")

__all__ = ["build_api_router"]

#: Hard ceilings on one job regardless of what the caller asks for. The
#: configured `thread_depth`/`thread_max_posts` are defaults, not limits, and a
#: crawl budget is an outbound request count against X from this server's IP.
SEARCH_LIMIT_MAX = 1000
THREAD_DEPTH_MAX = 10
THREAD_POSTS_MAX = 2000

#: X screen names. Anything else would be polled forever by the monitor daemon.
HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")

#: Swagger UI assets. Pinned to an exact build, not FastAPI's floating `@5`:
#: the docs page is same-origin with the API and the browser attaches the
#: session cookie, so a mutable third-party script is a live credential.
SWAGGER_CDN = "https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14"

_MISSING = object()


def _err(detail: str, code: int) -> JSONResponse:
    return JSONResponse({"detail": detail}, code)


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def _epoch(value: str | None) -> float | None:
    """Epoch seconds from the ``%Y-%m-%dT%H:%M:%SZ`` stamps this app writes."""
    if not value:
        return None
    try:
        return (
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except ValueError:
        return None


async def _body(request: Request) -> dict[str, Any] | None:
    """The request's JSON object, or None when it is not one."""
    try:
        payload = await request.json()
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return default


def _as_float(value: Any) -> float | None:
    """A positive float, or None for anything that is not one."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _as_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _str_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []


def build_api_router(
    *,
    settings: Settings,
    store: Store,
    dataset: Dataset,
    watchlist: Watchlist,
    jobs: JobRegistry,
    write: list,
) -> APIRouter:
    """The whole ``/api/v1`` surface. `write` is create_app's auth dependency."""

    async def _api_disabled() -> None:
        # Without a token `require_write` is a no-op, which would leave SQL,
        # dataset export and the job runner open to anyone who reaches the
        # port. The HTML UI keeps its open-by-default behaviour; the API does
        # not get to inherit it.
        raise HTTPException(503, "API disabled: set MAGPIE_AUTH_TOKEN")

    gate = list(write) if settings.auth_token else [Depends(_api_disabled)]
    router = APIRouter(prefix=f"{settings.base_path}/api/v1", dependencies=gate, tags=["api"])

    def _submit(
        kind: str, params: dict[str, Any], run: Callable[[Job], Awaitable[dict[str, Any]]]
    ) -> Response:
        try:
            job = jobs.submit(kind, params, run)
        except JobCapacityError as exc:
            return _err(f"job queue is full: {exc}", 503)
        return JSONResponse({"job": job.to_dict()}, 202)

    # ------------------------------------------------------------- dataset

    @router.get("/posts")
    async def list_posts(
        q: str | None = None,
        user: str | None = None,
        since: str | None = None,
        until: str | None = None,
        media: bool = False,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> Response:
        """Collected posts, newest first. `q` is full-text; `media=true` filters to posts with media."""
        rows, total = dataset.posts(
            handle=user,
            q=q,
            since=since,
            until=until,
            has_media=True if media else None,
            limit=limit,
            offset=offset,
        )
        return JSONResponse({"total": total, "limit": limit, "offset": offset, "rows": rows})

    @router.get("/posts/{tweet_id}")
    async def get_post(tweet_id: str) -> Response:
        """One post with its media rows."""
        row = dataset.post(tweet_id)
        if row is None:
            return _err("unknown post", 404)
        return JSONResponse(row)

    @router.get("/users")
    async def list_users(limit: int = Query(100, ge=1, le=1000)) -> Response:
        """Profiles seen while collecting, most-followed first."""
        return JSONResponse({"rows": dataset.users(limit=limit)})

    @router.get("/stats")
    async def stats() -> Response:
        """Dataset, capture-store and watchlist counters in one call."""

        def safe(name: str, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
            try:
                return dict(fn())
            except Exception as exc:  # one broken store must not 500 the rest
                log.warning("%s stats failed: %s", name, exc)
                return {}

        return JSONResponse(
            {
                "dataset": safe("dataset", dataset.stats),
                "captures": safe("captures", store.stats),
                "watchlist": safe("watchlist", watchlist.stats),
            }
        )

    @router.post("/query")
    async def run_query(request: Request) -> Response:
        """Read-only SQL over the dataset: `{"sql": "SELECT ...", "params": [...]}`.

        One SELECT/WITH statement on a `query_only` connection, with a deadline
        (`MAGPIE_API_QUERY_TIMEOUT`) and a row cap (`MAGPIE_API_QUERY_ROWS`):
        read-only does not mean cheap, and this runs off the event loop so a
        slow query stalls one worker thread rather than the whole server.
        """
        payload = await _body(request)
        if payload is None:
            return _err("expected {'sql': '...'}", 400)
        sql = payload.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            return _err("expected {'sql': '...'}", 400)
        raw_params = payload.get("params")
        if raw_params is not None and not isinstance(raw_params, list):
            return _err("'params' must be a list", 400)
        try:
            rows, truncated = await run_in_threadpool(
                dataset.query,
                sql,
                tuple(raw_params or ()),
                timeout=settings.api_query_timeout,
                max_rows=settings.api_query_rows,
            )
        except ValueError as exc:
            return _err(str(exc), 400)
        except Exception as exc:
            # The class is actionable; the message carries database paths and
            # proxy hosts, so it goes to the log instead of the response.
            log.warning("query failed: %s: %s", type(exc).__name__, exc)
            return _err(f"query failed: {type(exc).__name__}", 400)
        return JSONResponse({"rows": rows, "count": len(rows), "truncated": truncated})

    @router.get("/export/{fmt}")
    async def export(fmt: str, handle: str | None = None) -> Response:
        """Whole dataset as `jsonl` or `csv`, optionally for one `handle`.

        Streamed row by row: the dataset grows without bound, so buffering the
        whole export would make each request a memory multiplier.
        """
        if fmt == "jsonl":
            def rows() -> Any:
                for row in dataset.iter_export(handle):
                    yield json.dumps(row, ensure_ascii=False) + "\n"

            media_type = "application/x-ndjson"
        elif fmt == "csv":
            def rows() -> Any:
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(CSV_HEADER)
                for row in dataset.iter_export(handle):
                    writer.writerow(
                        ["" if row.get(c) is None else row.get(c) for c in CSV_HEADER]
                    )
                    yield buf.getvalue()
                    buf.seek(0)
                    buf.truncate(0)
                if buf.tell():  # header only: no posts matched
                    yield buf.getvalue()

            media_type = "text/csv; charset=utf-8"
        else:
            return _err("unknown export format; expected jsonl or csv", 404)
        return StreamingResponse(
            rows(),
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="magpie-posts-{_stamp()}.{fmt}"'
            },
        )

    # ----------------------------------------------------------- watchlist

    def _watch_row(entry: Any) -> dict[str, Any]:
        row = entry.to_dict()
        row["effective_interval"] = entry.effective_interval(settings.watch_interval)
        return row

    @router.get("/watchlist")
    async def list_watchlist(enabled: bool = False, tag: str | None = None) -> Response:
        """Watched accounts. `enabled=true` hides disabled ones."""
        entries = watchlist.list(enabled_only=enabled, tag=tag)
        return JSONResponse({"rows": [_watch_row(e) for e in entries]})

    @router.post("/watchlist")
    async def add_watchlist(request: Request) -> Response:
        """Watch accounts: `{"handles": ["@a", "@b"], "interval": 300, "tags": [...]}`.

        Idempotent - re-adding an account updates it and keeps its counters.
        """
        payload = await _body(request)
        if payload is None:
            return _err("expected {'handles': [...]}", 400)
        handles = _str_list(payload.get("handles")) or _str_list(payload.get("handle"))
        if not handles:
            return _err("expected {'handles': [...]}", 400)
        if len(handles) > settings.max_batch:
            return _err(f"at most {settings.max_batch} handles per request", 400)
        # Validate the whole batch first: a 400 halfway through the loop would
        # leave a partially mutated watchlist the caller cannot see.
        for handle in handles:
            if not HANDLE_RE.match(handle_key(handle)):
                return _err(f"not a screen name: {handle!r}", 400)

        interval = payload.get("interval", _MISSING)
        seconds: float | None = None
        if interval is not _MISSING and interval is not None:
            seconds = _as_float(interval)
            if seconds is None:
                return _err("'interval' must be a positive number of seconds", 400)
        enabled = payload.get("enabled", _MISSING)
        if enabled is not _MISSING and not isinstance(enabled, bool):
            return _err("'enabled' must be true or false", 400)

        tags = payload.get("tags")
        note = payload.get("note")
        added = [
            _watch_row(
                watchlist.add(
                    handle,
                    interval=seconds,
                    tags=_str_list(tags) if tags is not None else None,
                    note=None if note is None else str(note),
                    enabled=True if enabled is _MISSING else bool(enabled),
                )
            )
            for handle in handles
        ]
        return JSONResponse({"added": added}, 201)

    @router.patch("/watchlist/{handle}")
    async def patch_watchlist(handle: str, request: Request) -> Response:
        """Change one account's `interval` (null clears the override) or `enabled` flag.

        Both are validated rather than coerced: `{"interval": "soon"}` silently
        clearing the override, or `{"enabled": 0}` enabling the account, would
        be the opposite of what the caller asked for, reported as success.
        """
        payload = await _body(request)
        if payload is None:
            return _err("expected a JSON object", 400)
        if watchlist.get(handle) is None:
            return _err("not watched", 404)

        interval = payload.get("interval", _MISSING)
        if interval is not _MISSING and interval is not None:
            seconds = _as_float(interval)
            if seconds is None:
                return _err("'interval' must be a positive number of seconds or null", 400)
        enabled = payload.get("enabled", _MISSING)
        if enabled is not _MISSING and not isinstance(enabled, bool):
            return _err("'enabled' must be true or false", 400)

        if interval is not _MISSING:
            watchlist.set_interval(handle, None if interval is None else _as_float(interval))
        if enabled is not _MISSING:
            watchlist.enable(handle, enabled)

        entry = watchlist.get(handle)
        if entry is None:  # pragma: no cover - it existed a statement ago
            return _err("not watched", 404)
        return JSONResponse(_watch_row(entry))

    @router.delete("/watchlist/{handle}")
    async def remove_watchlist(handle: str) -> Response:
        """Stop watching an account. `removed` is false when it was not watched."""
        return JSONResponse({"removed": watchlist.remove(handle)})

    @router.post("/watchlist/{handle}/tune")
    async def tune_watchlist(handle: str) -> Response:
        """Set this account's cadence from its own measured posting rate.

        Never slower than the global default, and never slower than the current
        interval once a rollover has proven posts are being lost.
        """
        entry = watchlist.get(handle)
        if entry is None:
            return _err("not watched", 404)
        suggested = watchlist.suggest_interval(
            entry.handle, dataset, ceiling=settings.watch_interval
        )
        if suggested is None:
            return JSONResponse(
                {
                    "handle": entry.handle,
                    "interval": None,
                    "reason": "not enough recent history",
                }
            )
        reason = "measured from recent posting rate"
        if entry.rollovers:
            # A rollover is measured evidence of lost posts; the suggestion is
            # an estimate from a sparse sample. Evidence wins.
            current = entry.interval or settings.watch_interval
            chosen = min(suggested, current)
            if chosen != suggested:
                reason = (
                    f"kept {chosen:.0f}s: suggested {suggested:.0f}s but "
                    f"{entry.rollovers} rollover(s) seen"
                )
            suggested = chosen
        watchlist.set_interval(entry.handle, suggested)
        return JSONResponse(
            {"handle": entry.handle, "interval": suggested, "reason": reason}
        )

    # ----------------------------------------------------------- collection

    @router.post("/jobs/pull")
    async def job_pull(request: Request) -> Response:
        """Collect posts: `{"targets": ["@handle", "<url or id>"], "refresh": false, "deep": false}`."""
        payload = await _body(request)
        if payload is None:
            return _err("expected {'targets': [...]}", 400)
        # `pull()` re-splits every element on whitespace, so counting list
        # items would let one string carry an unbounded crawl past max_batch.
        targets = [tok for item in _str_list(payload.get("targets")) for tok in item.split()]
        if not targets:
            return _err("expected {'targets': [...]}", 400)
        if len(targets) > settings.max_batch:
            return _err(f"at most {settings.max_batch} targets per job", 400)
        refresh = _as_bool(payload.get("refresh"), False)
        deep = _as_bool(payload.get("deep"), False)

        async def run(job: Job) -> dict[str, Any]:
            report = await pull(targets, settings, dataset, refresh=refresh, deep=deep)
            return report.to_dict()

        return _submit(
            "pull", {"targets": targets, "refresh": refresh, "deep": deep}, run
        )

    @router.post("/jobs/search")
    async def job_search(request: Request) -> Response:
        """Search X and store the results.

        Either `{"query": "raw operator syntax"}` or the structured form
        (`text`, `since`, `until`, `from`, `to`, `lang`, `media`, `min_likes`,
        `replies`). Requires a stored account: search is unreachable with a
        guest token.
        """
        payload = await _body(request)
        if payload is None:
            return _err("expected {'query': '...'} or {'text': '...'}", 400)

        from .search import build_query

        raw = payload.get("query")
        if isinstance(raw, str) and raw.strip():
            query = raw.strip()
        else:
            query = build_query(
                str(payload.get("text") or ""),
                since=payload.get("since"),
                until=payload.get("until"),
                from_user=payload.get("from"),
                to_user=payload.get("to"),
                lang=payload.get("lang"),
                has_media=_as_bool(payload.get("media"), False),
                min_likes=_as_int(payload.get("min_likes")),
                replies=_as_bool(payload.get("replies"), True),
            )
        if not query.strip():
            return _err("expected {'query': '...'} or {'text': '...'}", 400)

        product = str(payload.get("product") or settings.search_product)
        if product.lower() not in {p.lower() for p in SEARCH_PRODUCTS}:
            return _err(
                f"unsupported product {product!r}; expected one of "
                + ", ".join(SEARCH_PRODUCTS),
                400,
            )
        limit = _as_int(payload.get("limit"), settings.search_limit) or settings.search_limit
        limit = max(1, min(int(limit), SEARCH_LIMIT_MAX))
        should_store = _as_bool(payload.get("store"), True)

        async def run(job: Job) -> dict[str, Any]:
            from .cli import _session_for
            from .pull import _store, client_for
            from .search import search

            session, _account_store = _session_for(settings)
            if not hasattr(session, "available") or not session.available:
                # A guest token cannot reach SearchTimeline; degrading to one
                # silently would return an empty result set that looks like
                # "no matches".
                raise RuntimeError(
                    "search requires an account: add one with `magpie accounts add`"
                )
            report = PullReport()
            async with client_for(settings) as client:
                posts, error = await search(
                    client, session, query, settings, product=product, limit=limit
                )
            job.progress = {"found": len(posts)}
            if should_store:
                _store(dataset, posts, report)
            return {
                "query": query,
                "product": product,
                "found": len(posts),
                "new": report.new_posts,
                "updated": report.updated_posts,
                "stored": should_store,
                "error": error,
            }

        return _submit(
            "search",
            {"query": query, "product": product, "limit": limit, "store": should_store},
            run,
        )

    @router.post("/jobs/thread")
    async def job_thread(request: Request) -> Response:
        """Crawl outward from one post: `{"tweet_id": "...", "depth": 2, "max_posts": 200}`."""
        payload = await _body(request)
        if payload is None:
            return _err("expected {'tweet_id': '...'}", 400)
        tweet_id = str(payload.get("tweet_id") or "").strip()
        if not tweet_id:
            return _err("expected {'tweet_id': '...'}", 400)
        # A crawl budget is an outbound request count against X from this
        # server's IP, so the caller gets a ceiling, not just a default.
        depth = _as_int(payload.get("depth"))
        if depth is not None and not 0 <= depth <= THREAD_DEPTH_MAX:
            return _err(f"'depth' must be between 0 and {THREAD_DEPTH_MAX}", 400)
        max_posts = _as_int(payload.get("max_posts"))
        if max_posts is not None and not 1 <= max_posts <= THREAD_POSTS_MAX:
            return _err(f"'max_posts' must be between 1 and {THREAD_POSTS_MAX}", 400)

        async def run(job: Job) -> dict[str, Any]:
            report = await expand_thread(
                tweet_id, settings, dataset, depth=depth, max_posts=max_posts
            )
            return report.to_dict()

        return _submit(
            "thread", {"tweet_id": tweet_id, "depth": depth, "max_posts": max_posts}, run
        )

    @router.post("/jobs/capture")
    async def job_capture(request: Request) -> Response:
        """Build evidence packages: `{"links": ["https://x.com/..."]}`.

        A capture renders PDF/PNG and may contact a timestamp authority, so it
        runs as a job rather than inside the request.
        """
        payload = await _body(request)
        if payload is None:
            return _err("expected {'links': [...]}", 400)
        links = _str_list(payload.get("links"))
        if not links:
            return _err("expected {'links': [...]}", 400)
        if len(links) > settings.max_batch:
            return _err(f"at most {settings.max_batch} links per job", 400)

        async def run(job: Job) -> dict[str, Any]:
            manifests = await capture_many(
                links, settings, store, operator=settings.operator
            )
            return {"manifests": [m.to_dict() for m in manifests]}

        return _submit("capture", {"links": links}, run)

    @router.get("/jobs")
    async def list_jobs(
        status: str | None = Query(None, pattern=f"^({'|'.join(STATUSES)})$"),
        kind: str | None = Query(None, pattern=f"^({'|'.join(JOB_KINDS)})$"),
        limit: int = Query(50, ge=1, le=500),
    ) -> Response:
        """Submitted jobs, newest first. Lives in this process only."""
        rows = jobs.list(status=status, kind=kind, limit=limit)
        return JSONResponse({"rows": [job.to_dict() for job in rows]})

    @router.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> Response:
        """One job: status, progress, and `result` once it completes."""
        job = jobs.get(job_id)
        if job is None:
            return _err("unknown job", 404)
        return JSONResponse(job.to_dict())

    @router.delete("/jobs/{job_id}")
    async def cancel_job(job_id: str) -> Response:
        """Cancel a queued or running job. Already-finished jobs report false."""
        return JSONResponse({"cancelled": await jobs.cancel(job_id)})

    # -------------------------------------------------------------- monitor

    @router.get("/monitor")
    async def monitor() -> Response:
        """Is every watched account actually being polled?

        Same rule as `magpie health`, as data rather than an exit code: an
        account is stale when it has never been polled or its last poll is
        older than three of its own intervals (minimum 180 s).
        """
        now = datetime.now(timezone.utc).timestamp()
        accounts: list[dict[str, Any]] = []
        stale: list[str] = []
        for entry in watchlist.list(enabled_only=True):
            interval = entry.interval or settings.watch_interval
            grace = max(3.0 * interval, 180.0)
            last = _epoch(entry.last_poll_utc)
            age = None if last is None else now - last
            is_stale = age is None or age > grace
            if is_stale:
                stale.append(entry.handle)
            accounts.append(
                {
                    "handle": entry.handle,
                    "interval": interval,
                    "last_poll_utc": entry.last_poll_utc,
                    "age_s": None if age is None else round(age, 1),
                    "stale": is_stale,
                    "rollovers": entry.rollovers,
                    "last_error": entry.last_error,
                }
            )
        stats = watchlist.stats()
        return JSONResponse(
            {
                "ok": not stale,
                "accounts": accounts,
                "stale": stale,
                "watchlist": stats,
            }
        )

    # ------------------------------------------------------------- captures

    @router.get("/captures")
    async def list_captures(
        q: str | None = None,
        user: str | None = None,
        date: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        limit: int = Query(50, ge=1, le=500),
        offset: int = Query(0, ge=0),
    ) -> Response:
        """Indexed evidence packages."""
        rows, total = store.search(
            q=q, user=user, date=date, tag=tag, status=status, limit=limit, offset=offset
        )
        return JSONResponse(
            {
                "total": total,
                "limit": limit,
                "offset": offset,
                "rows": [r.to_dict() for r in rows],
            }
        )

    @router.get("/captures/{folder}")
    async def get_capture(folder: str) -> Response:
        """One capture's manifest.json."""
        try:
            manifest = store.get_manifest(folder) if FOLDER_RE.match(folder or "") else None
        except ValueError:
            manifest = None
        if manifest is None:
            return _err("unknown capture", 404)
        return JSONResponse(manifest)

    @router.get("/captures/{folder}/verify")
    async def verify_capture(folder: str) -> Response:
        """Re-hash a package and compare it against its manifest.

        A package that is not there is a 404, not an `ok: false` verdict: the
        two mean different things and a client acting on integrity must be able
        to tell them apart.
        """
        if not FOLDER_RE.match(folder or ""):
            return _err("unknown capture", 404)
        try:
            if not store.package_path(folder).is_dir():
                return _err("unknown capture", 404)
            result = store.verify(folder)
        except ValueError:
            return _err("unknown capture", 404)
        return JSONResponse(result.to_dict())

    # ------------------------------------------------------------ openapi

    if settings.api_docs:
        from fastapi.openapi.docs import get_swagger_ui_html
        from fastapi.openapi.utils import get_openapi

        # The document only changes when the process does, and generating it
        # walks every route in the app - not something to redo per Swagger load.
        cached: dict[str, Any] = {}

        @router.get("/openapi.json", include_in_schema=False)
        async def openapi(request: Request) -> Response:
            """This app's OpenAPI document. Token-gated like everything else."""
            if not cached:
                cached.update(
                    get_openapi(
                        title=settings.site_name,
                        version=TOOL_VERSION,
                        routes=request.app.routes,
                    )
                )
            return JSONResponse(cached)

        @router.get("/docs", include_in_schema=False)
        async def docs() -> Response:
            """Swagger UI for this API.

            The page loads Swagger's JS/CSS from a CDN, so the docs (not the
            API) need internet access. In a browser it authenticates with the
            same `xw_token` cookie the HTML UI sets at /login - which is also
            why the asset URLs pin an exact build rather than FastAPI's
            floating `@5` range: a script here runs same-origin with that
            cookie, so a mutable third-party artifact would be a live key to
            the whole API.
            """
            return get_swagger_ui_html(
                openapi_url=f"{settings.base_path}/api/v1/openapi.json",
                title=f"{settings.site_name} API",
                swagger_js_url=f"{SWAGGER_CDN}/swagger-ui-bundle.js",
                swagger_css_url=f"{SWAGGER_CDN}/swagger-ui.css",
            )

    return router
