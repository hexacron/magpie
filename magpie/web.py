"""FastAPI application: capture form, evidence browser, JSON API.

Everything is server-rendered. The only client-side script in the whole app is
a three-line clipboard helper for the manifest hash.

Two auth gates (defects 8 and 9 of the original):

* ``require_write`` - when ``settings.auth_token`` is set, every mutating route
  and every ``/api/v1`` route needs the token.
* ``require_read``  - when ``settings.public_read`` is False, the same check is
  extended to every route except ``/healthz``, ``/static`` and ``/login``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import secrets
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, FastAPI, Form, Query, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask

from .api import build_api_router
from .capture import capture_many, load_timestamp
from .config import Settings, load_settings
from .dataset import Dataset
from .jobs import JobRegistry
from .models import TOOL_VERSION
from .store import Store
from .watchlist import Watchlist

log = logging.getLogger("magpie.web")

HERE = Path(__file__).resolve().parent
TEMPLATES_DIR = HERE / "templates"
STATIC_DIR = HERE / "static"

PER_PAGE = 24
RECENT_ON_INDEX = 8
COOKIE_NAME = "xw_token"

#: A capture folder is always ``<stamp>_<handle>_<id>``; nothing else is served.
FOLDER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,160}$")

#: The record is untrusted third-party HTML. It may show its own pictures and
#: its own inline CSS, and it may do nothing else - no fonts, no scripts, no
#: beacons, no outbound anything.
RECORD_CSP = (
    "default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'"
)

PRIMARY_FILES = (
    ("capture.html", "Record (HTML)"),
    ("capture.pdf", "Record (PDF)"),
    ("capture.png", "Record (PNG)"),
    ("manifest.json", "Manifest"),
    ("MANIFEST.sha256", "Manifest digest"),
    ("timestamp.tsr", "RFC 3161 token"),
    ("syndication.json", "Source: syndication"),
    ("fxtwitter.json", "Source: fxtwitter"),
    ("vxtwitter.json", "Source: vxtwitter"),
    ("x_page.html", "Source: x.com page"),
)

# --------------------------------------------------------------------------
# flag vocabulary
# --------------------------------------------------------------------------

# level: info (expected, explained), warn (look at it), error (capture is thin)
_FLAGS: dict[str, tuple[str, str, str]] = {
    "post_unavailable_everywhere": (
        "Post unavailable everywhere",
        "No source returned the post - deleted, suspended, protected, or it "
        "never existed. The package records the failed lookups.",
        "error",
    ),
    "text_differs_between_sources": (
        "Text differs between sources",
        "Two sources returned materially different text, after truncation was "
        "accounted for. Worth reading both copies side by side.",
        "warn",
    ),
    "author_differs_between_sources": (
        "Author differs between sources",
        "Sources disagree on the author - a rename between snapshots, or a "
        "redirect somewhere in the chain.",
        "warn",
    ),
    "created_at_differs_between_sources": (
        "Post timestamp differs between sources",
        "Sources disagree on when the post was created. Compare the raw source "
        "files before relying on the time.",
        "warn",
    ),
}

_FLAG_PREFIXES: dict[str, tuple[str, str, str]] = {
    # Defect 3: syndication truncates note tweets at 280 chars. The original
    # read that as evidence tampering. It is not a discrepancy.
    "source_truncated": (
        "Truncated copy from {v}",
        "A source returned a truncated copy - not a discrepancy. "
        "{v} caps long posts, so its text is a prefix of the full text.",
        "info",
    ),
    "source_error": (
        "{v} errored",
        "The {v} endpoint returned an error or could not be reached. Request "
        "and response headers are still recorded in the manifest.",
        "warn",
    ),
    "source_unavailable": (
        "{v} had no post",
        "The {v} endpoint answered but reported no such post.",
        "info",
    ),
    "counts_differ": (
        "Counter '{v}' differs",
        "Sources reported different values for {v}. Counters are sampled a few "
        "moments apart, so small gaps are normal; the manifest keeps every "
        "reported value with its source.",
        "info",
    ),
}


def flag_info(flag: str) -> dict[str, str]:
    """Human-readable label for one cross-check flag. Never raises."""
    raw = str(flag)
    known = _FLAGS.get(raw)
    if known:
        label, detail, level = known
        return {"flag": raw, "label": label, "detail": detail, "level": level}

    prefix, _, value = raw.partition(":")
    tmpl = _FLAG_PREFIXES.get(prefix)
    pretty_value = value.replace("_", " ") or "unknown"
    if tmpl:
        label, detail, level = tmpl
        return {
            "flag": raw,
            "label": label.format(v=pretty_value),
            "detail": detail.format(v=pretty_value),
            "level": level,
        }

    # Unknown flag: humanise rather than hide it.
    label = prefix.replace("_", " ").strip().capitalize() or raw
    if value:
        label = f"{label}: {pretty_value}"
    return {
        "flag": raw,
        "label": label,
        "detail": "Raised by the cross-check step; see manifest.json for the "
        "underlying comparison.",
        "level": "info",
    }


# --------------------------------------------------------------------------
# template filters
# --------------------------------------------------------------------------


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def fmt_dt(value: Any, fmt: str = "%Y-%m-%d %H:%M") -> str:
    dt = _parse_iso(value)
    if dt is None:
        return str(value or "-")
    return f"{dt.astimezone(timezone.utc).strftime(fmt)} UTC"


def fmt_date(value: Any) -> str:
    dt = _parse_iso(value)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d") if dt else "-"


def fmt_num(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "-"
    return f"{int(value):,}"


def short_hash(value: Any, head: int = 10, tail: int = 6) -> str:
    text = str(value or "")
    if len(text) <= head + tail + 1:
        return text or "-"
    return f"{text[:head]}\u2026{text[-tail:]}"


class _NotFound(Exception):
    """Raised inside helpers; turned into a 404 page by the route."""


class _AuthRequired(Exception):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


def _unlink(path: Path) -> None:
    try:
        path.unlink()
    except OSError:  # pragma: no cover - best effort cleanup
        pass


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    settings = settings or load_settings()
    settings.ensure_dirs()
    store = store if store is not None else Store(settings)
    # The monitored dataset is the primary workflow; the capture store is the
    # optional evidence path. Both are served from one UI.
    dataset = Dataset(settings)
    watchlist = Watchlist(settings)
    jobs = JobRegistry(settings)
    base = settings.base_path

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    templates.env.filters["dt"] = fmt_dt
    templates.env.filters["date"] = fmt_date
    templates.env.filters["num"] = fmt_num
    templates.env.filters["shorthash"] = short_hash

    log.info(
        "magpie %s | data=%s | sources=%s | media=%s | render=%s | tsa=%s | "
        "auth=%s | public_read=%s | base_path=%r",
        TOOL_VERSION,
        settings.data_dir,
        ",".join(settings.sources) or "none",
        "on" if settings.download_media else "off",
        "on" if settings.render else "off",
        settings.tsa_url if settings.tsa else "off",
        "token" if settings.auth_token else "OPEN",
        settings.public_read,
        base,
    )
    if not settings.auth_token:
        log.warning(
            "MAGPIE_AUTH_TOKEN is not set: capture, tag and delete routes are open to "
            "anyone who can reach this server. Set MAGPIE_AUTH_TOKEN before exposing it."
        )
    elif not settings.public_read:
        log.info("Private instance: reads require the token as well.")

    # ---------------------------------------------------------------- auth

    async def _supplied_token(request: Request) -> str | None:
        header = request.headers.get("authorization", "")
        if header[:7].lower() == "bearer ":
            candidate = header[7:].strip()
            if candidate:
                return candidate
        for value in (request.headers.get("x-auth-token"), request.cookies.get(COOKIE_NAME)):
            if value and value.strip():
                return value.strip()
        ctype = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        if ctype in ("application/x-www-form-urlencoded", "multipart/form-data"):
            try:
                form = await request.form()  # cached by starlette for the route
            except Exception:  # pragma: no cover - malformed body
                return None
            value = form.get("token")
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    async def _is_authed(request: Request) -> bool:
        expected = settings.auth_token
        if not expected:
            return True
        supplied = await _supplied_token(request)
        return bool(supplied) and secrets.compare_digest(supplied, expected)

    def _exempt(path: str) -> bool:
        return (
            path == f"{base}/healthz"
            or path == f"{base}/login"
            or path.startswith(f"{base}/static")
        )

    async def require_read(request: Request) -> None:
        """Gate every route when the instance is private (defect 9)."""
        request.state.authed = await _is_authed(request)
        if settings.public_read or _exempt(request.url.path):
            return
        if not request.state.authed:
            raise _AuthRequired("read")

    async def require_write(request: Request) -> None:
        """Gate mutations and the JSON API (defects 8 and 9)."""
        if not settings.auth_token:
            return
        if not getattr(request.state, "authed", False) and not await _is_authed(request):
            raise _AuthRequired("write")

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        # Jobs are in-process: a shutdown that leaves them running holds the
        # loop open and loses their state anyway. Cancel and wait.
        yield
        await jobs.shutdown()

    app = FastAPI(
        title=settings.site_name,
        version=TOOL_VERSION,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    app.mount(f"{base}/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
    router = APIRouter(prefix=base, dependencies=[Depends(require_read)])
    write = [Depends(require_write)]

    # ------------------------------------------------------------ rendering

    def _page(
        request: Request,
        template: str,
        *,
        status_code: int = 200,
        **extra: Any,
    ) -> Response:
        context: dict[str, Any] = {
            "settings": settings,
            "url": settings.url_for,
            "tool_version": TOOL_VERSION,
            "flag_info": flag_info,
            "authed": bool(getattr(request.state, "authed", False)),
            "auth_open": settings.auth_token is None,
            "needs_token": bool(settings.auth_token)
            and not bool(getattr(request.state, "authed", False)),
            "path": request.url.path,
            "error": None,
            "message": None,
        }
        context.update(extra)
        return templates.TemplateResponse(
            request=request, name=template, context=context, status_code=status_code
        )

    def _stats() -> dict[str, Any]:
        try:
            return dict(store.stats())
        except Exception as exc:  # pragma: no cover - a broken index must not 500
            log.warning("stats failed: %s", exc)
            return {}

    def _index_page(request: Request, **extra: Any) -> Response:
        recent = [] if extra.pop("blank", False) else store.recent(RECENT_ON_INDEX)
        return _page(request, "index.html", recent=recent, stats=_stats(), **extra)

    def _not_found(request: Request, what: str) -> Response:
        return _index_page(request, error=what, status_code=404)

    # ------------------------------------------------------------- filesystem

    def _package_dir(folder: str) -> Path:
        if not FOLDER_RE.match(folder or ""):
            raise _NotFound(folder)
        try:
            pkg = store.package_path(folder).resolve()
        except ValueError as exc:  # store rejects separators, dots, NUL
            raise _NotFound(folder) from exc
        if not pkg.is_dir():
            raise _NotFound(folder)
        return pkg

    def _safe_file(folder: str, rel: str) -> Path:
        """Resolve ``rel`` inside the package. Anything outside it is a 404."""
        pkg = _package_dir(folder)
        candidate = (pkg / rel).resolve()
        if candidate != pkg and pkg not in candidate.parents:
            raise _NotFound(rel)
        if not candidate.is_file():
            raise _NotFound(rel)
        return candidate

    def _file_response(path: Path) -> FileResponse:
        headers = {"X-Content-Type-Options": "nosniff"}
        if path.suffix.lower() in (".html", ".htm", ".svg"):
            headers["Content-Security-Policy"] = RECORD_CSP
        return FileResponse(path, headers=headers)

    def _listing(pkg: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
        hashes = manifest.get("files") or {}
        labels = dict(PRIMARY_FILES)
        out: list[dict[str, Any]] = []
        for path in sorted(pkg.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(pkg).as_posix()
            try:
                size = path.stat().st_size
            except OSError:
                size = 0
            out.append(
                {
                    "rel": rel,
                    "label": labels.get(rel, rel),
                    "size": size,
                    "sha256": hashes.get(rel),
                    "primary": rel in labels,
                }
            )
        out.sort(key=lambda f: (not f["primary"], f["rel"]))
        return out

    def _manifest_hash(pkg: Path, row: Any) -> str:
        recorded = getattr(row, "manifest_sha256", "") or ""
        if recorded:
            return recorded
        try:
            return (pkg / "MANIFEST.sha256").read_text(encoding="utf-8").split()[0]
        except (OSError, IndexError):
            return ""

    def _detail_page(
        request: Request,
        folder: str,
        *,
        verify: Any = None,
        status_code: int = 200,
        **extra: Any,
    ) -> Response:
        try:
            pkg = _package_dir(folder)
        except _NotFound:
            return _not_found(request, f"No capture named {folder!r}.")
        manifest = store.get_manifest(folder)
        if manifest is None:
            return _not_found(request, f"Capture {folder!r} has no manifest.json.")
        row = store.get_row(folder)
        crosscheck = manifest.get("crosscheck") or {}
        flags = crosscheck.get("flags") or (getattr(row, "flags", None) or [])
        tweet_id = manifest.get("tweet_id")
        return _page(
            request,
            "detail.html",
            status_code=status_code,
            folder=folder,
            manifest=manifest,
            row=row,
            post=manifest.get("post") or {},
            crosscheck=crosscheck,
            flags=flags,
            sources=manifest.get("sources") or [],
            render=manifest.get("render") or {},
            # manifest.json is sealed before the RFC 3161 token is requested, so
            # it only carries the request parameters. The outcome lives in the
            # timestamp.json sidecar; without this the panel reads "FAILED" on a
            # capture whose token actually verified.
            stamp=load_timestamp(pkg) or manifest.get("timestamp") or {},
            version=manifest.get("version") or {},
            warnings=manifest.get("warnings") or [],
            versions=store.versions(tweet_id) if tweet_id else [],
            meta=store.meta(folder),
            files=_listing(pkg, manifest),
            manifest_sha256=_manifest_hash(pkg, row),
            has_record=(pkg / "capture.html").is_file(),
            verify=verify,
            **extra,
        )

    # ------------------------------------------------------------------ HTML

    @router.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> Response:
        return _index_page(request)

    @router.post("/capture", response_class=HTMLResponse, dependencies=write)
    async def do_capture(
        request: Request,
        links: str = Form(""),
        token: str | None = Form(None),
    ) -> Response:
        text = (links or "").strip()
        if not text:
            return _index_page(
                request,
                error="Paste at least one post URL or numeric id.",
                status_code=400,
            )
        try:
            manifests = await capture_many(text, settings, store, operator=settings.operator)
        except Exception as exc:  # a bad batch must not take the server down
            log.exception("capture batch failed")
            return _index_page(request, error=f"Capture failed: {exc}", status_code=200)
        done = [m for m in manifests if getattr(m, "folder", "")]
        if len(done) == 1:
            return RedirectResponse(settings.url_for(f"/capture/{done[0].folder}"), 303)
        if not done:
            return _index_page(
                request,
                error="Nothing was captured - check the input format.",
                status_code=200,
            )
        return RedirectResponse(settings.url_for("/captures"), 303)

    def _safe(fn) -> dict[str, Any]:
        """Stats are decoration; a failure must not blank the page."""
        try:
            return fn() or {}
        except Exception:  # pragma: no cover - sqlite hiccup
            return {}

    @router.get("/posts", response_class=HTMLResponse)
    async def posts_view(
        request: Request,
        q: str | None = None,
        user: str | None = None,
        since: str | None = None,
        until: str | None = None,
        media: int | None = None,
        page: int = Query(1, ge=1, le=100000),
    ) -> Response:
        """Browse the monitored dataset - the output of `magpie monitor`."""
        filters = {"q": q, "user": user, "since": since, "until": until}
        rows, total = dataset.posts(
            q=q,
            handle=user,
            since=since,
            until=until,
            has_media=True if media else None,
            limit=PER_PAGE,
            offset=(page - 1) * PER_PAGE,
        )
        pages = max(1, ceil(total / PER_PAGE)) if total else 1

        def page_url(n: int) -> str:
            query = {k: v for k, v in filters.items() if v}
            if media:
                query["media"] = "1"
            if n > 1:
                query["page"] = str(n)
            suffix = f"?{urlencode(query)}" if query else ""
            return settings.url_for(f"/posts{suffix}")

        return _page(
            request,
            "posts.html",
            rows=rows,
            total=total,
            page=page,
            pages=pages,
            page_url=page_url,
            filters={**filters, "media": media},
            stats=_safe(dataset.stats),
        )

    @router.get("/accounts", response_class=HTMLResponse)
    async def accounts_view(request: Request) -> Response:
        """Watchlist state next to what each account has actually produced."""
        entries = [e.to_dict() for e in watchlist.list()]
        profiles = {u.get("screen_name"): u for u in dataset.users(limit=500)}
        for entry in entries:
            handle = (entry.get("handle") or "").lower()
            entry["profile"] = profiles.get(handle) or {}
            rows, count = dataset.posts(handle=handle, limit=1)
            entry["stored_posts"] = count
            entry["latest"] = rows[0] if rows else None
            entry["effective_interval"] = entry.get("interval") or settings.watch_interval
        entries.sort(key=lambda e: e.get("stored_posts") or 0, reverse=True)
        return _page(
            request,
            "accounts.html",
            entries=entries,
            default_interval=settings.watch_interval,
            stats=_safe(dataset.stats),
        )

    @router.get("/captures", response_class=HTMLResponse)
    async def captures(
        request: Request,
        q: str | None = None,
        user: str | None = None,
        date: str | None = None,
        tag: str | None = None,
        status: str | None = None,
        page: int = Query(1, ge=1, le=100000),
    ) -> Response:
        filters = {"q": q, "user": user, "date": date, "tag": tag, "status": status}
        rows, total = store.search(
            **filters, limit=PER_PAGE, offset=(page - 1) * PER_PAGE  # type: ignore[arg-type]
        )
        pages = max(1, ceil(total / PER_PAGE)) if total else 1

        def page_url(n: int) -> str:
            query = {k: v for k, v in filters.items() if v}
            if n > 1:
                query["page"] = str(n)
            suffix = f"?{urlencode(query)}" if query else ""
            return settings.url_for(f"/captures{suffix}")

        return _page(
            request,
            "captures.html",
            rows=rows,
            total=total,
            page=page,
            pages=pages,
            filters=filters,
            page_url=page_url,
        )

    @router.get("/capture/{folder}", response_class=HTMLResponse)
    async def detail(request: Request, folder: str) -> Response:
        return _detail_page(request, folder)

    @router.get("/capture/{folder}/verify")
    async def verify_capture(request: Request, folder: str) -> Response:
        try:
            _package_dir(folder)
        except _NotFound:
            if "application/json" in request.headers.get("accept", ""):
                return JSONResponse({"detail": "unknown capture"}, 404)
            return _not_found(request, f"No capture named {folder!r}.")
        result = store.verify(folder)
        if "application/json" in request.headers.get("accept", ""):
            return JSONResponse(result.to_dict())
        return _detail_page(request, folder, verify=result)

    @router.post("/capture/{folder}/meta", dependencies=write)
    async def set_meta(
        request: Request,
        folder: str,
        tags: str = Form(""),
        note: str = Form(""),
        token: str | None = Form(None),
    ) -> Response:
        try:
            _package_dir(folder)
        except _NotFound:
            return _not_found(request, f"No capture named {folder!r}.")
        parsed = [t.strip().lstrip("#") for t in re.split(r"[,\n]+", tags or "")]
        store.set_meta(folder, [t for t in parsed if t], (note or "").strip())
        return RedirectResponse(settings.url_for(f"/capture/{folder}"), 303)

    @router.post("/capture/{folder}/delete", dependencies=write)
    async def delete_capture(
        request: Request, folder: str, token: str | None = Form(None)
    ) -> Response:
        try:
            _package_dir(folder)
        except _NotFound:
            return _not_found(request, f"No capture named {folder!r}.")
        store.delete(folder)
        return RedirectResponse(settings.url_for("/captures"), 303)

    # ----------------------------------------------------------------- files

    @router.get("/capture/{folder}/view")
    async def view_record(request: Request, folder: str) -> Response:
        try:
            path = _safe_file(folder, "capture.html")
        except _NotFound:
            return _not_found(request, f"No rendered record for {folder!r}.")
        return FileResponse(
            path,
            media_type="text/html",
            headers={
                "Content-Security-Policy": RECORD_CSP,
                "X-Content-Type-Options": "nosniff",
            },
        )

    @router.get("/capture/{folder}/zip")
    async def download_zip(request: Request, folder: str) -> Response:
        try:
            _package_dir(folder)
        except _NotFound:
            return _not_found(request, f"No capture named {folder!r}.")
        try:
            archive = store.make_zip(folder)
        except Exception as exc:
            log.exception("zip failed for %s", folder)
            return _detail_page(request, folder, error=f"Could not build zip: {exc}")
        return FileResponse(
            archive,
            media_type="application/zip",
            filename=f"{folder}.zip",
            background=BackgroundTask(_unlink, Path(archive)),
        )

    @router.get("/capture/{folder}/file/{path:path}")
    async def package_file(request: Request, folder: str, path: str) -> Response:
        try:
            return _file_response(_safe_file(folder, path))
        except _NotFound:
            return _not_found(request, f"No file {path!r} in {folder!r}.")

    # The record references its media by relative path (defect 6: nothing is
    # base64-inlined any more), so `media/photo_01.jpg` seen from
    # /capture/<folder>/view resolves here. Registered last, so every named
    # route above still wins.
    @router.get("/capture/{folder}/{path:path}")
    async def package_asset(request: Request, folder: str, path: str) -> Response:
        try:
            return _file_response(_safe_file(folder, path))
        except _NotFound:
            return _not_found(request, f"No file {path!r} in {folder!r}.")

    @router.get("/export/json")
    async def export_json() -> Response:
        body = json.dumps(store.export_rows(), indent=2, ensure_ascii=False)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        return Response(
            body,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="magpie-{stamp}.json"'
            },
        )

    @router.get("/export/csv")
    async def export_csv() -> Response:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        return Response(
            store.export_csv(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="magpie-{stamp}.csv"'
            },
        )

    @router.get("/healthz")
    async def healthz() -> Response:
        try:
            _, total = store.search(limit=1)
        except Exception as exc:  # pragma: no cover
            return JSONResponse({"ok": False, "error": str(exc), "version": TOOL_VERSION}, 500)
        return JSONResponse({"ok": True, "captures": total, "version": TOOL_VERSION})

    # ----------------------------------------------------------------- login

    def _safe_next(value: str | None) -> str:
        if not value or not value.startswith("/") or value.startswith("//"):
            return settings.url_for("/")
        return value

    def _secure_cookie(request: Request) -> bool:
        if request.url.scheme == "https":
            return True
        if settings.trust_proxy:
            return request.headers.get("x-forwarded-proto", "").split(",")[0].strip() == "https"
        return False

    @router.get("/login", response_class=HTMLResponse)
    async def login_form(request: Request, next: str | None = None) -> Response:
        return _index_page(request, blank=True, login=True, next_url=_safe_next(next))

    @router.post("/login", response_class=HTMLResponse)
    async def login(
        request: Request, token: str = Form(""), next: str | None = Form(None)
    ) -> Response:
        target = _safe_next(next)
        if not settings.auth_token:
            return RedirectResponse(target, 303)
        if not token or not secrets.compare_digest(token.strip(), settings.auth_token):
            return _index_page(
                request,
                blank=True,
                login=True,
                next_url=target,
                error="Wrong token.",
                status_code=401,
            )
        response = RedirectResponse(target, 303)
        response.set_cookie(
            COOKIE_NAME,
            token.strip(),
            httponly=True,
            samesite="lax",
            secure=_secure_cookie(request),
            max_age=60 * 60 * 24 * 30,
            path=f"{base}/" if base else "/",
        )
        return response

    @router.post("/logout")
    async def logout(request: Request) -> Response:
        response = RedirectResponse(settings.url_for("/"), 303)
        response.delete_cookie(COOKIE_NAME, path=f"{base}/" if base else "/")
        return response

    app.include_router(router)
    app.include_router(
        build_api_router(
            settings=settings,
            store=store,
            dataset=dataset,
            watchlist=watchlist,
            jobs=jobs,
            write=write,
        )
    )

    @app.exception_handler(_AuthRequired)
    async def _auth_handler(request: Request, exc: _AuthRequired) -> Response:
        wants_html = "text/html" in request.headers.get("accept", "")
        if exc.kind == "read" and request.method == "GET" and wants_html:
            target = settings.url_for(f"/login?next={request.url.path}")
            return RedirectResponse(target, 303)
        return JSONResponse(
            {"detail": "authentication required"},
            401,
            headers={"WWW-Authenticate": 'Bearer realm="magpie"'},
        )

    return app


app_factory = create_app  # uvicorn --factory magpie.web:app_factory
