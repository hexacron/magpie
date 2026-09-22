"""Build the evidence record (capture.html) and render it to PDF/PNG.

Two rules shape this module:

* The record is a *document about a capture*, not a mirror of x.com.  It states
  what each endpoint returned, where the sources disagreed, and what was
  hashed -- and says so plainly in the footer.
* The record never phones home.  Small images are inlined as data URIs, larger
  ones are referenced by relative path, video is *always* referenced (never
  base64'd), and the renderer aborts every non-``file://`` request.
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import Settings
from .models import Manifest, RenderResult

PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / "templates"
STATIC_DIR = PACKAGE_DIR / "static"

_METRIC_ORDER = ("replies", "retweets", "quotes", "likes", "bookmarks", "views")
_METRIC_LABELS = {
    "replies": "Replies",
    "retweets": "Reposts",
    "quotes": "Quote posts",
    "likes": "Likes",
    "bookmarks": "Bookmarks",
    "views": "Views",
}
_IMAGE_MIME = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

_environment: Environment | None = None


def _env() -> Environment:
    global _environment
    if _environment is None:
        _environment = Environment(
            loader=FileSystemLoader(str(TEMPLATES_DIR)),
            autoescape=select_autoescape(default_for_string=True, default=True),
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _environment


# --------------------------------------------------------------------------
# formatting helpers
# --------------------------------------------------------------------------


def _num(value: Any) -> str:
    return f"{value:,}" if isinstance(value, int) and not isinstance(value, bool) else "-"


def _bytes_h(value: int | None) -> str:
    if not isinstance(value, int):
        return "-"
    if value < 1024:
        return f"{value} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KB"
    return f"{value / (1024 * 1024):.1f} MB"


def _agreement(flag: Any, sources: Any = None) -> str:
    if flag is True:
        return "agreed"
    if flag is False:
        return "SOURCES DISAGREE"
    if isinstance(sources, str) and sources:
        return f"single source: {sources}"
    if isinstance(sources, dict) and len(sources) == 1:
        return f"single source: {next(iter(sources))}"
    return "single source"


def _values_str(values: Any) -> str:
    if not isinstance(values, dict) or not values:
        return "-"
    parts = []
    for key in sorted(values):
        value = values[key]
        parts.append(f"{key}={_num(value) if isinstance(value, int) else value}")
    return "  ".join(parts)


def _image_src(pkg_dir: Path, rel: str | None, settings: Settings) -> str | None:
    """Data URI when the file is small enough, relative path otherwise."""
    if not rel:
        return None
    path = pkg_dir / rel
    if not path.is_file():
        return None
    mime = _IMAGE_MIME.get(path.suffix.lower())
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if mime and size <= settings.inline_image_bytes:
        try:
            payload = base64.b64encode(path.read_bytes()).decode("ascii")
        except OSError:
            return rel
        return f"data:{mime};base64,{payload}"
    return rel


# --------------------------------------------------------------------------
# view model
# --------------------------------------------------------------------------


def _post_context(manifest: Manifest, pkg_dir: Path, settings: Settings) -> dict[str, Any] | None:
    post = manifest.post
    if post is None:
        return None

    media: list[dict[str, Any]] = []
    for item in post.media:
        download = item.download
        thumb_src = _image_src(pkg_dir, item.thumb_local_file, settings)
        entry: dict[str, Any] = {
            "kind": item.type,
            "alt": item.alt,
            "width": item.width,
            "height": item.height,
            "duration_s": item.duration_s,
            "file": item.local_file,
            "thumb_file": item.thumb_local_file,
            "thumb_src": thumb_src,
            "remote_url": item.best_url,
            "sha256": download.sha256 if download else None,
            "size": _bytes_h(download.bytes if download else None),
            "error": download.error if download and not download.ok else None,
            "ocr_text": item.ocr_text,
            "is_video": item.type in ("video", "gif"),
        }
        # Images inline when small; video is referenced, never embedded.
        entry["src"] = None if entry["is_video"] else _image_src(pkg_dir, item.local_file, settings)
        media.append(entry)

    return {
        "screen_name": post.screen_name,
        "name": post.name,
        "text": post.text,
        "text_source": post.text_source,
        "created_at_utc": post.created_at_utc,
        "created_at_display": post.created_at_display,
        "lang": post.lang,
        "community_note": post.community_note,
        "avatar_src": _image_src(pkg_dir, post.avatar_local_file, settings),
        "avatar_url": post.avatar_url,
        "initials": (post.name or post.screen_name or "?")[:1].upper(),
        "media": media,
        "quoted_id": post.quoted_id,
        "quoted_screen_name": post.quoted_screen_name,
        "reply_to_id": post.reply_to_id,
        "reply_to_screen_name": post.reply_to_screen_name,
        "possibly_sensitive": post.possibly_sensitive,
        "blue_verified": post.blue_verified,
        "available_sources": post.available_sources,
        "ocr_text": post.ocr_text,
    }


def _facts(manifest: Manifest) -> list[tuple[str, str]]:
    post = manifest.post
    handle = (post.screen_name if post else None) or manifest.screen_name
    canonical = (
        f"https://x.com/{handle}/status/{manifest.tweet_id}"
        if handle and manifest.tweet_id
        else (f"https://x.com/i/status/{manifest.tweet_id}" if manifest.tweet_id else "-")
    )
    rows = [
        ("Author", f"@{handle}" if handle else "unknown"),
        ("Display name", (post.name if post else None) or "-"),
        ("Post id", manifest.tweet_id or "-"),
        ("Posted (UTC, normalised)", (post.created_at_utc if post else None) or "-"),
        ("Posted (as reported)", (post.created_at_display if post else None) or "-"),
        ("Language", (post.lang if post else None) or "-"),
        ("Canonical URL", canonical),
        ("Submitted input", manifest.input or "-"),
        ("Capture status", manifest.status),
    ]
    if post and post.reply_to_id:
        rows.append(
            (
                "In reply to",
                f"@{post.reply_to_screen_name} / {post.reply_to_id}"
                if post.reply_to_screen_name
                else post.reply_to_id,
            )
        )
    if post and post.quoted_id:
        rows.append(
            (
                "Quotes",
                f"@{post.quoted_screen_name} / {post.quoted_id}"
                if post.quoted_screen_name
                else post.quoted_id,
            )
        )
    return rows


def _engagement(manifest: Manifest) -> list[dict[str, Any]]:
    post = manifest.post
    if post is None or not post.counts:
        return []
    names = [m for m in _METRIC_ORDER if m in post.counts]
    names += [m for m in sorted(post.counts) if m not in _METRIC_ORDER]
    rows = []
    for metric in names:
        source = post.count_sources.get(metric)
        agreement = post.count_agreement.get(metric)
        rows.append(
            {
                "label": _METRIC_LABELS.get(metric, metric.replace("_", " ").title()),
                "value": _num(post.counts.get(metric)),
                "source": source or "-",
                "agreement": _agreement(agreement, source),
                "single": agreement is None,
                "conflict": agreement is False,
            }
        )
    return rows


def _sources(manifest: Manifest) -> list[dict[str, Any]]:
    rows = []
    for source in manifest.sources:
        headers = source.headers or {}
        rows.append(
            {
                "name": source.name,
                "status": source.http_status if source.http_status is not None else "no response",
                "ok": source.ok,
                "url": source.url,
                "saved_file": source.saved_file or "-",
                "date": headers.get("date") or headers.get("Date") or "-",
                "bytes": _bytes_h(source.bytes),
                "elapsed_ms": source.elapsed_ms,
                "sha256": source.body_sha256,
                "error": source.error,
                "tls": source.tls or None,
            }
        )
    return rows


def _crosscheck(manifest: Manifest) -> dict[str, Any] | None:
    crosscheck = manifest.crosscheck
    if crosscheck is None:
        return None

    text = crosscheck.text or {}
    values = text.get("values") if isinstance(text.get("values"), dict) else {}
    truncated = text.get("truncated") if isinstance(text.get("truncated"), list) else []
    text_rows = [
        {
            "source": name,
            "chars": len(values[name] or ""),
            "truncated": name in truncated,
            "text": values[name],
        }
        for name in sorted(values)
    ]

    count_rows = []
    for metric, entry in (crosscheck.counts or {}).items():
        entry = entry if isinstance(entry, dict) else {}
        count_rows.append(
            {
                "label": _METRIC_LABELS.get(metric, metric.replace("_", " ").title()),
                "per_source": _values_str(entry.get("values")),
                "agreement": _agreement(entry.get("agree"), entry.get("values")),
                "conflict": entry.get("agree") is False,
            }
        )

    field_rows = []
    for name, entry in (crosscheck.fields or {}).items():
        entry = entry if isinstance(entry, dict) else {}
        field_rows.append(
            {
                "label": name,
                "per_source": _values_str(entry.get("values")),
                "agreement": _agreement(entry.get("agree"), entry.get("values")),
                "conflict": entry.get("agree") is False,
            }
        )

    return {
        "availability": crosscheck.availability or {},
        "text_agreement": _agreement(text.get("agree"), values),
        "text_conflict": text.get("agree") is False,
        "longest_source": text.get("longest_source"),
        "truncated": truncated,
        "text_rows": text_rows,
        "counts": count_rows,
        "fields": field_rows,
        "flags": crosscheck.flags or [],
        "notes": crosscheck.notes or [],
    }


def _timestamp(manifest: Manifest) -> dict[str, Any]:
    ts = manifest.timestamp
    if ts is None or not ts.enabled:
        return {
            "enabled": False,
            "line": (
                "No RFC 3161 token was requested. Every digest in this document is "
                "self-computed by the capturing tool and is not attested by a third party."
            ),
        }
    return {
        "enabled": True,
        "tsa_url": ts.tsa_url,
        "hash_alg": ts.hash_alg,
        "line": (
            f"An RFC 3161 timestamp over the sha256 of manifest.json was requested from "
            f"{ts.tsa_url}; the token is stored beside this record as timestamp.tsr and its "
            f"outcome is recorded in timestamp.json. Verify with: "
            f"openssl ts -verify -data manifest.json -in timestamp.tsr -CAfile <tsa-ca.pem>"
        ),
    }


def _context(manifest: Manifest, pkg_dir: Path, settings: Settings) -> dict[str, Any]:
    version = manifest.version
    return {
        "css": (STATIC_DIR / "record.css").read_text("utf-8"),
        "tool": manifest.tool,
        "tool_version": manifest.tool_version,
        "manifest_version": manifest.manifest_version,
        "capture_time": manifest.capture_time_utc,
        "capture_duration_ms": manifest.capture_duration_ms,
        "tweet_id": manifest.tweet_id,
        "folder": manifest.folder,
        "operator": manifest.operator,
        "status": manifest.status,
        "site_name": settings.site_name,
        "post": _post_context(manifest, pkg_dir, settings),
        "facts": _facts(manifest),
        "engagement": _engagement(manifest),
        "sources": _sources(manifest),
        "crosscheck": _crosscheck(manifest),
        "version": (
            {
                "previous_folder": version.previous_folder,
                "previous_capture_time_utc": version.previous_capture_time_utc,
                "number": version.version,
                "changes": version.changes,
            }
            if version and version.previous_folder
            else None
        ),
        "files": [(rel, manifest.files[rel]) for rel in sorted(manifest.files)],
        "timestamp": _timestamp(manifest),
        "warnings": manifest.warnings,
    }


def build_record_html(manifest: Manifest, pkg_dir: Path, settings: Settings) -> Path:
    """Render the evidence record to ``<pkg_dir>/capture.html`` and return its path."""
    pkg_dir = Path(pkg_dir)
    html = _env().get_template("record.html").render(**_context(manifest, pkg_dir, settings))
    out = pkg_dir / "capture.html"
    out.write_text(html, encoding="utf-8")
    return out


# --------------------------------------------------------------------------
# headless rendering
# --------------------------------------------------------------------------


async def _block_remote(route: Any) -> None:
    """Abort anything that is not local: the record must not phone home."""
    try:
        url = route.request.url
        if url.startswith(("file://", "data:", "about:")):
            await route.continue_()
        else:
            await route.abort()
    except Exception:  # noqa: BLE001 - route may already be gone
        return


async def render_documents(html_path: Path, pkg_dir: Path, settings: Settings) -> RenderResult:
    """Render capture.html to capture.pdf / capture.png with headless chromium."""
    html_path = Path(html_path)
    pkg_dir = Path(pkg_dir)
    result = RenderResult(html=html_path.is_file())

    if not result.html:
        result.error = "capture.html is missing"
        return result
    if not settings.render or not (settings.render_pdf or settings.render_png):
        result.error = "render_disabled"
        return result

    try:
        from playwright.async_api import async_playwright  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001 - optional dependency
        result.error = f"renderer_unavailable: {type(exc).__name__}: {exc}"
        return result

    timeout_ms = int(settings.render_timeout * 1000)
    try:
        async with async_playwright() as playwright:
            launch: dict[str, Any] = {"args": ["--no-sandbox"]}
            if settings.chromium_path:
                launch["executable_path"] = settings.chromium_path
            browser = await playwright.chromium.launch(**launch)
            try:
                result.engine = f"chromium/{browser.version}"
                page = await browser.new_page(viewport={"width": 1000, "height": 1400})
                page.set_default_timeout(timeout_ms)
                await page.route("**/*", _block_remote)
                # resolve(): a relative package path has no file:// form.
                await page.goto(
                    Path(html_path).resolve().as_uri(), wait_until="load", timeout=timeout_ms
                )
                if settings.render_pdf:
                    await page.pdf(
                        path=str(pkg_dir / "capture.pdf"), format="A4", print_background=True
                    )
                    result.pdf = (pkg_dir / "capture.pdf").is_file()
                if settings.render_png:
                    await page.screenshot(path=str(pkg_dir / "capture.png"), full_page=True)
                    result.png = (pkg_dir / "capture.png").is_file()
            finally:
                await browser.close()
    except Exception as exc:  # noqa: BLE001 - a missing browser must not kill a capture
        result.error = f"renderer_unavailable: {type(exc).__name__}: {exc}"
    return result
