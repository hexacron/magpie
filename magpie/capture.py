"""Capture orchestrator: input -> sources -> merged post -> record -> sealed package.

Sealing order matters and is deliberate:

1. fetch every source, persist the raw bodies verbatim
2. merge + cross-check, download media, optional OCR
3. hash sources + media, render the evidence record from those hashes
4. re-hash the tree (now including capture.html/pdf/png) and write manifest.json
5. request an RFC 3161 token over sha256(manifest.json)

Step 5 must come last: a token that covers the manifest cannot live inside it.
The token lands in `timestamp.tsr` with a `timestamp.json` sidecar, both of
which are excluded from the hash tree.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx

from .config import Settings, load_settings
from .crosscheck import crosscheck
from .hashing import hash_tree, timestamp_manifest
from .ids import parse_inputs
from .media import download_assets, run_ocr
from .models import (
    TOOL_NAME,
    TOOL_VERSION,
    Manifest,
    Post,
    RawSource,
    RenderResult,
    TimestampResult,
    TweetRef,
    VersionLink,
)
from .normalize import merge, parse_sources
from .render import build_record_html, render_documents
from .sources import fetch_sources
from .store import Store

__all__ = ["capture_one", "capture_many", "load_timestamp"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=settings.http_timeout,
        follow_redirects=True,
        proxy=settings.proxy,
        headers={"Accept-Language": "en-US,en;q=0.9"},
    )


def _write_bodies(pkg_dir: Path, raws: list[RawSource], warnings: list[str]) -> None:
    """Persist each response body verbatim. Never lose bytes to a parse bug."""
    for raw in raws:
        if raw.body is None or not raw.saved_file:
            continue
        try:
            (pkg_dir / raw.saved_file).write_bytes(raw.body)
        except OSError as exc:  # pragma: no cover - disk failure
            warnings.append(f"write_failed:{raw.saved_file}: {exc}")


def _status_for(post: Post | None, available: list[str]) -> str:
    if available:
        return "live"
    if post is not None and post.text:
        return "unavailable"
    return "unavailable"


def _diff_previous(store: Store, tweet_id: str, folder: str, post: Post | None, status: str) -> VersionLink:
    """Chain re-captures of the same post and say what actually moved.

    The original tool carried a dead `versions: 0` field while writing a fresh
    duplicate folder each time; this is the feature that field implied.
    """
    link = VersionLink()
    previous = store.previous_capture(tweet_id, folder)
    if previous is None:
        return link

    link.previous_folder = previous.folder
    link.previous_capture_time_utc = previous.capture_time_utc
    link.version = (previous.version or 1) + 1

    changes: list[str] = []
    if previous.status != status:
        changes.append(f"status: {previous.status} -> {status}")

    new_text = (post.text if post else "") or ""
    old_text = previous.text or ""
    if old_text and new_text and old_text != new_text:
        changes.append("text changed")
    elif old_text and not new_text:
        changes.append("text no longer retrievable")

    old_sources = set(previous.available_sources or [])
    new_sources = set(post.available_sources if post else [])
    for gone in sorted(old_sources - new_sources):
        changes.append(f"source lost: {gone}")
    for gained in sorted(new_sources - old_sources):
        changes.append(f"source gained: {gained}")

    try:
        prev_manifest = store.get_manifest(previous.folder) or {}
        prev_counts = ((prev_manifest.get("post") or {}).get("counts")) or {}
    except Exception:  # pragma: no cover - unreadable previous package
        prev_counts = {}
    new_counts = post.counts if post else {}
    for metric in sorted(set(prev_counts) | set(new_counts)):
        before, after = prev_counts.get(metric), new_counts.get(metric)
        if before is not None and after is not None and before != after:
            changes.append(f"{metric}: {before:,} -> {after:,}")

    link.changes = changes
    return link


async def capture_one(
    ref: TweetRef,
    settings: Settings,
    store: Store,
    *,
    client: httpx.AsyncClient | None = None,
    operator: str | None = None,
) -> Manifest:
    """Capture one post into a sealed package. Never raises on source failure."""
    started = time.perf_counter()
    capture_time = _now()

    if not ref.ok:
        # Rejected at parse time (unsupported host, garbage input). Producing a
        # package here is what let a TikTok URL become a "deleted tweet".
        return Manifest(
            capture_time_utc=_iso(capture_time),
            input=ref.input,
            status="error",
            warnings=[ref.error or "unrecognised_input"],
            operator=operator or settings.operator,
        )

    assert ref.tweet_id is not None
    warnings: list[str] = []
    owns_client = client is None
    client = client or _client(settings)
    folder, pkg_dir = store.allocate(ref, capture_time)

    try:
        raws = await fetch_sources(client, ref, settings)
        _write_bodies(pkg_dir, raws, warnings)

        parsed = parse_sources(raws)
        post = merge(ref, parsed)
        check = crosscheck(parsed, raws)

        if settings.download_media:
            warnings += await download_assets(client, post, pkg_dir, settings)
        else:
            warnings.append("media_download_disabled")

        try:
            post.ocr_text = run_ocr(post, pkg_dir, settings)
        except Exception as exc:  # pragma: no cover - OCR is best effort
            warnings.append(f"ocr_failed: {exc}")

        status = _status_for(post, post.available_sources)
        version = _diff_previous(store, ref.tweet_id, folder, post, status)

        manifest = Manifest(
            tool=TOOL_NAME,
            tool_version=TOOL_VERSION,
            capture_time_utc=_iso(capture_time),
            input=ref.input,
            tweet_id=ref.tweet_id,
            screen_name=post.screen_name or ref.screen_name,
            folder=folder,
            status=status,
            sources=raws,
            post=post,
            crosscheck=check,
            render=RenderResult(),
            timestamp=TimestampResult(
                enabled=settings.tsa,
                tsa_url=settings.tsa_url if settings.tsa else None,
                file="timestamp.tsr" if settings.tsa else None,
                digest=None,
            ),
            version=version,
            warnings=warnings,
            operator=operator or settings.operator,
        )

        # Hash the evidence inputs first so the rendered record can show them;
        # the record cannot contain its own digest.
        manifest.files = hash_tree(pkg_dir)
        try:
            html_path = build_record_html(manifest, pkg_dir, settings)
            manifest.render = RenderResult(html=True)
            if settings.render:
                manifest.render = await render_documents(html_path, pkg_dir, settings)
                manifest.render.html = True
                if manifest.render.error:
                    warnings.append(manifest.render.error)
            else:
                manifest.render.error = "render_disabled"
        except Exception as exc:
            manifest.render = RenderResult(error=f"render_failed: {exc}")
            warnings.append(f"render_failed: {exc}")

        # Final tree, now covering capture.html/pdf/png, then seal.
        manifest.files = hash_tree(pkg_dir)
        manifest.capture_duration_ms = int((time.perf_counter() - started) * 1000)
        manifest_sha = store.write_manifest(pkg_dir, manifest)

        ts = await timestamp_manifest(manifest_sha, pkg_dir, settings)
        ts.digest = manifest_sha
        if settings.tsa and not ts.ok and ts.error:
            warnings.append(f"timestamp_failed: {ts.error}")
        if ts.enabled:
            (pkg_dir / "timestamp.json").write_text(
                json.dumps(ts.to_dict(), indent=2), encoding="utf-8"
            )
        manifest.timestamp = ts

        store.index_upsert(manifest)
        return manifest
    finally:
        if owns_client:
            await client.aclose()


async def capture_many(
    inputs: str | Iterable[str],
    settings: Settings | None = None,
    store: Store | None = None,
    *,
    operator: str | None = None,
) -> list[Manifest]:
    """Parse a batch of links, capture each with bounded concurrency."""
    settings = settings or load_settings()
    settings.ensure_dirs()
    store = store or Store(settings)

    refs = parse_inputs(inputs, settings.max_batch)
    if not refs:
        return []

    semaphore = asyncio.Semaphore(max(1, settings.concurrency))
    results: list[Manifest] = [None] * len(refs)  # type: ignore[list-item]

    async with _client(settings) as client:

        async def run(position: int, ref: TweetRef) -> None:
            async with semaphore:
                try:
                    results[position] = await capture_one(
                        ref, settings, store, client=client, operator=operator
                    )
                except Exception as exc:  # pragma: no cover - last-resort guard
                    results[position] = Manifest(
                        capture_time_utc=_iso(_now()),
                        input=ref.input,
                        tweet_id=ref.tweet_id,
                        status="error",
                        warnings=[f"capture_failed: {exc!r}"],
                        operator=operator or settings.operator,
                    )

        await asyncio.gather(*(run(i, ref) for i, ref in enumerate(refs)))

    return list(results)


def load_timestamp(pkg_dir: Path) -> dict[str, Any] | None:
    """Read the post-seal timestamp sidecar, if the capture requested one."""
    path = Path(pkg_dir) / "timestamp.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
