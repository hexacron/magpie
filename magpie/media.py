"""Download the post's assets next to the record, and OCR what can be read.

Everything here is best effort: a dead CDN URL, an oversized video or a
missing tesseract binary produces a warning string, never an exception.  Media
lands in ``<pkg>/media/`` and is referenced by relative path -- the record must
never inline a video (the original base64'd an 8 MB mp4 into its HTML, storing
the same bytes three times).
"""

from __future__ import annotations

import hashlib
import mimetypes
import shutil
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .config import Settings
from .models import DownloadResult, MediaItem, Post

_STREAM_CHUNK = 256 * 1024

_EXT_BY_CONTENT_TYPE = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
    "application/vnd.apple.mpegurl": ".m3u8",
    "application/x-mpegurl": ".m3u8",
}

_DEFAULT_EXT = {"photo": ".jpg", "video": ".mp4", "gif": ".mp4"}


def _ext_for(content_type: str | None, url: str, default: str) -> str:
    if content_type:
        known = _EXT_BY_CONTENT_TYPE.get(content_type.lower())
        if known:
            return known
        guessed = mimetypes.guess_extension(content_type)
        if guessed:
            return ".jpg" if guessed in (".jpe", ".jpeg") else guessed
    suffix = Path(urlsplit(url).path).suffix.lower()
    if 1 < len(suffix) <= 5 and suffix[1:].isalnum():
        return suffix
    return default


def _upgrade_avatar(url: str) -> str:
    """`..._normal.jpg` is a 48px thumbnail; `_400x400` is the same image, usable."""
    return url.replace("_normal.", "_400x400.") if "_normal." in url else url


async def _download(
    client: httpx.AsyncClient,
    url: str,
    media_dir: Path,
    stem: str,
    settings: Settings,
    default_ext: str,
) -> DownloadResult:
    result = DownloadResult(url=url)
    path: Path | None = None
    try:
        media_dir.mkdir(parents=True, exist_ok=True)
        async with client.stream("GET", url, follow_redirects=True) as response:
            result.content_type = (
                (response.headers.get("content-type") or "").split(";")[0].strip() or None
            )
            if response.status_code != 200:
                result.error = f"http_{response.status_code}"
                return result

            declared = response.headers.get("content-length")
            if declared and declared.isdigit() and int(declared) > settings.max_media_bytes:
                result.error = (
                    f"too_large: {int(declared)} bytes exceeds max_media_bytes "
                    f"({settings.max_media_bytes})"
                )
                return result

            path = media_dir / f"{stem}{_ext_for(result.content_type, url, default_ext)}"
            digest = hashlib.sha256()
            total = 0
            overflowed = False
            with path.open("wb") as fh:
                async for chunk in response.aiter_bytes(_STREAM_CHUNK):
                    total += len(chunk)
                    if total > settings.max_media_bytes:
                        overflowed = True
                        break
                    digest.update(chunk)
                    fh.write(chunk)

            if overflowed:
                path.unlink(missing_ok=True)
                result.error = (
                    f"too_large: stream exceeded max_media_bytes ({settings.max_media_bytes})"
                )
                return result

            result.ok = True
            result.bytes = total
            result.sha256 = digest.hexdigest()
            result.file = f"media/{path.name}"
            return result
    except Exception as exc:  # noqa: BLE001 - a broken CDN must not kill a capture
        if path is not None:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        result.error = f"{type(exc).__name__}: {exc}"
        return result


async def download_assets(
    client: httpx.AsyncClient, post: Post, pkg_dir: Path, settings: Settings
) -> list[str]:
    """Fetch avatar, banner, photos, videos and video thumbnails. Returns warnings."""
    if not settings.download_media:
        return ["media_download_disabled: assets are referenced by URL only, not stored"]

    pkg_dir = Path(pkg_dir)
    media_dir = pkg_dir / "media"
    warnings: list[str] = []

    if post.avatar_url:
        result = await _download(
            client, _upgrade_avatar(post.avatar_url), media_dir, "avatar", settings, ".jpg"
        )
        post.avatar_download = result
        if result.ok:
            post.avatar_local_file = result.file
        else:
            warnings.append(f"avatar download failed: {result.error}")

    if post.banner_url:
        result = await _download(client, post.banner_url, media_dir, "banner", settings, ".jpg")
        post.banner_download = result
        if result.ok:
            post.banner_local_file = result.file
        else:
            warnings.append(f"banner download failed: {result.error}")

    counters: dict[str, int] = {}
    for item in post.media:
        kind = item.type if item.type in _DEFAULT_EXT else "photo"
        counters[kind] = counters.get(kind, 0) + 1
        stem = f"{kind}_{counters[kind]:02d}"
        default_ext = _DEFAULT_EXT[kind]

        if item.best_url:
            result = await _download(client, item.best_url, media_dir, stem, settings, default_ext)
            item.download = result
            if result.ok:
                item.local_file = result.file
            else:
                warnings.append(f"{stem} download failed: {result.error}")
        else:
            warnings.append(f"{stem} has no downloadable url")

        if item.thumb_url:
            thumb = await _download(
                client, item.thumb_url, media_dir, f"{stem}_thumb", settings, ".jpg"
            )
            item.thumb_download = thumb
            if thumb.ok:
                item.thumb_local_file = thumb.file
            else:
                warnings.append(f"{stem} thumbnail download failed: {thumb.error}")

    return warnings


def run_ocr(post: Post, pkg_dir: Path, settings: Settings) -> str:
    """OCR downloaded photos and video thumbnails. Silent no-op without tesseract."""
    if not settings.ocr:
        return ""
    try:
        import pytesseract  # type: ignore[import-not-found]
        from PIL import Image  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001 - optional dependency
        return ""
    if not shutil.which("tesseract"):
        return ""

    pkg_dir = Path(pkg_dir)
    collected: list[str] = []
    for item in post.media:
        targets: list[str] = []
        if item.type == "photo" and item.local_file:
            targets.append(item.local_file)
        if item.thumb_local_file:
            targets.append(item.thumb_local_file)

        found: list[str] = []
        for rel in targets:
            path = pkg_dir / rel
            if not path.is_file():
                continue
            try:
                with Image.open(path) as image:
                    text = pytesseract.image_to_string(image)
            except Exception:  # noqa: BLE001 - unreadable image, keep going
                continue
            text = text.strip()
            if text:
                found.append(text)

        if found:
            item.ocr_text = "\n".join(found)
            collected.append(item.ocr_text)

    return "\n\n".join(collected).strip()
