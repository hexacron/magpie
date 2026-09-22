"""Shared data contract for every component.

Everything here is plain dataclasses that serialise to JSON via `to_dict()`.
Field names are kept compatible with the original Magpie manifest where
that costs nothing, so existing packages/exports stay readable.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

TOOL_NAME = "magpie"
TOOL_VERSION = "2.0.0"
MANIFEST_VERSION = 2

SOURCE_NAMES = ("syndication", "fxtwitter", "vxtwitter", "x_page")


def _clean(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # Honour per-class overrides: dataclasses.asdict() would recurse past
        # them and, for RawSource, copy every response body into the manifest.
        if isinstance(value, Model):
            return value.to_dict()
        return {f.name: _clean(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {k: _clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(v) for v in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


class Model:
    """Mixin: dataclass -> plain JSON-safe dict."""

    def to_dict(self) -> dict[str, Any]:
        return {
            f.name: _clean(getattr(self, f.name))
            for f in dataclasses.fields(self)  # type: ignore[arg-type]
        }


@dataclass
class TweetRef(Model):
    """One parsed input line."""

    input: str
    tweet_id: str | None = None
    screen_name: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.tweet_id is not None and self.error is None


@dataclass
class RawSource(Model):
    """Exactly what one endpoint returned. `body` is never persisted in JSON."""

    name: str
    url: str
    http_status: int | None = None
    ok: bool = False
    error: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    request_headers: dict[str, str] = field(default_factory=dict)
    elapsed_ms: int | None = None
    saved_file: str | None = None
    body_sha256: str | None = None
    bytes: int | None = None
    body: bytes | None = None
    tls: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        # Skip `body` outright: it is already on disk as `saved_file` and
        # covered by `body_sha256`; decoding it here would duplicate every
        # response into manifest.json (measured: +244 KB on one capture).
        return {
            f.name: _clean(getattr(self, f.name))
            for f in dataclasses.fields(self)
            if f.name != "body"
        }


@dataclass
class DownloadResult(Model):
    url: str
    ok: bool = False
    file: str | None = None
    bytes: int | None = None
    error: str | None = None
    content_type: str | None = None
    sha256: str | None = None


@dataclass
class MediaItem(Model):
    type: str  # photo | video | gif
    best_url: str | None = None
    thumb_url: str | None = None
    alt: str | None = None
    width: int | None = None
    height: int | None = None
    duration_s: float | None = None
    local_file: str | None = None
    thumb_local_file: str | None = None
    download: DownloadResult | None = None
    thumb_download: DownloadResult | None = None
    ocr_text: str | None = None


@dataclass
class Profile(Model):
    screen_name: str | None = None
    name: str | None = None
    description: str | None = None
    location: str | None = None
    website: str | None = None
    joined: str | None = None
    followers: int | None = None
    following: int | None = None
    tweets: int | None = None
    media_count: int | None = None
    likes: int | None = None
    protected: bool | None = None
    verified: bool | None = None
    avatar_url: str | None = None
    banner_url: str | None = None


@dataclass
class Post(Model):
    """Merged view. `count_sources`/`field_sources` record provenance per field."""

    id: str
    screen_name: str | None = None
    name: str | None = None
    text: str | None = None
    text_source: str | None = None
    created_at_utc: str | None = None
    created_at_display: str | None = None
    lang: str | None = None
    possibly_sensitive: bool | None = None
    blue_verified: bool | None = None
    avatar_url: str | None = None
    banner_url: str | None = None
    counts: dict[str, int] = field(default_factory=dict)
    count_sources: dict[str, str] = field(default_factory=dict)
    count_agreement: dict[str, bool | None] = field(default_factory=dict)
    media: list[MediaItem] = field(default_factory=list)
    media_source: str | None = None
    community_note: str | None = None
    profile: Profile | None = None
    quoted_id: str | None = None
    quoted_screen_name: str | None = None
    reply_to_id: str | None = None
    reply_to_screen_name: str | None = None
    source_url: str | None = None
    available_sources: list[str] = field(default_factory=list)
    avatar_local_file: str | None = None
    avatar_download: DownloadResult | None = None
    banner_local_file: str | None = None
    banner_download: DownloadResult | None = None
    ocr_text: str = ""


@dataclass
class ParsedSource(Model):
    """One source's answer, normalised but NOT merged."""

    name: str
    available: bool = False
    reason: str | None = None  # available | unavailable | error
    text: str | None = None
    text_truncated: bool = False
    created_at_utc: str | None = None
    created_at_display: str | None = None
    screen_name: str | None = None
    name_display: str | None = None
    lang: str | None = None
    possibly_sensitive: bool | None = None
    blue_verified: bool | None = None
    counts: dict[str, int] = field(default_factory=dict)
    media: list[MediaItem] = field(default_factory=list)
    profile: Profile | None = None
    community_note: str | None = None
    quoted_id: str | None = None
    quoted_screen_name: str | None = None
    reply_to_id: str | None = None
    reply_to_screen_name: str | None = None
    avatar_url: str | None = None
    banner_url: str | None = None


@dataclass
class CrossCheck(Model):
    availability: dict[str, str] = field(default_factory=dict)
    text: dict[str, Any] = field(default_factory=dict)
    counts: dict[str, Any] = field(default_factory=dict)
    fields: dict[str, Any] = field(default_factory=dict)
    flags: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class RenderResult(Model):
    html: bool = False
    pdf: bool = False
    png: bool = False
    engine: str | None = None
    error: str | None = None


@dataclass
class TimestampResult(Model):
    """RFC 3161 trusted timestamp over MANIFEST.sha256."""

    enabled: bool = False
    ok: bool = False
    tsa_url: str | None = None
    file: str | None = None
    gen_time: str | None = None
    digest: str | None = None
    hash_alg: str = "sha256"
    error: str | None = None
    serial: str | None = None


@dataclass
class VersionLink(Model):
    """Link back to the previous capture of the same tweet id."""

    previous_folder: str | None = None
    previous_capture_time_utc: str | None = None
    version: int = 1
    changes: list[str] = field(default_factory=list)


@dataclass
class Manifest(Model):
    tool: str = TOOL_NAME
    tool_version: str = TOOL_VERSION
    manifest_version: int = MANIFEST_VERSION
    capture_time_utc: str = ""
    capture_duration_ms: int | None = None
    input: str = ""
    tweet_id: str | None = None
    screen_name: str | None = None
    folder: str = ""
    status: str = "unavailable"  # live | unavailable | error
    sources: list[RawSource] = field(default_factory=list)
    post: Post | None = None
    crosscheck: CrossCheck | None = None
    render: RenderResult | None = None
    timestamp: TimestampResult | None = None
    version: VersionLink | None = None
    warnings: list[str] = field(default_factory=list)
    files: dict[str, str] = field(default_factory=dict)
    operator: str | None = None


@dataclass
class VerifyResult(Model):
    folder: str
    ok: bool = False
    manifest_sha256: str | None = None
    recorded_manifest_sha256: str | None = None
    files_checked: int = 0
    mismatched: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    unexpected: list[str] = field(default_factory=list)
    timestamp_ok: bool | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class IndexRow(Model):
    """One row of the searchable index (mirrors the export schema)."""

    folder: str
    capture_time_utc: str
    tweet_id: str | None
    screen_name: str | None
    name: str | None
    text: str
    created_at_utc: str | None
    available_sources: list[str] = field(default_factory=list)
    media_count: int = 0
    flags: list[str] = field(default_factory=list)
    status: str = "unavailable"
    tags: list[str] = field(default_factory=list)
    note: str = ""
    ocr_text: str = ""
    manifest_sha256: str = ""
    timestamped: bool = False
    version: int = 1
