"""Turn raw source bodies into one ``ParsedSource`` each, then merge to a ``Post``.

Two rules run through the whole module:

1. **Nothing raises.** Every parser is wrapped; a malformed payload becomes
   ``ParsedSource(available=False, reason="error")`` and the capture continues
   with whatever the other three sources returned.
2. **Provenance is recorded, not erased.** ``merge`` never silently blends
   values: the winning source for the text, the media list and every single
   count is written into the ``Post`` so the record can show where a number
   came from.

The x_page HTML *is* parsed here (the original saved 185 KB of server-rendered
markup and then never looked at it, so it could not contribute to the
cross-check).  No HTML dependency is added: stdlib ``html.parser`` is enough
for the handful of ``og:*`` meta tags X emits.
"""

from __future__ import annotations

import dataclasses
import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any

from .models import SOURCE_NAMES, MediaItem, ParsedSource, Post, Profile, RawSource, TweetRef

__all__ = ["merge", "normalise_text", "parse_sources"]

# Precedence tables. Media prefers syndication because its variant list carries
# real bitrates.
# Text prefers syndication: it is X's own endpoint, keeps the post exactly as
# authored (t.co links, display_text_range applied) and never rewrites them.
# Its 280-char cut on note tweets is caught by the prefix rule in _pick_text,
# which then falls through to fxtwitter/vxtwitter for the full body.
_TEXT_ORDER = ("syndication", "fxtwitter", "vxtwitter", "x_page")
_MEDIA_ORDER = ("syndication", "fxtwitter", "vxtwitter")
_COUNT_ORDER = ("syndication", "fxtwitter", "vxtwitter")
_PROFILE_ORDER = ("fxtwitter", "syndication", "vxtwitter")
_NAME_ORDER = ("fxtwitter", "syndication", "vxtwitter", "x_page")

_COUNT_METRICS = ("likes", "retweets", "replies", "quotes", "views", "bookmarks")

_TCO_TAIL_RE = re.compile(r"(?:\s*https?://t\.co/[A-Za-z0-9]+)+\s*$")
# Comparison form only: vxtwitter rewrites the trailing t.co into the expanded
# destination (`https://t.co/5GRFUfszbj` -> `https://okdiar.io/4Alj9lj`), which
# made three healthy sources look like truncated copies of it. Strip any
# trailing URL from both sides instead; parsers keep the t.co-specific rule.
_URL_TAIL_RE = re.compile(r"(?:\s*https?://\S+)+\s*$")
_ELLIPSIS_TAIL_RE = re.compile(r"(?:\u2026|\.\.\.)\s*$")
_WHITESPACE_RE = re.compile(r"\s+")
_OG_TITLE_RE = re.compile(r"^(?P<name>.*?)\s*\(@(?P<handle>[A-Za-z0-9_]{1,20})\)")
_TITLE_SUFFIX_RE = re.compile(r"\s*/\s*(?:X|Twitter)\s*$")
_UNAVAILABLE_MARKERS = (
    "this post is unavailable",
    "this tweet is unavailable",
    "post not found",
    "hmm...this page doesn",
    "sorry, that page",
    "account suspended",
)


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------


def _text(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return None


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _display(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _from_epoch(value: Any) -> tuple[str | None, str | None]:
    seconds = _int(value)
    if seconds is None or seconds <= 0:
        return None, None
    try:
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None, None
    return _iso(dt), _display(dt)


def _from_string(value: Any) -> tuple[str | None, str | None]:
    raw = _text(value)
    if raw is None:
        return None, None
    candidate = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            # Twitter's legacy "Thu Jan 01 00:00:00 +0000 2023" form.
            dt = datetime.strptime(raw, "%a %b %d %H:%M:%S %z %Y")
        except ValueError:
            return None, None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _iso(dt), _display(dt)


def _utf16_slice(text: str, start: int, end: int) -> str:
    """Slice by UTF-16 code units, which is what display_text_range counts."""
    units = text.encode("utf-16-le", "surrogatepass")
    return units[start * 2 : end * 2].decode("utf-16-le", "ignore")


def normalise_text(value: str) -> str:
    """Comparison form of a post text, shared with :mod:`crosscheck`.

    Neutralises the *mechanical* differences between endpoints -- collapsed
    whitespace, a trailing link tail (X auto-appends a ``t.co``; vxtwitter
    serves the same tail already expanded to its destination), a trailing
    ellipsis where the text was cut -- and nothing else. Case and punctuation
    are preserved, so two texts comparing equal really are the same text.
    """
    text = _WHITESPACE_RE.sub(" ", value).strip()
    for _ in range(3):
        before = text
        text = _URL_TAIL_RE.sub("", text).strip()
        text = _ELLIPSIS_TAIL_RE.sub("", text).strip()
        if text == before:
            break
    return text


# --------------------------------------------------------------------------
# syndication
# --------------------------------------------------------------------------


def _syndication_media(payload: dict[str, Any]) -> list[MediaItem]:
    items: list[MediaItem] = []

    for detail in _list(payload.get("mediaDetails")):
        entry = _dict(detail)
        kind = _text(entry.get("type")) or "photo"
        info = _dict(entry.get("original_info"))
        alt = _text(entry.get("ext_alt_text"))
        still = _text(entry.get("media_url_https"))
        if kind == "photo":
            items.append(
                MediaItem(
                    type="photo",
                    best_url=still,
                    alt=alt,
                    width=_int(info.get("width")),
                    height=_int(info.get("height")),
                )
            )
            continue
        video_info = _dict(entry.get("video_info"))
        best_url = _best_mp4(_list(video_info.get("variants")))
        duration_ms = _int(video_info.get("duration_millis"))
        items.append(
            MediaItem(
                type="gif" if kind == "animated_gif" else "video",
                best_url=best_url,
                thumb_url=still,
                alt=alt,
                width=_int(info.get("width")),
                height=_int(info.get("height")),
                duration_s=(duration_ms / 1000.0) if duration_ms else None,
            )
        )

    if items:
        return items

    # Older shape: separate `photos` list and a single `video` object.
    for photo in _list(payload.get("photos")):
        entry = _dict(photo)
        items.append(
            MediaItem(
                type="photo",
                best_url=_text(entry.get("url")),
                width=_int(entry.get("width")),
                height=_int(entry.get("height")),
            )
        )
    video = _dict(payload.get("video"))
    if video:
        best_url = _best_mp4(_list(video.get("variants")))
        duration_ms = _int(video.get("durationMs"))
        items.append(
            MediaItem(
                type="video",
                best_url=best_url,
                thumb_url=_text(video.get("poster")),
                duration_s=(duration_ms / 1000.0) if duration_ms else None,
            )
        )
    return items


def _best_mp4(variants: list[Any]) -> str | None:
    """Highest-bitrate progressive mp4 out of a Twitter variant list.

    Handles both shapes X uses: ``{bitrate, content_type, url}`` from
    ``mediaDetails`` and ``{type, src}`` from the legacy ``video`` object.
    The HLS variant is only a fallback -- an .m3u8 playlist is not something
    an archived package can replay offline.
    """
    best_url: str | None = None
    best_bitrate = -1
    fallback: str | None = None
    for variant in variants:
        entry = _dict(variant)
        content_type = _text(entry.get("content_type")) or _text(entry.get("type")) or ""
        url = _text(entry.get("url")) or _text(entry.get("src"))
        if not url:
            continue
        if fallback is None:
            fallback = url
        if content_type != "video/mp4":
            continue
        bitrate = _int(entry.get("bitrate")) or 0
        if bitrate > best_bitrate:
            best_bitrate = bitrate
            best_url = url
    return best_url if best_url is not None else fallback


def _syndication_text(payload: dict[str, Any]) -> tuple[str | None, bool]:
    note = _dict(payload.get("note_tweet"))
    note_text = _text(note.get("text"))
    if note_text is None and note:
        result = _dict(_dict(note.get("note_tweet_results")).get("result"))
        note_text = _text(result.get("text"))
    if note_text is not None:
        return note_text, False

    text = _text(payload.get("text"))
    if text is None:
        return None, False

    # display_text_range excludes the t.co link X appends for attached media or
    # a quoted post. Drop that suffix only -- never a URL the author typed.
    span = _list(payload.get("display_text_range"))
    if len(span) == 2:
        start, end = _int(span[0]), _int(span[1])
        if start is not None and end is not None and 0 <= start < end:
            body = _utf16_slice(text, start, end).strip()
            suffix = _utf16_slice(text, end, 2 * len(text) + 2)
            if body and (not suffix.strip() or _TCO_TAIL_RE.fullmatch(suffix)):
                text = body

    # Measured against the live endpoint: for a note tweet `tweet-result`
    # returns `note_tweet: {"id": "..."}` carrying NO text, and cuts `text` at a
    # word boundary at or below 280 (observed 268 and 279 chars). Text *length*
    # is therefore unusable as the signal in either direction -- a complete post
    # measured 279 displayable chars with no note_tweet at all. Presence of
    # note_tweet is the reliable marker, so that is what we key on.
    truncated = bool(note)
    return text, truncated


def _parse_syndication(raw: RawSource) -> ParsedSource:
    parsed = ParsedSource(name="syndication")
    payload = _dict(json.loads((raw.body or b"").decode("utf-8", "replace")))
    if not payload:
        parsed.reason = "unavailable"
        return parsed

    typename = _text(payload.get("__typename")) or ""
    if typename == "TweetTombstone" or "tombstone" in payload:
        # A tombstone carries X's "this post is unavailable" copy, never the post.
        parsed.reason = "unavailable"
        return parsed
    if _text(payload.get("error")) and not payload.get("text"):
        parsed.reason = "unavailable"
        return parsed

    text, truncated = _syndication_text(payload)
    parsed.text = text
    parsed.text_truncated = truncated
    parsed.created_at_utc, parsed.created_at_display = _from_string(payload.get("created_at"))
    parsed.lang = _text(payload.get("lang"))
    parsed.possibly_sensitive = _bool(payload.get("possibly_sensitive"))

    user = _dict(payload.get("user"))
    parsed.screen_name = _text(user.get("screen_name"))
    parsed.name_display = _text(user.get("name"))
    parsed.blue_verified = _bool(user.get("is_blue_verified"))
    parsed.avatar_url = _text(user.get("profile_image_url_https"))
    parsed.banner_url = _text(user.get("profile_banner_url"))
    parsed.profile = Profile(
        screen_name=parsed.screen_name,
        name=parsed.name_display,
        description=_text(user.get("description")),
        location=_text(user.get("location")),
        followers=_int(user.get("followers_count")),
        following=_int(user.get("friends_count")),
        tweets=_int(user.get("statuses_count")),
        protected=_bool(user.get("protected")),
        verified=_bool(user.get("verified")),
        avatar_url=parsed.avatar_url,
        banner_url=parsed.banner_url,
    )

    counts: dict[str, int] = {}
    for metric, key in (
        ("likes", "favorite_count"),
        ("replies", "conversation_count"),
        ("retweets", "retweet_count"),
        ("quotes", "quote_count"),
        ("views", "view_count"),
    ):
        value = _int(payload.get(key))
        if value is not None:
            counts[metric] = value
    if "replies" not in counts:
        value = _int(payload.get("reply_count"))
        if value is not None:
            counts["replies"] = value
    parsed.counts = counts

    parsed.media = _syndication_media(payload)

    quoted = _dict(payload.get("quoted_tweet"))
    if quoted:
        parsed.quoted_id = _text(quoted.get("id_str")) or _text(quoted.get("id"))
        parsed.quoted_screen_name = _text(_dict(quoted.get("user")).get("screen_name"))
    parsed.reply_to_id = _text(payload.get("in_reply_to_status_id_str"))
    parsed.reply_to_screen_name = _text(payload.get("in_reply_to_screen_name"))
    if parsed.reply_to_id is None:
        parent = _dict(payload.get("parent"))
        if parent:
            parsed.reply_to_id = _text(parent.get("id_str"))
            parsed.reply_to_screen_name = _text(_dict(parent.get("user")).get("screen_name"))

    parsed.available = parsed.text is not None or bool(parsed.media)
    parsed.reason = "available" if parsed.available else "unavailable"
    return parsed


# --------------------------------------------------------------------------
# fxtwitter
# --------------------------------------------------------------------------


def _fx_media(tweet: dict[str, Any]) -> list[MediaItem]:
    media = _dict(tweet.get("media"))
    entries = _list(media.get("all"))
    if not entries:
        entries = _list(media.get("photos")) + _list(media.get("videos"))
    items: list[MediaItem] = []
    for element in entries:
        entry = _dict(element)
        kind = (_text(entry.get("type")) or "photo").lower()
        if kind in ("image", "photo"):
            kind = "photo"
        elif kind not in ("video", "gif"):
            kind = "video"
        duration = entry.get("duration")
        items.append(
            MediaItem(
                type=kind,
                best_url=_text(entry.get("url")),
                thumb_url=_text(entry.get("thumbnail_url")),
                alt=_text(entry.get("altText")) or _text(entry.get("alt_text")),
                width=_int(entry.get("width")),
                height=_int(entry.get("height")),
                duration_s=float(duration) if isinstance(duration, (int, float)) else None,
            )
        )
    return items


def _fx_text(tweet: dict[str, Any]) -> str | None:
    """Untruncated post text, minus X's auto-appended media shortlink.

    ``raw_text`` is the wire text and is preferred because ``text`` can be the
    shortened display form. But the wire text still carries the ``t.co`` link X
    appends for attached media or a quoted post, which is not authored content
    (syndication excludes it via ``display_text_range``). Measured on a live
    post: ``raw_text`` ended ``... @Tagesspiegel https://t.co/90hvFsx93x`` while
    fxtwitter's own ``text`` ended ``... @Tagesspiegel``. So the shortlink is
    dropped only when fxtwitter's display text confirms it is not authored.
    """
    display = _text(tweet.get("text"))
    raw_text = tweet.get("raw_text")
    if isinstance(raw_text, dict):
        candidate = _text(raw_text.get("text"))
    elif isinstance(raw_text, str):
        candidate = _text(raw_text)
    else:
        candidate = None
    if candidate is None:
        return display

    tail = _TCO_TAIL_RE.search(candidate)
    if tail and (display is None or tail.group(0).strip() not in display):
        candidate = candidate[: tail.start()].strip() or candidate
    return candidate


def _parse_fxtwitter(raw: RawSource) -> ParsedSource:
    parsed = ParsedSource(name="fxtwitter")
    payload = _dict(json.loads((raw.body or b"").decode("utf-8", "replace")))
    tweet = _dict(payload.get("tweet"))
    if not tweet:
        parsed.reason = "unavailable"
        return parsed

    parsed.text = _fx_text(tweet)
    parsed.lang = _text(tweet.get("lang"))
    parsed.possibly_sensitive = _bool(tweet.get("possibly_sensitive"))
    parsed.created_at_utc, parsed.created_at_display = _from_epoch(tweet.get("created_timestamp"))
    if parsed.created_at_utc is None:
        parsed.created_at_utc, parsed.created_at_display = _from_string(tweet.get("created_at"))

    author = _dict(tweet.get("author"))
    parsed.screen_name = _text(author.get("screen_name"))
    parsed.name_display = _text(author.get("name"))
    parsed.avatar_url = _text(author.get("avatar_url"))
    parsed.banner_url = _text(author.get("banner_url"))
    website = author.get("website")
    parsed.profile = Profile(
        screen_name=parsed.screen_name,
        name=parsed.name_display,
        description=_text(author.get("description")),
        location=_text(author.get("location")),
        website=_text(_dict(website).get("url")) if isinstance(website, dict) else _text(website),
        joined=_text(author.get("joined")),
        followers=_int(author.get("followers")),
        following=_int(author.get("following")),
        tweets=_int(author.get("tweets")) or _int(author.get("statuses")),
        likes=_int(author.get("likes")),
        protected=_bool(author.get("protected")),
        avatar_url=parsed.avatar_url,
        banner_url=parsed.banner_url,
    )

    counts: dict[str, int] = {}
    for metric, key in (
        ("likes", "likes"),
        ("retweets", "retweets"),
        ("replies", "replies"),
        ("quotes", "quotes"),
        ("views", "views"),
        ("bookmarks", "bookmarks"),
    ):
        value = _int(tweet.get(key))
        if value is not None:
            counts[metric] = value
    parsed.counts = counts

    parsed.media = _fx_media(tweet)

    note = tweet.get("community_note")
    if isinstance(note, dict):
        parsed.community_note = _text(note.get("text"))
    else:
        parsed.community_note = _text(note)

    parsed.reply_to_screen_name = _text(tweet.get("replying_to"))
    parsed.reply_to_id = _text(tweet.get("replying_to_status"))
    quote = _dict(tweet.get("quote"))
    if quote:
        parsed.quoted_id = _text(quote.get("id"))
        parsed.quoted_screen_name = _text(_dict(quote.get("author")).get("screen_name"))

    parsed.available = parsed.text is not None or bool(parsed.media)
    parsed.reason = "available" if parsed.available else "unavailable"
    return parsed


# --------------------------------------------------------------------------
# vxtwitter
# --------------------------------------------------------------------------


def _vx_media(payload: dict[str, Any]) -> list[MediaItem]:
    items: list[MediaItem] = []
    for element in _list(payload.get("media_extended")):
        entry = _dict(element)
        kind = (_text(entry.get("type")) or "image").lower()
        if kind == "image":
            kind = "photo"
        elif kind not in ("video", "gif"):
            kind = "video"
        size = _dict(entry.get("size"))
        duration_ms = _int(entry.get("duration_millis"))
        items.append(
            MediaItem(
                type=kind,
                best_url=_text(entry.get("url")),
                thumb_url=_text(entry.get("thumbnail_url")),
                alt=_text(entry.get("altText")),
                width=_int(size.get("width")),
                height=_int(size.get("height")),
                duration_s=(duration_ms / 1000.0) if duration_ms else None,
            )
        )
    if items:
        return items
    for url in _list(payload.get("mediaURLs")):
        link = _text(url)
        if link:
            items.append(MediaItem(type="photo", best_url=link))
    return items


def _parse_vxtwitter(raw: RawSource) -> ParsedSource:
    parsed = ParsedSource(name="vxtwitter")
    payload = _dict(json.loads((raw.body or b"").decode("utf-8", "replace")))
    if not payload:
        parsed.reason = "unavailable"
        return parsed

    parsed.text = _text(payload.get("text"))
    parsed.possibly_sensitive = _bool(payload.get("possibly_sensitive"))
    parsed.created_at_utc, parsed.created_at_display = _from_epoch(payload.get("date_epoch"))
    if parsed.created_at_utc is None:
        parsed.created_at_utc, parsed.created_at_display = _from_string(payload.get("date"))

    parsed.screen_name = _text(payload.get("user_screen_name"))
    parsed.name_display = _text(payload.get("user_name"))
    parsed.avatar_url = _text(payload.get("user_profile_image_url"))
    parsed.profile = Profile(
        screen_name=parsed.screen_name,
        name=parsed.name_display,
        avatar_url=parsed.avatar_url,
    )

    counts: dict[str, int] = {}
    for metric, key in (
        ("likes", "likes"),
        ("retweets", "retweets"),
        ("replies", "replies"),
        ("quotes", "qrtCount"),
    ):
        value = _int(payload.get(key))
        if value is not None:
            counts[metric] = value
    parsed.counts = counts

    parsed.media = _vx_media(payload)

    note = payload.get("communityNote")
    parsed.community_note = _text(_dict(note).get("text")) if isinstance(note, dict) else _text(note)

    qrt = _text(payload.get("qrtURL"))
    if qrt:
        match = re.search(r"/([A-Za-z0-9_]{1,20})/status(?:es)?/(\d{5,25})", qrt)
        if match:
            parsed.quoted_screen_name = match.group(1)
            parsed.quoted_id = match.group(2)

    parsed.available = parsed.text is not None or bool(parsed.media)
    parsed.reason = "available" if parsed.available else "unavailable"
    return parsed


# --------------------------------------------------------------------------
# x_page (server-rendered HTML)
# --------------------------------------------------------------------------


class _MetaParser(HTMLParser):
    """Collects <meta> name/property content plus <title>. Tolerant by design."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.meta: dict[str, str] = {}
        self.title: str | None = None
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "meta":
            attributes = {k.lower(): (v or "") for k, v in attrs}
            key = attributes.get("property") or attributes.get("name")
            content = attributes.get("content")
            if key and content:
                self.meta.setdefault(key.strip().lower(), content)
        elif tag == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if self._in_title and not self.title:
            stripped = data.strip()
            if stripped:
                self.title = stripped


def _unwrap_og_description(value: str) -> tuple[str, bool]:
    """X wraps the post text in curly quotes and may append ' / X'."""
    text = _TITLE_SUFFIX_RE.sub("", value.strip()).strip()
    for opener, closer in (("\u201c", "\u201d"), ('"', '"'), ("\u00ab", "\u00bb")):
        if text.startswith(opener) and text.endswith(closer) and len(text) > 1:
            text = text[len(opener) : -len(closer)].strip()
            break
    truncated = text.endswith("\u2026") or text.endswith("...")
    return text, truncated


def _parse_x_page(raw: RawSource) -> ParsedSource:
    parsed = ParsedSource(name="x_page")
    html = (raw.body or b"").decode("utf-8", "replace")
    if not html.strip():
        parsed.reason = "unavailable"
        return parsed

    reader = _MetaParser()
    reader.feed(html)
    reader.close()
    meta = reader.meta

    description = meta.get("og:description") or meta.get("description")
    title = meta.get("og:title") or reader.title or ""
    image = meta.get("og:image")

    lowered = (title + " " + (description or "")).lower()
    if any(marker in lowered for marker in _UNAVAILABLE_MARKERS):
        parsed.reason = "unavailable"
        return parsed

    if description:
        text, truncated = _unwrap_og_description(description)
        parsed.text = text or None
        parsed.text_truncated = truncated

    heading = _TITLE_SUFFIX_RE.sub("", title.strip())
    match = _OG_TITLE_RE.match(heading)
    if match:
        parsed.name_display = _text(match.group("name"))
        parsed.screen_name = match.group("handle")
    if parsed.screen_name is None:
        link = re.search(r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']', html, re.I)
        if link:
            canonical = re.search(r"/([A-Za-z0-9_]{1,20})/status(?:es)?/\d{5,25}", link.group(1))
            if canonical and canonical.group(1).lower() != "i":
                parsed.screen_name = canonical.group(1)

    if image and "profile_images" not in image:
        parsed.media = [MediaItem(type="photo", best_url=image)]
    if image and "profile_images" in image:
        parsed.avatar_url = image

    has_tweet_container = bool(re.search(r'data-testid=["\']tweetText["\']', html, re.I))
    parsed.available = bool(parsed.text) or has_tweet_container
    parsed.reason = "available" if parsed.available else "unavailable"
    return parsed


_PARSERS = {
    "syndication": _parse_syndication,
    "fxtwitter": _parse_fxtwitter,
    "vxtwitter": _parse_vxtwitter,
    "x_page": _parse_x_page,
}


def parse_sources(raws: list[RawSource]) -> dict[str, ParsedSource]:
    """Parse each raw body. A broken payload degrades that source only."""
    out: dict[str, ParsedSource] = {}
    for raw in raws:
        name = raw.name
        parser = _PARSERS.get(name)
        if parser is None:
            out[name] = ParsedSource(name=name, available=False, reason="error")
            continue
        if not raw.ok or not raw.body:
            out[name] = ParsedSource(name=name, available=False, reason="error")
            continue
        try:
            out[name] = parser(raw)
        except Exception:  # noqa: BLE001 - one bad payload must not kill a capture
            out[name] = ParsedSource(name=name, available=False, reason="error")
    return out


# --------------------------------------------------------------------------
# merge
# --------------------------------------------------------------------------


def _available(parsed: dict[str, ParsedSource], order: tuple[str, ...]) -> list[ParsedSource]:
    return [parsed[name] for name in order if name in parsed and parsed[name].available]


def _pick_text(parsed: dict[str, ParsedSource]) -> tuple[str | None, str | None]:
    """Pick the fullest text, preferring evidence over a source's self-report.

    A source that is a strict prefix of another source's text is truncated as a
    matter of fact, whatever it claims about itself. That distinction is not
    academic: measured over 12 consecutive calls, api.fxtwitter.com returned
    the full 500-character note tweet 4 times and a silently truncated
    279-character copy 8 times -- the truncated responses even carried
    ``is_note_tweet: false``. Trusting the flag would store the short copy as
    the authoritative post text two thirds of the time.
    """
    candidates = [s for s in _available(parsed, _TEXT_ORDER) if s.text]
    if not candidates:
        return None, None

    normalised = {s.name: normalise_text(s.text or "") for s in candidates}

    def shortened(source: ParsedSource) -> bool:
        mine = normalised[source.name]
        if not mine:
            return True
        return any(
            other != mine and other.startswith(mine)
            for name, other in normalised.items()
            if name != source.name
        )

    def best(source: ParsedSource) -> tuple[str, str]:
        """Recover an authored tail a source dropped.

        Sources that tie in comparison form can still differ in raw form:
        fxtwitter drops a trailing t.co when its own display text omits it,
        which also drops an authored link (measured: `okdiario` lost
        "Noticia completa https://t.co/5GRFUfszbj"). When the chosen raw text
        is a strict prefix of another candidate's, take the longer one.
        """
        chosen = source.text or ""
        for other in candidates:
            text = other.text or ""
            if len(text) > len(chosen) and text.startswith(chosen):
                chosen = text
        return chosen, source.name

    for source in candidates:
        if not source.text_truncated and not shortened(source):
            return best(source)
    for source in candidates:
        if not shortened(source):
            return best(source)
    return best(candidates[0])


def _pick_media(parsed: dict[str, ParsedSource]) -> tuple[list[MediaItem], str | None]:
    for source in _available(parsed, _MEDIA_ORDER):
        if source.media:
            return list(source.media), source.name
    return [], None


def _merge_counts(
    parsed: dict[str, ParsedSource],
) -> tuple[dict[str, int], dict[str, str], dict[str, bool | None]]:
    values: dict[str, int] = {}
    sources: dict[str, str] = {}
    agreement: dict[str, bool | None] = {}

    reporting = _available(parsed, _COUNT_ORDER)
    metrics: list[str] = list(_COUNT_METRICS)
    for source in reporting:
        for metric in source.counts:
            if metric not in metrics:
                metrics.append(metric)

    for metric in metrics:
        seen = [(s.name, s.counts[metric]) for s in reporting if metric in s.counts]
        if not seen:
            continue
        winner, value = seen[0]
        values[metric] = value
        sources[metric] = winner
        # `None` keeps a single-source number visibly single-source instead of
        # letting it read as three sources agreeing.
        agreement[metric] = None if len(seen) == 1 else len({v for _, v in seen}) == 1
    return values, sources, agreement


def _merge_profile(parsed: dict[str, ParsedSource]) -> Profile | None:
    profiles = [s.profile for s in _available(parsed, _PROFILE_ORDER) if s.profile is not None]
    if not profiles:
        return None
    merged = Profile()
    for spec in dataclasses.fields(Profile):
        for profile in profiles:
            value = getattr(profile, spec.name, None)
            if value is not None:
                setattr(merged, spec.name, value)
                break
    return merged


def _first(parsed: dict[str, ParsedSource], order: tuple[str, ...], attr: str) -> Any:
    for source in _available(parsed, order):
        value = getattr(source, attr, None)
        if value is not None:
            return value
    return None


def merge(ref: TweetRef, parsed: dict[str, ParsedSource]) -> Post:
    """Collapse the per-source views into one ``Post`` with provenance kept."""
    tweet_id = ref.tweet_id or ""
    post = Post(id=tweet_id)

    post.text, post.text_source = _pick_text(parsed)
    post.media, post.media_source = _pick_media(parsed)
    post.counts, post.count_sources, post.count_agreement = _merge_counts(parsed)
    post.profile = _merge_profile(parsed)

    post.screen_name = _first(parsed, _NAME_ORDER, "screen_name") or ref.screen_name
    post.name = _first(parsed, _NAME_ORDER, "name_display")
    post.created_at_utc = _first(parsed, _COUNT_ORDER, "created_at_utc")
    post.created_at_display = _first(parsed, _COUNT_ORDER, "created_at_display")
    post.lang = _first(parsed, _NAME_ORDER, "lang")
    post.possibly_sensitive = _first(parsed, _NAME_ORDER, "possibly_sensitive")
    post.blue_verified = _first(parsed, _NAME_ORDER, "blue_verified")
    post.avatar_url = _first(parsed, _PROFILE_ORDER, "avatar_url")
    post.banner_url = _first(parsed, _PROFILE_ORDER, "banner_url")
    post.community_note = _first(parsed, _NAME_ORDER, "community_note")
    post.quoted_id = _first(parsed, _NAME_ORDER, "quoted_id")
    post.quoted_screen_name = _first(parsed, _NAME_ORDER, "quoted_screen_name")
    post.reply_to_id = _first(parsed, _NAME_ORDER, "reply_to_id")
    post.reply_to_screen_name = _first(parsed, _NAME_ORDER, "reply_to_screen_name")

    post.available_sources = [
        name for name in SOURCE_NAMES if name in parsed and parsed[name].available
    ]
    handle = post.screen_name or ref.screen_name or "i"
    post.source_url = f"https://x.com/{handle}/status/{tweet_id}"
    return post
