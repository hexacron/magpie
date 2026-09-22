"""Compare the sources against each other and record where they disagree.

The single most important behaviour here is that **truncation is not
disagreement**.  X's syndication endpoint caps ``text`` at 280 characters, so
for any note tweet the original tool raised ``text_differs_between_sources`` on
every single capture — a flag that fires always is a flag nobody reads.  Here a
text that is a strict prefix of a longer one is reported as
``source_truncated:<name>``, and ``text.agree`` stays ``True``.

Counts get the same treatment in the other direction: a metric only one source
reports is marked ``agree: None``, never ``True``.  "One source said 4,201" and
"three sources agree it is 4,201" are different evidentiary claims.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import SOURCE_NAMES, CrossCheck, ParsedSource, RawSource
from .normalize import normalise_text

__all__ = ["crosscheck"]

# Seconds of clock skew tolerated when comparing created_at across sources.
_TIME_SKEW_S = 2.0


def _parse_iso(value: str) -> datetime | None:
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# Endpoint answers that mean "this post does not exist" rather than
# "we could not reach the endpoint". Getting this wrong makes a deleted post
# look like a tooling failure, which is the opposite of the claim we want.
_NOT_FOUND_MARKERS = ("not found", "no status found", "does not exist", "empty_response")


def _fetch_says_gone(raw: RawSource) -> bool:
    if raw.http_status == 404:
        return True
    error = (raw.error or "").lower()
    return any(marker in error for marker in _NOT_FOUND_MARKERS)


def _availability(
    parsed: dict[str, ParsedSource], raws: dict[str, RawSource]
) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in SOURCE_NAMES:
        raw = raws.get(name)
        source = parsed.get(name)
        if raw is None and source is None:
            out[name] = "skipped"
        elif raw is not None and not raw.ok:
            out[name] = "unavailable" if _fetch_says_gone(raw) else "error"
        elif source is not None and source.available:
            out[name] = "available"
        elif source is not None and source.reason == "error":
            out[name] = "error"
        else:
            out[name] = "unavailable"
    return out


def _compare_text(
    parsed: dict[str, ParsedSource], check: CrossCheck, flags: set[str]
) -> None:
    values: dict[str, str] = {}
    for name in SOURCE_NAMES:
        source = parsed.get(name)
        if source is not None and source.available and source.text:
            values[name] = source.text

    normalised = {name: normalise_text(text) for name, text in values.items()}
    comparable = {name: text for name, text in normalised.items() if text}

    check.text = {
        "values": values,
        "normalised": normalised,
        "agree": None,
        "truncated": [],
        "longest_source": None,
    }

    if not comparable:
        return
    if len(comparable) == 1:
        check.text["agree"] = True
        check.text["longest_source"] = next(iter(comparable))
        return

    longest_source = max(comparable, key=lambda name: len(comparable[name]))
    longest = comparable[longest_source]
    check.text["longest_source"] = longest_source

    divergent: list[str] = []
    truncated: list[str] = []
    for name, text in comparable.items():
        if text == longest:
            continue
        if longest.startswith(text):
            truncated.append(name)
        else:
            divergent.append(name)

    # No pairwise pass is needed: two strings that are both prefixes of the
    # same `longest` are necessarily prefixes of each other.
    if divergent:
        check.text["agree"] = False
        flags.add("text_differs_between_sources")
        check.notes.append(
            "Sources returned genuinely different post text; "
            + ", ".join(sorted(divergent))
            + f" do not match {longest_source}."
        )
        return

    check.text["agree"] = True
    check.text["truncated"] = sorted(set(truncated))
    for name in check.text["truncated"]:
        flags.add(f"source_truncated:{name}")
        check.notes.append(
            f"{name} returned a truncated copy of the post text "
            f"({len(comparable[name])} of {len(longest)} chars, a prefix of "
            f"{longest_source}); not treated as a discrepancy."
        )


def _compare_counts(
    parsed: dict[str, ParsedSource], check: CrossCheck, flags: set[str]
) -> None:
    metrics: list[str] = []
    for name in SOURCE_NAMES:
        source = parsed.get(name)
        if source is None or not source.available:
            continue
        for metric in source.counts:
            if metric not in metrics:
                metrics.append(metric)

    for metric in metrics:
        values: dict[str, int] = {}
        for name in SOURCE_NAMES:
            source = parsed.get(name)
            if source is not None and source.available and metric in source.counts:
                values[name] = source.counts[metric]
        if not values:
            continue
        if len(values) == 1:
            agree: bool | None = None
        else:
            agree = len(set(values.values())) == 1
            if not agree:
                flags.add(f"counts_differ:{metric}")
                check.notes.append(
                    f"{metric} disagrees across sources: "
                    + ", ".join(f"{src}={val}" for src, val in values.items())
                    + "."
                )
        check.counts[metric] = {"values": values, "agree": agree}


def _collect(parsed: dict[str, ParsedSource], attr: str) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in SOURCE_NAMES:
        source = parsed.get(name)
        if source is None or not source.available:
            continue
        value = getattr(source, attr, None)
        if value is not None:
            values[name] = value
    return values


def _compare_fields(
    parsed: dict[str, ParsedSource], check: CrossCheck, flags: set[str]
) -> None:
    handles = _collect(parsed, "screen_name")
    if handles:
        distinct = {str(v).lower() for v in handles.values()}
        agree = None if len(handles) == 1 else len(distinct) == 1
        check.fields["screen_name"] = {"values": handles, "agree": agree}
        if agree is False:
            flags.add("author_differs_between_sources")
            check.notes.append(
                "Author handle disagrees across sources: "
                + ", ".join(f"{src}=@{val}" for src, val in handles.items())
                + "."
            )

    stamps = _collect(parsed, "created_at_utc")
    if stamps:
        parsed_stamps = {
            name: dt
            for name, value in stamps.items()
            if (dt := _parse_iso(str(value))) is not None
        }
        if len(parsed_stamps) < 2:
            agree = None if len(stamps) == 1 else True
        else:
            moments = list(parsed_stamps.values())
            spread = (max(moments) - min(moments)).total_seconds()
            agree = abs(spread) <= _TIME_SKEW_S
        check.fields["created_at_utc"] = {"values": stamps, "agree": agree}
        if agree is False:
            flags.add("created_at_differs_between_sources")
            check.notes.append(
                "Post timestamp disagrees across sources beyond "
                f"{_TIME_SKEW_S:g}s: "
                + ", ".join(f"{src}={val}" for src, val in stamps.items())
                + "."
            )


def crosscheck(parsed: dict[str, ParsedSource], raws: list[RawSource]) -> CrossCheck:
    """Compare every available source and return the agreement report."""
    raw_by_name = {raw.name: raw for raw in raws}
    check = CrossCheck()
    flags: set[str] = set()

    check.availability = _availability(parsed, raw_by_name)

    for name, state in check.availability.items():
        raw = raw_by_name.get(name)
        if state == "error":
            flags.add(f"source_error:{name}")
            reason = (raw.error if raw is not None else None) or "parse failed"
            check.notes.append(f"{name} did not return usable data: {reason}.")
        elif state == "unavailable":
            flags.add(f"source_unavailable:{name}")
            if raw is not None and not raw.ok:
                check.notes.append(
                    f"{name} reported the post as not present ({raw.error or 'HTTP 404'})."
                )

    if not any(state == "available" for state in check.availability.values()):
        flags.add("post_unavailable_everywhere")
        check.notes.append("No source returned the post; it is deleted, private or suspended.")

    _compare_text(parsed, check, flags)
    _compare_counts(parsed, check, flags)
    _compare_fields(parsed, check, flags)

    check.flags = sorted(flags)
    return check
