"""Input parsing and the syndication token derivation.

Two jobs:

* ``syndication_token`` reproduces the token X's own embed widget sends to
  ``cdn.syndication.twimg.com``.  It is ``((id / 1e15) * pi)`` printed in
  base 36 with the radix point and every run of ``0`` removed, where the
  digits come from a 20-iteration float-arithmetic loop -- drift and all.
  See ``_to_base36``: an exact expansion produces a *different* token.

* ``parse_inputs`` turns whatever the operator pasted into ``TweetRef``s.  It
  refuses to guess: a URL on a host that is not X or a known X mirror is an
  error, never a bare id.  (The original tool happily turned a TikTok URL's
  numeric path segment into a "tweet id" and captured a package for a post
  that never existed.)
"""

from __future__ import annotations

import math
import re
from fractions import Fraction
from urllib.parse import urlsplit

from .models import TweetRef

__all__ = ["parse_inputs", "syndication_token"]

_B36_DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"
# Exactly 20, recovered by brute-forcing the digit cap against 31 live tokens
# produced by the reference implementation: 20 matches all 31, 19 matches 11,
# 21 matches 22.
_B36_FRACTION_DIGITS = 20

# Hosts we accept a status URL from.  ``www.`` and ``api.`` prefixes are
# stripped before the lookup; ``nitter.*`` is matched by prefix because the
# instances are self-hosted under arbitrary domains.
_ALLOWED_HOSTS = frozenset(
    {
        "x.com",
        "twitter.com",
        "mobile.twitter.com",
        "mobile.x.com",
        "fxtwitter.com",
        "vxtwitter.com",
        "fixupx.com",
    }
)

# A *bare* number needs 5+ digits to be worth guessing at; inside a status URL
# the host and path already disambiguate, so short historic ids (jack's id 20)
# are accepted there.
_ID_RE = re.compile(r"^\d{5,25}$")
# /i/status/<id> and /i/web/status/<id> -- handle unknown
_PATH_ANON_RE = re.compile(r"^/i(?:/web)?/status(?:es)?/(\d{1,25})(?:$|[/?#])")
# /<handle>/status/<id>
_PATH_USER_RE = re.compile(r"^/([A-Za-z0-9_]{1,20})/status(?:es)?/(\d{1,25})(?:$|[/?#])")
# bare "x.com/user/status/1" with no scheme
_SCHEMELESS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]*\.[A-Za-z]{2,}/")
_SPLIT_RE = re.compile(r"[\s,]+")


def _to_base36(value: float, fraction_digits: int = _B36_FRACTION_DIGITS) -> str:
    """Base-36 rendering of a float64, digits extracted in float arithmetic.

    The arithmetic is deliberately *not* exact. Recovered from 31 live
    captures of the reference implementation: it extracts digits with a plain
    ``frac *= 36; digit = int(frac); frac -= digit`` loop, which drifts from
    the exact expansion once the mantissa runs out, and it always runs exactly
    20 iterations. Doing it exactly with ``Fraction`` produces a different
    token for small ids (id=20 -> ``6dq1a2xwd91cz...`` instead of
    ``6dq1a2xwd93``) and for ids around 1e17-1e18 (a spurious trailing
    ``di``/``i``). Reproducing the drift is the point: the token must match
    what X's own widget would have sent.
    """
    negative = value < 0
    if negative:
        value = -value

    whole = int(value)
    frac = value - whole

    if whole == 0:
        out = "0"
    else:
        digits: list[str] = []
        rest = whole
        while rest:
            rest, rem = divmod(rest, 36)
            digits.append(_B36_DIGITS[rem])
        out = "".join(reversed(digits))

    tail: list[str] = []
    for _ in range(fraction_digits):
        frac *= 36
        digit = int(frac)
        tail.append(_B36_DIGITS[digit])
        frac -= digit
    out += "." + "".join(tail)

    return ("-" + out) if negative else out


def syndication_token(tweet_id: str) -> str:
    """Token accepted by ``cdn.syndication.twimg.com/tweet-result``.

    Presence is what the endpoint currently enforces (a bogus token still
    returns data, a missing one returns ``{}``), but we derive the real value
    so the capture stays faithful if that ever tightens.
    """
    numeric = str(tweet_id).strip()
    if not numeric.isdigit():
        raise ValueError(f"tweet id must be numeric, got {tweet_id!r}")
    value = (int(numeric) / 1e15) * math.pi
    return re.sub(r"(0+|\.)", "", _to_base36(value))


def _normalise_host(host: str) -> str:
    host = host.lower().strip()
    if host.endswith("."):
        host = host[:-1]
    for prefix in ("www.", "api."):
        if host.startswith(prefix):
            host = host[len(prefix) :]
    return host


def _host_supported(host: str) -> bool:
    normalised = _normalise_host(host)
    if normalised.startswith("nitter."):
        return True
    return normalised in _ALLOWED_HOSTS


def _parse_token(token: str) -> TweetRef:
    raw = token.strip().strip("<>\"'")
    if not raw:
        return TweetRef(input=token, error="unrecognised_input")

    if _ID_RE.match(raw):
        return TweetRef(input=raw, tweet_id=raw)

    candidate = raw
    if "://" not in candidate and _SCHEMELESS_RE.match(candidate):
        candidate = "https://" + candidate

    if "://" not in candidate:
        return TweetRef(input=raw, error="unrecognised_input")

    try:
        parts = urlsplit(candidate)
    except ValueError:
        return TweetRef(input=raw, error="unrecognised_input")

    host = (parts.hostname or "").strip()
    if not host:
        return TweetRef(input=raw, error="unrecognised_input")
    if not _host_supported(host):
        return TweetRef(input=raw, error=f"unsupported_host: {host.lower()}")

    path = parts.path or "/"
    if not path.startswith("/"):
        path = "/" + path

    anon = _PATH_ANON_RE.match(path)
    if anon:
        return TweetRef(input=raw, tweet_id=anon.group(1))

    user = _PATH_USER_RE.match(path)
    if user:
        handle = user.group(1)
        # "i" is X's own placeholder for "handle unknown", not a real account.
        return TweetRef(
            input=raw,
            tweet_id=user.group(2),
            screen_name=None if handle.lower() == "i" else handle,
        )

    return TweetRef(input=raw, error="unrecognised_input")


def parse_inputs(text: str | list[str], max_items: int = 25) -> list[TweetRef]:
    """Split, parse and dedupe operator input into capture references.

    Anything past ``max_items`` is still returned, flagged ``too_many_inputs``,
    so the UI can tell the operator exactly what was dropped.
    """
    if isinstance(text, str):
        blob = text
    else:
        blob = "\n".join(str(part) for part in text)

    tokens = [tok for tok in _SPLIT_RE.split(blob) if tok.strip()]

    refs: list[TweetRef] = []
    seen_tokens: set[str] = set()
    seen_ids: set[str] = set()
    for token in tokens:
        key = token.strip()
        if key in seen_tokens:
            continue
        seen_tokens.add(key)
        ref = _parse_token(token)
        if ref.tweet_id is not None:
            if ref.tweet_id in seen_ids:
                continue
            seen_ids.add(ref.tweet_id)
        refs.append(ref)

    limit = max(0, int(max_items))
    for ref in refs[limit:]:
        ref.error = "too_many_inputs"
    return refs
