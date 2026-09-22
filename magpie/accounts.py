"""Account-cookie credential store with rotation, cooldown and burn tracking.

An X ``auth_token`` is a *full session*: it can post, DM and delete. That is a
categorically different secret from ``MAGPIE_AUTH_TOKEN`` (this app's own API key),
so it is deliberately kept out of the environment, out of every log line and
out of every error string:

* the file lives at ``settings.accounts_file`` (default ``data_dir/accounts.json``)
  with mode ``0600`` in a ``0700`` parent, and a too-permissive mode found on
  load is corrected and recorded in :attr:`AccountStore.warnings`;
* :class:`Account` overrides ``__repr__``/``__str__`` with the redacted form,
  so a stray ``print``, ``%r`` or traceback cannot leak a session.

Rotation is the other half. Accounts used for this get rate limited and
suspended, so the store treats that as normal operating condition rather than
an error: ``acquire`` hands out the least-recently-used live account, a
rate-limit reset parks one in ``cooling`` until the epoch X named, and three
consecutive failures retire it as ``dead`` with the reason kept.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import Model

__all__ = ["Account", "AccountStore", "redact"]

STATE_ACTIVE = "active"
STATE_COOLING = "cooling"
STATE_DEAD = "dead"

# Consecutive failures before an account is retired. Two is noise (a timeout
# plus a retry); three in a row is the account, not the network.
MAX_FAILURES = 3

# Cool an account *before* it 429s: the last couple of calls in a window are
# not worth the rate-limit penalty that follows them.
LOW_REMAINING = 2

# Fallback parking time when X sends a 429 without a usable reset header.
DEFAULT_COOLDOWN = 900.0

FILE_VERSION = 1


def _mask(value: str | None) -> str:
    """``"****abcd"`` -- enough to tell two credentials apart, not to use one."""
    text = (value or "").strip()
    if len(text) <= 4:
        return "****"
    return f"****{text[-4:]}"


def redact(text: str | None, *secrets: str | None) -> str:
    """Belt-and-braces: strip any known secret out of a free-form string."""
    out = text or ""
    for secret in secrets:
        cleaned = (secret or "").strip()
        if len(cleaned) >= 8 and cleaned in out:
            out = out.replace(cleaned, _mask(cleaned))
    return out


def _now() -> float:
    return time.time()


def _iso(epoch: float) -> str:
    return (
        datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _epoch(stamp: str | None) -> float | None:
    if not stamp:
        return None
    try:
        text = stamp.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _str(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass(repr=False)
class Account(Model):
    """One throwaway account's cookies plus its health.

    ``auth_token`` and ``ct0`` are the two cookies x.com's web client sends;
    ``ct0`` doubles as the CSRF header value, which is why a mismatch between
    the two is a 403 rather than a 401.
    """

    label: str
    auth_token: str
    ct0: str
    proxy: str | None = None
    state: str = STATE_ACTIVE
    last_used_utc: str | None = None
    cooldown_until_utc: str | None = None
    failures: int = 0
    note: str | None = None

    # -- display -----------------------------------------------------------

    def redacted(self) -> dict[str, Any]:
        """The full record with both cookies masked. Every display path uses this."""
        data = self.to_dict()
        data["auth_token"] = _mask(self.auth_token)
        data["ct0"] = _mask(self.ct0)
        return data

    def __repr__(self) -> str:
        parts = [
            f"label={self.label!r}",
            f"auth_token={_mask(self.auth_token)}",
            f"ct0={_mask(self.ct0)}",
            f"state={self.state!r}",
            f"failures={self.failures}",
        ]
        if self.cooldown_until_utc:
            parts.append(f"cooldown_until={self.cooldown_until_utc}")
        return f"Account({', '.join(parts)})"

    __str__ = __repr__

    # -- health ------------------------------------------------------------

    @property
    def cooldown_epoch(self) -> float | None:
        return _epoch(self.cooldown_until_utc)

    def usable_at(self, now: float) -> bool:
        """Live, and past any cooldown. Does not mutate -- see ``_promote``."""
        if self.state == STATE_DEAD:
            return False
        until = self.cooldown_epoch
        return until is None or until <= now

    def _promote(self, now: float) -> None:
        """A cooling account whose reset has passed goes back to active."""
        if self.state != STATE_COOLING:
            return
        until = self.cooldown_epoch
        if until is None or until <= now:
            self.state = STATE_ACTIVE
            self.cooldown_until_utc = None


def _account_from(data: Any) -> Account | None:
    if not isinstance(data, dict):
        return None
    label = _str(data.get("label"))
    auth_token = _str(data.get("auth_token"))
    ct0 = _str(data.get("ct0"))
    if not (label and auth_token and ct0):
        return None
    state = _str(data.get("state")) or STATE_ACTIVE
    if state not in (STATE_ACTIVE, STATE_COOLING, STATE_DEAD):
        state = STATE_ACTIVE
    return Account(
        label=label,
        auth_token=auth_token,
        ct0=ct0,
        proxy=_str(data.get("proxy")),
        state=state,
        last_used_utc=_str(data.get("last_used_utc")),
        cooldown_until_utc=_str(data.get("cooldown_until_utc")),
        failures=max(0, _int(data.get("failures"))),
        note=_str(data.get("note")),
    )


class AccountStore:
    """The credential file plus the rotation policy over it.

    Every mutating call persists immediately: a crawl that dies mid-run must
    not come back and re-use an account X has already suspended.
    """

    def __init__(self, settings: Any) -> None:
        self.settings = settings
        self.path = self._resolve_path(settings)
        self.warnings: list[str] = []
        self.error: str | None = None
        self._accounts: list[Account] = []
        self._load()

    # -- paths and io ------------------------------------------------------

    @staticmethod
    def _resolve_path(settings: Any) -> Path:
        configured = getattr(settings, "accounts_file", None)
        if configured:
            return Path(str(configured)).expanduser()
        data_dir = getattr(settings, "data_dir", None) or Path("./data")
        return Path(str(data_dir)).expanduser() / "accounts.json"

    def _load(self) -> None:
        self._accounts = []
        try:
            info = self.path.stat()
        except FileNotFoundError:
            return
        except OSError as exc:
            self.error = f"accounts file unreadable: {type(exc).__name__}"
            return
        if info.st_mode & 0o077:
            # Found group/world readable. Fix it rather than merely complain:
            # the credential is already on disk, tightening it is strictly
            # better than leaving it exposed until someone reads the warning.
            try:
                os.chmod(self.path, 0o600)
                self.warnings.append(
                    f"{self.path} was mode {info.st_mode & 0o777:04o}; tightened to 0600"
                )
            except OSError as exc:
                self.warnings.append(
                    f"{self.path} is mode {info.st_mode & 0o777:04o} and could not be "
                    f"tightened: {type(exc).__name__}"
                )
        try:
            raw = json.loads(self.path.read_text("utf-8"))
        except (OSError, ValueError) as exc:
            self.error = f"accounts file unparseable: {type(exc).__name__}"
            return
        entries = raw.get("accounts") if isinstance(raw, dict) else raw
        if not isinstance(entries, list):
            self.error = "accounts file has no account list"
            return
        seen: set[str] = set()
        for entry in entries:
            account = _account_from(entry)
            if account is None or account.label in seen:
                continue
            seen.add(account.label)
            self._accounts.append(account)

    def _save(self) -> None:
        payload = {
            "version": FILE_VERSION,
            "updated_utc": _iso(_now()),
            "accounts": [a.to_dict() for a in self._accounts],
        }
        body = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
        parent = self.path.parent
        try:
            parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        except OSError as exc:
            self.error = f"accounts dir unwritable: {type(exc).__name__}"
            return
        tmp = parent / f".{self.path.name}.tmp"
        try:
            # O_CREAT with an explicit 0600 mode: the secret is never, even
            # momentarily, visible to the rest of the machine.
            fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
            try:
                os.write(fd, body.encode("utf-8"))
            finally:
                os.close(fd)
            os.replace(tmp, self.path)
            os.chmod(self.path, 0o600)
        except OSError as exc:
            self.error = f"accounts file unwritable: {type(exc).__name__}"
            try:
                tmp.unlink()
            except OSError:
                pass
            return
        self.error = None

    def reload(self) -> None:
        self.warnings = []
        self._load()

    # -- membership --------------------------------------------------------

    def _find(self, label: str) -> Account | None:
        for account in self._accounts:
            if account.label == label:
                return account
        return None

    def add(
        self,
        label: str,
        auth_token: str,
        ct0: str,
        proxy: str | None = None,
        note: str | None = None,
    ) -> Account:
        """Add or replace one account. Re-adding a label revives a dead one."""
        account = Account(
            label=label,
            auth_token=(auth_token or "").strip(),
            ct0=(ct0 or "").strip(),
            proxy=proxy or None,
            note=note or None,
        )
        existing = self._find(label)
        if existing is not None:
            self._accounts[self._accounts.index(existing)] = account
        else:
            self._accounts.append(account)
        self._save()
        return account

    def remove(self, label: str) -> bool:
        account = self._find(label)
        if account is None:
            return False
        self._accounts.remove(account)
        self._save()
        return True

    def list(self) -> list[Account]:
        return list(self._accounts)

    def get(self, label: str) -> Account | None:
        return self._find(label)

    # -- rotation ----------------------------------------------------------

    def usable(self) -> list[Account]:
        """Live accounts, least-recently-used first. Promotes expired cooldowns."""
        now = _now()
        promoted = False
        ready: list[Account] = []
        for account in self._accounts:
            before = account.state
            account._promote(now)
            promoted = promoted or account.state != before
            if account.usable_at(now):
                ready.append(account)
        if promoted:
            self._save()
        ready.sort(key=lambda a: (_epoch(a.last_used_utc) or 0.0))
        return ready

    def acquire(self) -> Account | None:
        """The least-recently-used live account, or ``None`` if none is usable."""
        ready = self.usable()
        if not ready:
            return None
        account = ready[0]
        account.last_used_utc = _iso(_now())
        self._save()
        return account

    def release(
        self,
        label: str,
        *,
        ok: bool = True,
        status: int | None = None,
        rate_remaining: int | None = None,
        rate_reset: float | None = None,
        error: str | None = None,
    ) -> None:
        """Record the outcome of one call and apply the resulting policy."""
        account = self._find(label)
        if account is None:
            return
        account.last_used_utc = _iso(_now())
        if ok:
            account.failures = 0
            if status is not None and account.note and account.state == STATE_ACTIVE:
                account.note = None
            remaining = rate_remaining if rate_remaining is None else _int(rate_remaining, -1)
            if remaining is not None and 0 <= remaining <= LOW_REMAINING:
                until = float(rate_reset) if rate_reset else _now() + DEFAULT_COOLDOWN
                self._cool(account, until, f"rate window exhausted (remaining={remaining})")
        else:
            account.failures += 1
            reason = redact(error, account.auth_token, account.ct0) or (
                f"HTTP {status}" if status is not None else "request failed"
            )
            account.note = reason
            if account.failures >= MAX_FAILURES:
                self._kill(account, f"{MAX_FAILURES} consecutive failures: {reason}")
        self._save()

    def cooldown(self, label: str, until_epoch: float, reason: str) -> None:
        account = self._find(label)
        if account is None:
            return
        self._cool(account, until_epoch, reason)
        self._save()

    def mark_dead(self, label: str, reason: str) -> None:
        account = self._find(label)
        if account is None:
            return
        self._kill(account, reason)
        self._save()

    def _cool(self, account: Account, until_epoch: float, reason: str) -> None:
        if account.state == STATE_DEAD:
            return
        try:
            until = float(until_epoch)
        except (TypeError, ValueError):
            until = _now() + DEFAULT_COOLDOWN
        # A reset stamp already in the past would park the account for no time
        # at all; X sends those when the window rolled over mid-flight.
        until = max(until, _now() + 1.0)
        account.state = STATE_COOLING
        account.cooldown_until_utc = _iso(until)
        account.note = redact(reason, account.auth_token, account.ct0) or None

    def _kill(self, account: Account, reason: str) -> None:
        account.state = STATE_DEAD
        account.cooldown_until_utc = None
        account.note = redact(reason, account.auth_token, account.ct0) or None

    # -- reporting ---------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        """Counts plus the redacted roster -- safe to log or serve verbatim."""
        counts = {STATE_ACTIVE: 0, STATE_COOLING: 0, STATE_DEAD: 0}
        for account in self._accounts:
            counts[account.state] = counts.get(account.state, 0) + 1
        return {
            "file": str(self.path),
            "total": len(self._accounts),
            "active": counts.get(STATE_ACTIVE, 0),
            "cooling": counts.get(STATE_COOLING, 0),
            "dead": counts.get(STATE_DEAD, 0),
            "usable": len(self.usable()),
            "accounts": [a.redacted() for a in self._accounts],
            "warnings": list(self.warnings),
            "error": self.error,
        }
