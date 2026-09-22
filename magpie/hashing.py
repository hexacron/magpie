"""Hashing, RFC 3161 anchoring and offline verification of a capture package.

The evidence chain has exactly three links:

1. ``hash_tree()`` digests every immutable file in the package.
2. ``manifest.json`` records those digests; ``MANIFEST.sha256`` records the
   digest of ``manifest.json`` itself.
3. ``timestamp_manifest()`` asks an RFC 3161 TSA to sign a timestamp over the
   sha256 of ``manifest.json``.  That is the only link a third party attests
   to; everything above it is self-computed.

Because the token covers ``manifest.json``, the external check is::

    openssl ts -verify -data manifest.json -in timestamp.tsr -CAfile <tsa-ca.pem>

Files that are written *after* sealing (or that a human may edit) are excluded
from the hash tree via ``MUTABLE_FILES`` -- they are metadata about the
package, not evidence.

No third-party crypto dependency: the TimeStampReq is hand-built DER.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import shutil
import subprocess
from pathlib import Path

import httpx

from .config import Settings
from .models import TimestampResult, VerifyResult

# Written after the manifest is sealed, or editable by the operator.
MUTABLE_FILES = {
    "manifest.json",
    "MANIFEST.sha256",
    "meta.json",
    "timestamp.tsr",
    "timestamp.txt",
    "timestamp.json",
}

_CHUNK = 1024 * 1024

OID_SHA256 = "2.16.840.1.101.3.4.2.1"
OID_SIGNED_DATA = "1.2.840.113549.1.7.2"

# DER of AlgorithmIdentifier{sha256, NULL} followed by OCTET STRING of length 32.
_SHA256_IMPRINT_PREFIX = bytes.fromhex("300d060960864801650304020105000420")
# Same, with the optional NULL parameter omitted (both forms occur in the wild).
_SHA256_IMPRINT_PREFIX_NONULL = bytes.fromhex("300b06096086480165030402010420")


# --------------------------------------------------------------------------
# digests
# --------------------------------------------------------------------------


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        while True:
            chunk = fh.read(_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def hash_tree(pkg_dir: Path) -> dict[str, str]:
    """Digest every immutable file in the package.

    Keys are POSIX-style paths relative to ``pkg_dir``.  ``MUTABLE_FILES`` and
    anything hidden (a path component starting with ``.``) are skipped.
    """
    pkg_dir = Path(pkg_dir)
    out: dict[str, str] = {}
    for path in pkg_dir.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        rel = path.relative_to(pkg_dir).as_posix()
        if rel in MUTABLE_FILES:
            continue
        if any(part.startswith(".") for part in rel.split("/")):
            continue
        out[rel] = sha256_file(path)
    return {k: out[k] for k in sorted(out)}


# --------------------------------------------------------------------------
# minimal DER writer (enough for an RFC 3161 TimeStampReq)
# --------------------------------------------------------------------------


def _der_len(length: int) -> bytes:
    if length < 0x80:
        return bytes([length])
    body = length.to_bytes((length.bit_length() + 7) // 8, "big")
    return bytes([0x80 | len(body)]) + body


def _der_tlv(tag: int, body: bytes) -> bytes:
    return bytes([tag]) + _der_len(len(body)) + body


def _der_seq(*parts: bytes) -> bytes:
    return _der_tlv(0x30, b"".join(parts))


def _der_int(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative integers are not needed here")
    if value == 0:
        return _der_tlv(0x02, b"\x00")
    # +8 keeps a leading zero byte when the top bit would be set (unsigned).
    return _der_tlv(0x02, value.to_bytes((value.bit_length() + 8) // 8, "big"))


def _der_oid(dotted: str) -> bytes:
    parts = [int(p) for p in dotted.split(".")]
    body = bytearray([parts[0] * 40 + parts[1]])
    for node in parts[2:]:
        chunk = [node & 0x7F]
        node >>= 7
        while node:
            chunk.append((node & 0x7F) | 0x80)
            node >>= 7
        body.extend(reversed(chunk))
    return _der_tlv(0x06, bytes(body))


def _der_octet(data: bytes) -> bytes:
    return _der_tlv(0x04, data)


def _der_bool(value: bool) -> bytes:
    return _der_tlv(0x01, b"\xff" if value else b"\x00")


def _der_null() -> bytes:
    return b"\x05\x00"


def build_timestamp_request(digest: bytes, nonce: int | None = None) -> bytes:
    """RFC 3161 TimeStampReq over a raw sha256 digest, certReq = TRUE."""
    if len(digest) != 32:
        raise ValueError(f"sha256 digest must be 32 bytes, got {len(digest)}")
    if nonce is None:
        nonce = int.from_bytes(secrets.token_bytes(8), "big") | 1
    algorithm = _der_seq(_der_oid(OID_SHA256), _der_null())
    imprint = _der_seq(algorithm, _der_octet(digest))
    return _der_seq(_der_int(1), imprint, _der_int(nonce), _der_bool(True))


# --------------------------------------------------------------------------
# minimal DER scanning (best effort, never raises)
# --------------------------------------------------------------------------


def _short_tlv_at(der: bytes, index: int) -> tuple[bytes, int] | None:
    """Value and end offset of a short-form TLV at ``index``; None if not one."""
    if index + 1 >= len(der):
        return None
    length = der[index + 1]
    if length & 0x80:
        return None
    end = index + 2 + length
    if end > len(der):
        return None
    return der[index + 2 : end], end


def _scan_gen_time(der: bytes) -> tuple[str | None, int | None]:
    """Find the TSTInfo genTime (GeneralizedTime, tag 0x18)."""
    for i, tag in enumerate(der):
        if tag != 0x18:
            continue
        found = _short_tlv_at(der, i)
        if found is None:
            continue
        raw, _ = found
        if len(raw) < 14:
            continue
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError:
            continue
        if not text[:14].isdigit():
            continue
        if not 1990 <= int(text[:4]) <= 2200:
            continue
        iso = (
            f"{text[0:4]}-{text[4:6]}-{text[6:8]}T{text[8:10]}:{text[10:12]}:{text[12:14]}Z"
        )
        return iso, i
    return None, None


def _scan_serial(der: bytes, gen_time_index: int) -> str | None:
    """TSTInfo.serialNumber is the INTEGER immediately preceding genTime."""
    start = max(0, gen_time_index - 64)
    for j in range(gen_time_index - 2, start - 1, -1):
        if der[j] != 0x02:
            continue
        found = _short_tlv_at(der, j)
        if found is None:
            continue
        raw, end = found
        if raw and end == gen_time_index:
            return raw.hex()
    return None


def _token_imprints(der: bytes) -> set[str]:
    """Every sha256 message imprint embedded in a timestamp token."""
    out: set[str] = set()
    for prefix in (_SHA256_IMPRINT_PREFIX, _SHA256_IMPRINT_PREFIX_NONULL):
        start = 0
        while True:
            idx = der.find(prefix, start)
            if idx < 0:
                break
            value = der[idx + len(prefix) : idx + len(prefix) + 32]
            if len(value) == 32:
                out.add(value.hex())
            start = idx + 1
    return out


# --------------------------------------------------------------------------
# RFC 3161
# --------------------------------------------------------------------------


async def timestamp_manifest(
    manifest_sha256: str, pkg_dir: Path, settings: Settings
) -> TimestampResult:
    """Anchor ``manifest_sha256`` (the digest OF manifest.json) at a TSA.

    Called after the manifest is sealed, so the token covers the sealed file:
    ``openssl ts -verify -data manifest.json -in timestamp.tsr -CAfile ca.pem``.
    Never raises; any failure is reported in ``TimestampResult.error``.
    """
    if not settings.tsa:
        return TimestampResult(enabled=False)

    pkg_dir = Path(pkg_dir)
    result = TimestampResult(
        enabled=True, tsa_url=settings.tsa_url, digest=manifest_sha256, hash_alg="sha256"
    )
    try:
        request = build_timestamp_request(bytes.fromhex(manifest_sha256))
    except ValueError as exc:
        result.error = f"bad_digest: {exc}"
        return result

    kwargs: dict[str, object] = {"timeout": settings.tsa_timeout, "follow_redirects": True}
    if settings.proxy:
        kwargs["proxy"] = settings.proxy
    try:
        try:
            client = httpx.AsyncClient(**kwargs)  # type: ignore[arg-type]
        except TypeError:  # older httpx without proxy=
            client = httpx.AsyncClient(timeout=settings.tsa_timeout, follow_redirects=True)
        async with client:
            response = await client.post(
                settings.tsa_url,
                content=request,
                headers={
                    "Content-Type": "application/timestamp-query",
                    "Accept": "application/timestamp-reply",
                },
            )
    except Exception as exc:  # noqa: BLE001 - a TSA outage must not kill a capture
        result.error = f"tsa_request_failed: {type(exc).__name__}: {exc}"
        return result

    body = response.content or b""
    if response.status_code != 200 or not body:
        result.error = f"tsa_http_{response.status_code}: {len(body)} bytes"
        return result

    tsr_path = pkg_dir / "timestamp.tsr"
    try:
        tsr_path.write_bytes(body)
        result.file = "timestamp.tsr"
    except OSError as exc:
        result.error = f"tsr_write_failed: {exc}"
        return result

    if _der_oid(OID_SIGNED_DATA) not in body:
        result.error = "tsa_rejected: reply carries no signed timestamp token"
        return result

    gen_time, gen_index = _scan_gen_time(body)
    result.gen_time = gen_time
    if gen_index is not None:
        result.serial = _scan_serial(body, gen_index)
    if manifest_sha256.lower() not in _token_imprints(body):
        result.error = "token_imprint_mismatch: reply does not cover this manifest digest"
        return result

    result.ok = True
    await _write_timestamp_text(tsr_path)
    return result


async def _write_timestamp_text(tsr_path: Path) -> None:
    """Human-readable dump of the token, when openssl happens to be installed."""
    if not shutil.which("openssl"):
        return
    try:
        proc = await asyncio.to_thread(
            subprocess.run,
            ["openssl", "ts", "-reply", "-in", str(tsr_path), "-text"],
            capture_output=True,
            timeout=30,
        )
        if proc.returncode == 0 and proc.stdout:
            tsr_path.with_name("timestamp.txt").write_bytes(proc.stdout)
    except Exception:  # noqa: BLE001 - purely cosmetic output
        return


# --------------------------------------------------------------------------
# verification
# --------------------------------------------------------------------------


def _openssl_verify(pkg_dir: Path, tsr_path: Path) -> tuple[bool | None, str | None]:
    """`openssl ts -verify` against manifest.json. (ok, note)."""
    if not shutil.which("openssl"):
        return None, "openssl not installed: timestamp signature not checked"
    cmd = [
        "openssl",
        "ts",
        "-verify",
        "-data",
        str(pkg_dir / "manifest.json"),
        "-in",
        str(tsr_path),
    ]
    ca_file = os.environ.get("MAGPIE_TSA_CAFILE")
    if ca_file and Path(ca_file).is_file():
        cmd += ["-CAfile", ca_file]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=30)
    except Exception as exc:  # noqa: BLE001
        return None, f"openssl ts -verify failed to run: {type(exc).__name__}: {exc}"
    if proc.returncode == 0:
        return True, None
    detail = (proc.stderr or proc.stdout).decode("utf-8", "replace").strip().splitlines()
    return None, "openssl ts -verify inconclusive: " + (detail[-1] if detail else "no output")


def _check_timestamp(pkg_dir: Path, manifest_digest: str | None) -> tuple[bool | None, list[str]]:
    tsr_path = pkg_dir / "timestamp.tsr"
    if not tsr_path.is_file():
        return None, []
    notes: list[str] = []
    try:
        token = tsr_path.read_bytes()
    except OSError as exc:
        return None, [f"timestamp.tsr unreadable: {exc}"]

    # Cheap, dependency-free, and decisive in the negative direction: does the
    # token's message imprint actually cover this manifest?
    imprints = _token_imprints(token)
    if manifest_digest and imprints and manifest_digest.lower() not in imprints:
        return False, ["timestamp.tsr does not cover the current manifest.json"]

    sidecar = pkg_dir / "timestamp.json"
    if sidecar.is_file():
        try:
            recorded = json.loads(sidecar.read_text("utf-8"))
        except (OSError, ValueError) as exc:
            notes.append(f"timestamp.json unreadable: {exc}")
        else:
            if recorded.get("error"):
                notes.append(f"timestamp recorded an error: {recorded['error']}")
            recorded_digest = (recorded.get("digest") or "").lower()
            if recorded_digest and manifest_digest and recorded_digest != manifest_digest.lower():
                return False, notes + ["timestamp.json digest differs from manifest.json digest"]

    ok, note = _openssl_verify(pkg_dir, tsr_path)
    if note:
        notes.append(note)
    return ok, notes


def verify_package(pkg_dir: Path) -> VerifyResult:
    """Recompute the whole chain offline. Never raises."""
    pkg_dir = Path(pkg_dir)
    result = VerifyResult(folder=pkg_dir.name)

    manifest_path = pkg_dir / "manifest.json"
    if not manifest_path.is_file():
        result.errors.append("manifest.json is missing")
        return result
    try:
        manifest = json.loads(manifest_path.read_text("utf-8"))
    except (OSError, ValueError) as exc:
        result.errors.append(f"manifest.json unreadable: {exc}")
        return result

    recorded_files = manifest.get("files") or {}
    if not isinstance(recorded_files, dict):
        recorded_files = {}
        result.errors.append("manifest.json has no usable 'files' map")

    actual = hash_tree(pkg_dir)
    result.files_checked = len(recorded_files)
    for rel in sorted(recorded_files):
        current = actual.get(rel)
        if current is None:
            result.missing.append(rel)
        elif current != recorded_files[rel]:
            result.mismatched.append(rel)
    result.unexpected = [rel for rel in sorted(actual) if rel not in recorded_files]

    result.manifest_sha256 = sha256_file(manifest_path)
    sidecar = pkg_dir / "MANIFEST.sha256"
    manifest_ok = False
    if sidecar.is_file():
        try:
            raw = sidecar.read_text("utf-8").strip()
        except OSError as exc:
            result.errors.append(f"MANIFEST.sha256 unreadable: {exc}")
            raw = ""
        recorded_hash = raw.split()[0] if raw else ""
        result.recorded_manifest_sha256 = recorded_hash or None
        manifest_ok = bool(recorded_hash) and recorded_hash == result.manifest_sha256
        if not manifest_ok:
            result.errors.append("manifest.json does not match MANIFEST.sha256")
    else:
        result.errors.append("MANIFEST.sha256 is missing")

    for rel in result.mismatched:
        result.errors.append(f"content changed: {rel}")
    for rel in result.missing:
        result.errors.append(f"file listed in manifest is gone: {rel}")

    timestamp_ok, notes = _check_timestamp(pkg_dir, result.manifest_sha256)
    result.timestamp_ok = timestamp_ok
    result.errors.extend(notes)

    result.ok = not result.mismatched and not result.missing and manifest_ok
    return result
