"""Offline tests for the evidence chain: hash tree, verification, RFC 3161 DER."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest

from magpie.config import Settings
from magpie.hashing import (
    MUTABLE_FILES,
    OID_SHA256,
    _der_len,
    _der_oid,
    build_timestamp_request,
    hash_tree,
    sha256_bytes,
    timestamp_manifest,
    verify_package,
)

EVIDENCE_FILES = {
    "syndication.json": b'{"text": "hello"}',
    "fxtwitter.json": b'{"tweet": {"text": "hello world"}}',
    "x_page.html": b"<html><title>x</title></html>",
    "capture.html": b"<html>record</html>",
    "media/photo_01.jpg": b"\xff\xd8\xff\xe0not-really-a-jpeg",
    "media/video_01.mp4": b"\x00\x00\x00\x18ftypmp42" + b"payload" * 100,
}


def _seal(pkg_dir: Path) -> dict[str, str]:
    """Write manifest.json + MANIFEST.sha256 over the current tree."""
    files = hash_tree(pkg_dir)
    manifest = {"tool": "magpie", "folder": pkg_dir.name, "files": files}
    body = json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8")
    (pkg_dir / "manifest.json").write_bytes(body)
    (pkg_dir / "MANIFEST.sha256").write_text(f"{sha256_bytes(body)}  manifest.json\n")
    return files


@pytest.fixture()
def package(tmp_path: Path) -> Path:
    pkg = tmp_path / "20260922T101500Z_someone_1234567890"
    (pkg / "media").mkdir(parents=True)
    for rel, data in EVIDENCE_FILES.items():
        (pkg / rel).write_bytes(data)
    # Mutable and hidden files must stay out of the hash tree.
    (pkg / "meta.json").write_text('{"tags": ["x"], "note": "edited later"}')
    (pkg / "timestamp.tsr").write_bytes(b"\x30\x03\x02\x01\x00")
    (pkg / "timestamp.txt").write_text("human readable")
    (pkg / "timestamp.json").write_text('{"enabled": false}')
    (pkg / ".DS_Store").write_bytes(b"junk")
    _seal(pkg)
    return pkg


# ---------------------------------------------------------------- hash_tree


def test_hash_tree_covers_evidence_and_excludes_mutable_and_hidden(package: Path) -> None:
    tree = hash_tree(package)

    assert set(tree) == set(EVIDENCE_FILES)
    for rel, data in EVIDENCE_FILES.items():
        assert tree[rel] == hashlib.sha256(data).hexdigest()

    for name in MUTABLE_FILES:
        assert name not in tree
    assert ".DS_Store" not in tree
    assert list(tree) == sorted(tree)


def test_hash_tree_excludes_every_file_named_in_mutable_files(package: Path) -> None:
    # A future sidecar must be excluded by name, not by luck of the fixture.
    for name in MUTABLE_FILES:
        (package / name).write_bytes(b"whatever")
    assert set(hash_tree(package)) == set(EVIDENCE_FILES)


# ----------------------------------------------------------- verify_package


def test_verify_intact_package(package: Path) -> None:
    result = verify_package(package)

    assert result.ok is True
    assert result.mismatched == []
    assert result.missing == []
    assert result.unexpected == []
    assert result.files_checked == len(EVIDENCE_FILES)
    assert result.manifest_sha256 == result.recorded_manifest_sha256


def test_verify_pinpoints_a_single_flipped_byte(package: Path) -> None:
    target = package / "media" / "photo_01.jpg"
    data = bytearray(target.read_bytes())
    data[4] ^= 0x01
    target.write_bytes(bytes(data))

    result = verify_package(package)

    assert result.ok is False
    assert result.mismatched == ["media/photo_01.jpg"]
    assert result.missing == []
    assert any("media/photo_01.jpg" in err for err in result.errors)


def test_verify_reports_deleted_and_added_files(package: Path) -> None:
    (package / "x_page.html").unlink()
    (package / "media" / "photo_02.jpg").write_bytes(b"planted")

    result = verify_package(package)

    assert result.ok is False
    assert result.missing == ["x_page.html"]
    assert result.unexpected == ["media/photo_02.jpg"]
    assert result.mismatched == []


def test_verify_detects_a_rewritten_manifest(package: Path) -> None:
    manifest = json.loads((package / "manifest.json").read_text())
    manifest["files"]["media/photo_01.jpg"] = "0" * 64
    (package / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

    result = verify_package(package)

    assert result.ok is False
    assert result.manifest_sha256 != result.recorded_manifest_sha256
    assert result.mismatched == ["media/photo_01.jpg"]


def test_verify_survives_a_package_without_a_manifest(tmp_path: Path) -> None:
    empty = tmp_path / "20260922T101500Z_someone_1"
    empty.mkdir()

    result = verify_package(empty)

    assert result.ok is False
    assert result.errors


# ------------------------------------------------------------ RFC 3161 DER


def _tlv(data: bytes, index: int = 0) -> tuple[int, bytes, int]:
    """Decode one DER TLV: returns (tag, value, offset just past it)."""
    tag = data[index]
    length = data[index + 1]
    index += 2
    if length & 0x80:
        count = length & 0x7F
        length = int.from_bytes(data[index : index + count], "big")
        index += count
    return tag, data[index : index + length], index + length


def test_der_length_encoding_is_correct_across_the_form_boundary() -> None:
    assert _der_len(0) == b"\x00"
    assert _der_len(127) == b"\x7f"
    assert _der_len(128) == b"\x81\x80"
    assert _der_len(200) == b"\x81\xc8"
    assert _der_len(300) == b"\x82\x01\x2c"
    assert _der_len(65535) == b"\x82\xff\xff"
    assert _der_len(65536) == b"\x83\x01\x00\x00"


def test_timestamp_request_is_a_well_formed_rfc3161_query() -> None:
    digest = hashlib.sha256(b"manifest.json contents").digest()

    request = build_timestamp_request(digest, nonce=0x0102030405060708)

    tag, body, end = _tlv(request)
    assert tag == 0x30  # TimeStampReq ::= SEQUENCE
    assert end == len(request)  # outer length covers exactly the whole message

    version_tag, version_value, after_version = _tlv(body)
    assert (version_tag, version_value) == (0x02, b"\x01")

    imprint_tag, imprint, after_imprint = _tlv(body, after_version)
    assert imprint_tag == 0x30  # MessageImprint ::= SEQUENCE
    algorithm, algorithm_body, algorithm_end = _tlv(imprint)
    assert algorithm == 0x30
    oid_tag, oid_value, oid_end = _tlv(algorithm_body)
    assert oid_tag == 0x06
    assert bytes([oid_tag]) + bytes([len(oid_value)]) + oid_value == _der_oid(OID_SHA256)
    assert _tlv(algorithm_body, oid_end)[0] == 0x05  # NULL parameters

    octet_tag, octet_value, _ = _tlv(imprint, algorithm_end)
    assert octet_tag == 0x04
    assert octet_value == digest
    assert len(octet_value) == 32

    nonce_tag, nonce_value, nonce_end = _tlv(body, after_imprint)
    assert nonce_tag == 0x02
    assert int.from_bytes(nonce_value, "big") == 0x0102030405060708

    cert_req_tag, cert_req_value, cert_req_end = _tlv(body, nonce_end)
    assert (cert_req_tag, cert_req_value) == (0x01, b"\xff")
    assert cert_req_end == len(body)


def test_timestamp_request_rejects_a_non_sha256_imprint() -> None:
    with pytest.raises(ValueError):
        build_timestamp_request(b"too short")


def test_sha256_oid_encoding() -> None:
    # 2.16.840.1.101.3.4.2.1 -> 06 09 60 86 48 01 65 03 04 02 01
    assert _der_oid(OID_SHA256) == bytes.fromhex("0609608648016503040201")


# -------------------------------------------------------------- timestamping


def test_timestamping_disabled_makes_no_network_call(tmp_path: Path, monkeypatch) -> None:
    import magpie.hashing as hashing

    def explode(*args: object, **kwargs: object) -> None:
        raise AssertionError("no HTTP client may be constructed when MAGPIE_TSA is off")

    monkeypatch.setattr(hashing.httpx, "AsyncClient", explode)
    settings = Settings(tsa=False)

    result = asyncio.run(timestamp_manifest("ab" * 32, tmp_path, settings))

    assert result.enabled is False
    assert result.ok is False
    assert not (tmp_path / "timestamp.tsr").exists()


def test_timestamping_a_bad_digest_reports_instead_of_raising(tmp_path: Path) -> None:
    settings = Settings(tsa=True)

    result = asyncio.run(timestamp_manifest("not-a-digest", tmp_path, settings))

    assert result.enabled is True
    assert result.ok is False
    assert result.error and result.error.startswith("bad_digest")
