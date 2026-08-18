"""Canonical content-addressed raw-archive object keys."""

from __future__ import annotations

import re

from makolet.domain.errors import ArchiveIntegrityError

OBJECT_KEY_PATTERN = re.compile(
    r"sha256/(?P<first>[0-9a-f]{2})/(?P<second>[0-9a-f]{2})/(?P<digest>[0-9a-f]{64})\Z"
)


def key_for_digest(digest: str) -> str:
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("digest must be lowercase SHA-256 hex")
    return f"sha256/{digest[:2]}/{digest[2:4]}/{digest}"


def digest_for_key(object_key: str, *, expected_sha256: str | None = None) -> str:
    match = OBJECT_KEY_PATTERN.fullmatch(object_key)
    if match is None:
        raise ArchiveIntegrityError("Archive object key is not canonical")
    digest = match.group("digest")
    if match.group("first") != digest[:2] or match.group("second") != digest[2:4]:
        raise ArchiveIntegrityError("Archive object key is not canonical")
    if expected_sha256 is not None and digest != expected_sha256:
        raise ArchiveIntegrityError("Object key and expected SHA-256 disagree")
    return digest


def normalize_key_prefix(key_prefix: str) -> str:
    """Return the canonical service prefix used around archive object keys."""

    normalized = key_prefix.strip("/")
    if normalized and any(part in {"", ".", ".."} for part in normalized.split("/")):
        raise ValueError("S3 key prefix is not canonical")
    return normalized


def service_key_for_object(object_key: str, *, key_prefix: str) -> str:
    """Qualify one canonical content key with a canonical S3 prefix."""

    digest_for_key(object_key)
    normalized_prefix = normalize_key_prefix(key_prefix)
    return f"{normalized_prefix}/{object_key}" if normalized_prefix else object_key


def object_key_from_service_key(
    service_key: str,
    *,
    key_prefix: str,
    expected_sha256: str | None = None,
) -> str:
    """Remove the configured prefix and validate the complete content-addressed key."""

    normalized_prefix = normalize_key_prefix(key_prefix)
    marker = f"{normalized_prefix}/" if normalized_prefix else ""
    if not service_key.startswith(marker):
        raise ArchiveIntegrityError("S3 archive object is outside the configured key prefix")
    object_key = service_key[len(marker) :]
    digest_for_key(object_key, expected_sha256=expected_sha256)
    return object_key
