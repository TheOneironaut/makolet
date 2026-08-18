"""Verify complete reviewed license and notice coverage for frozen dependencies."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

type Identity = tuple[str, str]
type JsonValue = str | int | float | bool | list[JsonValue] | dict[str, JsonValue] | None

_ROOT = Path(__file__).resolve().parents[1]
_MAX_POLICY_BYTES = 2 * 1024 * 1024
_MAX_LOCK_BYTES = 16 * 1024 * 1024
_MAX_SBOM_BYTES = 64 * 1024 * 1024
_MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
_IDENTITY = re.compile(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s]+)\Z")
_HASH_OPTION = re.compile(r"--hash=sha256:[0-9a-f]{64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DOCUMENT_IDENTITY = re.compile(r"`([^`\s=]+)==([^`\s;)]+)")
_EVIDENCE_HASH = re.compile(r"/usr/share/doc/[^|]+/copyright\|sha256:[0-9a-f]{64}\Z")
_COMPATIBLE_LICENSES = frozenset(
    {
        "Apache-2.0",
        "Apache-2.0 AND BSD-2-Clause",
        "Apache-2.0 OR BSD-2-Clause",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "BSD-3-Clause AND (Apache-2.0 OR BSD-3-Clause)",
        "ISC",
        "MIT",
        "MIT AND PSF-2.0",
        "MIT AND PSF-2.0 AND Apache-2.0",
        "MIT OR Apache-2.0",
        "PSF-2.0",
    }
)
_KNOWN_OBLIGATIONS = frozenset(
    {
        "corresponding-source",
        "do-not-redistribute",
        "mpl-covered-file-modifications",
        "mpl-license-text",
        "preserve-debian-copyright",
        "preserve-embedded-license-bundle",
        "preserve-license-files",
        "preserve-notice",
        "preserve-packaged-license-set",
        "preserve-vendored-license-tree",
        "runtime-sbom",
        "source-availability",
    }
)
_NOTICE_PACKAGES = frozenset(
    {
        ("boto3", "1.43.68"),
        ("botocore", "1.43.68"),
        ("coverage", "7.15.4"),
        ("cyclonedx-python-lib", "11.11.1"),
        ("license-expression", "30.4.4"),
        ("prometheus-client", "0.26.0"),
        ("requests", "2.34.2"),
        ("s3transfer", "0.19.2"),
        ("structlog", "26.1.0"),
    }
)
_SPECIAL_COMPATIBLE_OBLIGATIONS: dict[Identity, frozenset[str]] = {
    ("ast-serialize", "0.8.0"): frozenset(
        {"preserve-license-files", "preserve-embedded-license-bundle"}
    ),
    ("pip", "26.2.1"): frozenset({"preserve-license-files", "preserve-vendored-license-tree"}),
    ("tzdata", "2026.3"): frozenset({"preserve-license-files", "preserve-packaged-license-set"}),
}
_RUFF_IDENTITY = ("ruff", "0.16.2")
_MPL_SOURCES: dict[Identity, tuple[str, str]] = {
    ("certifi", "2026.7.22"): (
        "https://files.pythonhosted.org/packages/a3/c2/"
        "24167ea9858356b47a87a50d39908bfdb72ceeefe0041586e704e5376b3a/"
        "certifi-2026.7.22.tar.gz",
        "741e2c3b351ddf169a738da9f2c048608ff7f2c5cc02f1ebc6b118bb090d5d55",
    ),
    ("pathspec", "1.1.1"): (
        "https://files.pythonhosted.org/packages/5a/82/"
        "42f767fc1c1143d6fd36efb827202a2d997a375e160a71eb2888a925aac1/"
        "pathspec-1.1.1.tar.gz",
        "17db5ecd524104a120e173814c90367a96a98d07c45b2e10c2f3919fff91bf5a",
    ),
}
_MPL_OBLIGATIONS = frozenset(
    {
        "preserve-license-files",
        "mpl-license-text",
        "source-availability",
        "mpl-covered-file-modifications",
    }
)
_BASE_IMAGE_CLASSIFICATION = "separately-licensed-base-image-components"
_BASE_IMAGE_OBLIGATIONS = frozenset(
    {"runtime-sbom", "preserve-debian-copyright", "corresponding-source"}
)
_APPROVED_BASE_IMAGE = (
    "python:3.14.7-slim-bookworm@"
    "sha256:23c59390fc717bf09f9336908199a0ae75d9c4264bf296123f94ad772fea3b52"
)
_APPROVED_DEBIAN_COUNT = 97


@dataclass(frozen=True)
class Review:
    license_expression: str
    classification: str
    obligations: frozenset[str]
    source_url: str | None
    source_sha256: str | None


@dataclass(frozen=True)
class CoverageResult:
    locked_count: int
    build_count: int
    reviewed_count: int
    debian_count: int


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _identity_text(identity: Identity) -> str:
    return f"{identity[0]}=={identity[1]}"


def _identity_list(identities: set[Identity]) -> str:
    return ", ".join(_identity_text(identity) for identity in sorted(identities))


def _read_bytes(path: Path, *, limit: int, label: str) -> bytes:
    payload = path.read_bytes()
    if not payload or len(payload) > limit:
        raise ValueError(f"{label} is empty or exceeds {limit} bytes: {path}")
    return payload


def _parse_identity(value: object, *, location: str) -> Identity:
    match = _IDENTITY.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"{location} must be an exact name==version identity")
    return (_canonical_name(match.group(1)), match.group(2))


def _dependency_names(value: object, *, location: str) -> set[str]:
    if not isinstance(value, list):
        raise TypeError(f"{location} must be an array")
    names: set[str] = set()
    for index, dependency in enumerate(value):
        if not isinstance(dependency, dict) or not isinstance(dependency.get("name"), str):
            raise TypeError(f"{location}[{index}] is not a dependency table")
        names.add(_canonical_name(cast(str, dependency["name"])))
    return names


def _load_lock(path: Path) -> tuple[set[Identity], set[Identity]]:
    document = tomllib.loads(_read_bytes(path, limit=_MAX_LOCK_BYTES, label="uv lock").decode())
    if document.get("version") != 1 or document.get("revision") != 3:
        raise ValueError("uv.lock has an unsupported format revision")
    packages = document.get("package")
    if not isinstance(packages, list) or not packages:
        raise ValueError("uv.lock has no package inventory")
    inventory: set[Identity] = set()
    referenced_names: set[str] = set()
    project_count = 0
    for index, package in enumerate(packages):
        if not isinstance(package, dict):
            raise TypeError(f"uv.lock package {index} is not a table")
        identity = _parse_identity(
            f"{package.get('name')}=={package.get('version')}",
            location=f"uv.lock package {index}",
        )
        source = package.get("source")
        if "dependencies" in package:
            referenced_names |= _dependency_names(
                package["dependencies"], location=f"uv.lock package {index} dependencies"
            )
        for table_name in ("optional-dependencies", "dev-dependencies"):
            table = package.get(table_name)
            if table is None:
                continue
            if not isinstance(table, dict):
                raise TypeError(f"uv.lock package {index} {table_name} must be a table")
            for group_name, dependencies in table.items():
                referenced_names |= _dependency_names(
                    dependencies,
                    location=f"uv.lock package {index} {table_name}.{group_name}",
                )
        if identity[0] == "makolet":
            if identity != ("makolet", "0.1.0") or source != {"editable": "."}:
                raise ValueError("uv.lock project identity or source changed")
            project_count += 1
            continue
        if source != {"registry": "https://pypi.org/simple"}:
            raise ValueError(f"{_identity_text(identity)} is not pinned to the reviewed registry")
        if identity in inventory:
            raise ValueError(f"uv.lock duplicates {_identity_text(identity)}")
        inventory.add(identity)
    if project_count != 1 or not inventory:
        raise ValueError("uv.lock must contain one Makolet project and third-party dependencies")
    available_names = {identity[0] for identity in inventory} | {"makolet"}
    missing_references = referenced_names - available_names
    if missing_references:
        raise ValueError(
            "uv.lock dependency graph references missing packages: "
            + ", ".join(sorted(missing_references))
        )

    manifest = document.get("manifest")
    build_rows = manifest.get("build-constraints") if isinstance(manifest, dict) else None
    if not isinstance(build_rows, list) or not build_rows:
        raise ValueError("uv.lock has no frozen build-constraint manifest")
    manifest_build: set[Identity] = set()
    for index, row in enumerate(build_rows):
        if not isinstance(row, dict):
            raise TypeError(f"uv.lock build constraint {index} is not a table")
        identity = _parse_identity(
            f"{row.get('name')}{row.get('specifier')}",
            location=f"uv.lock build constraint {index}",
        )
        if identity in manifest_build:
            raise ValueError(f"uv.lock duplicates build constraint {_identity_text(identity)}")
        manifest_build.add(identity)
    return inventory, manifest_build


def _load_build_constraints(path: Path) -> set[Identity]:
    text = _read_bytes(path, limit=128 * 1024, label="build constraints").decode()
    inventory: set[Identity] = set()
    for line in text.replace("\\\n", " ").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        tokens = shlex.split(stripped, posix=True)
        if not tokens:
            raise ValueError("build constraints contain an empty requirement")
        identity = _parse_identity(tokens[0], location="build constraint")
        if identity in inventory:
            raise ValueError(f"build constraints duplicate {_identity_text(identity)}")
        if len(tokens) != 3 or any(_HASH_OPTION.fullmatch(token) is None for token in tokens[1:]):
            raise ValueError(
                f"{_identity_text(identity)} must have exactly two SHA-256 artifact hashes"
            )
        inventory.add(identity)
    if not inventory:
        raise ValueError("build constraints have no dependency inventory")
    return inventory


def _reject_nonfinite(value: str) -> float:
    raise ValueError(f"non-finite JSON number {value!r}")


def _load_json(path: Path, *, label: str) -> dict[str, JsonValue]:
    payload = _read_bytes(path, limit=_MAX_SBOM_BYTES, label=label)
    document = cast(JsonValue, json.loads(payload, parse_constant=_reject_nonfinite))
    if not isinstance(document, dict):
        raise TypeError(f"{label} is not a JSON object")
    return document


def _load_sbom_inventory(path: Path, *, require_license: bool) -> dict[Identity, str | None]:
    document = _load_json(path, label="SBOM")
    if document.get("bomFormat") != "CycloneDX" or document.get("specVersion") != "1.5":
        raise ValueError(f"{path} is not CycloneDX 1.5")
    components = document.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError(f"{path} has no component inventory")
    inventory: dict[Identity, str | None] = {}
    for index, component in enumerate(components):
        if not isinstance(component, dict):
            raise TypeError(f"{path} component {index} is not an object")
        identity = _parse_identity(
            f"{component.get('name')}=={component.get('version')}",
            location=f"{path} component {index}",
        )
        if identity in inventory:
            raise ValueError(f"{path} duplicates {_identity_text(identity)}")
        expression: str | None = None
        if require_license:
            licenses = component.get("licenses")
            if (
                not isinstance(licenses, list)
                or len(licenses) != 1
                or not isinstance(licenses[0], dict)
                or not isinstance(licenses[0].get("expression"), str)
            ):
                raise ValueError(f"{_identity_text(identity)} has no exact build license")
            expression = cast(str, licenses[0]["expression"])
        inventory[identity] = expression
    return inventory


def _required_strings(value: object, *, location: str) -> frozenset[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{location} must be a non-empty string array")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{location} contains an invalid value")
    values = cast(list[str], value)
    if len(values) != len(set(values)):
        raise ValueError(f"{location} contains duplicates")
    return frozenset(values)


def _load_policy(path: Path) -> tuple[dict[Identity, Review], dict[str, object]]:
    document = tomllib.loads(
        _read_bytes(path, limit=_MAX_POLICY_BYTES, label="license policy").decode()
    )
    if document.get("schema_version") != 1:
        raise ValueError("license policy has an unsupported schema version")
    if document.get("notices_document") != "THIRD_PARTY_NOTICES.md":
        raise ValueError("license policy notices document changed")
    if document.get("audit_document") != "docs/research/dependency-license-audit.md":
        raise ValueError("license policy audit document changed")
    groups = document.get("group")
    if not isinstance(groups, list) or not groups:
        raise ValueError("license policy has no review groups")
    reviews: dict[Identity, Review] = {}
    for index, group in enumerate(groups):
        if not isinstance(group, dict):
            raise TypeError(f"license policy group {index} is not a table")
        license_expression = group.get("license_expression")
        classification = group.get("classification")
        if not isinstance(license_expression, str) or not license_expression:
            raise ValueError(f"license policy group {index} has no license expression")
        if not isinstance(classification, str) or not classification:
            raise ValueError(f"license policy group {index} has no classification")
        obligations = _required_strings(
            group.get("obligations"), location=f"license policy group {index} obligations"
        )
        unknown_obligations = obligations - _KNOWN_OBLIGATIONS
        if unknown_obligations:
            raise ValueError(
                f"license policy group {index} has unknown obligations: "
                f"{', '.join(sorted(unknown_obligations))}"
            )
        packages = group.get("packages")
        if not isinstance(packages, list) or not packages:
            raise ValueError(f"license policy group {index} has no packages")
        source_url = group.get("source_url")
        source_sha256 = group.get("source_sha256")
        if source_url is not None and not isinstance(source_url, str):
            raise ValueError(f"license policy group {index} has an invalid source URL")
        if source_sha256 is not None and not isinstance(source_sha256, str):
            raise ValueError(f"license policy group {index} has an invalid source SHA-256")
        for package_index, value in enumerate(packages):
            identity = _parse_identity(
                value, location=f"license policy group {index} package {package_index}"
            )
            if identity in reviews:
                raise ValueError(f"license policy duplicates {_identity_text(identity)}")
            reviews[identity] = Review(
                license_expression=license_expression,
                classification=classification,
                obligations=obligations,
                source_url=source_url,
                source_sha256=source_sha256,
            )
    base_image = document.get("base_image_exception")
    if not isinstance(base_image, dict):
        raise TypeError("license policy has no base-image exception contract")
    return reviews, cast(dict[str, object], base_image)


def _validate_review(identity: Identity, review: Review) -> None:
    identity_text = _identity_text(identity)
    if review.classification == "compatible":
        if review.license_expression not in _COMPATIBLE_LICENSES:
            raise ValueError(f"{identity_text} has an unapproved compatible license")
        required = _SPECIAL_COMPATIBLE_OBLIGATIONS.get(
            identity, frozenset({"preserve-license-files"})
        )
        if identity in _NOTICE_PACKAGES:
            required |= frozenset({"preserve-notice"})
        if not required.issubset(review.obligations):
            raise ValueError(f"{identity_text} has incomplete license/notice obligations")
        if review.source_url is not None or review.source_sha256 is not None:
            raise ValueError(f"{identity_text} has an unexpected source exception")
        return
    if review.classification == "external-tooling-only":
        if identity != _RUFF_IDENTITY:
            raise ValueError(f"external tooling exception is not approved for {identity_text}")
        if review.license_expression != "MIT" or review.obligations != {"do-not-redistribute"}:
            raise ValueError(f"{identity_text} does not preserve the approved Ruff boundary")
        if review.source_url is not None or review.source_sha256 is not None:
            raise ValueError(f"{identity_text} has an unexpected source exception")
        return
    if review.classification == "unmodified-mpl-2.0-artifact":
        expected_source = _MPL_SOURCES.get(identity)
        if expected_source is None:
            raise ValueError(f"MPL artifact exception is not approved for {identity_text}")
        if review.license_expression != "MPL-2.0" or review.obligations != _MPL_OBLIGATIONS:
            raise ValueError(f"{identity_text} has incomplete MPL obligations")
        if review.source_url != expected_source[0]:
            raise ValueError(f"{identity_text} has an unreviewed source URL")
        if (
            review.source_sha256 != expected_source[1]
            or _SHA256.fullmatch(review.source_sha256 or "") is None
        ):
            raise ValueError(f"{identity_text} has an invalid source SHA-256")
        return
    raise ValueError(f"{identity_text} has an unapproved license classification")


def _validate_base_image(root: Path, policy: dict[str, object]) -> int:
    if policy.get("classification") != _BASE_IMAGE_CLASSIFICATION:
        raise ValueError("base-image exception classification is not owner-approved")
    obligations = _required_strings(
        policy.get("obligations"), location="base-image exception obligations"
    )
    if obligations != _BASE_IMAGE_OBLIGATIONS:
        raise ValueError("base-image exception obligations changed")
    image = policy.get("image")
    runtime_sbom_name = policy.get("runtime_sbom")
    if image != _APPROVED_BASE_IMAGE:
        raise ValueError("base-image exception differs from the reviewed digest-pinned image")
    if runtime_sbom_name != "sbom.runtime-linux.cdx.json":
        raise ValueError("base-image exception must use the committed runtime SBOM")

    path = root / runtime_sbom_name
    document = _load_json(path, label="runtime SBOM")
    if document.get("bomFormat") != "CycloneDX" or document.get("specVersion") != "1.5":
        raise ValueError("runtime SBOM is not CycloneDX 1.5")
    metadata = document.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(metadata.get("component"), dict):
        raise TypeError("runtime SBOM has no root component")
    root_component = cast(dict[str, JsonValue], metadata["component"])
    properties = root_component.get("properties")
    base_images = []
    if isinstance(properties, list):
        base_images = [
            item.get("value")
            for item in properties
            if isinstance(item, dict) and item.get("name") == "makolet:base-image"
        ]
    if base_images != [image]:
        raise ValueError("runtime SBOM base image differs from the approved exception")

    components = document.get("components")
    if not isinstance(components, list):
        raise TypeError("runtime SBOM has no component inventory")
    debian_count = 0
    for index, component in enumerate(components):
        if not isinstance(component, dict) or component.get("type") != "operating-system":
            continue
        debian_count += 1
        licenses = component.get("licenses")
        if not isinstance(licenses, list) or not licenses:
            raise ValueError(f"runtime SBOM Debian component {index} has no license evidence")
        for license_index, entry in enumerate(licenses):
            license_value = entry.get("license") if isinstance(entry, dict) else None
            if not isinstance(license_value, dict) or not any(
                isinstance(license_value.get(key), str) and license_value.get(key)
                for key in ("id", "name")
            ):
                raise ValueError(
                    f"runtime SBOM Debian component {index} license {license_index} is invalid"
                )
        properties = component.get("properties")
        if not isinstance(properties, list):
            raise TypeError(f"runtime SBOM Debian component {index} has no evidence properties")
        evidence = [
            item.get("value")
            for item in properties
            if isinstance(item, dict) and item.get("name") == "makolet:debian-copyright-evidence"
        ]
        if (
            len(evidence) != 1
            or not isinstance(evidence[0], str)
            or _EVIDENCE_HASH.fullmatch(evidence[0]) is None
        ):
            raise ValueError(
                f"runtime SBOM Debian component {index} lacks exact copyright evidence"
            )
        for property_name in ("makolet:debian-source", "makolet:debian-source-version"):
            values = [
                item.get("value")
                for item in properties
                if isinstance(item, dict) and item.get("name") == property_name
            ]
            if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
                raise ValueError(
                    f"runtime SBOM Debian component {index} lacks {property_name} evidence"
                )
    if debian_count != _APPROVED_DEBIAN_COUNT:
        raise ValueError(
            "runtime SBOM base-image package count changed: "
            f"expected {_APPROVED_DEBIAN_COUNT}, found {debian_count}"
        )
    return debian_count


def _document_inventory(path: Path) -> tuple[set[Identity], str]:
    text = _read_bytes(path, limit=_MAX_DOCUMENT_BYTES, label="license documentation").decode()
    identities = {
        (_canonical_name(match.group(1)), match.group(2))
        for match in _DOCUMENT_IDENTITY.finditer(text)
    }
    return identities, text


def _validate_documentation(root: Path, reviews: dict[Identity, Review]) -> None:
    audit_inventory, audit_text = _document_inventory(
        root / "docs" / "research" / "dependency-license-audit.md"
    )
    missing_audit = set(reviews) - audit_inventory
    if missing_audit:
        raise ValueError(
            "dependency license audit is missing reviewed identities: "
            f"{_identity_list(missing_audit)}"
        )
    _, notices_text = _document_inventory(root / "THIRD_PARTY_NOTICES.md")
    for identity, (source_url, source_sha256) in _MPL_SOURCES.items():
        if source_url not in notices_text or source_sha256 not in notices_text:
            raise ValueError(
                f"THIRD_PARTY_NOTICES.md lacks source evidence for {_identity_text(identity)}"
            )
    required_notices = (
        "Do not redistribute Ruff.",
        "vendored-license tree",
        "corresponding-source",
        "covered-file",
    )
    missing_notices = [value for value in required_notices if value not in notices_text]
    if missing_notices:
        raise ValueError(
            "THIRD_PARTY_NOTICES.md lacks required obligation text: " + ", ".join(missing_notices)
        )
    if (
        "pip-licenses" not in audit_text
        or "prettytable" not in audit_text
        or "wcwidth" not in audit_text
    ):
        raise ValueError("dependency license audit omits the pip-licenses execution closure")


def check_repository(root: Path) -> CoverageResult:
    root = root.resolve()
    locked, lock_build = _load_lock(root / "uv.lock")
    build = _load_build_constraints(root / "build-constraints.txt")
    main_sbom = set(_load_sbom_inventory(root / "sbom.cdx.json", require_license=False))
    build_sbom = _load_sbom_inventory(root / "sbom.build.cdx.json", require_license=True)
    reviews, base_image_policy = _load_policy(
        root / "docs" / "research" / "dependency-license-policy.toml"
    )

    if main_sbom != locked:
        raise ValueError("frozen lock and committed Python SBOM inventories differ")
    if lock_build != build:
        raise ValueError("frozen lock and hash-constrained build inventories differ")
    if set(build_sbom) != build:
        raise ValueError("build constraints and committed build SBOM inventories differ")
    expected = locked | build
    missing = expected - set(reviews)
    stale = set(reviews) - expected
    if missing:
        raise ValueError(
            f"license policy is missing frozen dependencies: {_identity_list(missing)}"
        )
    if stale:
        raise ValueError(f"license policy has stale dependencies: {_identity_list(stale)}")

    for identity, review in reviews.items():
        _validate_review(identity, review)
    for identity, expression in build_sbom.items():
        if reviews[identity].license_expression != expression:
            raise ValueError(f"{_identity_text(identity)} differs from its build SBOM license")
    if {
        identity
        for identity, review in reviews.items()
        if review.classification == "external-tooling-only"
    } != {_RUFF_IDENTITY}:
        raise ValueError("license policy changed the sole external-tooling exception")
    if {
        identity
        for identity, review in reviews.items()
        if review.classification == "unmodified-mpl-2.0-artifact"
    } != set(_MPL_SOURCES):
        raise ValueError("license policy changed the approved MPL artifact set")

    debian_count = _validate_base_image(root, base_image_policy)
    _validate_documentation(root, reviews)
    return CoverageResult(
        locked_count=len(locked),
        build_count=len(build),
        reviewed_count=len(reviews),
        debian_count=debian_count,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=_ROOT)
    arguments = parser.parse_args(argv)
    try:
        result = check_repository(arguments.root)
    except (OSError, TypeError, ValueError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        sys.stderr.write(f"license coverage check error: {error}\n")
        return 1
    sys.stdout.write(
        "verified complete reviewed license/notice coverage for "
        f"{result.locked_count} locked, {result.build_count} build, "
        f"{result.reviewed_count} unique Python, and {result.debian_count} base-image "
        "distributions\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
