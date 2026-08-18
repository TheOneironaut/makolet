from __future__ import annotations

import copy
import hashlib
import json
import re
from pathlib import Path
from typing import Any, cast

import pytest

from scripts import check_sbom, generate_runtime_sbom

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_SBOM = REPOSITORY_ROOT / "sbom.runtime-linux.cdx.json"

type RuntimeDocument = dict[str, Any]


def _runtime_document() -> RuntimeDocument:
    return cast(RuntimeDocument, json.loads(RUNTIME_SBOM.read_bytes()))


def _write_document(path: Path, document: RuntimeDocument) -> None:
    path.write_text(json.dumps(document), encoding="utf-8")


def _component(document: RuntimeDocument, prefix: str) -> RuntimeDocument:
    return next(
        component for component in document["components"] if component["bom-ref"].startswith(prefix)
    )


def _property(owner: RuntimeDocument, name: str) -> RuntimeDocument:
    return next(item for item in owner["properties"] if item["name"] == name)


def _compare_documents(
    tmp_path: Path,
    committed: RuntimeDocument,
    generated: RuntimeDocument,
) -> int:
    committed_path = tmp_path / "committed.json"
    generated_path = tmp_path / "generated.json"
    _write_document(committed_path, committed)
    _write_document(generated_path, generated)
    return check_sbom._compare_runtime_semantics(committed_path, generated_path)


def _mutate_runtime_document(document: RuntimeDocument, mutation: str) -> None:
    python_component = _component(document, "pkg:pypi/")
    debian_component = _component(document, "pkg:deb/debian/")
    native_component = next(
        component
        for component in document["components"]
        if any(item["name"] == "makolet:native-dynamic-links" for item in component["properties"])
    )
    root = document["metadata"]["component"]

    match mutation:
        case "license":
            python_component["licenses"][0]["license"]["name"] = "Different-License"
        case "artifact-evidence-hash":
            evidence = _property(python_component, "makolet:artifact-evidence")
            evidence["value"] = evidence["value"].rsplit(":", maxsplit=1)[0] + ":" + "a" * 64
        case "debian-copyright-hash":
            evidence = _property(debian_component, "makolet:debian-copyright-evidence")
            evidence["value"] = evidence["value"].rsplit(":", maxsplit=1)[0] + ":" + "b" * 64
        case "debian-license":
            debian_component["licenses"][0]["license"]["name"] = "Different-Debian-License"
        case "debian-license-missing":
            del debian_component["licenses"]
        case "debian-source":
            _property(debian_component, "makolet:debian-source")["value"] = "different-source"
        case "debian-source-version":
            _property(debian_component, "makolet:debian-source-version")["value"] = "999"
        case "native-dynamic-links":
            _property(native_component, "makolet:native-dynamic-links")["value"] += ",libchanged.so"
        case "base-image":
            _property(root, "makolet:base-image")["value"] = "python:test@sha256:" + "c" * 64
        case "purl":
            python_component["purl"] += "?changed=true"
        case "type":
            python_component["type"] = "framework"
        case "bom-ref":
            old_ref = debian_component["bom-ref"]
            new_ref = old_ref + "&changed=true"
            debian_component["bom-ref"] = new_ref
            debian_component["purl"] = new_ref
            dependency = next(item for item in document["dependencies"] if item["ref"] == old_ref)
            dependency["ref"] = new_ref
        case "dependency-edge":
            root_dependency = next(
                item for item in document["dependencies"] if item["ref"] == root["bom-ref"]
            )
            root_dependency["dependsOn"].pop()
        case "tool-version":
            document["metadata"]["tools"][0]["version"] = "changed"
        case "stable-extra-field":
            document["metadata"]["stable-extra"] = "changed"
        case _:
            raise AssertionError(f"unknown mutation {mutation}")


def test_runtime_semantic_comparison_ignores_only_platform_value_and_array_order(
    tmp_path: Path,
) -> None:
    committed = _runtime_document()
    generated = copy.deepcopy(committed)
    root = generated["metadata"]["component"]
    _property(root, "makolet:platform")["value"] = "different-host-kernel"
    root["properties"].reverse()
    generated["components"].reverse()
    generated["dependencies"].reverse()
    for component in generated["components"]:
        component["properties"].reverse()
        if "licenses" in component:
            component["licenses"].reverse()
    for dependency in generated["dependencies"]:
        if "dependsOn" in dependency:
            dependency["dependsOn"].reverse()

    assert _compare_documents(tmp_path, committed, generated) == 139


@pytest.mark.parametrize(
    "mutation",
    [
        "license",
        "artifact-evidence-hash",
        "debian-copyright-hash",
        "debian-license",
        "debian-license-missing",
        "debian-source",
        "debian-source-version",
        "native-dynamic-links",
        "base-image",
        "purl",
        "type",
        "bom-ref",
        "dependency-edge",
        "tool-version",
        "stable-extra-field",
    ],
)
def test_runtime_semantic_comparison_rejects_stable_content_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    committed = _runtime_document()
    generated = copy.deepcopy(committed)
    _mutate_runtime_document(generated, mutation)

    with pytest.raises((TypeError, ValueError), match=r"runtime SBOM|must be|must equal"):
        _compare_documents(tmp_path, committed, generated)


def test_runtime_semantic_comparison_requires_platform_property(tmp_path: Path) -> None:
    committed = _runtime_document()
    generated = copy.deepcopy(committed)
    root = generated["metadata"]["component"]
    root["properties"] = [item for item in root["properties"] if item["name"] != "makolet:platform"]

    with pytest.raises(ValueError, match="requires exactly one makolet:platform"):
        _compare_documents(tmp_path, committed, generated)


def test_inventory_comparison_remains_name_and_version_only(tmp_path: Path) -> None:
    committed = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "components": [{"name": "Example_Name", "version": "1.0", "type": "library"}],
    }
    generated = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "components": [{"name": "example-name", "version": "1.0", "type": "framework"}],
    }
    committed_path = tmp_path / "committed.json"
    generated_path = tmp_path / "generated.json"
    _write_document(committed_path, committed)
    _write_document(generated_path, generated)

    assert check_sbom._inventory(committed_path) == check_sbom._inventory(generated_path)


def test_debian_license_names_extracts_dep5_and_scoped_declarations(tmp_path: Path) -> None:
    copyright_path = tmp_path / "copyright"
    copyright_path.write_text(
        "Format: https://www.debian.org/doc/packaging-manuals/copyright-format/1.0/\n"
        "License: GPL-2+ or Artistic\n"
        " full license text\n"
        "License: MIT\n"
        "License (library): LGPLv2.1+\n",
        encoding="utf-8",
    )

    assert generate_runtime_sbom._debian_license_names(
        copyright_path,
        "example",
        hashlib.sha256(copyright_path.read_bytes()).hexdigest(),
    ) == ("GPL-2+ or Artistic", "LGPLv2.1+ (library)", "MIT")


def test_debian_license_names_binds_reviewed_legacy_mapping_to_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    copyright_path = tmp_path / "copyright"
    copyright_path.write_text("legacy copyright without DEP-5 fields\n", encoding="utf-8")
    digest = hashlib.sha256(copyright_path.read_bytes()).hexdigest()
    monkeypatch.setitem(
        generate_runtime_sbom._LEGACY_DEBIAN_LICENSES,
        "example-legacy",
        (digest, ("GPL-2.0-or-later",)),
    )

    assert generate_runtime_sbom._debian_license_names(
        copyright_path,
        "example-legacy",
        digest,
    ) == ("GPL-2.0-or-later",)
    with pytest.raises(ValueError, match="legacy copyright evidence changed"):
        generate_runtime_sbom._debian_license_names(
            copyright_path,
            "example-legacy",
            "0" * 64,
        )


def test_debian_license_names_rejects_unreviewed_legacy_file(tmp_path: Path) -> None:
    copyright_path = tmp_path / "copyright"
    copyright_path.write_text("legacy copyright without DEP-5 fields\n", encoding="utf-8")

    with pytest.raises(ValueError, match="no DEP-5 License fields or reviewed legacy mapping"):
        generate_runtime_sbom._debian_license_names(
            copyright_path,
            "unreviewed",
            hashlib.sha256(copyright_path.read_bytes()).hexdigest(),
        )


def test_runtime_base_image_is_read_from_repository_runtime_stage() -> None:
    expected = (
        "python:3.14.7-slim-bookworm@sha256:"
        "23c59390fc717bf09f9336908199a0ae75d9c4264bf296123f94ad772fea3b52"
    )

    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert generate_runtime_sbom._runtime_base_image(dockerfile) == expected
    assert generate_runtime_sbom._base_image() == expected


def test_runtime_base_image_accepts_one_continued_case_insensitive_stage() -> None:
    image = f"registry.example:5000/python:3.14@sha256:{'a' * 64}"
    dockerfile = f"# escape=`\nFrOm {image} `\n  As RuNtImE\n"

    assert generate_runtime_sbom._runtime_base_image(dockerfile) == image


@pytest.mark.parametrize(
    ("dockerfile", "message"),
    [
        ("FROM python:3.14 AS builder\n", "exactly one FROM stage named runtime; found 0"),
        (
            f"ARG BASE=python:3.14@sha256:{'a' * 64}\nFROM ${{BASE}} AS runtime\n",
            "must be a literal tag pinned by a lowercase SHA-256 digest",
        ),
        (
            f"ARG TAG=3.14\nFROM python:${{TAG}}@sha256:{'a' * 64} AS runtime\n",
            "must be a literal tag pinned by a lowercase SHA-256 digest",
        ),
        (
            "FROM python:3.14 AS runtime\n",
            "must be a literal tag pinned by a lowercase SHA-256 digest",
        ),
        (
            f"FROM python:3.14@sha256:{'A' * 64} AS runtime\n",
            "must be a literal tag pinned by a lowercase SHA-256 digest",
        ),
        (
            f"FROM python:3.14@sha256:{'a' * 63} AS runtime\n",
            "must be a literal tag pinned by a lowercase SHA-256 digest",
        ),
        (
            f"FROM python:3.14@sha256:{'a' * 64} AS runtime\n"
            f"FROM python:3.14@sha256:{'b' * 64} AS RUNTIME\n",
            "exactly one FROM stage named runtime; found 2",
        ),
        (
            f"FROM python:3.14@sha256:{'a' * 64} AS runtime\n"
            f"FROM python:3.14@sha256:{'b' * 64} \\\n"
            "  AS runtime\n",
            "exactly one FROM stage named runtime; found 2",
        ),
        (
            f"FROM python:3.14@sha256:{'a' * 64} AS runtime\n"
            "RUN true\n"
            "# escape=`\n"
            f"FROM python:3.14@sha256:{'b' * 64} \\\n"
            "  AS runtime\n",
            "exactly one FROM stage named runtime; found 2",
        ),
    ],
)
def test_runtime_base_image_rejects_ambiguous_or_mutable_stages(
    dockerfile: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=re.escape(message)):
        generate_runtime_sbom._runtime_base_image(dockerfile)


def test_runtime_semantic_gate_is_wired_into_ci_and_documented_commands() -> None:
    expected = (
        "scripts/check_sbom.py --runtime-semantic sbom.runtime-linux.cdx.json .ci-runtime-sbom.json"
    )
    for relative_path in (
        Path(".github/workflows/ci.yml"),
        Path("THIRD_PARTY_NOTICES.md"),
        Path("docs/testing.md"),
        Path("docs/research/dependency-license-audit.md"),
    ):
        content = (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")
        normalized = " ".join(content.replace("\\\n", " ").split())
        assert expected in normalized
