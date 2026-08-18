"""Verify the hash-pinned PEP 517 build closure and its committed SBOM."""

from __future__ import annotations

import json
import re
import shlex
import sys
import tomllib
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_CONSTRAINTS = _ROOT / "build-constraints.txt"
_PYPROJECT = _ROOT / "pyproject.toml"
_SBOM = _ROOT / "sbom.build.cdx.json"
_REQUIREMENT = re.compile(r"([A-Za-z0-9][A-Za-z0-9._-]*)==([^\s]+)\Z")
_HASH = re.compile(r"--hash=sha256:([0-9a-f]{64})\Z")
_APPROVED_LICENSE_EXPRESSIONS = frozenset(
    {"Apache-2.0", "Apache-2.0 OR BSD-2-Clause", "MIT", "MPL-2.0"}
)


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _constraint_inventory() -> dict[tuple[str, str], set[str]]:
    text = _CONSTRAINTS.read_text(encoding="utf-8")
    if len(text.encode()) > 128 * 1024:
        raise ValueError("build constraints exceed the size limit")
    logical_lines = text.replace("\\\n", " ").splitlines()
    inventory: dict[tuple[str, str], set[str]] = {}
    for line in logical_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        tokens = shlex.split(stripped, posix=True)
        match = _REQUIREMENT.fullmatch(tokens[0]) if tokens else None
        if match is None:
            raise ValueError(f"invalid build constraint: {stripped!r}")
        identity = (_canonical_name(match.group(1)), match.group(2))
        if identity in inventory:
            raise ValueError(f"duplicate build constraint: {tokens[0]}")
        hashes: set[str] = set()
        for token in tokens[1:]:
            hash_match = _HASH.fullmatch(token)
            if hash_match is None:
                raise ValueError(f"unsupported build constraint option: {token!r}")
            hashes.add(hash_match.group(1))
        if len(hashes) != 2:
            raise ValueError(f"{tokens[0]} must pin exactly its wheel and sdist hashes")
        inventory[identity] = hashes
    if not inventory:
        raise ValueError("build constraints are empty")
    return inventory


def _pyproject_inventory() -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    document = tomllib.loads(_PYPROJECT.read_text(encoding="utf-8"))
    build_requires = document.get("build-system", {}).get("requires")
    uv_constraints = document.get("tool", {}).get("uv", {}).get("build-constraint-dependencies")
    if not isinstance(build_requires, list) or not isinstance(uv_constraints, list):
        raise TypeError("pyproject build requirements or uv build constraints are missing")

    def parse(values: list[object]) -> set[tuple[str, str]]:
        result: set[tuple[str, str]] = set()
        for value in values:
            match = _REQUIREMENT.fullmatch(value) if isinstance(value, str) else None
            if match is None:
                raise ValueError(f"build requirement is not exactly pinned: {value!r}")
            result.add((_canonical_name(match.group(1)), match.group(2)))
        return result

    return parse(build_requires), parse(uv_constraints)


def _sbom_inventory() -> dict[tuple[str, str], set[str]]:
    document = json.loads(_SBOM.read_bytes())
    if (
        not isinstance(document, dict)
        or document.get("bomFormat") != "CycloneDX"
        or document.get("specVersion") != "1.5"
    ):
        raise ValueError("build SBOM is not CycloneDX 1.5")
    components = document.get("components")
    if not isinstance(components, list):
        raise TypeError("build SBOM has no component list")
    inventory: dict[tuple[str, str], set[str]] = {}
    for component in components:
        if not isinstance(component, dict):
            raise TypeError("build SBOM contains an invalid component")
        name = component.get("name")
        version = component.get("version")
        if not isinstance(name, str) or not isinstance(version, str):
            raise TypeError("build SBOM contains an incomplete component")
        identity = (_canonical_name(name), version)
        if identity in inventory:
            raise ValueError(f"build SBOM contains duplicate component {name}=={version}")
        licenses = component.get("licenses")
        if (
            not isinstance(licenses, list)
            or len(licenses) != 1
            or not isinstance(licenses[0], dict)
            or licenses[0].get("expression") not in _APPROVED_LICENSE_EXPRESSIONS
        ):
            raise ValueError(f"{name}=={version} has an unapproved build license")
        references = component.get("externalReferences")
        if not isinstance(references, list) or len(references) != 2:
            raise ValueError(f"{name}=={version} must identify its wheel and sdist")
        hashes: set[str] = set()
        for reference in references:
            if not isinstance(reference, dict) or reference.get("type") != "distribution":
                raise ValueError(f"{name}=={version} has an invalid artifact reference")
            url = reference.get("url")
            reference_hashes = reference.get("hashes")
            if (
                not isinstance(url, str)
                or not url.startswith("https://files.pythonhosted.org/")
                or not isinstance(reference_hashes, list)
                or len(reference_hashes) != 1
                or not isinstance(reference_hashes[0], dict)
                or reference_hashes[0].get("alg") != "SHA-256"
            ):
                raise ValueError(f"{name}=={version} has incomplete artifact evidence")
            content = reference_hashes[0].get("content")
            if not isinstance(content, str) or re.fullmatch(r"[0-9a-f]{64}", content) is None:
                raise ValueError(f"{name}=={version} has an invalid artifact hash")
            hashes.add(content)
        inventory[identity] = hashes
    return inventory


def main() -> int:
    try:
        constraints = _constraint_inventory()
        build_requires, uv_constraints = _pyproject_inventory()
        sbom = _sbom_inventory()
    except (OSError, TypeError, ValueError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        sys.stderr.write(f"build closure check error: {error}\n")
        return 1
    identities = set(constraints)
    if build_requires != {("hatchling", "1.32.0")}:
        sys.stderr.write("build closure check error: Hatchling build backend pin differs\n")
        return 1
    if uv_constraints != identities:
        sys.stderr.write("build closure check error: pyproject build constraints differ\n")
        return 1
    if sbom != constraints:
        sys.stderr.write("build closure check error: constraints and build SBOM differ\n")
        return 1
    sys.stdout.write(f"verified {len(identities)} hash-pinned build distributions and their SBOM\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
