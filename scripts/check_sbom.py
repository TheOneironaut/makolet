"""Compare a committed CycloneDX SBOM with a freshly generated document."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from pathlib import Path
from typing import cast

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | dict[str, JsonValue]
type JsonObject = dict[str, JsonValue]

_MAX_SBOM_BYTES = 64 * 1024 * 1024
_RUNTIME_PLATFORM_PROPERTY = "makolet:platform"
_RUNTIME_PLATFORM_SENTINEL = "<host-dependent-platform>"
_SHA256_EVIDENCE = re.compile(r".+\|sha256:[0-9a-f]{64}\Z")
_PINNED_BASE_IMAGE = re.compile(r".+@sha256:[0-9a-f]{64}\Z")


def _canonical_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _reject_nonfinite(value: str) -> float:
    raise ValueError(f"non-finite JSON number {value!r}")


def _load_document(path: Path) -> JsonObject:
    payload = path.read_bytes()
    if len(payload) > _MAX_SBOM_BYTES:
        raise ValueError(f"{path} exceeds the SBOM size limit")
    document = cast(JsonValue, json.loads(payload, parse_constant=_reject_nonfinite))
    if (
        not isinstance(document, dict)
        or document.get("bomFormat") != "CycloneDX"
        or document.get("specVersion") != "1.5"
    ):
        raise ValueError(f"{path} is not a CycloneDX 1.5 document")
    return document


def _required_object(value: JsonValue | None, *, location: str) -> JsonObject:
    if not isinstance(value, dict):
        raise TypeError(f"{location} must be an object")
    return value


def _required_array(value: JsonValue | None, *, location: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise TypeError(f"{location} must be an array")
    return value


def _required_string(document: JsonObject, key: str, *, location: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{location}.{key} must be a non-empty string")
    return value


def _inventory(path: Path) -> set[tuple[str, str]]:
    document = _load_document(path)
    components = _required_array(document.get("components"), location=f"{path}.components")
    inventory: set[tuple[str, str]] = set()
    for index, value in enumerate(components):
        component = _required_object(value, location=f"{path}.components[{index}]")
        name = _required_string(component, "name", location=f"{path}.components[{index}]")
        version = _required_string(
            component,
            "version",
            location=f"{path}.components[{index}]",
        )
        identity = (_canonical_name(name), version)
        if identity in inventory:
            raise ValueError(f"{path} contains duplicate component {name}=={version}")
        inventory.add(identity)
    return inventory


def _properties(component: JsonObject, *, location: str) -> list[JsonObject]:
    values = _required_array(component.get("properties"), location=f"{location}.properties")
    properties: list[JsonObject] = []
    for index, value in enumerate(values):
        item_location = f"{location}.properties[{index}]"
        item = _required_object(value, location=item_location)
        _required_string(item, "name", location=item_location)
        _required_string(item, "value", location=item_location)
        properties.append(item)
    return properties


def _property_values(properties: list[JsonObject], name: str) -> list[str]:
    return [cast(str, item["value"]) for item in properties if item["name"] == name]


def _validate_license_evidence(component: JsonObject, *, location: str) -> None:
    licenses = _required_array(component.get("licenses"), location=f"{location}.licenses")
    if not licenses:
        raise ValueError(f"{location}.licenses must not be empty")
    for index, value in enumerate(licenses):
        entry_location = f"{location}.licenses[{index}]"
        entry = _required_object(value, location=entry_location)
        license_value = _required_object(entry.get("license"), location=f"{entry_location}.license")
        identifiers = [license_value.get("id"), license_value.get("name")]
        if not any(isinstance(identifier, str) and identifier for identifier in identifiers):
            raise ValueError(f"{entry_location}.license requires a non-empty id or name")


def _validate_evidence_hashes(values: list[str], *, location: str) -> None:
    if not values:
        raise ValueError(f"{location} requires at least one evidence hash")
    if any(_SHA256_EVIDENCE.fullmatch(value) is None for value in values):
        raise ValueError(f"{location} contains an invalid SHA-256 evidence value")


def _validate_runtime_component(value: JsonValue, *, index: int, path: Path) -> str:
    location = f"{path}.components[{index}]"
    component = _required_object(value, location=location)
    component_type = _required_string(component, "type", location=location)
    _required_string(component, "name", location=location)
    _required_string(component, "version", location=location)
    bom_ref = _required_string(component, "bom-ref", location=location)
    purl = _required_string(component, "purl", location=location)
    if purl != bom_ref:
        raise ValueError(f"{location}.purl must equal its package bom-ref")
    properties = _properties(component, location=location)

    if bom_ref.startswith("pkg:pypi/"):
        if component_type != "library":
            raise ValueError(f"{location}.type must be library for a PyPI component")
        _validate_license_evidence(component, location=location)
        _validate_evidence_hashes(
            _property_values(properties, "makolet:artifact-evidence"),
            location=f"{location}.properties[makolet:artifact-evidence]",
        )
    elif bom_ref.startswith("pkg:deb/debian/"):
        if component_type != "operating-system":
            raise ValueError(f"{location}.type must be operating-system for a Debian component")
        _validate_license_evidence(component, location=location)
        _validate_evidence_hashes(
            _property_values(properties, "makolet:debian-copyright-evidence"),
            location=f"{location}.properties[makolet:debian-copyright-evidence]",
        )
        for property_name in ("makolet:debian-source", "makolet:debian-source-version"):
            if len(_property_values(properties, property_name)) != 1:
                raise ValueError(f"{location} requires exactly one {property_name} property")
    elif bom_ref.startswith("pkg:generic/cpython@"):
        if component_type != "framework":
            raise ValueError(f"{location}.type must be framework for CPython")
        _validate_license_evidence(component, location=location)
        _validate_evidence_hashes(
            _property_values(properties, "makolet:artifact-evidence"),
            location=f"{location}.properties[makolet:artifact-evidence]",
        )
    else:
        raise ValueError(f"{location}.bom-ref uses an unsupported runtime package namespace")
    return bom_ref


def _validate_runtime_metadata(document: JsonObject, *, path: Path) -> str:
    metadata = _required_object(document.get("metadata"), location=f"{path}.metadata")
    root = _required_object(metadata.get("component"), location=f"{path}.metadata.component")
    if _required_string(root, "type", location=f"{path}.metadata.component") != "application":
        raise ValueError(f"{path}.metadata.component.type must be application")
    _required_string(root, "name", location=f"{path}.metadata.component")
    _required_string(root, "version", location=f"{path}.metadata.component")
    root_ref = _required_string(root, "bom-ref", location=f"{path}.metadata.component")
    root_properties = _properties(root, location=f"{path}.metadata.component")
    base_images = _property_values(root_properties, "makolet:base-image")
    platforms = _property_values(root_properties, _RUNTIME_PLATFORM_PROPERTY)
    if len(base_images) != 1 or _PINNED_BASE_IMAGE.fullmatch(base_images[0]) is None:
        raise ValueError(f"{path}.metadata.component requires one digest-pinned base image")
    if len(platforms) != 1:
        raise ValueError(
            f"{path}.metadata.component requires exactly one {_RUNTIME_PLATFORM_PROPERTY} property"
        )

    tools = _required_array(metadata.get("tools"), location=f"{path}.metadata.tools")
    if not tools:
        raise ValueError(f"{path}.metadata.tools must not be empty")
    for index, value in enumerate(tools):
        location = f"{path}.metadata.tools[{index}]"
        tool = _required_object(value, location=location)
        _required_string(tool, "name", location=location)
        _required_string(tool, "version", location=location)
    return root_ref


def _validate_runtime_dependencies(
    document: JsonObject,
    *,
    path: Path,
    root_ref: str,
    component_refs: set[str],
) -> None:
    dependencies = _required_array(document.get("dependencies"), location=f"{path}.dependencies")
    dependency_refs: set[str] = set()
    for index, value in enumerate(dependencies):
        location = f"{path}.dependencies[{index}]"
        dependency = _required_object(value, location=location)
        reference = _required_string(dependency, "ref", location=location)
        if reference in dependency_refs:
            raise ValueError(f"{path} contains duplicate dependency ref {reference}")
        dependency_refs.add(reference)
        depends_on_value = dependency.get("dependsOn")
        if depends_on_value is None:
            continue
        depends_on = _required_array(depends_on_value, location=f"{location}.dependsOn")
        edges: set[str] = set()
        for edge_index, edge in enumerate(depends_on):
            if not isinstance(edge, str) or not edge:
                raise ValueError(f"{location}.dependsOn[{edge_index}] must be a non-empty string")
            if edge not in component_refs:
                raise ValueError(
                    f"{location}.dependsOn[{edge_index}] references an unknown component"
                )
            if edge in edges:
                raise ValueError(f"{location}.dependsOn contains duplicate edge {edge}")
            edges.add(edge)
    expected_refs = component_refs | {root_ref}
    if dependency_refs != expected_refs:
        raise ValueError(f"{path}.dependencies does not cover every runtime bom-ref exactly once")


def _validate_runtime_document(document: JsonObject, *, path: Path) -> int:
    if document.get("version") != 1:
        raise ValueError(f"{path}.version must be 1")
    root_ref = _validate_runtime_metadata(document, path=path)
    components = _required_array(document.get("components"), location=f"{path}.components")
    if not components:
        raise ValueError(f"{path}.components must not be empty")
    component_refs: set[str] = set()
    for index, value in enumerate(components):
        bom_ref = _validate_runtime_component(value, index=index, path=path)
        if bom_ref in component_refs:
            raise ValueError(f"{path} contains duplicate component bom-ref {bom_ref}")
        component_refs.add(bom_ref)
    if root_ref in component_refs:
        raise ValueError(f"{path} uses the root bom-ref for a child component")
    _validate_runtime_dependencies(
        document,
        path=path,
        root_ref=root_ref,
        component_refs=component_refs,
    )
    return len(components)


def _canonicalize(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        return {key: _canonicalize(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        normalized = [_canonicalize(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
        )
    return value


def _runtime_semantics(document: JsonObject) -> JsonObject:
    normalized = copy.deepcopy(document)
    metadata = cast(JsonObject, normalized["metadata"])
    component = cast(JsonObject, metadata["component"])
    properties = cast(list[JsonValue], component["properties"])
    for value in properties:
        item = cast(JsonObject, value)
        if item["name"] == _RUNTIME_PLATFORM_PROPERTY:
            item["value"] = _RUNTIME_PLATFORM_SENTINEL
    return cast(JsonObject, _canonicalize(normalized))


def _display_value(value: JsonValue | object) -> str:
    rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if len(rendered) <= 240:
        return rendered
    return f"{rendered[:237]}..."


def _first_difference(
    committed: JsonValue,
    generated: JsonValue,
    *,
    location: str = "$",
) -> str | None:
    if type(committed) is not type(generated):
        return (
            f"{location}: committed type {type(committed).__name__}, "
            f"generated type {type(generated).__name__}"
        )
    if isinstance(committed, dict) and isinstance(generated, dict):
        committed_keys = set(committed)
        generated_keys = set(generated)
        if committed_keys != generated_keys:
            missing = sorted(generated_keys - committed_keys)
            stale = sorted(committed_keys - generated_keys)
            return f"{location}: missing keys {missing!r}; stale keys {stale!r}"
        for key in sorted(committed):
            difference = _first_difference(
                committed[key],
                generated[key],
                location=f"{location}.{key}",
            )
            if difference is not None:
                return difference
        return None
    if isinstance(committed, list) and isinstance(generated, list):
        if len(committed) != len(generated):
            return (
                f"{location}: committed length {len(committed)}, generated length {len(generated)}"
            )
        for index, (committed_item, generated_item) in enumerate(
            zip(committed, generated, strict=True)
        ):
            difference = _first_difference(
                committed_item,
                generated_item,
                location=f"{location}[{index}]",
            )
            if difference is not None:
                return difference
        return None
    if committed != generated:
        return (
            f"{location}: committed={_display_value(committed)}, "
            f"generated={_display_value(generated)}"
        )
    return None


def _compare_runtime_semantics(committed_path: Path, generated_path: Path) -> int:
    committed = _load_document(committed_path)
    generated = _load_document(generated_path)
    committed_count = _validate_runtime_document(committed, path=committed_path)
    generated_count = _validate_runtime_document(generated, path=generated_path)
    difference = _first_difference(_runtime_semantics(committed), _runtime_semantics(generated))
    if difference is not None:
        raise ValueError(f"runtime SBOM semantic mismatch at {difference}")
    if committed_count != generated_count:
        raise ValueError("runtime SBOM component counts differ")
    return committed_count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-semantic",
        action="store_true",
        help=(
            "compare all stable runtime CycloneDX content while ignoring only the "
            "host-dependent makolet:platform property value"
        ),
    )
    parser.add_argument("committed", type=Path)
    parser.add_argument("generated", type=Path)
    arguments = parser.parse_args()
    try:
        if arguments.runtime_semantic:
            component_count = _compare_runtime_semantics(arguments.committed, arguments.generated)
        else:
            committed = _inventory(arguments.committed)
            generated = _inventory(arguments.generated)
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        sys.stderr.write(f"SBOM check error: {error}\n")
        return 1

    if arguments.runtime_semantic:
        sys.stdout.write(
            "committed runtime SBOM matches "
            f"{component_count} components and all stable CycloneDX semantics "
            f"(ignored only {_RUNTIME_PLATFORM_PROPERTY} value)\n"
        )
        return 0

    missing = generated - committed
    stale = committed - generated
    if missing or stale:
        if missing:
            sys.stderr.write(
                "components missing from committed SBOM: "
                + ", ".join(f"{name}=={version}" for name, version in sorted(missing))
                + "\n"
            )
        if stale:
            sys.stderr.write(
                "stale components in committed SBOM: "
                + ", ".join(f"{name}=={version}" for name, version in sorted(stale))
                + "\n"
            )
        return 1
    sys.stdout.write(f"committed SBOM matches {len(committed)} locked components\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
