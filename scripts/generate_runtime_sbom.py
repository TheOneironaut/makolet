"""Generate a deterministic CycloneDX inventory from the built Linux runtime image."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

_ROOT = Path(__file__).resolve().parents[1]
_MAX_EVIDENCE_BYTES = 16 * 1024 * 1024
_SAFE_METADATA = re.compile(r"[^\x00-\x1f\x7f]{1,256}\Z")
_DOCKERFILE_DIRECTIVE = re.compile(
    r"^\s*#\s*(?P<name>syntax|escape|check)\s*=\s*(?P<value>\S+)\s*$",
    re.IGNORECASE,
)
_DOCKERFILE_FROM = re.compile(
    r"^\s*FROM(?:\s+--platform=\S+)?\s+(?P<image>\S+)"
    r"(?:\s+AS\s+(?P<alias>[A-Za-z0-9_.-]+))?\s*$",
    re.IGNORECASE,
)
_PINNED_LITERAL_IMAGE = re.compile(
    r"[^\s@$]+:[^\s@$]+@sha256:[0-9a-f]{64}\Z",
)
_DEBIAN_LICENSE_FIELD = re.compile(r"^License:[ \t]*(\S[^\r\n]*)[ \t]*$", re.MULTILINE)
_DEBIAN_SCOPED_LICENSE_FIELD = re.compile(
    r"^License \(([^\r\n:]+)\):[ \t]*(\S[^\r\n]*)[ \t]*$",
    re.MULTILINE,
)
# These Debian 12 copyright files predate DEP-5 License fields. The labels below
# are a reviewed, package-specific summary of the exact file shipped by the pinned
# base image. The copyright-file digest remains beside the labels in the SBOM, so
# an upstream text change cannot silently reuse this review.
_LEGACY_DEBIAN_LICENSES: dict[str, tuple[str, tuple[str, ...]]] = {
    "base-files": (
        "fd7e4aae7e7b05f217bcf2d02322825c360e66c52c4c2f1b28d784d6297a1c23",
        ("GPL-2.0-or-later", "Artistic-1.0-Perl"),
    ),
    "debian-archive-keyring": (
        "b32aecaae84643700a33bc9ee83fa9b36938d35aa7b61b5042092eca77ddb732",
        ("GPL-2.0-or-later", "Public-domain archive keys"),
    ),
    "gcc-12-base": (
        "da8191658b3452ce9caf31638ba61dab31a38c619fa39df119812e050f592fd3",
        ("GPL-3.0-or-later",),
    ),
    "libc-bin": (
        "40c7e1f2118531f038ca22999bd976901254e1bc5cd1b0f0211bdd064c599987",
        ("GPL-2.0-or-later", "LGPL-2.1-or-later", "BSD/permissive file-level terms"),
    ),
    "libc6": (
        "40c7e1f2118531f038ca22999bd976901254e1bc5cd1b0f0211bdd064c599987",
        ("LGPL-2.1-or-later", "GPL-2.0-or-later", "BSD/permissive file-level terms"),
    ),
    "libcrypt1": (
        "5a5e7ca0e9f3f9679977e3a3e9ede45ad92885a3297ea78e766979f9866c5a16",
        (
            "LGPL-2.1-or-later",
            "BSD/GPL/permissive file-level terms",
            "Public-domain file-level material",
        ),
    ),
    "libgcc-s1": (
        "da8191658b3452ce9caf31638ba61dab31a38c619fa39df119812e050f592fd3",
        ("GPL-3.0-or-later WITH GCC-exception-3.1",),
    ),
    "libselinux1": (
        "641806738b0c6a3d03168c2649e6b94205198bb66cd557e6bae3a20b71c9bb0b",
        (
            "Public-domain libselinux",
            "GPL-2.0-only Debian and utility files",
            "LGPL-2.1-or-later glibc excerpt",
        ),
    ),
    "libsemanage-common": (
        "e00e6a86c598f9e24ced0180a70a5f2f2e45e133d93adf2b17ce8a06ce4b1b51",
        ("LGPL-2.1-or-later", "GPL-2.0-only Debian changes"),
    ),
    "libsemanage2": (
        "e00e6a86c598f9e24ced0180a70a5f2f2e45e133d93adf2b17ce8a06ce4b1b51",
        ("LGPL-2.1-or-later", "GPL-2.0-only Debian changes"),
    ),
    "libstdc++6": (
        "da8191658b3452ce9caf31638ba61dab31a38c619fa39df119812e050f592fd3",
        ("GPL-3.0-or-later WITH GCC-exception-3.1",),
    ),
    "libtasn1-6": (
        "572ad60ad184d8f52c6dc66d83a61a68f137db560b3452ce00588c9ecf128a65",
        ("LGPL-2.1-or-later", "GPL-3.0-or-later", "GFDL-1.3-or-later"),
    ),
}
_DISTRIBUTION_FILES = (
    Path("/opt/makolet/THIRD_PARTY_NOTICES.md"),
    Path("/opt/makolet/sbom.cdx.json"),
    Path("/opt/makolet/sbom.build.cdx.json"),
    Path("/opt/makolet/sbom.runtime-linux.cdx.json"),
    Path("/opt/makolet/build-constraints.txt"),
)


def _sha256(path: Path) -> str:
    if not path.is_file() or path.stat().st_size > _MAX_EVIDENCE_BYTES:
        raise ValueError(f"missing or oversized evidence file: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _license_label(metadata: importlib.metadata.PackageMetadata) -> str:
    expression = metadata.get("License-Expression")
    if isinstance(expression, str) and _SAFE_METADATA.fullmatch(expression):
        return expression
    label = metadata.get("License")
    if (
        isinstance(label, str)
        and label.casefold() not in {"unknown", "none"}
        and _SAFE_METADATA.fullmatch(label)
    ):
        return label
    prefix = "License :: OSI Approved :: "
    classifiers = metadata.get_all("Classifier", [])
    matches = [item.removeprefix(prefix) for item in classifiers if item.startswith(prefix)]
    if len(matches) == 1 and _SAFE_METADATA.fullmatch(matches[0]):
        return str(matches[0])
    raise ValueError(f"no unambiguous license label for {metadata.get('Name')!r}")


def _evidence_properties(
    distribution: importlib.metadata.Distribution,
) -> tuple[list[dict[str, str]], list[Path]]:
    evidence: list[Path] = []
    native: list[Path] = []
    for relative in distribution.files or ():
        parts = [part.casefold() for part in relative.parts]
        if not any(part.endswith(".dist-info") for part in parts):
            continue
        path = Path(str(distribution.locate_file(relative)))
        name = path.name.casefold()
        if (
            any(name.startswith(prefix) for prefix in ("license", "copying", "notice", "authors"))
            or "sboms" in parts
        ):
            evidence.append(path)
    for relative in distribution.files or ():
        path = Path(str(distribution.locate_file(relative)))
        if path.suffix.casefold() in {".so", ".pyd", ".dll"}:
            native.append(path)
    properties = [
        {
            "name": "makolet:artifact-evidence",
            "value": f"{path.name}|sha256:{_sha256(path)}",
        }
        for path in sorted(set(evidence))
    ]
    if not properties:
        raise ValueError(f"distribution {distribution.metadata['Name']} has no license evidence")
    return properties, sorted(set(native))


def _dynamic_link_properties(native_paths: list[Path]) -> list[dict[str, str]]:
    properties: list[dict[str, str]] = []
    for path in native_paths:
        result = subprocess.run(  # noqa: S603 - fixed tool and wheel-record paths only.
            ["/usr/bin/ldd", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if "not found" in result.stdout:
            raise ValueError(f"unresolved native dependency for {path}")
        libraries: set[str] = set()
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("linux-vdso"):
                continue
            library = stripped.split("=>", maxsplit=1)[0].strip().split(maxsplit=1)[0]
            if library:
                libraries.add(library)
        properties.append(
            {
                "name": "makolet:native-dynamic-links",
                "value": f"{path.name}|{','.join(sorted(libraries))}",
            }
        )
    return properties


def _debian_license_names(
    copyright_path: Path,
    document_name: str,
    copyright_digest: str,
) -> tuple[str, ...]:
    if not copyright_path.is_file() or copyright_path.stat().st_size > _MAX_EVIDENCE_BYTES:
        raise ValueError(f"missing or oversized Debian copyright file: {copyright_path}")
    copyright_text = copyright_path.read_text(encoding="utf-8")
    declared_names = {
        " ".join(match.group(1).split()) for match in _DEBIAN_LICENSE_FIELD.finditer(copyright_text)
    }
    declared_names.update(
        f"{' '.join(match.group(2).split())} ({' '.join(match.group(1).split())})"
        for match in _DEBIAN_SCOPED_LICENSE_FIELD.finditer(copyright_text)
    )
    if declared_names:
        if any(_SAFE_METADATA.fullmatch(name) is None for name in declared_names):
            raise ValueError(f"invalid Debian license label for {document_name}")
        return tuple(sorted(declared_names, key=lambda name: (name.casefold(), name)))
    legacy_review = _LEGACY_DEBIAN_LICENSES.get(document_name)
    if legacy_review is None:
        raise ValueError(
            f"Debian package {document_name} has no DEP-5 License fields or reviewed legacy mapping"
        )
    expected_digest, legacy_names = legacy_review
    if copyright_digest != expected_digest:
        raise ValueError(f"Debian package {document_name} legacy copyright evidence changed")
    return legacy_names


def _python_components() -> list[dict[str, object]]:
    components: list[dict[str, object]] = []
    for distribution in sorted(
        importlib.metadata.distributions(),
        key=lambda item: (item.metadata["Name"].casefold(), item.version),
    ):
        name = distribution.metadata["Name"]
        version = distribution.version
        if name.casefold() == "makolet":
            continue
        properties, native_paths = _evidence_properties(distribution)
        properties.extend(_dynamic_link_properties(native_paths))
        properties.sort(key=lambda item: (item["name"], item["value"]))
        components.append(
            {
                "type": "library",
                "bom-ref": f"pkg:pypi/{name.casefold()}@{version}",
                "name": name,
                "version": version,
                "purl": f"pkg:pypi/{quote(name.casefold())}@{quote(version)}",
                "licenses": [{"license": {"name": _license_label(distribution.metadata)}}],
                "properties": properties,
            }
        )
    return components


def _debian_components() -> list[dict[str, object]]:
    query_format = (
        "${binary:Package}\t${Version}\t${Architecture}\t${source:Package}\t${source:Version}\n"
    )
    result = subprocess.run(  # noqa: S603 - fixed dpkg-query invocation.
        ["/usr/bin/dpkg-query", "-W", f"-f={query_format}"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    components: list[dict[str, object]] = []
    for line in result.stdout.splitlines():
        name, version, architecture, source_name, source_version = line.split("\t")
        document_name = name.split(":", maxsplit=1)[0]
        copyright_path = Path("/usr/share/doc") / document_name / "copyright"
        source_name = source_name or document_name
        source_version = source_version or version
        purl = f"pkg:deb/debian/{quote(document_name)}@{quote(version)}?arch={quote(architecture)}"
        copyright_digest = _sha256(copyright_path)
        license_names = _debian_license_names(copyright_path, document_name, copyright_digest)
        components.append(
            {
                "type": "operating-system",
                "bom-ref": purl,
                "name": name,
                "version": version,
                "purl": purl,
                "licenses": [{"license": {"name": name}} for name in license_names],
                "properties": [
                    {
                        "name": "makolet:debian-copyright-evidence",
                        "value": f"{copyright_path}|sha256:{copyright_digest}",
                    },
                    {"name": "makolet:debian-source", "value": source_name},
                    {"name": "makolet:debian-source-version", "value": source_version},
                ],
            }
        )
    return sorted(components, key=lambda item: (str(item["name"]), str(item["version"])))


def _cpython_component() -> dict[str, object]:
    version = platform.python_version()
    license_path = (
        Path(sys.base_prefix)
        / "lib"
        / f"python{sys.version_info.major}.{sys.version_info.minor}"
        / "LICENSE.txt"
    )
    return {
        "type": "framework",
        "bom-ref": f"pkg:generic/cpython@{version}",
        "name": "CPython",
        "version": version,
        "purl": f"pkg:generic/cpython@{version}",
        "licenses": [{"license": {"id": "PSF-2.0"}}],
        "properties": [
            {
                "name": "makolet:artifact-evidence",
                "value": f"{license_path}|sha256:{_sha256(license_path)}",
            }
        ],
    }


def _dockerfile_logical_lines(document: str) -> list[tuple[int, str]]:
    raw_lines = document.splitlines()
    if raw_lines and raw_lines[0].startswith("\ufeff"):
        raw_lines[0] = raw_lines[0].removeprefix("\ufeff")

    escape_character = "\\"
    seen_directives: set[str] = set()
    for line_number, line in enumerate(raw_lines, start=1):
        if not line.strip():
            if seen_directives:
                break
            continue
        directive = _DOCKERFILE_DIRECTIVE.fullmatch(line)
        if directive is None:
            break
        name = directive.group("name").casefold()
        if name in seen_directives:
            raise ValueError(f"Dockerfile:{line_number} repeats the {name} directive")
        seen_directives.add(name)
        value = directive.group("value")
        if name == "escape" and value not in {"\\", "`"}:
            raise ValueError(f"Dockerfile:{line_number} has an invalid escape directive")
        if name == "escape":
            escape_character = value

    logical_lines: list[tuple[int, str]] = []
    pending = ""
    pending_line = 0
    for line_number, line in enumerate(raw_lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not pending:
            pending_line = line_number
        fragment = line.rstrip()
        trailing_escapes = len(fragment) - len(fragment.rstrip(escape_character))
        continuation = trailing_escapes % 2 == 1
        if continuation:
            fragment = fragment[:-1]
        pending += fragment.lstrip()
        if not continuation:
            logical_lines.append((pending_line, pending.strip()))
            pending = ""
    if pending:
        raise ValueError(f"Dockerfile:{pending_line} has an unterminated continuation")
    return logical_lines


def _runtime_base_image(document: str) -> str:
    runtime_stages: list[tuple[int, str]] = []
    for line_number, line in _dockerfile_logical_lines(document):
        match = _DOCKERFILE_FROM.fullmatch(line)
        if match is None or (match.group("alias") or "").casefold() != "runtime":
            continue
        runtime_stages.append((line_number, match.group("image")))

    if len(runtime_stages) != 1:
        raise ValueError(
            "Dockerfile must contain exactly one FROM stage named runtime; "
            f"found {len(runtime_stages)}"
        )
    line_number, image = runtime_stages[0]
    if _PINNED_LITERAL_IMAGE.fullmatch(image) is None:
        raise ValueError(
            f"Dockerfile:{line_number} runtime FROM image must be a literal tag "
            "pinned by a lowercase SHA-256 digest"
        )
    return image


def _base_image() -> str:
    dockerfile = (_ROOT / "Dockerfile").read_text(encoding="utf-8")
    return _runtime_base_image(dockerfile)


def _document() -> dict[str, object]:
    for path in _DISTRIBUTION_FILES:
        _sha256(path)
    components = [*_python_components(), *_debian_components(), _cpython_component()]
    components.sort(key=lambda item: str(item["bom-ref"]))
    python_refs = [
        str(component["bom-ref"])
        for component in components
        if str(component["bom-ref"]).startswith("pkg:pypi/")
    ]
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "tools": [{"name": "scripts/generate_runtime_sbom.py", "version": "2"}],
            "component": {
                "type": "application",
                "bom-ref": "pkg:pypi/makolet@0.1.0",
                "name": "makolet",
                "version": "0.1.0",
                "properties": [
                    {"name": "makolet:base-image", "value": _base_image()},
                    {"name": "makolet:platform", "value": platform.platform()},
                ],
            },
        },
        "components": components,
        "dependencies": [
            {"ref": "pkg:pypi/makolet@0.1.0", "dependsOn": sorted(python_refs)},
            *({"ref": str(component["bom-ref"])} for component in components),
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        payload = json.dumps(_document(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        arguments.output.write_text(payload, encoding="utf-8", newline="\n")
    except (OSError, RuntimeError, TypeError, ValueError, subprocess.SubprocessError) as error:
        sys.stderr.write(f"runtime SBOM error: {error}\n")
        return 1
    sys.stdout.write(f"wrote runtime SBOM to {arguments.output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
