"""Build reproducibly and validate Makolet wheel and sdist artifacts."""

from __future__ import annotations

import asyncio
import base64
import configparser
import csv
import io
import ipaddress
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from email import policy
from email.message import Message
from email.parser import BytesParser
from hashlib import sha256
from importlib import metadata
from os import environ
from os import name as operating_system_name
from pathlib import Path, PurePosixPath
from typing import cast
from urllib.parse import urlsplit

_ROOT = Path(__file__).resolve().parents[1]
_MAXIMUM_ARTIFACT_BYTES = 32 * 1024 * 1024
_MAXIMUM_MEMBER_BYTES = 16 * 1024 * 1024
_MAXIMUM_MEMBERS = 5_000
_MAXIMUM_EXPANDED_BYTES = 64 * 1024 * 1024
_MAXIMUM_TEXT_MEMBER_BYTES = 2 * 1024 * 1024
_MAXIMUM_COMMAND_OUTPUT_BYTES = 2 * 1024 * 1024
_COMMAND_TIMEOUT_SECONDS = 300
_SOURCE_DATE_EPOCH = "946684800"
_WINDOWS_DRIVE = re.compile(r"[A-Za-z]:")
_BUILD_CONSTRAINTS = _ROOT / "build-constraints.txt"
_REPRODUCIBILITY_ARGUMENT = "--reproducible"
_INSTALLED_ARGUMENT = "--verify-installed"
_INSTALLED_POSTGRES_ARGUMENT = "--verify-installed-postgres"
_POSTGRES_URL_VARIABLE = "MAKOLET_DISTRIBUTION_TEST_DATABASE_URL"
_POSTGRES_CONFIRM_VARIABLE = "MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM"
_DISTRIBUTION_TEST_DATABASE = re.compile(r"makolet_test_distribution_[0-9a-f]{8,32}\Z")
_EXPECTED_MIGRATION_HEAD = "0011_resource_probe_budgets"
_CONSOLE_ENTRY_POINT = "makolet.interfaces.cli:main"
_SAFE_TEXT_NAMES = frozenset(
    {
        ".dockerignore",
        ".editorconfig",
        ".env.example",
        ".gitattributes",
        ".gitignore",
        ".python-version",
        "Dockerfile",
        "LICENSE",
        "METADATA",
        "PKG-INFO",
        "RECORD",
        "WHEEL",
        "entry_points.txt",
    }
)
_TEXT_SUFFIXES = frozenset(
    {
        ".html",
        ".ini",
        ".json",
        ".lock",
        ".mako",
        ".md",
        ".ps1",
        ".py",
        ".sh",
        ".toml",
        ".txt",
        ".typed",
        ".xml",
        ".yaml",
        ".yml",
    }
)
_FORBIDDEN_DIRECTORY_PARTS = frozenset(
    {
        ".agent",
        ".git",
        ".github",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "data",
        "dist",
        "exports",
        "htmlcov",
        "raw-archive",
    }
)
_FORBIDDEN_FILE_SUFFIXES = frozenset(
    {
        ".backup",
        ".dump",
        ".hmac-sha256",
        ".key",
        ".log",
        ".p12",
        ".pem",
        ".pfx",
        ".pyc",
        ".pyo",
    }
)
_SDIST_ROOT_FILES = (
    ".dockerignore",
    ".editorconfig",
    ".env.example",
    ".gitattributes",
    ".gitignore",
    ".python-version",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "Dockerfile",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "THIRD_PARTY_NOTICES.md",
    "alembic.ini",
    "build-constraints.txt",
    "compose.yaml",
    "pyproject.toml",
    "sbom.build.cdx.json",
    "sbom.cdx.json",
    "sbom.runtime-linux.cdx.json",
    "uv.lock",
)
_SDIST_ROOT_DIRECTORIES = (
    "benchmarks",
    "deployment",
    "docs",
    "migrations",
    "scripts",
    "src",
    "tests",
)
_EXPECTED_WHEEL_PACKAGES = ("src/makolet", "benchmarks")
_EXPECTED_WHEEL_EXCLUDES = (
    "benchmarks/__pycache__/**",
    "benchmarks/results/**",
    "src/makolet/**/AGENTS.md",
    "migrations/AGENTS.md",
)
_EXPECTED_FORCE_INCLUDES = {
    "migrations/env.py": "makolet/_migrations/env.py",
    "migrations/script.py.mako": "makolet/_migrations/script.py.mako",
    "migrations/versions": "makolet/_migrations/versions",
}
_EXPECTED_SDIST_EXCLUDES = (
    "/benchmarks/results/**",
    "/**/AGENTS.md",
    "/**/__pycache__/**",
)
_SCAN_ALLOW_MARKER = "secret-scan: allow"
_KNOWN_SECRET_RULES = (
    (
        "private-key",
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"),
    ),
    ("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    (
        "github-token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    ),
    ("gitlab-token", re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("stripe-live-key", re.compile(r"\b(?:sk|rk)_live_[A-Za-z0-9]{20,}\b")),
    ("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    ("npm-token", re.compile(r"\bnpm_[A-Za-z0-9]{36}\b")),
    ("pypi-token", re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{30,}\b")),
)
_CREDENTIAL_ASSIGNMENT = re.compile(
    r"(?ix)(?:^|[\s,;({])"
    r"(?P<name>[A-Za-z][A-Za-z0-9_-]*)"
    r"\s*(?:=|:)\s*[\"']?(?P<value>[^\s\"'#,;}]{8,})"
)
_CREDENTIAL_IDENTIFIER_PARTS = frozenset({"password", "passwd", "pwd", "secret"})
_CREDENTIAL_IDENTIFIER_SUFFIXES = (
    "api_key",
    "apikey",
    "access_key",
    "accesskey",
    "private_key",
    "privatekey",
)
_CREDENTIAL_TOKEN_IDENTIFIERS = frozenset(
    {"token", "api_token", "access_token", "auth_token", "secret_token", "bearer_token"}
)
_URI_CREDENTIAL = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|ftp|https?)://"
    r"[^\s/:@]+:(?P<value>[^\s/@]+)@"
)
_LOCAL_HOST_PATH = re.compile(
    r"(?i)(?:[A-Za-z]:[\\/](?:Users|Documents and Settings)[\\/]|"
    r"/(?:home|Users)/[^/\s]+/)"
)
_SAFE_EXACT_SECRET_VALUES = frozenset(
    {
        "changeme",
        "change-me",
        "dummy-secret",
        "do-not-print",
        "example-secret",
        "fake-secret",
        "makolet-development",
        "makolet-development-only-change-me",
        "never-record-this",
        "not-a-secret",
        "pass",
        "password",
        "placeholder",
        "private-password",
        "secret",
        "secret-value",
        "test-password",
        "test-access",
        "test-secret",
        "test-secret-key",
    }
)
_SAFE_SECRET_FRAGMENTS = (
    "development-only",
    "fixture",
    "placeholder",
    "redacted",
    "test-password",
    "test-only",
)
_SAFE_SECRET_EXPRESSION_PREFIXES = (
    "${",
    "$env:",
    "env(",
    "environ",
    "getenv(",
    "os.environ",
    "path(",
    "process.env",
    "secret(",
    "secretstr(",
    "settings.",
)
_SAFE_SECRET_REFERENCE = re.compile(r"^[a-z_][a-z0-9_.]*$")
_SAFE_SECRET_CALL = re.compile(r"^[a-z_][a-z0-9_.]*\(")

ADVERTISED_CLI_HELP_PATHS: tuple[tuple[str, ...], ...] = (
    (),
    ("database",),
    ("database", "migrate"),
    ("database", "status"),
    ("sources",),
    ("sources", "list"),
    ("sources", "inspect"),
    ("sources", "test"),
    ("ingest",),
    ("ingest", "source"),
    ("ingest", "retailer"),
    ("ingest", "all"),
    ("ingest", "backfill"),
    ("ingest", "replay"),
    ("ingest", "replay-range"),
    ("ingest", "rebuild-normalized"),
    ("ingest", "resume-rebuild"),
    ("ingest", "rebuild-status"),
    ("ingest", "worker"),
    ("status",),
    ("freshness",),
    ("source-status",),
    ("failures",),
    ("quarantine",),
    ("quarantine", "list"),
    ("quarantine", "inspect"),
    ("doctor",),
    ("matching",),
    ("matching", "generate"),
    ("matching", "list"),
    ("matching", "inspect"),
    ("matching", "accept"),
    ("matching", "reject"),
    ("products",),
    ("retailers",),
    ("retailers", "list"),
    ("stores",),
    ("stores", "find"),
    ("products", "search"),
    ("products", "get"),
    ("products", "find-barcode"),
    ("products", "find-retailer-item"),
    ("prices",),
    ("prices", "current"),
    ("prices", "compare"),
    ("prices", "history"),
    ("availability",),
    ("availability", "current"),
    ("promotions",),
    ("promotions", "active"),
    ("promotions", "history"),
    ("api",),
    ("api", "serve"),
    ("mcp",),
    ("mcp", "serve"),
    ("export",),
    ("export", "parquet"),
    ("benchmark",),
    ("benchmark", "run"),
)


def _registered_cli_help_paths(application: object) -> tuple[tuple[str, ...], ...]:
    from typer import Typer

    if not isinstance(application, Typer):
        raise TypeError("CLI application is not a Typer instance")

    paths: list[tuple[str, ...]] = []

    def collect(current: Typer, prefix: tuple[str, ...]) -> None:
        paths.append(prefix)
        for command in current.registered_commands:
            if not isinstance(command.name, str) or not command.name:
                raise ValueError("CLI command requires an explicit non-empty name")
            paths.append((*prefix, command.name))
        for group in current.registered_groups:
            if not isinstance(group.name, str) or not group.name:
                raise ValueError("CLI group requires an explicit non-empty name")
            if not isinstance(group.typer_instance, Typer):
                raise TypeError("CLI group does not contain a Typer application")
            collect(group.typer_instance, (*prefix, group.name))

    collect(application, ())
    if len(paths) != len(set(paths)):
        raise ValueError("CLI command tree contains a duplicate help path")
    return tuple(paths)


@dataclass(frozen=True, slots=True)
class _PostgresProofTarget:
    url: str
    database_name: str


class _CaseSensitiveConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


def _project_document() -> dict[str, object]:
    document = cast(
        object,
        tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8")),
    )
    if not isinstance(document, dict) or any(not isinstance(key, str) for key in document):
        raise TypeError("pyproject document is invalid")
    return cast(dict[str, object], document)


def _string_list(value: object, *, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError(f"{label} must be a string list")
    return tuple(cast(list[str], value))


def _string_mapping(value: object, *, label: str) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or any(not isinstance(key, str) for key in value)
        or any(not isinstance(item, str) for item in value.values())
    ):
        raise TypeError(f"{label} must be a string mapping")
    return cast(dict[str, str], value)


def _project_table(document: Mapping[str, object]) -> dict[str, object]:
    project = document.get("project")
    if not isinstance(project, dict) or any(not isinstance(key, str) for key in project):
        raise TypeError("pyproject project table is missing or invalid")
    return cast(dict[str, object], project)


def _tool_hatch_table(document: Mapping[str, object]) -> dict[str, object]:
    tool = document.get("tool")
    hatch = tool.get("hatch") if isinstance(tool, dict) else None
    build = hatch.get("build") if isinstance(hatch, dict) else None
    targets = build.get("targets") if isinstance(build, dict) else None
    if not isinstance(targets, dict):
        raise TypeError("pyproject Hatch build targets are missing or invalid")
    return cast(dict[str, object], targets)


def _require_build_configuration() -> None:
    targets = _tool_hatch_table(_project_document())
    wheel = targets.get("wheel")
    sdist = targets.get("sdist")
    if not isinstance(wheel, dict) or not isinstance(sdist, dict):
        raise TypeError("pyproject wheel or sdist target is missing")
    wheel = cast(dict[str, object], wheel)
    sdist = cast(dict[str, object], sdist)
    expected_sdist_includes = tuple(
        f"/{name}" for name in (*_SDIST_ROOT_FILES, *_SDIST_ROOT_DIRECTORIES)
    )
    if _string_list(wheel.get("packages"), label="wheel packages") != _EXPECTED_WHEEL_PACKAGES:
        raise ValueError("wheel package roots differ from the reviewed inventory")
    if _string_list(wheel.get("exclude"), label="wheel excludes") != _EXPECTED_WHEEL_EXCLUDES:
        raise ValueError("wheel exclusions differ from the reviewed inventory")
    if (
        _string_mapping(wheel.get("force-include"), label="wheel force-includes")
        != _EXPECTED_FORCE_INCLUDES
    ):
        raise ValueError("wheel force-includes differ from the reviewed inventory")
    sdist_includes = _string_list(sdist.get("include"), label="sdist includes")
    if len(sdist_includes) != len(set(sdist_includes)) or set(sdist_includes) != set(
        expected_sdist_includes
    ):
        raise ValueError("sdist includes differ from the reviewed inventory")
    if _string_list(sdist.get("exclude"), label="sdist excludes") != _EXPECTED_SDIST_EXCLUDES:
        raise ValueError("sdist exclusions differ from the reviewed inventory")


def _add_expected_file(
    inventory: dict[str, Path],
    member_name: str,
    source_path: Path,
) -> None:
    if source_path.is_symlink() or not source_path.is_file():
        raise ValueError(f"distribution source is missing, non-regular, or linked: {source_path}")
    if _forbidden_member(member_name):
        raise ValueError(
            f"reviewed distribution inventory contains forbidden member: {member_name}"
        )
    if not _text_member(member_name):
        raise ValueError(f"distribution source uses an unreviewed payload type: {member_name}")
    if member_name in inventory:
        raise ValueError(f"reviewed distribution inventory repeats member: {member_name}")
    inventory[member_name] = source_path


def _expected_tree_files(
    directory: Path,
    *,
    member_prefix: str,
) -> dict[str, Path]:
    expected: dict[str, Path] = {}
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError(f"distribution source directory is missing or linked: {directory}")
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory).as_posix()
        member_name = f"{member_prefix}/{relative}" if member_prefix else relative
        if path.is_dir():
            if path.is_symlink():
                raise ValueError(f"distribution source directory is linked: {path}")
            continue
        if _forbidden_member(member_name):
            continue
        _add_expected_file(expected, member_name, path)
    return expected


def _merge_expected_files(target: dict[str, Path], source: Mapping[str, Path]) -> None:
    for member_name, source_path in source.items():
        _add_expected_file(target, member_name, source_path)


def _expected_wheel_repository_files() -> dict[str, Path]:
    _require_build_configuration()
    expected: dict[str, Path] = {}
    _merge_expected_files(
        expected,
        _expected_tree_files(_ROOT / "src" / "makolet", member_prefix="makolet"),
    )
    _merge_expected_files(
        expected,
        _expected_tree_files(_ROOT / "benchmarks", member_prefix="benchmarks"),
    )
    _merge_expected_files(expected, _expected_migrations("makolet/_migrations"))
    return expected


def _expected_sdist_repository_files() -> dict[str, Path]:
    _require_build_configuration()
    expected: dict[str, Path] = {}
    for relative_name in _SDIST_ROOT_FILES:
        _add_expected_file(expected, relative_name, _ROOT / relative_name)
    for directory_name in _SDIST_ROOT_DIRECTORIES:
        _merge_expected_files(
            expected,
            _expected_tree_files(_ROOT / directory_name, member_prefix=directory_name),
        )
    return expected


def _require_exact_file_inventory(
    actual: set[str],
    expected: set[str],
    *,
    label: str,
) -> None:
    if actual == expected:
        return
    missing = sorted(expected - actual)
    unexpected = sorted(actual - expected)
    raise ValueError(f"{label} file inventory differs; missing={missing}; unexpected={unexpected}")


def _credential_identifier(name: str) -> bool:
    normalized = name.strip().replace("-", "_").casefold()
    if normalized in _CREDENTIAL_TOKEN_IDENTIFIERS or normalized in _CREDENTIAL_IDENTIFIER_PARTS:
        return True
    parts = [part for part in normalized.split("_") if part]
    if parts and (
        parts[-1] in _CREDENTIAL_IDENTIFIER_PARTS
        or (
            len(parts) >= 2
            and parts[-2] in _CREDENTIAL_IDENTIFIER_PARTS
            and parts[-1] in {"key", "id", "token", "value"}
        )
    ):
        return True
    compact = normalized.replace("_", "")
    return any(
        compact.endswith(suffix.replace("_", "")) for suffix in _CREDENTIAL_IDENTIFIER_SUFFIXES
    )


def _safe_secret_literal(candidate: str) -> bool:
    normalized = candidate.strip().strip("\"'").casefold()
    return (
        normalized in _SAFE_EXACT_SECRET_VALUES
        or any(fragment in normalized for fragment in _SAFE_SECRET_FRAGMENTS)
        or normalized.startswith(_SAFE_SECRET_EXPRESSION_PREFIXES)
        or _SAFE_SECRET_REFERENCE.fullmatch(normalized) is not None
        or _SAFE_SECRET_CALL.match(normalized) is not None
    )


def _safe_secret_value(candidate: str) -> bool:
    normalized = candidate.strip().strip("\"'")
    if "://" in normalized:
        matches = tuple(_URI_CREDENTIAL.finditer(normalized))
        return bool(matches) and all(
            _safe_secret_literal(match.group("value")) for match in matches
        )
    return _safe_secret_literal(normalized)


def _text_member(member_name: str) -> bool:
    path = PurePosixPath(member_name)
    return path.name in _SAFE_TEXT_NAMES or path.suffix.casefold() in _TEXT_SUFFIXES


def _scan_distribution_text(member_name: str, payload: bytes) -> None:
    if not _text_member(member_name):
        raise ValueError(f"distribution member uses an unreviewed payload type: {member_name}")
    if len(payload) > _MAXIMUM_TEXT_MEMBER_BYTES:
        raise ValueError(f"text distribution member exceeds the scan limit: {member_name}")
    if b"\0" in payload:
        raise ValueError(f"text distribution member contains a NUL byte: {member_name}")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"text distribution member is not UTF-8: {member_name}") from error
    for line_number, line in enumerate(text.splitlines(), start=1):
        if _SCAN_ALLOW_MARKER in line:
            continue
        rules = [name for name, pattern in _KNOWN_SECRET_RULES if pattern.search(line)]
        if any(
            _credential_identifier(assignment.group("name"))
            and not _safe_secret_value(assignment.group("value"))
            for assignment in _CREDENTIAL_ASSIGNMENT.finditer(line)
        ):
            rules.append("credential-assignment")
        if any(
            not _safe_secret_value(uri_credential.group("value"))
            for uri_credential in _URI_CREDENTIAL.finditer(line)
        ):
            rules.append("credential-in-uri")
        if _LOCAL_HOST_PATH.search(line):
            rules.append("local-host-path")
        if rules:
            raise ValueError(
                f"distribution text rejected at {member_name}:{line_number}: {','.join(rules)}"
            )


def _one_metadata_header(metadata_message: Message, name: str, *, label: str) -> str:
    values = metadata_message.get_all(name, ())
    if len(values) != 1 or not isinstance(values[0], str):
        raise ValueError(f"{label} requires exactly one {name} header")
    return values[0]


def _validate_project_metadata(metadata_message: Message, *, label: str) -> None:
    project = _project_table(_project_document())
    version = project.get("version")
    description = project.get("description")
    license_expression = project.get("license")
    requires_python = project.get("requires-python")
    readme = project.get("readme")
    if not all(
        isinstance(value, str) and value
        for value in (version, description, license_expression, requires_python, readme)
    ):
        raise ValueError("pyproject release metadata is incomplete")
    dependencies = _string_list(project.get("dependencies"), label="project dependencies")
    optional = project.get("optional-dependencies")
    if not isinstance(optional, dict) or set(optional) != {"benchmark"}:
        raise ValueError("project optional dependencies differ from the reviewed benchmark extra")
    benchmark_dependencies = _string_list(optional.get("benchmark"), label="benchmark dependencies")
    if benchmark_dependencies != ("psutil==7.2.2",):
        raise ValueError("benchmark extra must exactly require psutil==7.2.2")
    expected_requirements = {
        *dependencies,
        *(f"{requirement}; extra == 'benchmark'" for requirement in benchmark_dependencies),
    }
    actual_requirements = metadata_message.get_all("Requires-Dist", ())
    if (
        len(actual_requirements) != len(set(actual_requirements))
        or set(actual_requirements) != expected_requirements
    ):
        raise ValueError(f"{label} Requires-Dist headers differ from pyproject")
    actual_requires_python = tuple(
        clause.strip()
        for clause in _one_metadata_header(metadata_message, "Requires-Python", label=label).split(
            ","
        )
    )
    expected_requires_python = tuple(
        clause.strip() for clause in cast(str, requires_python).split(",")
    )
    if len(actual_requires_python) != len(set(actual_requires_python)) or set(
        actual_requires_python
    ) != set(expected_requires_python):
        raise ValueError(f"{label} Requires-Python differs from pyproject")
    expected_single_headers = {
        "Metadata-Version": "2.5",
        "Name": "makolet",
        "Version": cast(str, version),
        "Summary": cast(str, description),
        "License-Expression": cast(str, license_expression),
        "Description-Content-Type": "text/markdown",
    }
    for name, expected_value in expected_single_headers.items():
        if _one_metadata_header(metadata_message, name, label=label) != expected_value:
            raise ValueError(f"{label} {name} differs from pyproject")
    license_files = metadata_message.get_all("License-File", ())
    if license_files != ["LICENSE", "THIRD_PARTY_NOTICES.md"]:
        raise ValueError(f"{label} License-File headers differ from pyproject")
    extras = metadata_message.get_all("Provides-Extra", ())
    if extras != ["benchmark"]:
        raise ValueError(f"{label} Provides-Extra headers differ from pyproject")
    expected_classifiers = _string_list(project.get("classifiers"), label="project classifiers")
    if metadata_message.get_all("Classifier", ()) != list(expected_classifiers):
        raise ValueError(f"{label} classifiers differ from pyproject")
    expected_keywords = ",".join(sorted(_string_list(project.get("keywords"), label="keywords")))
    if _one_metadata_header(metadata_message, "Keywords", label=label) != expected_keywords:
        raise ValueError(f"{label} keywords differ from pyproject")
    expected_readme = (_ROOT / cast(str, readme)).read_text(encoding="utf-8")
    if metadata_message.get_payload() != expected_readme:
        raise ValueError(f"{label} long description differs from the repository README")


def _validate_entry_points_text(text: str, *, label: str) -> None:
    parser = _CaseSensitiveConfigParser(interpolation=None)
    try:
        parser.read_string(text)
    except configparser.Error as error:
        raise ValueError(f"{label} is invalid") from error
    if parser.sections() != ["console_scripts"] or parser.items("console_scripts") != [
        ("makolet", _CONSOLE_ENTRY_POINT)
    ]:
        raise ValueError(f"{label} console entry point differs from pyproject")


def _safe_member_name(name: str) -> None:
    if not name or "\\" in name or name.startswith("/"):
        raise ValueError(f"unsafe distribution member path: {name!r}")
    parts = PurePosixPath(name).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"unsafe distribution member path: {name!r}")
    if _WINDOWS_DRIVE.fullmatch(parts[0]) is not None:
        raise ValueError(f"unsafe distribution member path: {name!r}")


def _artifact(output: Path, pattern: str, label: str) -> Path:
    matches = tuple(output.glob(pattern))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one Makolet {label}, found {len(matches)}")
    artifact = matches[0]
    size = artifact.stat().st_size
    if not artifact.is_file() or not 0 < size <= _MAXIMUM_ARTIFACT_BYTES:
        raise ValueError(f"{label} is missing, empty, or exceeds the size limit")
    return artifact


def _build_output(value: str) -> Path:
    output = Path(value).resolve(strict=True)
    if not output.is_dir():
        raise ValueError("build output is not a directory")
    return output


def _expected_migrations(prefix: str) -> dict[str, Path]:
    expected = _expected_tree_files(_ROOT / "migrations", member_prefix=prefix)
    if not expected:
        raise ValueError("repository migration inventory is empty")
    return expected


def _expected_benchmarks(prefix: str) -> dict[str, Path]:
    expected = _expected_tree_files(_ROOT / "benchmarks", member_prefix=prefix)
    if f"{prefix}/run.py" not in expected:
        raise ValueError("repository benchmark package is incomplete")
    return expected


def _require_exact_bytes(actual: bytes, expected_path: Path, label: str) -> None:
    expected = expected_path.read_bytes()
    if actual != expected:
        raise ValueError(f"{label} differs from the repository")


def _project_version() -> str:
    version = _project_table(_project_document()).get("version")
    if not isinstance(version, str) or not version or len(version) > 128:
        raise ValueError("pyproject project version is missing or invalid")
    return version


def _forbidden_member(name: str) -> bool:
    parts = PurePosixPath(name).parts
    folded_parts = tuple(part.casefold() for part in parts)
    contains_benchmark_results = any(
        folded_parts[index : index + 2] == ("benchmarks", "results")
        for index in range(max(0, len(folded_parts) - 1))
    )
    filename = folded_parts[-1] if folded_parts else ""
    suffix = PurePosixPath(filename).suffix
    return (
        "agents.md" in folded_parts
        or any(part in _FORBIDDEN_DIRECTORY_PARTS for part in folded_parts)
        or suffix in _FORBIDDEN_FILE_SUFFIXES
        or filename in {".coverage", "coverage.xml"}
        or (filename.startswith(".env") and filename != ".env.example")
        or (filename.startswith(".ci-") and filename.endswith(".json"))
        or contains_benchmark_results
    )


def _wheel_generated_files(dist_info: str) -> set[str]:
    return {
        f"{dist_info}/METADATA",
        f"{dist_info}/RECORD",
        f"{dist_info}/WHEEL",
        f"{dist_info}/entry_points.txt",
        f"{dist_info}/licenses/LICENSE",
        f"{dist_info}/licenses/THIRD_PARTY_NOTICES.md",
    }


def _validate_wheel_descriptor(payload: bytes, *, label: str) -> None:
    descriptor = BytesParser(policy=policy.default).parsebytes(payload)
    expected = {
        "Wheel-Version": "1.0",
        "Generator": "hatchling 1.32.0",
        "Root-Is-Purelib": "true",
        "Tag": "py3-none-any",
    }
    if set(descriptor.keys()) != set(expected):
        raise ValueError(f"{label} headers differ from the reviewed pure-Python wheel")
    for name, value in expected.items():
        if _one_metadata_header(descriptor, name, label=label) != value:
            raise ValueError(f"{label} {name} differs")


def _validate_wheel_record(
    payload: bytes,
    *,
    record_name: str,
    member_payloads: Mapping[str, bytes],
) -> None:
    try:
        rows = list(csv.reader(io.StringIO(payload.decode("utf-8"))))
    except (UnicodeDecodeError, csv.Error) as error:
        raise ValueError("wheel RECORD is invalid") from error
    if any(len(row) != 3 for row in rows):
        raise ValueError("wheel RECORD contains an invalid row")
    row_names = [row[0] for row in rows]
    if len(row_names) != len(set(row_names)):
        raise ValueError("wheel RECORD contains duplicate paths")
    _require_exact_file_inventory(set(row_names), set(member_payloads), label="wheel RECORD")
    for name, digest_value, size_value in rows:
        if name == record_name:
            if digest_value or size_value:
                raise ValueError("wheel RECORD self-row must omit hash and size")
            continue
        member_payload = member_payloads[name]
        encoded_digest = base64.urlsafe_b64encode(sha256(member_payload).digest()).rstrip(b"=")
        expected_digest = f"sha256={encoded_digest.decode('ascii')}"
        if digest_value != expected_digest or size_value != str(len(member_payload)):
            raise ValueError(f"wheel RECORD evidence differs for {name}")


def _validate_wheel(path: Path) -> int:
    repository_files = _expected_wheel_repository_files()
    dist_info = f"makolet-{_project_version()}.dist-info"
    generated_files = _wheel_generated_files(dist_info)
    with zipfile.ZipFile(path) as archive:
        members = archive.infolist()
        if not 0 < len(members) <= _MAXIMUM_MEMBERS:
            raise ValueError("wheel member count is empty or exceeds the limit")
        names = [member.filename for member in members]
        if len(names) != len(set(names)):
            raise ValueError("wheel contains duplicate member paths")
        total_size = 0
        for member in members:
            _safe_member_name(member.filename)
            if member.is_dir():
                raise ValueError(
                    f"wheel contains an unexpected directory member: {member.filename}"
                )
            if member.file_size > _MAXIMUM_MEMBER_BYTES:
                raise ValueError(f"wheel member exceeds the size limit: {member.filename}")
            total_size += member.file_size
            if total_size > _MAXIMUM_EXPANDED_BYTES:
                raise ValueError("wheel expanded payload exceeds the size limit")
            if _forbidden_member(member.filename):
                raise ValueError(f"wheel contains forbidden member: {member.filename}")
        expected_names = set(repository_files) | generated_files
        _require_exact_file_inventory(set(names), expected_names, label="wheel")
        member_payloads = {name: archive.read(name) for name in names}

    for member_name, source_path in repository_files.items():
        _require_exact_bytes(
            member_payloads[member_name], source_path, f"wheel member {member_name}"
        )
    metadata_name = f"{dist_info}/METADATA"
    _validate_project_metadata(
        BytesParser(policy=policy.default).parsebytes(member_payloads[metadata_name]),
        label="wheel METADATA",
    )
    _validate_wheel_descriptor(member_payloads[f"{dist_info}/WHEEL"], label="wheel WHEEL")
    try:
        entry_points_text = member_payloads[f"{dist_info}/entry_points.txt"].decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError("wheel entry points are not UTF-8") from error
    _validate_entry_points_text(entry_points_text, label="wheel entry points")
    for license_name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
        _require_exact_bytes(
            member_payloads[f"{dist_info}/licenses/{license_name}"],
            _ROOT / license_name,
            f"wheel {license_name}",
        )
    record_name = f"{dist_info}/RECORD"
    _validate_wheel_record(
        member_payloads[record_name],
        record_name=record_name,
        member_payloads=member_payloads,
    )
    for member_name, payload in member_payloads.items():
        _scan_distribution_text(member_name, payload)
    return len(members)


def _allowed_sdist_directories(expected_files: set[str]) -> set[str]:
    directories: set[str] = set()
    for filename in expected_files:
        parent = PurePosixPath(filename).parent
        while str(parent) != ".":
            directories.add(parent.as_posix())
            parent = parent.parent
    return directories


def _validate_sdist(path: Path) -> int:
    repository_files = _expected_sdist_repository_files()
    expected_root = f"makolet-{_project_version()}"
    expected_files = set(repository_files) | {"PKG-INFO"}
    allowed_directories = _allowed_sdist_directories(expected_files)
    member_payloads: dict[str, bytes] = {}
    with tarfile.open(path, "r:gz") as archive:
        members = archive.getmembers()
        if not 0 < len(members) <= _MAXIMUM_MEMBERS:
            raise ValueError("sdist member count is empty or exceeds the limit")
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise ValueError("sdist contains duplicate member paths")
        roots: set[str] = set()
        actual_directories: set[str] = set()
        total_size = 0
        for member in members:
            _safe_member_name(member.name)
            if not (member.isfile() or member.isdir()):
                raise ValueError(f"sdist contains a link or special member: {member.name}")
            if member.size > _MAXIMUM_MEMBER_BYTES:
                raise ValueError(f"sdist member exceeds the size limit: {member.name}")
            total_size += member.size
            if total_size > _MAXIMUM_EXPANDED_BYTES:
                raise ValueError("sdist expanded payload exceeds the size limit")
            parts = PurePosixPath(member.name).parts
            roots.add(parts[0])
            relative_name = "/".join(parts[1:])
            if not relative_name:
                if not member.isdir():
                    raise ValueError("sdist top-level member must be a directory")
                continue
            if _forbidden_member(relative_name):
                raise ValueError(f"sdist contains forbidden member: {relative_name}")
            if member.isdir():
                actual_directories.add(relative_name)
                continue
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError(f"sdist member is not a regular file: {relative_name}")
            member_payloads[relative_name] = extracted.read()
        if roots != {expected_root}:
            raise ValueError(f"sdist top-level directory differs: {sorted(roots)}")
    _require_exact_file_inventory(set(member_payloads), expected_files, label="sdist")
    unexpected_directories = sorted(actual_directories - allowed_directories)
    if unexpected_directories:
        raise ValueError(f"sdist contains unexpected directories: {unexpected_directories}")
    for member_name, source_path in repository_files.items():
        _require_exact_bytes(
            member_payloads[member_name], source_path, f"sdist member {member_name}"
        )
    _validate_project_metadata(
        BytesParser(policy=policy.default).parsebytes(member_payloads["PKG-INFO"]),
        label="sdist PKG-INFO",
    )
    for member_name, payload in member_payloads.items():
        _scan_distribution_text(member_name, payload)
    return len(members)


def _validate_output(output: Path) -> tuple[Path, Path, int, int]:
    wheel = _artifact(output, "makolet-*.whl", "wheel")
    sdist = _artifact(output, "makolet-*.tar.gz", "sdist")
    return wheel, sdist, _validate_wheel(wheel), _validate_sdist(sdist)


def _controlled_environment(source: Mapping[str, str] | None = None) -> dict[str, str]:
    environment = dict(environ if source is None else source)
    cache_directory = environment.get("UV_CACHE_DIR")
    for key in tuple(environment):
        if key.startswith(("HATCH_", "PIP_", "UV_")) or key in {
            "PYTHONHASHSEED",
            "PYTHONHOME",
            "PYTHONPATH",
            "SOURCE_DATE_EPOCH",
            "VIRTUAL_ENV",
        }:
            environment.pop(key)
    environment.update(
        {
            "FORCE_COLOR": "0",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "NO_COLOR": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONHASHSEED": "0",
            "SOURCE_DATE_EPOCH": _SOURCE_DATE_EPOCH,
            "TZ": "UTC",
            "UV_FROZEN": "1",
            "UV_NO_PROGRESS": "1",
            "UV_OFFLINE": "1",
            "UV_PYTHON_DOWNLOADS": "never",
        }
    )
    if cache_directory:
        environment["UV_CACHE_DIR"] = str(Path(cache_directory).expanduser().resolve())
    return environment


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    label: str,
) -> str:
    with tempfile.TemporaryFile() as output:
        try:
            completed = subprocess.run(  # noqa: S603 - every executable and argument is local.
                command,
                cwd=cwd,
                env=environment,
                stdout=output,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            raise ValueError(f"{label} exceeded {_COMMAND_TIMEOUT_SECONDS} seconds") from error
        output_size = output.tell()
        if output_size > _MAXIMUM_COMMAND_OUTPUT_BYTES:
            raise ValueError(f"{label} output exceeds the size limit")
        output.seek(0)
        rendered = output.read().decode("utf-8", errors="replace")
    if completed.returncode != 0:
        detail = rendered.strip()
        suffix = f": {detail}" if detail else ""
        raise ValueError(f"{label} failed with exit code {completed.returncode}{suffix}")
    return rendered


def _uv_build_command(uv: str, output: Path, source: Path, *kinds: str) -> list[str]:
    return [
        uv,
        "build",
        *kinds,
        "--no-sources",
        "--offline",
        "--no-progress",
        "--color",
        "never",
        "--no-python-downloads",
        "--python",
        sys.executable,
        "--build-constraints",
        str(_BUILD_CONSTRAINTS),
        "--require-hashes",
        "--no-create-gitignore",
        "--out-dir",
        str(output),
        str(source),
    ]


def _uv_runtime_export_command(uv: str, output: Path) -> list[str]:
    return [
        uv,
        "export",
        "--frozen",
        "--no-dev",
        "--extra",
        "benchmark",
        "--no-emit-project",
        "--format",
        "requirements-txt",
        "--no-header",
        "--no-annotate",
        "--quiet",
        "--output-file",
        str(output),
    ]


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_identical_artifact(first: Path, second: Path, label: str) -> str:
    if first.name != second.name:
        raise ValueError(f"{label} filenames differ between reproducibility builds")
    first_digest = _file_sha256(first)
    second_digest = _file_sha256(second)
    if first.stat().st_size != second.stat().st_size or first_digest != second_digest:
        raise ValueError(
            f"{label} is not reproducible; first_sha256={first_digest}; "
            f"second_sha256={second_digest}"
        )
    with first.open("rb") as first_handle, second.open("rb") as second_handle:
        while first_chunk := first_handle.read(1024 * 1024):
            if first_chunk != second_handle.read(len(first_chunk)):
                raise ValueError(f"{label} differs despite matching artifact metadata")
        if second_handle.read(1):
            raise ValueError(f"{label} differs despite matching artifact metadata")
    return first_digest


def _virtual_environment_python(environment: Path) -> Path:
    scripts = "Scripts" if operating_system_name == "nt" else "bin"
    executable = "python.exe" if operating_system_name == "nt" else "python"
    return environment / scripts / executable


def _virtual_environment_cli(environment: Path) -> Path:
    scripts = "Scripts" if operating_system_name == "nt" else "bin"
    executable = "makolet.exe" if operating_system_name == "nt" else "makolet"
    return environment / scripts / executable


def _postgres_proof_target(
    source: Mapping[str, str] | None = None,
) -> _PostgresProofTarget:
    environment = environ if source is None else source
    raw_url = environment.get(_POSTGRES_URL_VARIABLE, "")
    confirmation = environment.get(_POSTGRES_CONFIRM_VARIABLE, "")
    try:
        parsed = urlsplit(raw_url)
        host = parsed.hostname or ""
        port = parsed.port
        address = ipaddress.ip_address(host)
    except ValueError as error:
        raise ValueError("distribution PostgreSQL proof target is invalid") from error
    database_name = parsed.path.removeprefix("/")
    if (
        parsed.scheme not in {"postgresql", "postgresql+asyncpg"}
        or not parsed.username
        or str(address) not in {"127.0.0.1", "::1"}
        or parsed.query
        or parsed.fragment
        or parsed.path != f"/{database_name}"
        or _DISTRIBUTION_TEST_DATABASE.fullmatch(database_name) is None
        or confirmation != database_name
        or (port is not None and not 1 <= port <= 65_535)
    ):
        raise ValueError(
            "distribution PostgreSQL proof requires an exact confirmed, uniquely named, "
            "literal-loopback test database without query or fragment overrides"
        )
    return _PostgresProofTarget(url=raw_url, database_name=database_name)


def _validate_postgres_status(payload: Mapping[str, object], *, expect_empty: bool) -> None:
    version_number = payload.get("postgresql_version_number")
    if not isinstance(version_number, int) or not 180_000 <= version_number < 190_000:
        raise ValueError("installed distribution proof requires PostgreSQL 18")
    expected_heads = payload.get("expected_migration_heads")
    if expected_heads != [_EXPECTED_MIGRATION_HEAD]:
        raise ValueError("installed distribution expected migration head differs")
    if expect_empty:
        if (
            payload.get("status") != "not_ready"
            or payload.get("schema_ready") is not False
            or payload.get("current_migration_revisions") != []
            or payload.get("migration_revision") is not None
        ):
            raise ValueError("installed distribution database was not empty before migration")
        return
    if (
        payload.get("status") != "ready"
        or payload.get("schema_ready") is not True
        or payload.get("current_migration_revisions") != [_EXPECTED_MIGRATION_HEAD]
        or payload.get("migration_revision") != _EXPECTED_MIGRATION_HEAD
    ):
        raise ValueError("installed distribution did not reach the exact migration head")


def _json_object_output(rendered: str, *, label: str) -> dict[str, object]:
    for line in reversed(rendered.splitlines()):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return cast(dict[str, object], value)
    raise ValueError(f"{label} did not emit a bounded JSON object")


def _installed_runtime_environment(
    source: Mapping[str, str],
    *,
    target: _PostgresProofTarget,
    runtime_directory: Path,
) -> dict[str, str]:
    environment = {
        key: value for key, value in source.items() if not key.casefold().startswith("makolet_")
    }
    environment.update(
        {
            "MAKOLET_ARCHIVE_BACKEND": "local",
            "MAKOLET_ARCHIVE_ROOT": str(runtime_directory / "raw-archive"),
            "MAKOLET_DATABASE_ALLOW_INSECURE_LOCAL": "true",
            "MAKOLET_DATABASE_URL": target.url,
            "MAKOLET_ENABLED_SOURCES": "[]",
            "MAKOLET_ENVIRONMENT": "test",
            "MAKOLET_EXPORT_ROOT": str(runtime_directory / "exports"),
            "MAKOLET_LOG_LEVEL": "ERROR",
        }
    )
    return environment


async def _require_empty_postgres_database(target: _PostgresProofTarget) -> None:
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    url = make_url(target.url).set(drivername="postgresql+asyncpg")
    engine = create_async_engine(url, pool_pre_ping=True)
    try:
        async with engine.connect() as connection:
            version_number = int(
                (await connection.execute(text("SHOW server_version_num"))).scalar_one()
            )
            relation_count = int(
                (
                    await connection.execute(
                        text(
                            "SELECT count(*) FROM pg_class AS c "
                            "JOIN pg_namespace AS n ON n.oid = c.relnamespace "
                            "WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') "
                            "AND n.nspname NOT LIKE 'pg_toast%' "
                            "AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')"
                        )
                    )
                ).scalar_one()
            )
            schema_count = int(
                (
                    await connection.execute(
                        text(
                            "SELECT count(*) FROM pg_namespace "
                            "WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'public') "
                            "AND nspname NOT LIKE 'pg_toast%' "
                            "AND nspname NOT LIKE 'pg_temp_%'"
                        )
                    )
                ).scalar_one()
            )
            extension_names = {
                str(value)
                for value in (
                    await connection.execute(text("SELECT extname FROM pg_extension"))
                ).scalars()
            }
    finally:
        await engine.dispose()
    if not 180_000 <= version_number < 190_000:
        raise ValueError("installed distribution proof requires PostgreSQL 18")
    if relation_count or schema_count or not extension_names.issubset({"plpgsql"}):
        raise ValueError("installed distribution PostgreSQL target is not an empty database")


def _verify_installed_postgres(runtime_directory: Path) -> None:
    from makolet.adapters.persistence.destructive_target import require_test_database_target

    target = _postgres_proof_target()
    if require_test_database_target(target.url, confirmation=target.database_name) != target.url:
        raise ValueError("installed distribution database target validation changed")
    try:
        asyncio.run(_require_empty_postgres_database(target))
    except Exception as error:
        raise ValueError(
            "installed distribution PostgreSQL empty-target preflight failed"
        ) from error
    cli = _virtual_environment_cli(Path(sys.prefix))
    database_environment = _installed_runtime_environment(
        environ,
        target=target,
        runtime_directory=runtime_directory,
    )
    pre_status = _json_object_output(
        _run_command(
            [str(cli), "database", "status", "--json"],
            cwd=runtime_directory,
            environment=database_environment,
            label="installed empty-database status",
        ),
        label="installed empty-database status",
    )
    _validate_postgres_status(pre_status, expect_empty=True)
    migration = _json_object_output(
        _run_command(
            [str(cli), "database", "migrate", "--json"],
            cwd=runtime_directory,
            environment=database_environment,
            label="installed empty-database migration",
        ),
        label="installed empty-database migration",
    )
    _validate_postgres_status(migration, expect_empty=False)
    status = _json_object_output(
        _run_command(
            [str(cli), "database", "status", "--json"],
            cwd=runtime_directory,
            environment=database_environment,
            label="installed migrated-database status",
        ),
        label="installed migrated-database status",
    )
    _validate_postgres_status(status, expect_empty=False)


def _verify_installed(*, verify_postgres: bool = False) -> int:
    import psutil  # type: ignore[import-untyped]
    from alembic.config import Config as AlembicConfig
    from alembic.script import ScriptDirectory
    from typer.testing import CliRunner

    import benchmarks
    import makolet
    from benchmarks.run import PROFILES
    from makolet.composition import _alembic_configuration_path
    from makolet.interfaces.cli import build_cli

    package_file = makolet.__file__
    if package_file is None:
        raise ValueError("installed Makolet package has no filesystem location")
    package_root = Path(package_file).resolve().parent
    environment_root = Path(sys.prefix).resolve()
    if not package_root.is_relative_to(environment_root):
        raise ValueError(
            "Makolet import did not resolve from the isolated verification environment"
        )

    expected_migrations = _expected_migrations("makolet/_migrations")
    installed_migrations: set[str] = set()
    migration_root = package_root / "_migrations"
    for path in migration_root.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts:
            installed_migrations.add(
                f"makolet/_migrations/{path.relative_to(migration_root).as_posix()}"
            )
    if installed_migrations != set(expected_migrations):
        raise ValueError("installed migration inventory differs from the repository")
    for migration_name, source_path in expected_migrations.items():
        installed_path = package_root.parent.joinpath(*PurePosixPath(migration_name).parts)
        _require_exact_bytes(
            installed_path.read_bytes(), source_path, f"installed member {migration_name}"
        )

    configuration_path = _alembic_configuration_path().resolve()
    expected_configuration_path = (package_root / "_alembic.ini").resolve()
    if configuration_path != expected_configuration_path:
        raise ValueError("installed Makolet did not select its packaged Alembic configuration")
    installed_heads = tuple(
        sorted(ScriptDirectory.from_config(AlembicConfig(str(configuration_path))).get_heads())
    )
    repository_heads = tuple(
        sorted(ScriptDirectory.from_config(AlembicConfig(str(_ROOT / "alembic.ini"))).get_heads())
    )
    if installed_heads != (_EXPECTED_MIGRATION_HEAD,) or installed_heads != repository_heads:
        raise ValueError("installed Alembic graph does not resolve to the repository heads")

    benchmark_file = benchmarks.__file__
    if benchmark_file is None or not Path(benchmark_file).resolve().is_relative_to(
        environment_root
    ):
        raise ValueError("installed benchmark package resolved outside the environment")
    if tuple(sorted(PROFILES)) != ("quick", "smoke", "standard"):
        raise ValueError("installed benchmark profiles are incomplete")
    if metadata.version("psutil") != "7.2.2" or not Path(psutil.__file__).resolve().is_relative_to(
        environment_root
    ):
        raise ValueError("installed benchmark dependency is missing or differs")

    distribution = metadata.distribution("makolet")
    if distribution.version != _project_version():
        raise ValueError("installed Makolet version differs")
    installed_metadata_text = distribution.read_text("METADATA")
    if installed_metadata_text is None:
        raise ValueError("installed distribution is missing METADATA")
    _validate_project_metadata(
        BytesParser(policy=policy.default).parsebytes(installed_metadata_text.encode()),
        label="installed METADATA",
    )
    installed_entry_points = [
        (entry_point.group, entry_point.name, entry_point.value)
        for entry_point in distribution.entry_points
    ]
    if installed_entry_points != [("console_scripts", "makolet", _CONSOLE_ENTRY_POINT)]:
        raise ValueError("installed console entry point differs from pyproject")
    distribution_files = tuple(distribution.files or ())
    for evidence_name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
        candidates = [
            path
            for path in distribution_files
            if PurePosixPath(str(path)).parts[-2:] == ("licenses", evidence_name)
        ]
        if len(candidates) != 1:
            raise ValueError(f"installed distribution is missing {evidence_name}")
        installed_evidence_path = Path(str(distribution.locate_file(candidates[0]))).resolve()
        if not installed_evidence_path.is_relative_to(environment_root):
            raise ValueError(f"installed {evidence_name} resolved outside the environment")
        _require_exact_bytes(
            installed_evidence_path.read_bytes(),
            _ROOT / evidence_name,
            f"installed {evidence_name}",
        )
    runner = CliRunner()
    application = build_cli()
    registered_cli_paths = _registered_cli_help_paths(application)
    if len(registered_cli_paths) != len(ADVERTISED_CLI_HELP_PATHS) or set(
        registered_cli_paths
    ) != set(ADVERTISED_CLI_HELP_PATHS):
        raise ValueError("installed CLI command tree differs from the advertised help paths")
    for command_path in ADVERTISED_CLI_HELP_PATHS:
        result = runner.invoke(application, [*command_path, "--help"])
        if result.exit_code != 0 or "Usage:" not in result.output:
            rendered_path = "makolet " + " ".join(command_path)
            raise ValueError(f"installed CLI help failed for {rendered_path.strip()}")
    if verify_postgres:
        _verify_installed_postgres(Path.cwd())
    sys.stdout.write(
        "verified installed Makolet package, exact metadata, migration graph, "
        f"{len(ADVERTISED_CLI_HELP_PATHS)} CLI help paths, benchmark extra, license evidence"
        f"{' and PostgreSQL 18 migration/status' if verify_postgres else ''}\n"
    )
    return 0


def _verify_reproducible_distribution(
    *,
    verify_installed_postgres: bool,
) -> tuple[str, str, int, int]:
    uv = shutil.which("uv")
    if uv is None:
        raise ValueError("uv executable is unavailable")
    environment = _controlled_environment()
    if verify_installed_postgres:
        _postgres_proof_target(environment)
    with tempfile.TemporaryDirectory(prefix="makolet-distribution-") as raw_temporary_root:
        temporary_root = Path(raw_temporary_root).resolve()
        first_output = temporary_root / "first"
        second_output = temporary_root / "second"
        sdist_output = temporary_root / "from-sdist"
        virtual_environment = temporary_root / "environment"
        runtime_directory = temporary_root / "runtime"
        runtime_requirements = temporary_root / "runtime-requirements.txt"
        for directory in (first_output, second_output, sdist_output, runtime_directory):
            directory.mkdir()

        for label, output in (
            ("first distribution build", first_output),
            ("second distribution build", second_output),
        ):
            _run_command(
                _uv_build_command(uv, output, _ROOT, "--sdist", "--wheel"),
                cwd=_ROOT,
                environment=environment,
                label=label,
            )

        first_wheel, first_sdist, wheel_members, sdist_members = _validate_output(first_output)
        second_wheel, second_sdist, _, _ = _validate_output(second_output)
        wheel_digest = _require_identical_artifact(first_wheel, second_wheel, "wheel")
        sdist_digest = _require_identical_artifact(first_sdist, second_sdist, "sdist")

        _run_command(
            _uv_build_command(uv, sdist_output, first_sdist, "--wheel"),
            cwd=runtime_directory,
            environment=environment,
            label="sdist wheel build",
        )
        sdist_wheel = _artifact(sdist_output, "makolet-*.whl", "sdist-built wheel")
        _validate_wheel(sdist_wheel)
        _require_identical_artifact(first_wheel, sdist_wheel, "sdist-built wheel")

        _run_command(
            [
                uv,
                "venv",
                "--no-project",
                "--offline",
                "--no-progress",
                "--color",
                "never",
                "--no-python-downloads",
                "--python",
                sys.executable,
                str(virtual_environment),
            ],
            cwd=runtime_directory,
            environment=environment,
            label="verification environment creation",
        )
        environment_python = _virtual_environment_python(virtual_environment)
        _run_command(
            _uv_runtime_export_command(uv, runtime_requirements),
            cwd=_ROOT,
            environment=environment,
            label="frozen runtime and benchmark dependency export",
        )
        _run_command(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(environment_python),
                "--offline",
                "--no-progress",
                "--color",
                "never",
                "--no-build",
                "--strict",
                "--require-hashes",
                "--requirement",
                str(runtime_requirements),
            ],
            cwd=runtime_directory,
            environment=environment,
            label="frozen runtime and benchmark dependency installation",
        )
        _run_command(
            [
                uv,
                "pip",
                "install",
                "--python",
                str(environment_python),
                "--offline",
                "--no-progress",
                "--color",
                "never",
                "--no-build",
                "--strict",
                "--no-deps",
                "--reinstall-package",
                "makolet",
                str(sdist_wheel),
            ],
            cwd=runtime_directory,
            environment=environment,
            label="sdist-built wheel installation",
        )
        installed_verification_command = [
            str(environment_python),
            "-I",
            str(Path(__file__).resolve()),
            _INSTALLED_ARGUMENT,
        ]
        if verify_installed_postgres:
            installed_verification_command.append(_INSTALLED_POSTGRES_ARGUMENT)
        _run_command(
            installed_verification_command,
            cwd=runtime_directory,
            environment=environment,
            label="installed distribution verification",
        )
        cli_output = _run_command(
            [str(_virtual_environment_cli(virtual_environment)), "--help"],
            cwd=runtime_directory,
            environment=environment,
            label="installed CLI help",
        )
        if not all(marker in cli_output for marker in ("Usage:", "database", "sources", "ingest")):
            raise ValueError("installed CLI help is incomplete")
        benchmark_output = runtime_directory / "benchmark-smoke.json"
        _run_command(
            [
                str(_virtual_environment_cli(virtual_environment)),
                "benchmark",
                "run",
                "--quick",
                "--scenario",
                "parser",
                "--output",
                str(benchmark_output),
            ],
            cwd=runtime_directory,
            environment=environment,
            label="installed benchmark parser smoke",
        )
        if not benchmark_output.is_file() or benchmark_output.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("installed benchmark parser smoke did not create a bounded result")
        benchmark_result = json.loads(benchmark_output.read_text(encoding="utf-8"))
        if (
            benchmark_result.get("profile") != "quick"
            or benchmark_result.get("scenario") != "parser"
            or benchmark_result.get("acceptance_evidence") is not False
            or not isinstance(benchmark_result.get("parser"), dict)
            or "database" in benchmark_result
        ):
            raise ValueError("installed benchmark parser smoke result is incomplete")
    return wheel_digest, sdist_digest, wheel_members, sdist_members


def main(arguments: list[str] | None = None) -> int:
    values = sys.argv[1:] if arguments is None else arguments
    if values in (
        [_INSTALLED_ARGUMENT],
        [_INSTALLED_ARGUMENT, _INSTALLED_POSTGRES_ARGUMENT],
    ):
        try:
            return _verify_installed(verify_postgres=_INSTALLED_POSTGRES_ARGUMENT in values)
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            sys.stderr.write(f"distribution check error: {error}\n")
            return 1
    if values in (
        [_REPRODUCIBILITY_ARGUMENT],
        [_REPRODUCIBILITY_ARGUMENT, _INSTALLED_POSTGRES_ARGUMENT],
    ):
        verify_installed_postgres = _INSTALLED_POSTGRES_ARGUMENT in values
        try:
            wheel_digest, sdist_digest, wheel_members, sdist_members = (
                _verify_reproducible_distribution(
                    verify_installed_postgres=verify_installed_postgres
                )
            )
        except (
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            tarfile.TarError,
            zipfile.BadZipFile,
        ) as error:
            sys.stderr.write(f"distribution check error: {error}\n")
            return 1
        sys.stdout.write(
            "verified reproducible Makolet distributions and isolated sdist installation; "
            f"postgresql_proof={'passed' if verify_installed_postgres else 'not-requested'}; "
            f"wheel_sha256={wheel_digest} ({wheel_members} members); "
            f"sdist_sha256={sdist_digest} ({sdist_members} members)\n"
        )
        return 0
    if len(values) != 1:
        sys.stderr.write(
            "usage: check_distribution.py BUILD_OUTPUT_DIRECTORY | --reproducible "
            "[--verify-installed-postgres]\n"
        )
        return 2
    try:
        output = _build_output(values[0])
        _, _, wheel_members, sdist_members = _validate_output(output)
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        sys.stderr.write(f"distribution check error: {error}\n")
        return 1
    sys.stdout.write(
        f"verified Makolet wheel ({wheel_members} members) and sdist ({sdist_members} members)\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
