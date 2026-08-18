"""Validate immutable pins and reviewed licenses for every third-party image."""

from __future__ import annotations

import json
import re
import shlex
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_LOCK = _ROOT / "deployment" / "container-images.lock"
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_REFERENCE = re.compile(r"[^\s@]+:[^\s@]+\Z")
_PINNED_IMAGE = re.compile(r"(?P<reference>[^\s@]+:[^\s@]+)@(?P<digest>sha256:[0-9a-f]{64})\Z")
_DOCKERFILE_DIRECTIVE = re.compile(
    r"^\s*#\s*(?P<name>syntax|escape)\s*=\s*(?P<value>\S+)\s*$",
    re.IGNORECASE,
)
_DOCKERFILE_ARGUMENT = re.compile(
    r"^\s*ARG\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?:=(?P<value>\S+))?\s*$",
    re.IGNORECASE,
)
_DOCKERFILE_FROM = re.compile(
    r"^\s*FROM(?:\s+--platform=\S+)?\s+(?P<image>\S+)"
    r"(?:\s+AS\s+(?P<alias>[A-Za-z0-9_.-]+))?\s*$",
    re.IGNORECASE,
)
_DOCKERFILE_VARIABLE = re.compile(r"^\$\{?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\}?$")
_COMPOSE_IMAGE = re.compile(r"^(?P<indent> +)image:\s*(?P<value>.+?)\s*$")
_COMPOSE_KEY = re.compile(r"^(?P<indent> +)(?P<key>[A-Za-z_][A-Za-z0-9_-]*):(?P<value>.*?)\s*$")
_NONCANONICAL_IMAGE_KEY = re.compile(r"(?:^|[{,]\s*)[\"']?image[\"']?\s*:", re.IGNORECASE)
_QUOTED_COMPOSE_KEY = re.compile(r"^\s*[\"'][^\"']+[\"']\s*:")
_LOCAL_COMPOSE_IMAGE = "makolet:local"
_DEFAULT_COMPOSE_OVERRIDES = (
    "compose.override.yaml",
    "compose.override.yml",
    "docker-compose.override.yaml",
    "docker-compose.override.yml",
)
_APPROVED_LICENSES = frozenset({"Apache-2.0", "MIT OR Apache-2.0", "PSF-2.0", "PostgreSQL"})


def _one_scalar(value: str, *, location: str) -> str:
    try:
        tokens = shlex.split(value, comments=True, posix=True)
    except ValueError as error:
        raise ValueError(f"{location} contains an invalid image value") from error
    if len(tokens) != 1:
        raise ValueError(f"{location} must contain exactly one image value")
    return tokens[0]


def _require_pinned_image(value: str, *, location: str) -> str:
    match = _PINNED_IMAGE.fullmatch(value)
    if match is None:
        raise ValueError(f"{location} image is not pinned by SHA-256 digest: {value!r}")
    reference = match.group("reference")
    if _REFERENCE.fullmatch(reference) is None:
        raise ValueError(f"{location} image reference is invalid: {reference!r}")
    return value


def _dockerfile_images(document: str) -> set[str]:
    arguments: dict[str, str] = {}
    stage_aliases: set[str] = set()
    images: set[str] = set()
    raw_lines = document.splitlines()
    if raw_lines and raw_lines[0].startswith("\ufeff"):
        raw_lines[0] = raw_lines[0].removeprefix("\ufeff")
    escape_character = "\\"
    seen_directives: set[str] = set()
    for line_number, line in enumerate(raw_lines, start=1):
        directive = _DOCKERFILE_DIRECTIVE.fullmatch(line)
        if directive is None:
            continue
        name = directive.group("name").casefold()
        value = directive.group("value")
        if name in seen_directives:
            raise ValueError(f"Dockerfile:{line_number} repeats parser directive {name!r}")
        seen_directives.add(name)
        if name == "syntax":
            images.add(
                _require_pinned_image(
                    value,
                    location=f"Dockerfile:{line_number} syntax frontend",
                )
            )
        elif value not in {"\\", "`"}:
            raise ValueError(f"Dockerfile:{line_number} has an invalid escape directive")
        else:
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

    for line_number, line in logical_lines:
        if argument_match := _DOCKERFILE_ARGUMENT.fullmatch(line):
            value = argument_match.group("value")
            if value is not None:
                arguments[argument_match.group("name")] = value
            continue
        from_match = _DOCKERFILE_FROM.fullmatch(line)
        if from_match is None:
            continue
        image = from_match.group("image")
        if variable_match := _DOCKERFILE_VARIABLE.fullmatch(image):
            name = variable_match.group("name")
            try:
                image = arguments[name]
            except KeyError as error:
                raise ValueError(
                    f"Dockerfile:{line_number} FROM uses unresolved argument {name!r}"
                ) from error
        if image.casefold() != "scratch" and image.casefold() not in stage_aliases:
            images.add(_require_pinned_image(image, location=f"Dockerfile:{line_number} FROM"))
        alias = from_match.group("alias")
        if alias is not None:
            stage_aliases.add(alias.casefold())
    for line_number, line in logical_lines:
        try:
            tokens = shlex.split(line, comments=True, posix=True)
        except ValueError as error:
            raise ValueError(f"Dockerfile:{line_number} contains invalid syntax") from error
        if not tokens:
            continue
        for token in tokens[1:]:
            candidates: list[str] = []
            if token.casefold().startswith("--from="):
                candidates.append(token.split("=", 1)[1])
            if token.casefold().startswith("--mount="):
                mount = token.split("=", 1)[1]
                for component in mount.split(","):
                    key, separator, value = component.partition("=")
                    if separator and key.casefold() == "from":
                        candidates.append(value)
            for candidate in candidates:
                image = candidate
                if variable_match := _DOCKERFILE_VARIABLE.fullmatch(image):
                    name = variable_match.group("name")
                    try:
                        image = arguments[name]
                    except KeyError as error:
                        raise ValueError(
                            f"Dockerfile:{line_number} uses unresolved source argument {name!r}"
                        ) from error
                if image.casefold() in stage_aliases:
                    continue
                images.add(
                    _require_pinned_image(
                        image,
                        location=f"Dockerfile:{line_number} external build source",
                    )
                )
    if not images:
        raise ValueError("Dockerfile contains no external image references")
    return images


def _compose_block(lines: list[str], line_index: int, indent: int) -> list[str]:
    start = line_index - 1
    while start >= 0:
        candidate = lines[start]
        if candidate.strip() and len(candidate) - len(candidate.lstrip(" ")) < indent:
            break
        start -= 1
    end = line_index + 1
    while end < len(lines):
        candidate = lines[end]
        if candidate.strip() and len(candidate) - len(candidate.lstrip(" ")) < indent:
            break
        end += 1
    return lines[start + 1 : end]


def _compose_children(lines: list[str], line_index: int, indent: int) -> list[str]:
    children: list[str] = []
    for candidate in lines[line_index + 1 :]:
        if not candidate.strip():
            children.append(candidate)
            continue
        candidate_indent = len(candidate) - len(candidate.lstrip(" "))
        if candidate_indent <= indent:
            break
        children.append(candidate)
    return children


def _compose_local_path(value: str, *, location: str) -> Path:
    scalar = _one_scalar(value, location=location)
    path = Path(scalar)
    if (
        not scalar
        or "$" in scalar
        or scalar.startswith("~")
        or "\\" in scalar
        or ":" in scalar
        or path.is_absolute()
        or ".." in path.parts
    ):
        raise ValueError(f"{location} must be a repository-relative local path")
    return path


def _compose_build(lines: list[str], line_index: int, indent: int, value: str) -> tuple[Path, Path]:
    if value:
        if value.startswith("{"):
            raise ValueError(
                f"compose.yaml:{line_index + 1} flow-style build mappings are unsupported"
            )
        return (
            _compose_local_path(
                value,
                location=f"compose.yaml:{line_index + 1} build context",
            ),
            Path("Dockerfile"),
        )
    child_values: dict[str, str] = {}
    child_indent: int | None = None
    for child in _compose_children(lines, line_index, indent):
        if not child.strip():
            continue
        match = _COMPOSE_KEY.fullmatch(child)
        if match is None:
            raise ValueError(f"compose.yaml:{line_index + 1} build mapping uses unsupported syntax")
        current_indent = len(match.group("indent"))
        if child_indent is None:
            child_indent = current_indent
        if current_indent != child_indent:
            continue
        key = match.group("key").casefold()
        if key in child_values:
            raise ValueError(f"compose.yaml:{line_index + 1} build mapping repeats {key!r}")
        child_values[key] = match.group("value").strip()
    context = _compose_local_path(
        child_values.get("context", "."),
        location=f"compose.yaml:{line_index + 1} build context",
    )
    dockerfile = _compose_local_path(
        child_values.get("dockerfile", "Dockerfile"),
        location=f"compose.yaml:{line_index + 1} dockerfile",
    )
    return context, dockerfile


def _compose_configuration(document: str) -> tuple[set[str], set[tuple[Path, Path]]]:
    lines = document.splitlines()
    images: set[str] = set()
    builds: set[tuple[Path, Path]] = set()
    for line_number, line in enumerate(lines, start=1):
        if "\t" in line:
            raise ValueError(f"compose.yaml:{line_number} contains a tab")
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(("!", "&", "?", ":", "*", "%", "[", "{")):
            raise ValueError(
                f"compose.yaml:{line_number} uses unsupported tagged or explicit-key syntax"
            )
        if stripped in {"---", "..."} or _QUOTED_COMPOSE_KEY.match(line):
            raise ValueError(
                f"compose.yaml:{line_number} uses unsupported document or quoted-key syntax"
            )
        key_match = _COMPOSE_KEY.fullmatch(line)
        if key_match is not None:
            key = key_match.group("key").casefold()
            value = key_match.group("value").strip()
            if ("{" in value and not value.startswith("${")) or value in {"|", ">"}:
                raise ValueError(f"compose.yaml:{line_number} flow-style mappings are unsupported")
            if key in {
                "additional_contexts",
                "args",
                "cache_from",
                "cache_to",
                "dockerfile_inline",
                "extends",
                "include",
            }:
                raise ValueError(
                    f"compose.yaml:{line_number} uses unsupported image-bearing key {key!r}"
                )
            if key == "build":
                builds.add(
                    _compose_build(
                        lines,
                        line_number - 1,
                        len(key_match.group("indent")),
                        value,
                    )
                )
        match = _COMPOSE_IMAGE.fullmatch(line)
        if match is None:
            if _NONCANONICAL_IMAGE_KEY.search(stripped):
                raise ValueError(
                    f"compose.yaml:{line_number} image keys must use canonical block syntax"
                )
            continue
        image = _one_scalar(
            match.group("value"),
            location=f"compose.yaml:{line_number}",
        )
        if "$" in image:
            raise ValueError(f"compose.yaml:{line_number} image cannot use variable expansion")
        if image == _LOCAL_COMPOSE_IMAGE:
            image_indent = len(match.group("indent"))
            block = _compose_block(lines, line_number - 1, image_indent)
            sibling_values: dict[str, str] = {}
            for candidate in block:
                candidate_match = _COMPOSE_KEY.fullmatch(candidate)
                if (
                    candidate_match is not None
                    and len(candidate_match.group("indent")) == image_indent
                ):
                    sibling_values[candidate_match.group("key").casefold()] = candidate_match.group(
                        "value"
                    ).strip()
            if "build" not in sibling_values:
                raise ValueError(
                    f"compose.yaml:{line_number} local image exception requires a sibling build"
                )
            if sibling_values.get("pull_policy") != "build":
                raise ValueError(
                    f"compose.yaml:{line_number} local image requires pull_policy: build"
                )
            continue
        images.add(_require_pinned_image(image, location=f"compose.yaml:{line_number}"))
    if not images:
        raise ValueError("compose.yaml contains no external image references")
    if not builds:
        raise ValueError("compose.yaml contains no explicit application build")
    return images, builds


def _configured_images(root: Path, compose: str) -> set[str]:
    images, builds = _compose_configuration(compose)
    resolved_root = root.resolve(strict=False)
    for context_path, dockerfile_path in builds:
        context = (resolved_root / context_path).resolve(strict=False)
        resolved = (context / dockerfile_path).resolve(strict=False)
        if (
            not context.is_relative_to(resolved_root)
            or not context.is_dir()
            or not resolved.is_relative_to(context)
            or not resolved.is_file()
        ):
            raise ValueError(
                "configured build context or Dockerfile is missing or unsafe: "
                f"{context_path}/{dockerfile_path}"
            )
        images.update(_dockerfile_images(resolved.read_text(encoding="utf-8")))
    return images


def _require_no_default_overrides(root: Path) -> None:
    for override_name in _DEFAULT_COMPOSE_OVERRIDES:
        if (root / override_name).exists():
            raise ValueError(f"default Compose override is forbidden: {override_name}")


def main() -> int:
    try:
        _require_no_default_overrides(_ROOT)
        document = json.loads(_LOCK.read_bytes())
        compose = (_ROOT / "compose.yaml").read_text(encoding="utf-8")
        configured = _configured_images(_ROOT, compose)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
        sys.stderr.write(f"container image lock error: {error}\n")
        return 1
    if not isinstance(document, dict):
        sys.stderr.write("container image lock error: document must be an object\n")
        return 1
    images = document.get("images")
    if document.get("schema_version") != 1 or not isinstance(images, list) or not images:
        sys.stderr.write("container image lock error: unsupported or empty inventory\n")
        return 1
    locked: set[str] = set()
    seen_references: set[str] = set()
    for image in images:
        if not isinstance(image, dict):
            sys.stderr.write("container image lock error: invalid row\n")
            return 1
        reference = image.get("reference")
        digest = image.get("digest")
        license_name = image.get("license")
        source = image.get("source")
        if (
            not isinstance(reference, str)
            or _REFERENCE.fullmatch(reference) is None
            or reference in seen_references
            or not isinstance(digest, str)
            or _DIGEST.fullmatch(digest) is None
            or license_name not in _APPROVED_LICENSES
            or not isinstance(source, str)
            or not source.startswith("https://github.com/")
        ):
            sys.stderr.write(f"container image lock error: invalid metadata for {reference!r}\n")
            return 1
        seen_references.add(reference)
        locked.add(f"{reference}@{digest}")
    forbidden = ("minio", "redis", "kafka")
    if any(token in image.casefold() for image in configured for token in forbidden):
        sys.stderr.write(
            "container configuration contains a forbidden or floating image reference\n"
        )
        return 1
    if unlisted := sorted(configured - locked):
        sys.stderr.write(f"container image is not present in the reviewed lock: {unlisted[0]}\n")
        return 1
    if unused := sorted(locked - configured):
        sys.stderr.write(f"container image pin is unused or differs: {unused[0]}\n")
        return 1
    sys.stdout.write(f"verified {len(locked)} exact immutable, OSI-licensed container image pins\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
