"""Validate repository-local Markdown links without network access."""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlsplit

_ROOT = Path(__file__).resolve().parents[1]
_SKIPPED_PARTS = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "htmlcov",
        "results",
    }
)
_INLINE_LINK = re.compile(
    r"!?\[[^\]]*\]\(\s*(?P<target><[^>]+>|[^\s)]+)"
    r"(?:\s+(?:\"[^\"]*\"|'[^']*'|\([^)]*\)))?\s*\)"
)
_REFERENCE_LINK = re.compile(r"^\s*\[[^\]]+\]:\s*(?P<target><[^>]+>|\S+)", re.MULTILINE)
_HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(?P<text>.+?)\s*#*\s*$")
_HTML_TAG = re.compile(r"<[^>]+>")
_PUNCTUATION = re.compile(r"[^\w\- ]", re.UNICODE)


def _markdown_files() -> tuple[Path, ...]:
    return tuple(
        sorted(
            path
            for path in _ROOT.rglob("*.md")
            if path.is_file()
            and not path.is_symlink()
            and not (_SKIPPED_PARTS & set(path.relative_to(_ROOT).parts))
        )
    )


def _targets(text: str) -> tuple[tuple[int, str], ...]:
    found: list[tuple[int, str]] = []
    for pattern in (_INLINE_LINK, _REFERENCE_LINK):
        for match in pattern.finditer(text):
            target = match.group("target")
            if target.startswith("<") and target.endswith(">"):
                target = target[1:-1]
            line = text.count("\n", 0, match.start()) + 1
            found.append((line, target))
    return tuple(found)


def _slug(value: str) -> str:
    without_markup = _HTML_TAG.sub("", value).strip().casefold()
    without_punctuation = _PUNCTUATION.sub("", without_markup)
    return re.sub(r"\s+", "-", without_punctuation).strip("-")


def _anchors(path: Path) -> frozenset[str]:
    counts: defaultdict[str, int] = defaultdict(int)
    anchors: set[str] = set()
    fenced = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced or (match := _HEADING.match(line)) is None:
            continue
        base = _slug(match.group("text"))
        if not base:
            continue
        occurrence = counts[base]
        counts[base] += 1
        anchors.add(base if occurrence == 0 else f"{base}-{occurrence}")
    return frozenset(anchors)


def _local_target(source: Path, raw_target: str) -> tuple[Path, str] | None:
    parsed = urlsplit(raw_target)
    if parsed.scheme or parsed.netloc:
        return None
    decoded_path = unquote(parsed.path)
    if decoded_path:
        candidate = (
            _ROOT / decoded_path.lstrip("/")
            if decoded_path.startswith("/")
            else source.parent / decoded_path
        )
    else:
        candidate = source
    resolved = candidate.resolve(strict=False)
    if not resolved.is_relative_to(_ROOT):
        raise ValueError("link escapes the repository")
    return resolved, unquote(parsed.fragment).casefold()


def main() -> int:
    errors: list[str] = []
    checked_links = 0
    anchor_cache: dict[Path, frozenset[str]] = {}
    markdown_files = _markdown_files()
    for source in markdown_files:
        text = source.read_text(encoding="utf-8")
        for line, raw_target in _targets(text):
            try:
                selected = _local_target(source, raw_target)
            except ValueError as error:
                errors.append(f"{source.relative_to(_ROOT)}:{line}: {error}: {raw_target}")
                continue
            if selected is None:
                continue
            target, anchor = selected
            checked_links += 1
            if not target.exists():
                errors.append(
                    f"{source.relative_to(_ROOT)}:{line}: missing local target: {raw_target}"
                )
                continue
            if not anchor or target.suffix.casefold() != ".md":
                continue
            anchors = anchor_cache.setdefault(target, _anchors(target))
            if anchor not in anchors:
                errors.append(
                    f"{source.relative_to(_ROOT)}:{line}: missing Markdown anchor "
                    f"#{anchor} in {target.relative_to(_ROOT)}"
                )
    if errors:
        sys.stderr.write(
            "Documentation link check failed:\n" + "".join(f"- {error}\n" for error in errors)
        )
        return 1
    sys.stdout.write(
        f"Documentation link check passed for {len(markdown_files)} Markdown files "
        f"and {checked_links} local links.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
