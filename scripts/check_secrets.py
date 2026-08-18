#!/usr/bin/env python3
"""Fail when repository files contain likely committed credentials.

This deliberately has no third-party dependency so the exact CI check is also
available in a clean local checkout. Findings name the rule and location but
never echo the candidate secret.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

MAX_FILE_BYTES = 2 * 1024 * 1024
ALLOW_MARKER = "secret-scan: allow"


@dataclass(frozen=True, slots=True)
class SecretRule:
    name: str
    pattern: re.Pattern[str]


KNOWN_SECRET_RULES = (
    SecretRule(
        "private-key",
        re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----"),
    ),
    SecretRule("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    SecretRule(
        "github-token",
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    ),
    SecretRule("gitlab-token", re.compile(r"\bglpat-[A-Za-z0-9_-]{20,}\b")),
    SecretRule("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    SecretRule("stripe-live-key", re.compile(r"\b(?:sk|rk)_live_[A-Za-z0-9]{20,}\b")),
    SecretRule("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b")),
    SecretRule("npm-token", re.compile(r"\bnpm_[A-Za-z0-9]{36}\b")),
    SecretRule("pypi-token", re.compile(r"\bpypi-AgEIcHlwaS5vcmc[A-Za-z0-9_-]{30,}\b")),
)

ASSIGNMENT = re.compile(
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
URI_CREDENTIAL = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|ftp|https?)://"
    r"[^\s/:@]+:(?P<value>[^\s/@]+)@"
)

SAFE_EXACT_VALUES = frozenset(
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
SAFE_VALUE_FRAGMENTS = (
    "development-only",
    "fixture",
    "placeholder",
    "redacted",
    "test-password",
    "test-only",
)
SAFE_EXPRESSION_PREFIXES = (
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
_SAFE_REFERENCE = re.compile(r"^[a-z_][a-z0-9_.]*$")
_SAFE_CALL = re.compile(r"^[a-z_][a-z0-9_.]*\(")


def _repository_files(repository_root: Path) -> list[Path]:
    git_executable = shutil.which("git")
    if git_executable is None:
        return sorted(
            path
            for path in repository_root.rglob("*")
            if path.is_file() and ".git" not in path.parts
        )
    # The executable is resolved without a shell and every argument is fixed here.
    result = subprocess.run(  # noqa: S603
        [git_executable, "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=repository_root,
        check=False,
        capture_output=True,
    )
    if result.returncode == 0:
        relative_paths = (
            Path(item.decode("utf-8", errors="surrogateescape"))
            for item in result.stdout.split(b"\0")
            if item
        )
        return sorted(
            path
            for relative_path in relative_paths
            if (path := repository_root / relative_path).is_file() and not path.is_symlink()
        )
    return sorted(
        path for path in repository_root.rglob("*") if path.is_file() and ".git" not in path.parts
    )


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


def _safe_literal(candidate: str) -> bool:
    normalized = candidate.strip().strip("\"'").casefold()
    return (
        normalized in SAFE_EXACT_VALUES
        or any(fragment in normalized for fragment in SAFE_VALUE_FRAGMENTS)
        or normalized.startswith(SAFE_EXPRESSION_PREFIXES)
        or _SAFE_REFERENCE.fullmatch(normalized) is not None
        or _SAFE_CALL.match(normalized) is not None
    )


def _safe_value(candidate: str) -> bool:
    normalized = candidate.strip().strip("\"'")
    if "://" in normalized:
        matches = tuple(URI_CREDENTIAL.finditer(normalized))
        return bool(matches) and all(_safe_literal(match.group("value")) for match in matches)
    return _safe_literal(normalized)


def _scan_line(line: str) -> list[str]:
    if ALLOW_MARKER in line:
        return []
    findings = [rule.name for rule in KNOWN_SECRET_RULES if rule.pattern.search(line)]
    if any(
        _credential_identifier(assignment.group("name"))
        and not _safe_value(assignment.group("value"))
        for assignment in ASSIGNMENT.finditer(line)
    ):
        findings.append("credential-assignment")
    if any(
        not _safe_value(uri_credential.group("value"))
        for uri_credential in URI_CREDENTIAL.finditer(line)
    ):
        findings.append("credential-in-uri")
    return findings


def scan_repository(repository_root: Path) -> list[tuple[Path, int, str]]:
    findings: list[tuple[Path, int, str]] = []
    for path in _repository_files(repository_root):
        try:
            if path.stat().st_size > MAX_FILE_BYTES:
                continue
            contents = path.read_bytes()
        except OSError as error:
            raise RuntimeError(f"cannot read repository file {path}: {error}") from error
        if b"\0" in contents:
            continue
        text = contents.decode("utf-8", errors="replace")
        for line_number, line in enumerate(text.splitlines(), start=1):
            findings.extend(
                (path.relative_to(repository_root), line_number, rule_name)
                for rule_name in _scan_line(line)
            )
    return findings


def main() -> int:
    repository_root = Path(__file__).resolve().parent.parent
    findings = scan_repository(repository_root)
    if findings:
        finding_lines = "\n".join(
            f"  {path}:{line_number}: {rule_name}" for path, line_number, rule_name in findings
        )
        sys.stderr.write(
            "Potential committed secrets detected (candidate values are redacted):\n"
            f"{finding_lines}\n"
            f"Secret scan failed with {len(findings)} finding(s).\n"
        )
        return 1
    sys.stdout.write(
        f"Secret scan passed for {len(_repository_files(repository_root))} repository files.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
