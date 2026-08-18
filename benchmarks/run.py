"""Command-line entry point for repeatable Makolet performance measurements."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from benchmarks.database import DatabaseScale, run_database_benchmark
from benchmarks.measure import hardware_environment
from benchmarks.parser import run_parser_benchmark
from makolet.adapters.persistence.destructive_target import (
    DestructiveDatabaseTargetError,
    require_benchmark_database_target,
)

_SOURCE_ROOT: Final = Path(__file__).resolve().parents[1]
WORKSPACE: Final = _SOURCE_ROOT if (_SOURCE_ROOT / "pyproject.toml").is_file() else Path.cwd()


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    name: str
    parser_records: int
    database: DatabaseScale
    acceptance_evidence: bool


PROFILES: Final = {
    "smoke": BenchmarkProfile(
        name="smoke",
        parser_records=1_000,
        database=DatabaseScale(
            normalized_records=1_000,
            ingestion_store_count=10,
            store_count=30,
            reconciliation_drop_records=0,
            history_rows=100,
            query_repetitions=3,
            query_warmups=1,
        ),
        acceptance_evidence=False,
    ),
    "quick": BenchmarkProfile(
        name="quick",
        parser_records=10_000,
        database=DatabaseScale(
            normalized_records=10_000,
            ingestion_store_count=10,
            store_count=100,
            reconciliation_drop_records=100,
            history_rows=1_000,
            query_repetitions=20,
            query_warmups=3,
        ),
        acceptance_evidence=False,
    ),
    "standard": BenchmarkProfile(
        name="standard",
        parser_records=1_000_000,
        database=DatabaseScale(
            normalized_records=1_000_000,
            ingestion_store_count=10,
            store_count=100,
            reconciliation_drop_records=10_000,
            history_rows=10_000,
            query_repetitions=100,
            query_warmups=10,
        ),
        acceptance_evidence=True,
    ),
}


class BenchmarkUsageError(ValueError):
    """A benchmark invocation is incomplete or internally inconsistent."""


def _arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run deterministic streaming-parser and isolated PostgreSQL scale benchmarks. "
            "The standard profile is the required 1m/10m acceptance workload."
        )
    )
    parser.add_argument(
        "--profile",
        choices=tuple(PROFILES),
        default="quick",
        help="workload size; quick/smoke are diagnostics, not scale acceptance evidence",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("MAKOLET_BENCHMARK_DATABASE_URL"),
        help="PostgreSQL URL, or set MAKOLET_BENCHMARK_DATABASE_URL",
    )
    parser.add_argument(
        "--database-confirmation",
        default=os.environ.get("MAKOLET_BENCHMARK_DATABASE_CONFIRM"),
        help=("exact database name confirmation, or set MAKOLET_BENCHMARK_DATABASE_CONFIRM"),
    )
    parser.add_argument(
        "--scenario",
        choices=("all", "parser", "database"),
        default="all",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON result path; defaults under benchmarks/results/",
    )
    parser.add_argument(
        "--keep-schema",
        action="store_true",
        help="retain the fixed makolet_benchmark schema for manual plan inspection",
    )
    return parser.parse_args(argv)


async def _run(arguments: argparse.Namespace) -> dict[str, object]:
    profile = PROFILES[str(arguments.profile)]
    database_url: str | None = None
    if arguments.scenario in {"all", "database"} and not arguments.database_url:
        raise BenchmarkUsageError(
            "database scenario requires --database-url or MAKOLET_BENCHMARK_DATABASE_URL"
        )
    if arguments.scenario in {"all", "database"}:
        try:
            database_url = require_benchmark_database_target(
                str(arguments.database_url),
                confirmation=arguments.database_confirmation,
            )
        except DestructiveDatabaseTargetError as error:
            raise BenchmarkUsageError(str(error)) from error
    began = datetime.now(UTC)
    started = time.perf_counter()
    scenario = str(arguments.scenario)
    acceptance = _acceptance_metadata(profile, scenario)
    result: dict[str, object] = {
        "format_version": 1,
        "profile": profile.name,
        "scenario": scenario,
        # Whole-result acceptance means every scenario in the named profile ran.
        # A deliberately isolated standard parser or database run is still useful
        # scenario evidence, but must not claim completion of the other workload.
        **acceptance,
        "started_at": began.isoformat(),
        "git_revision": _git_revision(),
        "source_tree_sha256": _source_tree_digest(WORKSPACE),
        "environment": hardware_environment(WORKSPACE),
        "profile_inputs": {
            "parser_records": profile.parser_records,
            "database": asdict(profile.database),
        },
    }
    if scenario in {"all", "parser"}:
        print(f"Running parser scenario with {profile.parser_records:,} records...")
        result["parser"] = await run_parser_benchmark(profile.parser_records)
    if scenario in {"all", "database"}:
        if database_url is None:  # Defensive: validation above owns this branch.
            raise BenchmarkUsageError("database target validation did not produce a URL")
        print(
            "Running PostgreSQL scenario with "
            f"{profile.database.normalized_records:,} normalized and "
            f"{profile.database.expected_current_prices:,} current-price rows..."
        )
        result["database"] = await run_database_benchmark(
            database_url,
            profile.database,
            database_confirmation=arguments.database_confirmation,
            keep_schema=bool(arguments.keep_schema),
        )
    finished = datetime.now(UTC)
    result["finished_at"] = finished.isoformat()
    result["wall_duration_seconds"] = round(time.perf_counter() - started, 6)
    return result


def _acceptance_metadata(profile: BenchmarkProfile, scenario: str) -> dict[str, object]:
    """Describe exactly which standard scenarios the invocation actually executes."""

    if scenario not in {"all", "parser", "database"}:
        raise BenchmarkUsageError(f"unsupported benchmark scenario: {scenario}")
    is_standard = profile.acceptance_evidence
    complete = is_standard and scenario == "all"
    note = (
        "standard profile executes the required parser and PostgreSQL scale workloads"
        if complete
        else (
            f"standard {scenario} scenario evidence; the other standard scenario was not run"
            if is_standard
            else "smoke/quick profile is diagnostic only and cannot satisfy scale acceptance"
        )
    )
    return {
        "acceptance_evidence": complete,
        "scenario_acceptance_evidence": {
            "parser": is_standard and scenario in {"all", "parser"},
            "database": is_standard and scenario in {"all", "database"},
        },
        "acceptance_note": note,
    }


def _git_revision() -> str | None:
    head_path = WORKSPACE / ".git" / "HEAD"
    if not head_path.is_file():
        return None
    head = head_path.read_text(encoding="utf-8").strip()
    if head.startswith("ref: "):
        reference_path = WORKSPACE / ".git" / head.removeprefix("ref: ")
        return (
            reference_path.read_text(encoding="utf-8").strip() if reference_path.is_file() else None
        )
    return head if len(head) == 40 else None


def _source_tree_digest(root: Path) -> str:
    """Hash every benchmark-relevant input when a Git revision is unavailable."""

    candidates: list[Path] = []
    for directory in ("src", "migrations", "benchmarks"):
        base = root / directory
        if base.is_dir():
            candidates.extend(
                path
                for path in base.rglob("*")
                if path.is_file()
                and "__pycache__" not in path.parts
                and not (directory == "benchmarks" and "results" in path.parts)
                and path.suffix not in {".pyc", ".pyo"}
            )
    for filename in ("pyproject.toml", "uv.lock", "build-constraints.txt"):
        path = root / filename
        if path.is_file():
            candidates.append(path)
    digest = hashlib.sha256()
    for path in sorted(set(candidates), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _output_path(arguments: argparse.Namespace) -> Path:
    if arguments.output is not None:
        path = cast(Path, arguments.output)
        return path if path.is_absolute() else WORKSPACE / path
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return WORKSPACE / "benchmarks" / "results" / f"{stamp}-{arguments.profile}.json"


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark harness and return a process-compatible exit code."""

    arguments = _arguments(argv)
    output_path = _output_path(arguments)
    try:
        result = asyncio.run(_run(arguments))
    except BenchmarkUsageError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(f"Benchmark result: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
