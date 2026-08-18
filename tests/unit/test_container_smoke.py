from __future__ import annotations

import os
import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import cast

import pytest

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def _smoke_script() -> str:
    return (_REPOSITORY_ROOT / "scripts" / "container-smoke.sh").read_text(encoding="utf-8")


def test_container_smoke_forces_the_bundled_local_s3_environment() -> None:
    script = _smoke_script()
    setup = script[: script.index("is_windows_posix_shell() {")]

    assert 'export MAKOLET_COMPOSE_ENV_FILE=".env.example"' in setup
    assert "${MAKOLET_COMPOSE_ENV_FILE:-" not in setup
    assert "unset COMPOSE_FILE COMPOSE_ENV_FILES" in setup
    assert "export MAKOLET_ENVIRONMENT=development" in setup
    assert "export MAKOLET_S3_ENDPOINT=http://seaweedfs:8333" in setup
    assert "export MAKOLET_S3_ALLOW_INSECURE_LOCAL=true" in setup
    assert "export MAKOLET_S3_BUCKET=makolet-raw" in setup
    assert "export MAKOLET_S3_REGION=us-east-1" in setup
    assert "export MAKOLET_S3_ACCESS_KEY=makolet-development" in setup
    assert "export MAKOLET_S3_SECRET_KEY=makolet-development-only-change-me" in setup
    assert "export MAKOLET_PROMETHEUS_PORT=0" in setup
    assert "export MAKOLET_ENABLED_SOURCES='[]'" in setup


@pytest.mark.skipif(os.name == "nt", reason="POSIX smoke isolation harness")
def test_container_smoke_overrides_ambient_external_s3_before_compose(
    tmp_path: Path,
) -> None:
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "smoke-environment.txt"
    docker = fake_bin / "docker"
    docker.write_text(
        """#!/usr/bin/env bash
case " $* " in
  *" config --quiet "*)
    printf '%s\n' \
      "$MAKOLET_COMPOSE_ENV_FILE" \
      "$MAKOLET_ENVIRONMENT" \
      "$MAKOLET_S3_ENDPOINT" \
      "$MAKOLET_S3_BUCKET" \
      "$MAKOLET_S3_ACCESS_KEY" \
      "$MAKOLET_S3_SECRET_KEY" \
      "$COMPOSE_FILE" \
      "$*" >"$MAKOLET_CAPTURE"
    exit 73
    ;;
  *) exit 0 ;;
esac
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    environment = dict(os.environ)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["COMPOSE_FILE"] = "/external/compose.yaml"
    environment["MAKOLET_COMPOSE_ENV_FILE"] = "/external/production.env"
    environment["MAKOLET_ENVIRONMENT"] = "production"
    environment["MAKOLET_S3_ENDPOINT"] = "https://external.invalid"
    environment["MAKOLET_S3_BUCKET"] = "production-archive"
    environment["MAKOLET_S3_ACCESS_KEY"] = "external-access"
    environment["MAKOLET_S3_SECRET_KEY"] = "external-secret"

    completed = subprocess.run(  # noqa: S603 - controlled shell harness
        [bash, str(_REPOSITORY_ROOT / "scripts" / "container-smoke.sh")],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 73, completed.stderr
    assert capture.read_text(encoding="utf-8").splitlines() == [
        ".env.example",
        "development",
        "http://seaweedfs:8333",
        "makolet-raw",
        "makolet-development",
        "makolet-development-only-change-me",
        "",
        f"compose --file {_REPOSITORY_ROOT}/compose.yaml --env-file .env.example config --quiet",
    ]


def test_container_coverage_unsets_all_application_settings_in_subshell() -> None:
    script = _smoke_script()
    coverage_command = "uv run pytest --cov=makolet"
    coverage_position = script.index(coverage_command)
    subshell_start = script.rfind("\n(\n", 0, coverage_position)
    subshell_end = script.index("\n)\n", coverage_position)

    assert subshell_start >= 0
    coverage_subshell = script[subshell_start:subshell_end]
    assert 'unset "${!MAKOLET_@}"' in coverage_subshell
    assert coverage_subshell.count(coverage_command) == 1
    pytest_environment = coverage_subshell.split('unset "${!MAKOLET_@}"', 1)[1]
    assert "MAKOLET_TEST_DATABASE_CONFIRM=makolet_test_coverage" in pytest_environment
    assert not re.search(r"(?m)^\s*MAKOLET_(?!TEST_)[A-Z0-9_]+=", pytest_environment)
    assert script.index("export MAKOLET_DATABASE_URL=") < subshell_start
    assert script.index("export MAKOLET_API_PORT=") < subshell_start
    assert script.index("run_archive_operation archive-backup") > subshell_end


def test_container_smoke_restores_clean_demo_before_archive_inventory() -> None:
    script = _smoke_script()
    coverage = script.index("uv run pytest --cov=makolet")
    database_backup = script.index(
        'bash scripts/database-backup.sh "$temporary_root/database/makolet.dump"'
    )
    database_restore = script.index(
        'bash scripts/database-restore.sh "$temporary_root/database/makolet.dump"'
    )
    reseed = script.index('"${compose[@]}" --profile demo run --rm demo-seed', coverage)
    archive_backup = script.index("run_archive_operation archive-backup")

    assert database_backup < coverage < database_restore < reseed < archive_backup
    assert script.count("bash scripts/database-backup.sh") == 1
    assert script.count("bash scripts/database-restore.sh") == 1


def test_container_smoke_exercises_worker_and_prometheus_without_publishers() -> None:
    script = _smoke_script()

    assert script.count("up -d --wait api worker") == 2
    assert script.count("--profile monitoring up -d --wait prometheus") == 2
    assert 'expected_jobs = {"makolet-api", "makolet-worker", "seaweedfs"}' in script
    assert "for application_service in api worker" in script
    assert '"$prometheus_url/-/healthy"' in script
    coverage_position = script.index("uv run pytest --cov=makolet")
    assert script.index("--profile monitoring stop prometheus") < coverage_position
    assert script.index("stop api worker") < coverage_position
    cleanup = script[script.index("cleanup() {") : script.index("trap cleanup EXIT")]
    assert "--profile monitoring down --volumes --remove-orphans" in cleanup


def test_container_smoke_routes_windows_archive_operations_through_powershell() -> None:
    script = _smoke_script()
    powershell = (_REPOSITORY_ROOT / "scripts" / "operations.ps1").read_text(encoding="utf-8")
    powershell_parameters = powershell[: powershell.index('$ErrorActionPreference = "Stop"')]
    routing_functions = script[
        script.index("is_windows_posix_shell() {") : script.index("\ncompose=(\n")
    ]

    assert re.search(r"MINGW\*\|MSYS\*\|CYGWIN\*\) return 0", routing_functions)
    assert 'cygpath -w "$repository_root/scripts/operations.ps1"' in routing_functions
    assert 'cygpath -w "$backup_directory"' in routing_functions
    assert 'cygpath -w "$MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"' in routing_functions
    assert "command -v pwsh.exe" in routing_functions
    assert "PowerShell 7" in routing_functions
    assert 'pwsh.exe "${arguments[@]}"' in routing_functions
    assert "powershell.exe" not in routing_functions
    assert (
        '-File "$operations_script_windows" "$operation" "$backup_directory_windows"'
        in routing_functions
    )
    assert 'MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE="$authentication_key_windows"' in routing_functions
    for operation in ("backup", "verify", "restore"):
        assert f'"archive-{operation}"' in powershell_parameters
        assert f"archive-{operation}) bash scripts/archive-{operation}.sh" in routing_functions
        assert f"run_archive_operation archive-{operation}" in script

    assert re.search(r"\[Parameter\(Position = 1\)\]\s*\[string\]\$Path", powershell_parameters)
    assert re.search(r"\[Parameter\(Position = 2\)\]\s*\[string\]\$Confirm", powershell_parameters)
    assert "if ($Confirm -cne $Bucket)" in powershell

    restore_invocation = script[script.index('MAKOLET_S3_BUCKET="$restore_bucket" \\\n') :]
    assert (
        'run_archive_operation archive-restore "$temporary_root/archive" "$restore_bucket"'
        in restore_invocation
    )
    assert (
        restore_invocation.count('run_archive_operation archive-verify "$temporary_root/archive"')
        >= 1
    )
    assert "Re-verifying its complete inventory" in restore_invocation


def test_docker_builder_has_every_declared_project_license_file_before_install() -> None:
    dockerfile = (_REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    project = tomllib.loads((_REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]
    license_files = cast(list[str], project["license-files"])
    project_install_position = dockerfile.index("uv sync --frozen --no-dev --no-editable")
    builder_inputs = dockerfile[:project_install_position]

    for license_file in license_files:
        assert re.search(rf"\bCOPY\b[^\n]*\b{re.escape(license_file)}\b", builder_inputs)


def test_docker_builder_has_every_forced_wheel_input_before_project_install() -> None:
    dockerfile = (_REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    configuration = tomllib.loads((_REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    force_includes = cast(
        dict[str, str],
        configuration["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"],
    )
    project_install_position = dockerfile.index("uv sync --frozen --no-dev --no-editable")
    builder_inputs = dockerfile[:project_install_position]

    required_roots = {Path(source).parts[0] for source in force_includes}
    for root in required_roots:
        assert re.search(rf"(?m)^COPY\s+{re.escape(root)}/\s+{re.escape(root)}/$", builder_inputs)


def test_container_smoke_refreshes_random_api_port_after_database_restore() -> None:
    script = _smoke_script()
    restore_position = script.index(
        'bash scripts/database-restore.sh "$temporary_root/database/makolet.dump"'
    )
    restored_section = script[restore_position:]

    assert restored_section.index('"${compose[@]}" up -d --wait api') < restored_section.index(
        'api_binding="$("${compose[@]}" port api 8000 | tail -n 1)"'
    )
    assert restored_section.index('api_url="http://127.0.0.1:$api_port"') < (
        restored_section.index('"$api_url/api/v1/barcodes/')
    )
