from __future__ import annotations

import json
import os
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.engine import URL, make_url

from makolet.interfaces import database_restore

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
STAGING_DATABASE = "makolet_restore_20260812123456_1234"


class CommandRunner:
    def __init__(self, statuses: Sequence[subprocess.CompletedProcess[Any]]) -> None:
        self._statuses = iter(statuses)
        self.calls: list[tuple[tuple[str, ...], dict[str, object]]] = []

    def __call__(
        self, arguments: Sequence[str], **kwargs: object
    ) -> subprocess.CompletedProcess[Any]:
        self.calls.append((tuple(arguments), kwargs))
        return next(self._statuses)


def _completed(returncode: int, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess((), returncode, stdout=stdout)


def _database_url(password: str) -> str:
    return URL.create(
        "postgresql",
        username="makolet",
        password=password,
        host="postgres",
        port=5432,
        database="active",
    ).render_as_string(hide_password=False)


def _operations_environment(fake_bin: Path) -> dict[str, str]:
    environment = dict(os.environ)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    for selector in (
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)
    return environment


def _compile_docker_capture_executable(
    fake_bin: Path,
) -> Path:
    source = fake_bin / "DockerCapture.cs"
    source.write_text(
        """
using System;
using System.IO;

public static class DockerCapture
{
    public static void Main(string[] arguments)
    {
        File.WriteAllLines(
            Environment.GetEnvironmentVariable("MAKOLET_DOCKER_CAPTURE"),
            arguments
        );
    }
}
""",
        encoding="utf-8",
    )
    executable = fake_bin / "docker.exe"
    windows_directory = Path(os.environ.get("WINDIR", "C:/Windows"))
    compiler_candidates = (
        windows_directory / "Microsoft.NET/Framework64/v4.0.30319/csc.exe",
        windows_directory / "Microsoft.NET/Framework/v4.0.30319/csc.exe",
    )
    compiler = next((candidate for candidate in compiler_candidates if candidate.is_file()), None)
    if compiler is None:
        pytest.skip("the Windows C# compiler is unavailable")
    completed = subprocess.run(  # noqa: S603 - controlled local compiler harness
        [
            str(compiler),
            "/nologo",
            f"/out:{executable}",
            str(source),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert executable.is_file()
    return executable


def test_restore_preparation_migrates_and_verifies_only_the_staging_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status = {
        "schema_ready": True,
        "expected_migration_heads": ["0006_head"],
        "current_migration_revisions": ["0006_head"],
    }
    runner = CommandRunner((_completed(0), _completed(0, json.dumps(status))))
    monkeypatch.setattr(subprocess, "run", runner)

    revision = database_restore.prepare_staging_database(
        STAGING_DATABASE,
        _database_url("sentinel-secret"),
    )

    assert revision == "0006_head"
    assert [call[0][3:] for call in runner.calls] == [
        ("database", "migrate", "--json"),
        ("database", "status", "--json"),
    ]
    for _arguments, options in runner.calls:
        environment = options["env"]
        assert isinstance(environment, dict)
        database_url = make_url(environment["MAKOLET_DATABASE_URL"])
        assert database_url.database == STAGING_DATABASE
        assert database_url.password == "sentinel-secret"


@pytest.mark.parametrize(
    "status",
    [
        {
            "schema_ready": False,
            "expected_migration_heads": ["0006_head"],
            "current_migration_revisions": ["0005_previous"],
        },
        {
            "schema_ready": True,
            "expected_migration_heads": ["0006_head"],
            "current_migration_revisions": ["other_head"],
        },
        {
            "schema_ready": True,
            "expected_migration_heads": ["0006_head"],
            "current_migration_revisions": [],
        },
    ],
)
def test_restore_preparation_rejects_any_non_exact_schema(
    monkeypatch: pytest.MonkeyPatch,
    status: dict[str, object],
) -> None:
    runner = CommandRunner((_completed(0), _completed(0, json.dumps(status))))
    monkeypatch.setattr(subprocess, "run", runner)

    with pytest.raises(RuntimeError, match="not exactly"):
        database_restore.prepare_staging_database(
            STAGING_DATABASE,
            _database_url("secret"),
        )


def test_restore_preparation_rejects_non_staging_target_before_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CommandRunner(())
    monkeypatch.setattr(subprocess, "run", runner)

    with pytest.raises(ValueError, match="staging database name"):
        database_restore.prepare_staging_database(
            "active",
            _database_url("secret"),
        )

    assert runner.calls == []


def test_restore_preparation_rejects_active_database_as_staging_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = CommandRunner(())
    monkeypatch.setattr(subprocess, "run", runner)

    with pytest.raises(ValueError, match="must differ"):
        database_restore.prepare_staging_database(
            STAGING_DATABASE,
            _database_url("secret").replace("/active", f"/{STAGING_DATABASE}"),
        )

    assert runner.calls == []


def test_restore_command_failure_does_not_disclose_database_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    runner = CommandRunner((_completed(1),))
    monkeypatch.setattr(subprocess, "run", runner)
    monkeypatch.setenv("MAKOLET_RESTORE_STAGING_DATABASE", STAGING_DATABASE)
    monkeypatch.setenv(
        "MAKOLET_DATABASE_URL",
        _database_url("sentinel-secret"),
    )

    assert database_restore.main() == 1

    output = capsys.readouterr()
    assert "active database is unchanged" in output.err
    assert "sentinel-secret" not in output.err
    assert output.out == ""


def test_restore_scripts_prepare_staging_before_stopping_or_swapping() -> None:
    shell = (REPOSITORY_ROOT / "scripts" / "database-restore.sh").read_text(encoding="utf-8")
    powershell = (REPOSITORY_ROOT / "scripts" / "operations.ps1").read_text(encoding="utf-8")
    shell_backup = (REPOSITORY_ROOT / "scripts" / "database-backup.sh").read_text(encoding="utf-8")
    smoke = (REPOSITORY_ROOT / "scripts" / "container-smoke.sh").read_text(encoding="utf-8")

    module_name = "makolet.interfaces.database_restore"
    assert shell.index(module_name) < shell.index("stop api worker") < shell.index("ALTER DATABASE")
    assert '"${compose[@]}" --progress quiet run --rm --no-deps' in shell
    assert (
        powershell.index(module_name)
        < powershell.index("stop api worker")
        < powershell.index("ALTER DATABASE")
    )
    assert '"--progress", "quiet", "run", "--rm", "--no-deps"' in powershell
    shell_after_swap = shell[shell.index("swapped=true") :]
    assert '"${compose[@]}" run --rm migrate' not in shell_after_swap
    restore_function = powershell[
        powershell.index("function Restore-Database") : powershell.index(
            "function Invoke-ArchiveOperation"
        )
    ]
    backup_function = powershell[
        powershell.index("function Backup-Database") : powershell.index("function Restore-Database")
    ]
    assert 'Invoke-Compose -Arguments @("run", "--rm", "migrate")' not in restore_function
    assert '"$swapped" == false && "$staging_created" == true' in shell
    assert "if (-not $Swapped -and $StagingCreated)" in restore_function
    authentication_module = "makolet.interfaces.database_backup_auth"
    assert authentication_module in shell_backup
    assert authentication_module in powershell
    assert "capture-command" in shell_backup
    assert "capture-command" in backup_function
    assert "validate-command" in shell_backup
    assert "validate-command" in backup_function
    assert "write-sidecars" in shell_backup
    assert "write-sidecars" in backup_function
    assert '>"$temporary"' not in shell_backup
    assert '--file="$1"' not in backup_function
    assert 'docker cp "${ContainerId}:$Remote" $Temporary' not in backup_function
    assert "docker cp $Temporary" not in backup_function
    assert "Get-PostgresContainerId" not in backup_function
    assert shell.index("verify-copy") < shell.index("pg_restore --list")
    assert '"$verified_root"' in shell
    assert 'read-checksum "$checksum_file"' in shell
    assert shell.count('<"$verified_backup"') == 2
    assert restore_function.index('"verify-copy"') < restore_function.index("pg_restore")
    assert "CapacityLockDirectory" in restore_function
    assert "read-checksum $ChecksumPath" in restore_function
    assert "Get-Content -LiteralPath $ChecksumPath" not in restore_function
    assert restore_function.index('"verify-copy"') < restore_function.index(
        "Get-PostgresContainerId"
    )
    assert "& docker cp $VerifiedBackup" in restore_function
    assert '"$backup.hmac-sha256"' in shell
    assert '"$Backup.hmac-sha256"' in restore_function
    assert "generate-key" in smoke
    assert "$temporary_root/protected/database-backup-auth.key" in smoke


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell backup harness")
def test_powershell_database_backup_routes_pg_dump_through_bounded_capture(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    uv_capture = tmp_path / "uv-arguments.txt"
    docker_capture = tmp_path / "docker-arguments.txt"
    fake_uv = fake_bin / "uv.cmd"
    fake_uv.write_text(
        """@echo off
>> "%MAKOLET_UV_CAPTURE%" echo %*
if "%~5"=="capture-command" (
  > "%~6" echo PGDMP independently authored bounded backup
  exit /b 0
)
if "%~5"=="validate-command" exit /b 0
if "%~5"=="write-sidecars" (
  > "%~7" echo 0000000000000000000000000000000000000000000000000000000000000000  makolet.dump
  > "%~8" echo independently-authored-authentication
  echo 0000000000000000000000000000000000000000000000000000000000000000
  exit /b 0
)
exit /b 1
""",
        encoding="ascii",
    )
    fake_docker = fake_bin / "docker.cmd"
    fake_docker.write_text(
        """@echo off
>> "%MAKOLET_DOCKER_CAPTURE%" echo %*
if "%~1"=="compose" if "%~2"=="ps" (
  echo 0000000000000000000000000000000000000000000000000000000000000000
)
exit /b 0
""",
        encoding="ascii",
    )
    destination = tmp_path / "backup" / "makolet.dump"
    environment = dict(os.environ)
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_UV_CAPTURE"] = str(uv_capture)
    environment["MAKOLET_DOCKER_CAPTURE"] = str(docker_capture)
    for selector in (
        "COMPOSE_PROJECT_NAME",
        "COMPOSE_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_PATH_SEPARATOR",
        "COMPOSE_PROFILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "DOCKER_CONFIG",
        "DOCKER_CONTEXT",
        "DOCKER_HOST",
        "MAKOLET_COMPOSE_ENV_FILE",
    ):
        environment.pop(selector, None)

    completed = subprocess.run(  # noqa: S603 - controlled PowerShell harness
        [
            powershell,
            "-NoProfile",
            "-File",
            str(REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "database-backup",
            str(destination),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    capture_arguments = uv_capture.read_text(encoding="ascii")
    assert "capture-command" in capture_arguments
    assert "pg_dump" in capture_arguments
    assert "--file=" not in capture_arguments
    assert destination.read_bytes().startswith(b"PGDMP")
    assert destination.with_name("makolet.dump.sha256").is_file()
    assert destination.with_name("makolet.dump.hmac-sha256").is_file()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell watchdog harness")
@pytest.mark.parametrize("extension", ["cmd", "bat"])
def test_powershell_watchdog_rejects_docker_command_script_before_start(
    tmp_path: Path,
    extension: str,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_started = tmp_path / "docker-started.txt"
    injected = tmp_path / "injected.txt"
    fake_docker = fake_bin / f"docker.{extension}"
    fake_docker.write_text(
        '@echo off\r\n> "%MAKOLET_DOCKER_STARTED%" echo started\r\nexit /b 0\r\n',
        encoding="ascii",
    )
    backup = tmp_path / "backup"
    backup.mkdir()
    key_file = tmp_path / "archive.key"
    key_file.write_bytes(bytes(range(32)))
    malicious_value = f'1" & echo injected>"{injected}" & rem "'
    environment = _operations_environment(fake_bin)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES"] = malicious_value
    environment["MAKOLET_DOCKER_STARTED"] = str(docker_started)

    completed = subprocess.run(  # noqa: S603 - controlled PowerShell harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-verify",
            str(backup),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    normalized_error = " ".join(completed.stderr.split())
    assert "Refusing command-script Docker" in normalized_error
    assert "resolutions for watchdog-supervised operations; install" in normalized_error
    assert "docker.exe" in normalized_error
    assert not docker_started.exists()
    assert not injected.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell watchdog harness")
def test_powershell_watchdog_preserves_metacharacters_as_docker_exe_argv(
    tmp_path: Path,
) -> None:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if powershell is None:
        pytest.skip("PowerShell is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _compile_docker_capture_executable(fake_bin)
    capture = tmp_path / "docker-arguments.txt"
    injected = tmp_path / "injected.txt"
    backup = tmp_path / "backup"
    backup.mkdir()
    key_file = tmp_path / "archive.key"
    key_file.write_bytes(bytes(range(32)))
    malicious_value = f'1" & echo injected>"{injected}" & rem "'
    environment = _operations_environment(fake_bin)
    environment["MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"] = str(key_file)
    environment["MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES"] = malicious_value
    environment["MAKOLET_DOCKER_CAPTURE"] = str(capture)

    completed = subprocess.run(  # noqa: S603 - controlled PowerShell harness
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-File",
            str(REPOSITORY_ROOT / "scripts" / "operations.ps1"),
            "archive-verify",
            str(backup),
        ],
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert (
        f"MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES={malicious_value}"
        in capture.read_text(encoding="utf-8").splitlines()
    )
    assert not injected.exists()
