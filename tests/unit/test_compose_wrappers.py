from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_TARGET_SELECTORS = (
    "COMPOSE_FILE",
    "COMPOSE_ENV_FILES",
    "COMPOSE_PATH_SEPARATOR",
    "COMPOSE_PROJECT_NAME",
    "COMPOSE_PROFILES",
    "COMPOSE_DISABLE_ENV_FILE",
    "DOCKER_CONFIG",
    "DOCKER_CONTEXT",
    "DOCKER_HOST",
)
_WRAPPERS = ("database-migrate.sh", "database-status.sh", "seed-demo.sh")


def _bash() -> str | None:
    if os.name == "nt":
        git_bash = Path("C:/Program Files/Git/bin/bash.exe")
        return str(git_bash) if git_bash.is_file() else None
    return shutil.which("bash")


def _clean_environment() -> dict[str, str]:
    environment = dict(os.environ)
    for selector in (*_TARGET_SELECTORS, "MAKOLET_COMPOSE_ENV_FILE"):
        environment.pop(selector, None)
    return environment


def _native_path(value: str) -> Path:
    if os.name == "nt" and len(value) >= 3 and value[0] == "/" and value[2] == "/":
        value = f"{value[1]}:{value[2:]}"
    return Path(value).resolve()


def test_database_and_demo_wrappers_share_the_pinned_compose_launcher() -> None:
    launcher = (_REPOSITORY_ROOT / "scripts" / "compose-launcher.sh").read_text(encoding="utf-8")
    assert '"$selected_repository_root/compose.yaml"' in launcher
    assert '--project-directory "$selected_repository_root"' in launcher
    assert '--project-name "$compose_project_name"' in launcher
    for selector in _TARGET_SELECTORS:
        assert selector in launcher
    for filename in _WRAPPERS:
        wrapper = (_REPOSITORY_ROOT / "scripts" / filename).read_text(encoding="utf-8")
        assert 'source "$repository_root/scripts/compose-launcher.sh"' in wrapper
        assert 'makolet_initialize_compose "$repository_root"' in wrapper
        assert "docker compose" not in wrapper


@pytest.mark.parametrize("filename", _WRAPPERS)
@pytest.mark.parametrize("selector", ["COMPOSE_FILE", "DOCKER_HOST", "COMPOSE_PROJECT_NAME"])
def test_database_and_demo_wrappers_reject_ambient_targets_before_docker(
    tmp_path: Path,
    filename: str,
    selector: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-invoked"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        '#!/usr/bin/env bash\nprintf invoked >"$MAKOLET_CAPTURE"\n',
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = _clean_environment()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment[selector] = "attacker-controlled"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [bash, str(_REPOSITORY_ROOT / "scripts" / filename)],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode != 0
    assert selector in completed.stderr
    assert not capture.exists()


@pytest.mark.parametrize("filename", _WRAPPERS)
def test_database_and_demo_wrappers_preserve_the_validated_smoke_project(
    tmp_path: Path,
    filename: str,
) -> None:
    bash = _bash()
    if bash is None:
        pytest.skip("Git/POSIX Bash is unavailable")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "docker-arguments"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$@" >"$MAKOLET_CAPTURE"\n',
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)
    environment = _clean_environment()
    environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"
    environment["MAKOLET_CAPTURE"] = str(capture)
    environment["COMPOSE_PROJECT_NAME"] = "makolet-smoke-local-12345"
    environment["MAKOLET_COMPOSE_ENV_FILE"] = ".env.example"
    environment["MAKOLET_ENVIRONMENT"] = "development"
    environment["POSTGRES_DB"] = "makolet_test_coverage"

    completed = subprocess.run(  # noqa: S603 - controlled wrapper harness
        [bash, str(_REPOSITORY_ROOT / "scripts" / filename)],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stderr
    arguments = capture.read_text(encoding="utf-8").splitlines()
    assert arguments[0] == "compose"
    assert (
        _native_path(arguments[arguments.index("--file") + 1])
        == (_REPOSITORY_ROOT / "compose.yaml").resolve()
    )
    assert _native_path(arguments[arguments.index("--project-directory") + 1]) == (
        _REPOSITORY_ROOT.resolve()
    )
    assert arguments[arguments.index("--project-name") + 1] == ("makolet-smoke-local-12345")
    assert (
        _native_path(arguments[arguments.index("--env-file") + 1])
        == (_REPOSITORY_ROOT / ".env.example").resolve()
    )
