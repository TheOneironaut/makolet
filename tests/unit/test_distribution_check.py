"""Deterministic distribution-gate policy tests."""

import json
from email import policy
from email.parser import BytesParser
from hashlib import sha256
from importlib import metadata
from pathlib import Path

import pytest

from scripts import check_distribution


def test_reproducibility_environment_is_offline_and_ignores_build_overrides() -> None:
    environment = check_distribution._controlled_environment(
        {
            "CUSTOM_VALUE": "preserved",
            "HATCH_BUILD_HOOKS_ENABLE": "unexpected",
            "PATH": "tool-path",
            "PIP_INDEX_URL": "https://index.invalid/simple",
            "PYTHONPATH": "unexpected",
            "SOURCE_DATE_EPOCH": "1",
            "UV_CACHE_DIR": "relative-cache",
            "UV_OFFLINE": "0",
            "UV_INDEX_URL": "https://index.invalid/simple",
        }
    )

    assert environment["CUSTOM_VALUE"] == "preserved"
    assert environment["PATH"] == "tool-path"
    assert environment["SOURCE_DATE_EPOCH"] == "946684800"
    assert environment["PYTHONHASHSEED"] == "0"
    assert environment["TZ"] == "UTC"
    assert environment["UV_FROZEN"] == "1"
    assert Path(environment["UV_CACHE_DIR"]).is_absolute()
    assert Path(environment["UV_CACHE_DIR"]).name == "relative-cache"
    assert environment["UV_OFFLINE"] == "1"
    assert environment["UV_PYTHON_DOWNLOADS"] == "never"
    assert "HATCH_BUILD_HOOKS_ENABLE" not in environment
    assert "PIP_INDEX_URL" not in environment
    assert "PYTHONPATH" not in environment
    assert "UV_INDEX_URL" not in environment


def test_build_command_requires_hashes_constraints_and_offline_mode(tmp_path: Path) -> None:
    output = tmp_path / "output"

    command = check_distribution._uv_build_command(
        "uv", output, tmp_path / "source", "--sdist", "--wheel"
    )

    assert command[:4] == ["uv", "build", "--sdist", "--wheel"]
    assert "--offline" in command
    assert "--no-sources" in command
    assert "--no-python-downloads" in command
    assert "--require-hashes" in command
    constraint_index = command.index("--build-constraints")
    assert Path(command[constraint_index + 1]).name == "build-constraints.txt"
    assert command[-1] == str(tmp_path / "source")


def test_runtime_export_selects_frozen_benchmark_extra(tmp_path: Path) -> None:
    output = tmp_path / "runtime-requirements.txt"

    command = check_distribution._uv_runtime_export_command("uv", output)

    assert command[:3] == ["uv", "export", "--frozen"]
    assert "--no-dev" in command
    assert command[command.index("--extra") + 1] == "benchmark"
    assert "--no-emit-project" in command
    assert command[command.index("--output-file") + 1] == str(output)


@pytest.mark.parametrize(
    "member",
    [
        "AGENTS.md",
        "src/makolet/adapters/sources/AGENTS.md",
        ".agent/execplans/platform.md",
        "nested/.agent/state.json",
        "benchmarks/results/result.json",
        "nested/benchmarks/results/result.json",
        "benchmarks/__pycache__/run.cpython-314.pyc",
        "benchmarks/run.pyc",
    ],
)
def test_distribution_forbids_repository_control_and_generated_payloads(member: str) -> None:
    assert check_distribution._forbidden_member(member) is True


@pytest.mark.parametrize(
    "member",
    [
        "benchmarks/run.py",
        "docs/performance.md",
        "makolet/_migrations/versions/0009_collection_charge_budgets.py",
        "makolet/_migrations/versions/0010_bounded_query_paths.py",
    ],
)
def test_distribution_allows_reviewed_source_payloads(member: str) -> None:
    assert check_distribution._forbidden_member(member) is False


def test_distribution_expected_inventories_are_exhaustive_and_clean() -> None:
    wheel = check_distribution._expected_wheel_repository_files()
    sdist = check_distribution._expected_sdist_repository_files()

    assert "makolet/interfaces/cli.py" in wheel
    assert "makolet/_migrations/versions/0011_resource_probe_budgets.py" in wheel
    assert "SECURITY.md" in sdist
    assert "scripts/check_distribution.py" in sdist
    assert all(not check_distribution._forbidden_member(name) for name in wheel)
    assert all(not check_distribution._forbidden_member(name) for name in sdist)
    assert all(check_distribution._text_member(name) for name in wheel)
    assert all(check_distribution._text_member(name) for name in sdist)
    assert not any("benchmarks/results/" in name for name in wheel | sdist)


def test_distribution_rejects_unreviewed_payload_types(tmp_path: Path) -> None:
    source = tmp_path / "retailer-dump.sqlite"
    source.write_bytes(b"unreviewed payload")

    with pytest.raises(ValueError, match="unreviewed payload type"):
        check_distribution._add_expected_file(
            {},
            "makolet/retailer-dump.sqlite",
            source,
        )
    with pytest.raises(ValueError, match="unreviewed payload type"):
        check_distribution._scan_distribution_text(
            "makolet/retailer-dump.sqlite",
            source.read_bytes(),
        )


def test_distribution_scans_dockerfile_as_text() -> None:
    assert check_distribution._text_member("Dockerfile") is True
    payload = b"PASSWORD=placeholder; API_KEY=production-value-123456\n"  # secret-scan: allow

    with pytest.raises(ValueError, match="credential-assignment"):
        check_distribution._scan_distribution_text("Dockerfile", payload)


def test_distribution_inventory_rejects_missing_and_unexpected_members() -> None:
    expected = {"makolet/__init__.py", "makolet/py.typed"}

    check_distribution._require_exact_file_inventory(expected, expected, label="wheel")
    with pytest.raises(ValueError, match=r"missing=.*py.typed"):
        check_distribution._require_exact_file_inventory(
            {"makolet/__init__.py"}, expected, label="wheel"
        )
    with pytest.raises(ValueError, match=r"unexpected=.*local.key"):
        check_distribution._require_exact_file_inventory(
            {*expected, "makolet/local.key"}, expected, label="wheel"
        )


def test_distribution_text_scan_rejects_secrets_and_local_host_paths_without_echoing() -> None:
    access_key = "AKIA" + "A" * 16
    local_path = "C:" + "\\Users\\release-user\\makolet"

    with pytest.raises(ValueError, match="aws-access-key") as secret_error:
        check_distribution._scan_distribution_text(
            "makolet/settings.py", f"VALUE = '{access_key}'\n".encode()
        )
    assert access_key not in str(secret_error.value)

    with pytest.raises(ValueError, match="local-host-path") as path_error:
        check_distribution._scan_distribution_text(
            "makolet/settings.py", f"PATH = '{local_path}'\n".encode()
        )
    assert local_path not in str(path_error.value)


def test_distribution_text_scan_accepts_documented_placeholders() -> None:
    check_distribution._scan_distribution_text(
        ".env.example",
        b"PASSWORD=makolet-development-only-change-me\nAPI_KEY=example-secret\n",
    )


def test_distribution_text_scan_checks_every_assignment_and_uri_candidate() -> None:
    assignment_payload = (
        b"PASSWORD=placeholder; API_KEY=production-value-123456\n"  # secret-scan: allow
    )
    uri_payload = (
        b"postgresql://user:placeholder@localhost/first "
        b"postgresql://user:production-value-123456@localhost/second\n"  # secret-scan: allow
    )

    with pytest.raises(ValueError, match="credential-assignment") as assignment_error:
        check_distribution._scan_distribution_text("makolet/settings.py", assignment_payload)
    with pytest.raises(ValueError, match="credential-in-uri") as uri_error:
        check_distribution._scan_distribution_text("makolet/settings.py", uri_payload)

    assert b"production-value-123456".decode() not in str(assignment_error.value)
    assert b"production-value-123456".decode() not in str(uri_error.value)


def test_distribution_text_scan_rejects_uri_valued_and_prefixed_credentials() -> None:
    payloads = (
        b"PASSWORD=https://example.invalid/production-credential-9f3b1c2d\n",  # secret-scan: allow
        b"AWS_SECRET_ACCESS_KEY=foo://production-credential-9f3b1c2d\n",  # secret-scan: allow
        b"TOKEN=contest-exampled-production-credential-9f3b1c2d\n",  # secret-scan: allow
        b"MAKOLET_S3_SECRET_KEY=production-credential-9f3b1c2d\n",  # secret-scan: allow
    )
    for payload in payloads:
        with pytest.raises(ValueError, match="credential-assignment") as error:
            check_distribution._scan_distribution_text("makolet/settings.py", payload)
        assert b"production-credential-9f3b1c2d".decode() not in str(error.value)


def test_every_reviewed_sdist_text_member_passes_the_bounded_scan() -> None:
    for member_name, source_path in check_distribution._expected_sdist_repository_files().items():
        check_distribution._scan_distribution_text(member_name, source_path.read_bytes())


def test_distribution_metadata_matches_the_complete_project_contract() -> None:
    distribution = metadata.distribution("makolet")
    raw_metadata = distribution.read_text("METADATA")
    assert raw_metadata is not None
    installed = BytesParser(policy=policy.default).parsebytes(raw_metadata.encode())

    check_distribution._validate_project_metadata(installed, label="installed metadata")

    changed = BytesParser(policy=policy.default).parsebytes(raw_metadata.encode())
    changed.replace_header("Requires-Python", ">=3.14")
    with pytest.raises(ValueError, match="Requires-Python"):
        check_distribution._validate_project_metadata(changed, label="changed metadata")

    changed = BytesParser(policy=policy.default).parsebytes(raw_metadata.encode())
    changed.replace_header("Requires-Python", "<3.15,>=3.14.7,>=3.14.7")
    with pytest.raises(ValueError, match="Requires-Python"):
        check_distribution._validate_project_metadata(changed, label="changed metadata")

    changed = BytesParser(policy=policy.default).parsebytes(raw_metadata.encode())
    changed.replace_header("Requires-Dist", "unexpected==1")
    with pytest.raises(ValueError, match="Requires-Dist"):
        check_distribution._validate_project_metadata(changed, label="changed metadata")


def test_distribution_entry_point_is_exact() -> None:
    check_distribution._validate_entry_points_text(
        "[console_scripts]\nmakolet = makolet.interfaces.cli:main\n",
        label="wheel entry points",
    )

    with pytest.raises(ValueError, match="console entry point"):
        check_distribution._validate_entry_points_text(
            "[console_scripts]\nmakolet = other.module:main\n",
            label="wheel entry points",
        )


def test_advertised_cli_paths_equal_the_registered_command_tree() -> None:
    from makolet.interfaces.cli import build_cli

    registered = check_distribution._registered_cli_help_paths(build_cli())

    assert len(registered) == 59
    assert len(check_distribution.ADVERTISED_CLI_HELP_PATHS) == 59
    assert set(registered) == set(check_distribution.ADVERTISED_CLI_HELP_PATHS)


def test_installed_postgres_proof_target_is_explicit_unique_and_loopback() -> None:
    database_name = "makolet_test_distribution_01234567"
    target = check_distribution._postgres_proof_target(
        {
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_URL": (
                f"postgresql://makolet:test-password@127.0.0.1:55432/{database_name}"
            ),
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM": database_name,
        }
    )

    assert target.database_name == database_name
    assert target.url.endswith(f"/{database_name}")


@pytest.mark.parametrize(
    ("url", "confirmation"),
    [
        (
            "postgresql://makolet:test-password@db.example.test/makolet_test_distribution_01234567",
            "makolet_test_distribution_01234567",
        ),
        (
            "postgresql://makolet:test-password@127.0.0.2/makolet_test_distribution_01234567",
            "makolet_test_distribution_01234567",
        ),
        (
            "postgresql://@127.0.0.1/makolet_test_distribution_01234567",
            "makolet_test_distribution_01234567",
        ),
        (
            "postgresql://makolet:test-password@127.0.0.1/"
            "makolet_test_distribution_01234567?host=db.example.test",
            "makolet_test_distribution_01234567",
        ),
        (
            "postgresql://makolet:test-password@127.0.0.1/makolet_test_coverage",
            "makolet_test_coverage",
        ),
        (
            "postgresql://makolet:test-password@127.0.0.1/makolet_test_distribution_01234567",
            "makolet_test_distribution_89abcdef",
        ),
    ],
)
def test_installed_postgres_proof_rejects_unowned_targets(
    url: str,
    confirmation: str,
) -> None:
    with pytest.raises(ValueError, match="distribution PostgreSQL proof"):
        check_distribution._postgres_proof_target(
            {
                "MAKOLET_DISTRIBUTION_TEST_DATABASE_URL": url,
                "MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM": confirmation,
            }
        )


def test_installed_postgres_status_requires_postgresql_18_and_exact_head() -> None:
    empty = {
        "status": "not_ready",
        "postgresql_version_number": 180004,
        "schema_ready": False,
        "expected_migration_heads": ["0011_resource_probe_budgets"],
        "current_migration_revisions": [],
        "migration_revision": None,
    }
    ready = {
        **empty,
        "status": "ready",
        "schema_ready": True,
        "current_migration_revisions": ["0011_resource_probe_budgets"],
        "migration_revision": "0011_resource_probe_budgets",
    }

    check_distribution._validate_postgres_status(empty, expect_empty=True)
    check_distribution._validate_postgres_status(ready, expect_empty=False)
    with pytest.raises(ValueError, match="PostgreSQL 18"):
        check_distribution._validate_postgres_status(
            {**empty, "postgresql_version_number": 170009}, expect_empty=True
        )
    with pytest.raises(ValueError, match="migration head"):
        check_distribution._validate_postgres_status(
            {**ready, "migration_revision": "0010_bounded_query_paths"},
            expect_empty=False,
        )


def test_installed_postgres_proof_only_runs_status_migrate_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = check_distribution._postgres_proof_target(
        {
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_URL": (
                "postgresql://makolet:test-password@127.0.0.1:55432/"
                "makolet_test_distribution_01234567"
            ),
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM": ("makolet_test_distribution_01234567"),
        }
    )
    preflight_targets: list[object] = []
    commands: list[list[str]] = []
    empty = {
        "status": "not_ready",
        "postgresql_version_number": 180004,
        "schema_ready": False,
        "expected_migration_heads": ["0011_resource_probe_budgets"],
        "current_migration_revisions": [],
        "migration_revision": None,
    }
    ready = {
        **empty,
        "status": "ready",
        "schema_ready": True,
        "current_migration_revisions": ["0011_resource_probe_budgets"],
        "migration_revision": "0011_resource_probe_budgets",
    }
    responses = iter((empty, ready, ready))

    async def require_empty(candidate: object) -> None:
        preflight_targets.append(candidate)

    def run_command(
        command: list[str],
        *,
        cwd: Path,
        environment: object,
        label: str,
    ) -> str:
        del environment, label
        assert cwd == tmp_path
        commands.append(command)
        return json.dumps(next(responses))

    monkeypatch.setattr(check_distribution, "_postgres_proof_target", lambda: target)
    monkeypatch.setattr(check_distribution, "_require_empty_postgres_database", require_empty)
    monkeypatch.setattr(
        check_distribution,
        "_virtual_environment_cli",
        lambda environment: environment / "makolet",
    )
    monkeypatch.setattr(check_distribution, "_run_command", run_command)

    check_distribution._verify_installed_postgres(tmp_path)

    assert preflight_targets == [target]
    assert [command[1:] for command in commands] == [
        ["database", "status", "--json"],
        ["database", "migrate", "--json"],
        ["database", "status", "--json"],
    ]
    assert not {"create", "drop"}.intersection(
        part.casefold() for command in commands for part in command
    )


def test_installed_runtime_environment_uses_a_supported_log_level(tmp_path: Path) -> None:
    target = check_distribution._postgres_proof_target(
        {
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_URL": (
                "postgresql://makolet:test-password@127.0.0.1:55432/"
                "makolet_test_distribution_01234567"
            ),
            "MAKOLET_DISTRIBUTION_TEST_DATABASE_CONFIRM": ("makolet_test_distribution_01234567"),
        }
    )

    environment = check_distribution._installed_runtime_environment(
        {"PATH": "test-path", "MAKOLET_UNRELATED": "must-not-leak"},
        target=target,
        runtime_directory=tmp_path,
    )

    assert environment["MAKOLET_LOG_LEVEL"] == "ERROR"
    assert environment["MAKOLET_ENVIRONMENT"] == "test"
    assert environment["MAKOLET_DATABASE_ALLOW_INSECURE_LOCAL"] == "true"
    assert environment["MAKOLET_DATABASE_URL"] == target.url
    assert "MAKOLET_UNRELATED" not in environment


def test_artifact_comparison_requires_exact_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first" / "makolet-0.1.0.tar.gz"
    second = tmp_path / "second" / first.name
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_bytes(b"same artifact")
    second.write_bytes(b"same artifact")

    digest = check_distribution._require_identical_artifact(first, second, "sdist")

    assert digest == sha256(b"same artifact").hexdigest()
    second.write_bytes(b"other artifact")
    with pytest.raises(ValueError, match="sdist is not reproducible"):
        check_distribution._require_identical_artifact(first, second, "sdist")


def test_reproducible_mode_reports_both_artifact_hashes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    received: list[bool] = []

    def verify(*, verify_installed_postgres: bool) -> tuple[str, str, int, int]:
        received.append(verify_installed_postgres)
        return ("a" * 64, "b" * 64, 100, 269)

    monkeypatch.setattr(
        check_distribution,
        "_verify_reproducible_distribution",
        verify,
    )

    assert check_distribution.main(["--reproducible"]) == 0
    assert check_distribution.main(["--reproducible", "--verify-installed-postgres"]) == 0

    output = capsys.readouterr().out
    assert received == [False, True]
    assert f"wheel_sha256={'a' * 64} (100 members)" in output
    assert f"sdist_sha256={'b' * 64} (269 members)" in output
