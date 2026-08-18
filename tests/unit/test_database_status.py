from __future__ import annotations

import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from makolet.adapters.persistence.database import DatabaseHealth
from makolet.composition import (
    _alembic_configuration_path,
    _database_is_ready,
    _expected_alembic_heads,
    _PostgresDatabaseOperations,
)


class _ScalarResult:
    def __init__(self, values: Sequence[object]) -> None:
        self._values = tuple(values)

    def scalar_one_or_none(self) -> object | None:
        if len(self._values) > 1:
            raise AssertionError("scalar result contains more than one row")
        return self._values[0] if self._values else None

    def scalars(self) -> _ScalarResult:
        return self

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self._values)


class _Connection:
    def __init__(self, revisions: Sequence[str] | None) -> None:
        self._revisions = revisions
        self.statements: list[str] = []

    async def execute(self, statement: object) -> _ScalarResult:
        sql = str(statement)
        self.statements.append(sql)
        if "to_regclass" in sql:
            return (
                _ScalarResult(())
                if self._revisions is None
                else _ScalarResult(("alembic_version",))
            )
        if "FROM alembic_version" in sql:
            return _ScalarResult(self._revisions or ())
        raise AssertionError(f"unexpected schema-status statement: {sql}")


class _ConnectionContext:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _Connection:
        return self._connection

    async def __aexit__(self, *args: object) -> None:
        del args


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    def connect(self) -> _ConnectionContext:
        return _ConnectionContext(self._connection)


class _Database:
    def __init__(self, revisions: Sequence[str] | None) -> None:
        self.connection = _Connection(revisions)
        self.engine = _Engine(self.connection)

    async def health(self) -> DatabaseHealth:
        return DatabaseHealth(server_version_number=180004, server_version="PostgreSQL 18.4")


def _operations(revisions: Sequence[str] | None) -> _PostgresDatabaseOperations:
    return _PostgresDatabaseOperations(
        cast(Any, _Database(revisions)),
        "postgresql://unused",
        cast(Any, object()),
        cast(Any, object()),
        expected_migration_revisions=("0002_head",),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("revisions", "expected_current"),
    [
        (None, ()),
        (("0001_previous",), ("0001_previous",)),
        (("other_branch",), ("other_branch",)),
        (("0002_head", "other_branch"), ("0002_head", "other_branch")),
    ],
)
async def test_database_status_reports_non_ready_schema(
    revisions: Sequence[str] | None,
    expected_current: tuple[str, ...],
) -> None:
    operations = _operations(revisions)

    status = await operations.status()

    assert status == {
        "status": "not_ready",
        "postgresql_version_number": 180004,
        "postgresql_version": "PostgreSQL 18.4",
        "schema_ready": False,
        "expected_migration_heads": ("0002_head",),
        "current_migration_revisions": expected_current,
        "migration_revision": expected_current[0] if len(expected_current) == 1 else None,
    }
    assert await _database_is_ready(operations) is False


@pytest.mark.asyncio
async def test_database_status_is_ready_only_at_exact_head() -> None:
    operations = _operations(("0002_head",))

    status = await operations.status()

    assert status["status"] == "ready"
    assert status["schema_ready"] is True
    assert status["expected_migration_heads"] == ("0002_head",)
    assert status["current_migration_revisions"] == ("0002_head",)
    assert status["migration_revision"] == "0002_head"
    assert await _database_is_ready(operations) is True


@pytest.mark.asyncio
async def test_database_readiness_is_false_when_database_is_unreachable() -> None:
    class UnavailableOperations:
        async def status(self) -> dict[str, object]:
            raise OSError("database is unavailable")

    assert await _database_is_ready(cast(Any, UnavailableOperations())) is False


def test_expected_alembic_head_comes_from_the_repository_graph() -> None:
    heads = _expected_alembic_heads()

    assert heads
    assert tuple(sorted(heads)) == heads
    assert all(re.fullmatch(r"[A-Za-z0-9_-]{1,64}", revision) for revision in heads)


def test_alembic_configuration_falls_back_to_packaged_migrations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import makolet.composition as composition_module

    missing_repository = tmp_path / "missing-repository.ini"
    missing_migrations = tmp_path / "missing-migrations"
    packaged_configuration = tmp_path / "makolet" / "_alembic.ini"
    packaged_migrations = tmp_path / "makolet" / "_migrations"
    packaged_configuration.parent.mkdir()
    packaged_configuration.write_text("[alembic]\n", encoding="utf-8")
    packaged_migrations.mkdir()
    monkeypatch.setattr(
        composition_module,
        "_REPOSITORY_ALEMBIC_CONFIGURATION",
        missing_repository,
    )
    monkeypatch.setattr(composition_module, "_REPOSITORY_MIGRATIONS", missing_migrations)
    monkeypatch.setattr(
        composition_module,
        "_PACKAGED_ALEMBIC_CONFIGURATION",
        packaged_configuration,
    )
    monkeypatch.setattr(composition_module, "_PACKAGED_MIGRATIONS", packaged_migrations)

    assert _alembic_configuration_path() == packaged_configuration
