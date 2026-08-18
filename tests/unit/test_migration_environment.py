from __future__ import annotations

import runpy
import ssl
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from alembic import context as alembic_context
from pydantic import SecretStr
from sqlalchemy.engine import URL

import makolet.adapters.persistence.database as database_module
import makolet.config as config_module
from makolet.config import MakoletSettings

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class _FakeMigrationConnection:
    async def __aenter__(self) -> _FakeMigrationConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def run_sync(self, _callback: object) -> None:
        return None


class _FakeMigrationEngine:
    def __init__(self) -> None:
        self.disposed = False

    def connect(self) -> _FakeMigrationConnection:
        return _FakeMigrationConnection()

    async def dispose(self) -> None:
        self.disposed = True


def test_migration_logging_preserves_existing_library_loggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment="test",
        database_url=SecretStr("postgresql://makolet@localhost/makolet_test"),
    )
    calls: list[tuple[str, bool]] = []

    def configure_file_logging(
        filename: str,
        *,
        disable_existing_loggers: bool,
    ) -> None:
        calls.append((filename, disable_existing_loggers))

    monkeypatch.setattr("logging.config.fileConfig", configure_file_logging)
    monkeypatch.setattr(config_module, "load_settings", lambda: settings)
    monkeypatch.setattr(
        alembic_context,
        "config",
        SimpleNamespace(config_file_name="alembic.ini"),
        raising=False,
    )
    monkeypatch.setattr(alembic_context, "is_offline_mode", lambda: True)
    monkeypatch.setattr(alembic_context, "configure", lambda **_arguments: None)
    monkeypatch.setattr(alembic_context, "begin_transaction", nullcontext)
    monkeypatch.setattr(alembic_context, "run_migrations", lambda: None)

    runpy.run_path(str(REPOSITORY_ROOT / "migrations/env.py"))

    assert calls == [("alembic.ini", False)]


@pytest.mark.parametrize(
    ("database_url", "allow_insecure_local", "verified"),
    [
        (
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full",
            False,
            True,
        ),
        ("postgresql://makolet@127.0.0.1/makolet", True, False),
    ],
)
def test_online_migration_uses_validated_database_tls_factory(
    monkeypatch: pytest.MonkeyPatch,
    database_url: str,
    *,
    allow_insecure_local: bool,
    verified: bool,
) -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(database_url),
        database_allow_insecure_local=allow_insecure_local,
    )
    settings_loads: list[None] = []
    central_calls: list[tuple[URL, dict[str, object]]] = []
    legacy_calls: list[tuple[dict[str, object], dict[str, object]]] = []
    engine = _FakeMigrationEngine()

    def load_settings() -> MakoletSettings:
        settings_loads.append(None)
        return settings

    def create_engine(url: URL, **kwargs: object) -> Any:
        central_calls.append((url, kwargs))
        return engine

    def create_legacy_engine(
        section: dict[str, object],
        **kwargs: object,
    ) -> _FakeMigrationEngine:
        legacy_calls.append((section, kwargs))
        return engine

    monkeypatch.setattr(config_module, "load_settings", load_settings)
    monkeypatch.setattr(database_module, "create_async_engine", create_engine)
    monkeypatch.setenv("MAKOLET_DATABASE_URL", database_url)
    monkeypatch.setattr(
        "sqlalchemy.ext.asyncio.async_engine_from_config",
        create_legacy_engine,
    )
    monkeypatch.setattr(
        alembic_context,
        "config",
        SimpleNamespace(
            config_file_name=None,
            config_ini_section="alembic",
            get_section=lambda _section: {},
        ),
        raising=False,
    )
    monkeypatch.setattr(alembic_context, "is_offline_mode", lambda: False)

    runpy.run_path(str(REPOSITORY_ROOT / "migrations/env.py"))

    assert settings_loads == [None]
    assert legacy_calls == []
    assert len(central_calls) == 1
    url, kwargs = central_calls[0]
    assert "ssl" not in url.query
    connect_args = cast(dict[str, object], kwargs["connect_args"])
    tls_argument = connect_args["ssl"]
    if verified:
        assert isinstance(tls_argument, ssl.SSLContext)
        assert tls_argument.check_hostname is True
        assert tls_argument.verify_mode is ssl.CERT_REQUIRED
    else:
        assert tls_argument is False
    assert engine.disposed is True
