from __future__ import annotations

import ssl
from typing import Any, cast

import pytest
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import NullPool

import makolet.adapters.persistence.database as database_module
from makolet.adapters.persistence.collection import PostgresCollectionLeaseManager
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.leases import PostgresLeaseManager


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


@pytest.mark.parametrize(
    ("pool_size", "max_overflow", "statement_timeout_ms", "message"),
    [
        (0, 20, 30_000, "pool_size must be positive"),
        (10, -1, 30_000, "max_overflow cannot be negative"),
        (10, 20, 0, "statement_timeout_ms must be positive"),
    ],
)
def test_database_rejects_invalid_resource_bounds_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
    pool_size: int,
    max_overflow: int,
    statement_timeout_ms: int,
    message: str,
) -> None:
    def unexpected_engine_creation(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("invalid database bounds must fail before engine creation")

    monkeypatch.setattr(database_module, "create_async_engine", unexpected_engine_creation)

    with pytest.raises(ValueError, match=message):
        Database.from_url(
            "postgresql://makolet@postgres/makolet",
            pool_size=pool_size,
            max_overflow=max_overflow,
            statement_timeout_ms=statement_timeout_ms,
        )


@pytest.mark.parametrize(
    ("url", "message"),
    [
        ("sqlite:///makolet.db", "requires a PostgreSQL URL"),
        (
            "postgresql://makolet@postgres/makolet?sslmode=trust-anything",
            "unsupported TLS mode",
        ),
    ],
)
def test_database_rejects_unsupported_backend_or_tls_before_engine_creation(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    message: str,
) -> None:
    def unexpected_engine_creation(*_args: object, **_kwargs: object) -> Any:
        raise AssertionError("invalid connection policy must fail before engine creation")

    monkeypatch.setattr(database_module, "create_async_engine", unexpected_engine_creation)

    with pytest.raises(ValueError, match=message):
        Database.from_url(url)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tls_query", "expected_tls"),
    [("", None), ("&sslmode=require", "require")],
)
async def test_unpooled_database_preserves_validated_connection_policy_and_disposes(
    monkeypatch: pytest.MonkeyPatch,
    tls_query: str,
    expected_tls: str | None,
) -> None:
    captured: dict[str, object] = {}
    engine = _FakeEngine()

    def create_engine(url: URL, **kwargs: object) -> Any:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return engine

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)

    database = Database.unpooled_from_url(
        "postgresql://makolet@postgres/makolet?target_session_attrs=read-write" + tls_query,
        application_name="makolet-backup-test",
    )
    await database.dispose()

    url = cast(URL, captured["url"])
    assert url.drivername == "postgresql+asyncpg"
    assert url.query == {"target_session_attrs": "read-write"}
    kwargs = cast(dict[str, object], captured["kwargs"])
    assert kwargs["poolclass"] is NullPool
    connect_args = cast(dict[str, object], kwargs["connect_args"])
    assert connect_args["server_settings"] == {
        "application_name": "makolet-backup-test",
        "timezone": "UTC",
    }
    if expected_tls is None:
        assert "ssl" not in connect_args
    else:
        assert connect_args["ssl"] == expected_tls
    assert engine.disposed is True


@pytest.mark.parametrize("parameter", ["sslmode", "ssl"])
def test_database_engine_propagates_verified_tls_context(
    monkeypatch: pytest.MonkeyPatch,
    parameter: str,
) -> None:
    captured: dict[str, object] = {}
    engine = cast(AsyncEngine, _FakeEngine())

    def create_engine(url: URL, **kwargs: object) -> Any:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return engine

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)

    database = Database.from_url(
        f"postgresql://makolet@database.example.test/makolet?{parameter}=verify-full"
    )

    assert database.engine is engine
    url = cast(URL, captured["url"])
    assert "ssl" not in url.query
    assert "sslmode" not in url.query
    kwargs = cast(dict[str, object], captured["kwargs"])
    connect_args = cast(dict[str, object], kwargs["connect_args"])
    context = connect_args["ssl"]
    assert isinstance(context, ssl.SSLContext)
    assert context.check_hostname is True
    assert context.verify_mode is ssl.CERT_REQUIRED


def test_database_engine_propagates_explicit_disabled_tls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def create_engine(url: URL, **kwargs: object) -> Any:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _FakeEngine()

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)

    Database.from_url("postgresql://makolet@postgres/makolet?sslmode=disable")

    kwargs = cast(dict[str, object], captured["kwargs"])
    connect_args = cast(dict[str, object], kwargs["connect_args"])
    assert connect_args["ssl"] is False


def test_database_engine_rejects_ambiguous_tls_query_variants() -> None:
    with pytest.raises(ValueError, match="exactly one TLS mode"):
        Database.from_url(
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full&ssl=disable"
        )


@pytest.mark.parametrize(
    ("url", "verified"),
    [
        (
            "postgresql://makolet@database.example.test/makolet?ssl=verify-full",
            True,
        ),
        ("postgresql://makolet@postgres/makolet?ssl=disable", False),
    ],
)
def test_collection_lease_engine_inherits_validated_database_tls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    *,
    verified: bool,
) -> None:
    engines: list[tuple[URL, dict[str, object]]] = []

    def create_engine(engine_url: URL, **kwargs: object) -> Any:
        engines.append((engine_url, kwargs))
        return _FakeEngine()

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)

    database = Database.from_url(url)
    PostgresCollectionLeaseManager(database)

    assert len(engines) == 2
    for engine_url, kwargs in engines:
        assert "ssl" not in engine_url.query
        connect_args = cast(dict[str, object], kwargs["connect_args"])
        tls_argument = connect_args["ssl"]
        if verified:
            assert isinstance(tls_argument, ssl.SSLContext)
            assert tls_argument.check_hostname is True
            assert tls_argument.verify_mode is ssl.CERT_REQUIRED
        else:
            assert tls_argument is False


def test_derived_lease_engines_inherit_database_statement_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engines: list[dict[str, object]] = []

    def create_engine(_engine_url: URL, **kwargs: object) -> Any:
        engines.append(kwargs)
        return _FakeEngine()

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)

    database = Database.from_url(
        "postgresql://makolet@postgres/makolet?sslmode=disable",
        application_name="makolet-test",
        statement_timeout_ms=1_234,
    )
    PostgresCollectionLeaseManager(database)
    PostgresLeaseManager(database)

    assert len(engines) == 3
    assert [
        cast(dict[str, str], cast(dict[str, object], engine["connect_args"])["server_settings"])
        for engine in engines
    ] == [
        {
            "application_name": "makolet-test",
            "timezone": "UTC",
            "statement_timeout": "1234",
        },
        {
            "application_name": "makolet-collection-lock",
            "timezone": "UTC",
            "statement_timeout": "1234",
        },
        {
            "application_name": "makolet-ingestion-lock",
            "timezone": "UTC",
            "statement_timeout": "1234",
        },
    ]
    assert "poolclass" not in engines[0]
    assert engines[1]["poolclass"] is NullPool
    assert engines[2]["poolclass"] is NullPool
