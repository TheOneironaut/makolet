"""Async PostgreSQL engine lifecycle and health checks."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from typing import Final

from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

MINIMUM_POSTGRESQL_VERSION: Final = 180000
DEFAULT_STATEMENT_TIMEOUT_MS: Final = 30_000
_POSTGRES_TLS_QUERY_KEYS: Final = frozenset({"ssl", "sslmode"})
_POSTGRES_TLS_MODES: Final = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)


@dataclass(frozen=True, slots=True)
class DatabaseHealth:
    server_version_number: int
    server_version: str


class Database:
    """Own one process-wide SQLAlchemy engine backed by asyncpg."""

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        sqlalchemy_url: URL,
        tls_mode: str | None,
        statement_timeout_ms: int | None = None,
    ) -> None:
        self.engine = engine
        self._sqlalchemy_url = sqlalchemy_url
        self._tls_mode = tls_mode
        self._statement_timeout_ms = statement_timeout_ms

    @classmethod
    def from_url(
        cls,
        url: str | URL,
        *,
        pool_size: int = 10,
        max_overflow: int = 20,
        statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS,
        application_name: str = "makolet",
    ) -> Database:
        if pool_size < 1 or max_overflow < 0:
            raise ValueError("pool_size must be positive and max_overflow cannot be negative")
        if statement_timeout_ms < 1:
            raise ValueError("statement_timeout_ms must be positive")
        sqlalchemy_url, tls_mode = _asyncpg_url(url)
        engine = create_async_engine(
            sqlalchemy_url,
            connect_args=_asyncpg_connect_args(
                tls_mode=tls_mode,
                application_name=application_name,
                statement_timeout_ms=statement_timeout_ms,
            ),
            hide_parameters=True,
            max_overflow=max_overflow,
            pool_pre_ping=True,
            pool_size=pool_size,
        )
        return cls(
            engine,
            sqlalchemy_url=sqlalchemy_url,
            tls_mode=tls_mode,
            statement_timeout_ms=statement_timeout_ms,
        )

    @classmethod
    def unpooled_from_url(
        cls,
        url: str | URL,
        *,
        application_name: str,
    ) -> Database:
        """Own an unpooled engine with the same centralized TLS handling."""

        sqlalchemy_url, tls_mode = _asyncpg_url(url)
        engine = cls._create_unpooled_engine(
            sqlalchemy_url,
            tls_mode=tls_mode,
            application_name=application_name,
        )
        return cls(
            engine,
            sqlalchemy_url=sqlalchemy_url,
            tls_mode=tls_mode,
            statement_timeout_ms=None,
        )

    def create_unpooled_engine(self, *, application_name: str) -> AsyncEngine:
        """Create a derived engine without dropping validated connection policy."""

        return self._create_unpooled_engine(
            self._sqlalchemy_url,
            tls_mode=self._tls_mode,
            application_name=application_name,
            statement_timeout_ms=self._statement_timeout_ms,
        )

    @staticmethod
    def _create_unpooled_engine(
        sqlalchemy_url: URL,
        *,
        tls_mode: str | None,
        application_name: str,
        statement_timeout_ms: int | None = None,
    ) -> AsyncEngine:
        return create_async_engine(
            sqlalchemy_url,
            connect_args=_asyncpg_connect_args(
                tls_mode=tls_mode,
                application_name=application_name,
                statement_timeout_ms=statement_timeout_ms,
            ),
            hide_parameters=True,
            poolclass=NullPool,
        )

    async def health(self) -> DatabaseHealth:
        async with self.engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT current_setting('server_version_num')::integer AS version_number, "
                        "version() AS version"
                    )
                )
            ).one()
        version_number = int(row.version_number)
        if version_number < MINIMUM_POSTGRESQL_VERSION:
            raise RuntimeError(
                f"PostgreSQL 18 or newer is required; server reports {version_number}"
            )
        return DatabaseHealth(
            server_version_number=version_number,
            server_version=str(row.version),
        )

    async def dispose(self) -> None:
        await self.engine.dispose()


def _asyncpg_url(url: str | URL) -> tuple[URL, str | None]:
    parsed = make_url(url) if isinstance(url, str) else url
    if parsed.get_backend_name() != "postgresql":
        raise ValueError("Makolet persistence requires a PostgreSQL URL")
    tls_modes: list[str] = []
    retained_query: dict[str, str | tuple[str, ...]] = {}
    for key, value in parsed.query.items():
        if key.strip().casefold() not in _POSTGRES_TLS_QUERY_KEYS:
            retained_query[key] = value
            continue
        values = value if isinstance(value, tuple) else (value,)
        tls_modes.extend(item.strip().casefold() for item in values)
    if len(tls_modes) > 1:
        raise ValueError("database URL must specify exactly one TLS mode")
    tls_mode = tls_modes[0] if tls_modes else None
    if tls_mode is not None and tls_mode not in _POSTGRES_TLS_MODES:
        raise ValueError("database URL contains an unsupported TLS mode")
    return parsed.set(drivername="postgresql+asyncpg", query=retained_query), tls_mode


def _asyncpg_ssl_argument(mode: str) -> ssl.SSLContext | str | bool:
    if mode == "disable":
        return False
    if mode == "verify-full":
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        return context
    return mode


def _asyncpg_connect_args(
    *,
    tls_mode: str | None,
    application_name: str,
    statement_timeout_ms: int | None = None,
) -> dict[str, object]:
    server_settings = {
        "application_name": application_name,
        "timezone": "UTC",
    }
    if statement_timeout_ms is not None:
        server_settings["statement_timeout"] = str(statement_timeout_ms)
    connect_args: dict[str, object] = {"server_settings": server_settings}
    if tls_mode is not None:
        connect_args["ssl"] = _asyncpg_ssl_argument(tls_mode)
    return connect_args
