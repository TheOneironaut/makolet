"""Fail-closed ownership checks for destructive test and benchmark databases."""

from __future__ import annotations

import re
from collections.abc import Callable

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

_POSTGRESQL_DRIVERS = frozenset({"postgresql", "postgresql+asyncpg"})
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})
_TEST_DATABASE = re.compile(r"makolet_test_[a-z0-9_]{1,48}\Z")


class DestructiveDatabaseTargetError(ValueError):
    """Raised before destructive tooling reaches an unproved database target."""


def _require_target(
    raw_url: str,
    *,
    confirmation: str | None,
    purpose: str,
    database_matches: Callable[[str], bool],
) -> str:
    if not raw_url or "#" in raw_url:
        raise DestructiveDatabaseTargetError(
            f"{purpose} database URL is empty or contains a fragment"
        )
    try:
        parsed = make_url(raw_url)
    except (ArgumentError, ValueError) as error:
        raise DestructiveDatabaseTargetError(f"{purpose} database URL is invalid") from error
    if parsed.drivername not in _POSTGRESQL_DRIVERS:
        raise DestructiveDatabaseTargetError(f"{purpose} requires a PostgreSQL database URL")
    if parsed.username is None:
        raise DestructiveDatabaseTargetError(f"{purpose} database URL requires an explicit user")
    host = (parsed.host or "").casefold().rstrip(".")
    if host not in _LOOPBACK_HOSTS:
        raise DestructiveDatabaseTargetError(
            f"{purpose} database must use an explicit loopback authority"
        )
    if parsed.query:
        raise DestructiveDatabaseTargetError(
            f"{purpose} database URL cannot contain driver query parameters"
        )
    database_name = parsed.database or ""
    if not database_matches(database_name):
        raise DestructiveDatabaseTargetError(f"{purpose} database name is not allowlisted")
    if confirmation != database_name:
        raise DestructiveDatabaseTargetError(
            f"{purpose} requires exact confirmation of database {database_name!r}"
        )
    return raw_url


def require_test_database_target(raw_url: str, *, confirmation: str | None) -> str:
    """Validate the exact owned target used by destructive integration/E2E fixtures."""

    return _require_target(
        raw_url,
        confirmation=confirmation,
        purpose="test setup",
        database_matches=lambda value: _TEST_DATABASE.fullmatch(value) is not None,
    )


def require_benchmark_database_target(raw_url: str, *, confirmation: str | None) -> str:
    """Validate the exact isolated scale database before its fixed schema is reset."""

    return _require_target(
        raw_url,
        confirmation=confirmation,
        purpose="benchmark",
        database_matches=lambda value: value == "makolet_benchmark",
    )
