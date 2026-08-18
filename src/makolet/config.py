"""Secret-safe runtime configuration loaded from ``MAKOLET_*`` settings."""

from __future__ import annotations

import ipaddress
import os
import re
import socket
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from makolet.application.ports import MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES

_SOURCE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z")
_POSTGRES_SCHEMES = frozenset({"postgresql", "postgresql+asyncpg"})
_POSTGRES_TLS_QUERY_KEYS = frozenset({"ssl", "sslmode"})
_POSTGRES_TLS_MODES = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)
_POSTGRES_HOST_OVERRIDE_QUERY_KEYS = frozenset({"host", "service", "servicefile"})
_S3_BUCKET = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_BUNDLED_DEVELOPMENT_SECRET = "makolet-development-only-change-me"  # noqa: S105  # secret-scan: allow
_BUNDLED_DEVELOPMENT_S3_ACCESS_KEY = "makolet-development"  # secret-scan: allow
_COMPOSE_ONLY_DOTENV_VARIABLES = frozenset(
    {
        "MAKOLET_BIND_ADDRESS",
        "MAKOLET_POSTGRES_PORT",
        "MAKOLET_PROMETHEUS_PORT",
        "MAKOLET_S3_PORT",
        "POSTGRES_DB",
        "POSTGRES_PASSWORD",
        "POSTGRES_USER",
    }
)
type IntervalSeconds = Annotated[int, Field(ge=1, le=7 * 24 * 60 * 60)]


class _ApplicationDotEnvSettingsSource(DotEnvSettingsSource):
    """Exclude only documented Compose interpolation variables from app settings."""

    def __call__(self) -> dict[str, Any]:
        values = super().__call__()
        for variable in _COMPOSE_ONLY_DOTENV_VARIABLES:
            values.pop(self._apply_case_sensitive(variable), None)
        return values


class ConfigurationError(RuntimeError):
    """Runtime settings are absent or invalid without echoing secret values."""

    code = "configuration_error"


class MakoletSettings(BaseSettings):
    """One validated configuration shared by every process entry point."""

    model_config = SettingsConfigDict(
        env_prefix="MAKOLET_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        case_sensitive=False,
        validate_default=True,
    )

    environment: Literal["development", "test", "production"] = "development"
    database_url: SecretStr = SecretStr("postgresql://makolet@127.0.0.1:5432/makolet")
    database_allow_insecure_local: bool = False
    database_pool_size: Annotated[int, Field(ge=1, le=100)] = 10
    database_max_overflow: Annotated[int, Field(ge=0, le=200)] = 20
    database_statement_timeout_ms: Annotated[int, Field(ge=100, le=600_000)] = 30_000
    archive_root: Path = Path("raw-archive")
    archive_backend: Literal["local", "s3"] = "local"
    archive_maximum_object_bytes: Annotated[int, Field(ge=1024, le=16 * 1024 * 1024 * 1024)] = (
        2 * 1024 * 1024 * 1024
    )
    archive_minimum_free_bytes: Annotated[int, Field(ge=0, le=16 * 1024 * 1024 * 1024 * 1024)] = (
        1024 * 1024 * 1024
    )
    s3_endpoint: str = "http://127.0.0.1:8333"
    s3_allow_insecure_local: bool = False
    s3_bucket: str = "makolet-raw"
    s3_region: str = "us-east-1"
    s3_access_key: SecretStr = SecretStr("")
    s3_secret_key: SecretStr = SecretStr("")
    s3_key_prefix: str = "raw"
    s3_path_style: bool = True
    export_root: Path = Path("exports")

    source_listing_timeout_seconds: Annotated[float, Field(gt=0, le=300)] = 20
    source_download_timeout_seconds: Annotated[float, Field(gt=0, le=86_400)] = 600
    allow_insecure_ftp: bool = False
    ingestion_discovery_page_size: Annotated[int, Field(ge=1, le=500)] = 100
    ingestion_maximum_files_per_source_run: Annotated[int, Field(ge=1, le=10_000)] = 10_000
    ingestion_maximum_charged_bytes_per_source_run: Annotated[
        int, Field(ge=1024, le=16 * 1024 * 1024 * 1024 * 1024)
    ] = 8 * 1024 * 1024 * 1024
    ingestion_maximum_charged_bytes_per_source_day: Annotated[
        int, Field(ge=1024, le=16 * 1024 * 1024 * 1024 * 1024)
    ] = 32 * 1024 * 1024 * 1024
    ingestion_maximum_source_identities_per_source_day: Annotated[int, Field(ge=1, le=100_000)] = (
        2_000
    )
    ingestion_maximum_transfer_attempts_per_source_day: Annotated[int, Field(ge=1, le=100_000)] = (
        4_000
    )
    ingestion_maximum_successes_per_source_day: Annotated[int, Field(ge=1, le=100_000)] = 2_000
    ingestion_minimum_full_records: Annotated[int, Field(ge=0, le=10_000_000)] | None = None
    ingestion_minimum_full_store_records: Annotated[int, Field(ge=0, le=10_000_000)] = 1
    ingestion_minimum_full_price_records: Annotated[int, Field(ge=0, le=10_000_000)] = 100
    ingestion_minimum_full_promotion_records: Annotated[int, Field(ge=0, le=10_000_000)] = 1
    ingestion_maximum_full_snapshot_drop_fraction: Annotated[float, Field(ge=0, lt=1)] = 0.50
    ingestion_maximum_record_rejection_fraction: Annotated[float, Field(ge=0, lt=1)] = 0.10
    ingestion_maximum_validation_issues: Annotated[int, Field(ge=1, le=5_000_000)] = 100_000
    ingestion_maximum_validation_issue_bytes: Annotated[int, Field(ge=1, le=1024 * 1024 * 1024)] = (
        64 * 1024 * 1024
    )
    ingestion_maximum_validation_issue_evidence: Annotated[int, Field(ge=1, le=100_000)] = 1_000

    api_host: str = "127.0.0.1"
    api_port: Annotated[int, Field(ge=1, le=65_535)] = 8000
    api_http_maximum_concurrency: Annotated[int, Field(ge=1, le=10_000)] = 100
    mcp_host: str = "127.0.0.1"
    mcp_port: Annotated[int, Field(ge=1, le=65_535)] = 8001
    mcp_allowed_origins: tuple[str, ...] = ()
    mcp_http_body_timeout_seconds: Annotated[float, Field(gt=0, le=300)] = 10
    mcp_http_maximum_concurrency: Annotated[int, Field(ge=1, le=10_000)] = 100

    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    worker_metrics_host: str = "127.0.0.1"
    worker_metrics_port: Annotated[int, Field(ge=1, le=65_535)] = 9100
    worker_concurrency: Annotated[int, Field(ge=1, le=64)] = 4
    worker_queue_capacity: Annotated[int, Field(ge=1, le=10_000)] = 64
    worker_maximum_sources: Annotated[int, Field(ge=1, le=10_000)] = 1_000
    worker_heartbeat_seconds: IntervalSeconds = 30
    worker_poll_seconds: IntervalSeconds = 1
    worker_stale_after_seconds: IntervalSeconds = 2 * 60 * 60
    worker_stale_recovery_seconds: IntervalSeconds = 15 * 60
    worker_shutdown_grace_seconds: IntervalSeconds = 30
    worker_jitter_ratio: Annotated[float, Field(ge=0, le=1)] = 0.10
    source_intervals_seconds: dict[str, IntervalSeconds] = Field(default_factory=dict)
    enabled_sources: tuple[str, ...] = ()

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        if not isinstance(dotenv_settings, DotEnvSettingsSource):
            return init_settings, env_settings, dotenv_settings, file_secret_settings
        application_dotenv = _ApplicationDotEnvSettingsSource(
            settings_cls,
            env_file=dotenv_settings.env_file,
            env_file_encoding=dotenv_settings.env_file_encoding,
            dotenv_filtering=dotenv_settings.dotenv_filtering,
            case_sensitive=dotenv_settings.case_sensitive,
            env_prefix=dotenv_settings.env_prefix,
            env_prefix_target=dotenv_settings.env_prefix_target,
            env_nested_delimiter=dotenv_settings.env_nested_delimiter,
            env_nested_max_split=dotenv_settings.env_nested_max_split,
            env_ignore_empty=dotenv_settings.env_ignore_empty,
            env_parse_none_str=dotenv_settings.env_parse_none_str,
            env_parse_enums=dotenv_settings.env_parse_enums,
            _init_state=dotenv_settings._init_state,
        )
        return init_settings, env_settings, application_dotenv, file_secret_settings

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        parsed = urlsplit(value.get_secret_value())
        if (
            parsed.scheme not in _POSTGRES_SCHEMES
            or not parsed.hostname
            or not parsed.path.strip("/")
            or parsed.fragment
        ):
            raise ValueError(
                "database URL must identify a PostgreSQL host and database without a fragment"
            )
        _database_tls_mode(_database_query_pairs(parsed.query))
        return value

    @field_validator("archive_root", "export_root")
    @classmethod
    def validate_local_path(cls, value: Path) -> Path:
        if "\x00" in str(value):
            raise ValueError("local path contains a null byte")
        return value.expanduser().resolve()

    @field_validator("s3_endpoint")
    @classmethod
    def validate_s3_endpoint(cls, value: str) -> str:
        return validate_s3_endpoint_transport(
            value,
            environment="development",
            allow_insecure_local=False,
            path_style=True,
        )

    @field_validator("s3_bucket")
    @classmethod
    def validate_s3_bucket(cls, value: str) -> str:
        if _S3_BUCKET.fullmatch(value) is None or ".." in value:
            raise ValueError("S3 bucket name is invalid")
        return value

    @field_validator("s3_region")
    @classmethod
    def validate_s3_region(cls, value: str) -> str:
        selected = value.strip()
        if not selected or len(selected) > 64 or any(ord(character) < 33 for character in selected):
            raise ValueError("S3 region is invalid")
        return selected

    @field_validator("s3_key_prefix")
    @classmethod
    def validate_s3_key_prefix(cls, value: str) -> str:
        selected = value.strip("/")
        if any(part in {"", ".", ".."} for part in selected.split("/")):
            raise ValueError("S3 key prefix is not canonical")
        return selected

    @field_validator("api_host", "mcp_host", "worker_metrics_host")
    @classmethod
    def validate_bind_host(cls, value: str) -> str:
        selected = value.strip()
        if (
            not selected
            or len(selected) > 253
            or any(character.isspace() for character in selected)
        ):
            raise ValueError("bind host is empty or invalid")
        return selected

    @field_validator("mcp_allowed_origins")
    @classmethod
    def validate_origins(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        unique: dict[str, None] = {}
        for value in values:
            parsed = urlsplit(value)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise ValueError("MCP origins must be absolute HTTP(S) origins")
            if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
                raise ValueError("MCP origins cannot include a path, query, or fragment")
            unique[value.rstrip("/")] = None
        return tuple(unique)

    @field_validator("worker_id")
    @classmethod
    def validate_worker_id(cls, value: str) -> str:
        selected = value.strip()
        if _SOURCE_ID.fullmatch(selected) is None:
            raise ValueError(
                "worker ID must use 1-200 ASCII letters, digits, dots, underscores, "
                "colons, or hyphens and must start with a letter or digit"
            )
        return selected

    @field_validator("source_intervals_seconds")
    @classmethod
    def validate_source_intervals(
        cls, values: dict[str, IntervalSeconds]
    ) -> dict[str, IntervalSeconds]:
        if len(values) > 10_000:
            raise ValueError("too many source intervals are configured")
        for source_id in values:
            if _SOURCE_ID.fullmatch(source_id) is None:
                raise ValueError(f"invalid source ID: {source_id!r}")
        return dict(sorted(values.items()))

    @field_validator("enabled_sources")
    @classmethod
    def validate_enabled_sources(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        unique: dict[str, None] = {}
        for source_id in values:
            if _SOURCE_ID.fullmatch(source_id) is None:
                raise ValueError(f"invalid source ID: {source_id!r}")
            unique[source_id] = None
        return tuple(unique)

    @model_validator(mode="after")
    def validate_source_selection(self) -> MakoletSettings:
        if self.enabled_sources:
            missing = set(self.enabled_sources).difference(self.source_intervals_seconds)
            if missing:
                raise ValueError("enabled sources require configured collection intervals")
        if self.archive_backend == "s3" and (
            not self.s3_access_key.get_secret_value() or not self.s3_secret_key.get_secret_value()
        ):
            raise ValueError("S3 archive credentials are required for the S3 backend")
        if not (
            self.archive_maximum_object_bytes + MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES
            <= self.ingestion_maximum_charged_bytes_per_source_run
            <= self.ingestion_maximum_charged_bytes_per_source_day
        ):
            raise ValueError(
                "charged-byte limits must cover the archive object plus bounded protocol "
                "and transfer-frame headroom, then be ordered by source run and source day"
            )
        if (
            self.ingestion_maximum_validation_issue_evidence
            > self.ingestion_maximum_validation_issues
        ):
            raise ValueError("validation issue evidence cannot exceed the issue-count limit")
        if (
            self.ingestion_maximum_source_identities_per_source_day
            > self.ingestion_maximum_transfer_attempts_per_source_day
        ):
            raise ValueError("source identity limit cannot exceed the transfer-attempt limit")
        return self

    @model_validator(mode="after")
    def validate_production_credentials(self) -> MakoletSettings:
        if self.environment != "production":
            return self
        parsed_database = urlsplit(self.database_dsn())
        query_passwords = tuple(
            value
            for key, value in parse_qsl(parsed_database.query, keep_blank_values=True)
            if key.casefold() == "password"
        )
        database_passwords = (
            *((parsed_database.password,) if parsed_database.password is not None else ()),
            *query_passwords,
        )
        if any(unquote(password) == _BUNDLED_DEVELOPMENT_SECRET for password in database_passwords):
            raise ValueError("production rejects the bundled development database credential")
        if self.archive_backend == "s3" and (
            self.s3_access_key.get_secret_value() == _BUNDLED_DEVELOPMENT_S3_ACCESS_KEY
            or self.s3_secret_key.get_secret_value() == _BUNDLED_DEVELOPMENT_SECRET
        ):
            raise ValueError("production rejects bundled development S3 credentials")
        return self

    @model_validator(mode="after")
    def validate_production_database_transport(self) -> MakoletSettings:
        if self.environment != "production":
            return self
        parsed = urlsplit(self.database_url.get_secret_value())
        query = _database_query_pairs(parsed.query)
        tls_mode = _database_tls_mode(query)
        if tls_mode == "verify-full":
            return self
        host = (parsed.hostname or "").casefold()
        has_host_override = any(
            key.strip().casefold() in _POSTGRES_HOST_OVERRIDE_QUERY_KEYS for key, _value in query
        )
        if (
            self.database_allow_insecure_local
            and _is_literal_loopback(host)
            and not has_host_override
            and tls_mode in {None, "disable"}
        ):
            return self
        raise ValueError(
            "production database connections require authenticated TLS with verify-full; "
            "the explicit insecure-local exception is limited to literal loopback IP "
            "endpoints"
        )

    @model_validator(mode="after")
    def validate_production_s3_transport(self) -> MakoletSettings:
        if self.archive_backend != "s3":
            return self
        validate_s3_endpoint_transport(
            self.s3_endpoint,
            environment=self.environment,
            allow_insecure_local=self.s3_allow_insecure_local,
            path_style=self.s3_path_style,
        )
        return self

    def database_dsn(self) -> str:
        """Reveal the DSN only at the database adapter boundary."""

        value = self.database_url.get_secret_value()
        default_tls_mode: str | None = None
        if self.environment == "production" and self.database_allow_insecure_local:
            parsed = urlsplit(value)
            query = _database_query_pairs(parsed.query)
            has_host_override = any(
                key.strip().casefold() in _POSTGRES_HOST_OVERRIDE_QUERY_KEYS
                for key, _value in query
            )
            if (
                _is_literal_loopback((parsed.hostname or "").casefold())
                and not has_host_override
                and _database_tls_mode(query) is None
            ):
                default_tls_mode = "disable"
        return _canonical_database_url(value, default_tls_mode=default_tls_mode)

    def s3_credentials(self) -> tuple[str, str]:
        """Reveal object-store credentials only at the client boundary."""

        return (
            self.s3_access_key.get_secret_value(),
            self.s3_secret_key.get_secret_value(),
        )

    def s3_direct_connection_required(self) -> bool:
        """Return whether production plaintext loopback must bypass ambient proxies."""

        return (
            self.environment == "production"
            and self.archive_backend == "s3"
            and urlsplit(self.s3_endpoint).scheme.casefold() == "http"
        )

    def configured_source_ids(self) -> tuple[str, ...]:
        if self.enabled_sources:
            return self.enabled_sources
        return tuple(self.source_intervals_seconds)

    def source_interval(self, source_id: str) -> timedelta:
        try:
            seconds = self.source_intervals_seconds[source_id]
        except KeyError as error:
            raise ConfigurationError(
                f"No collection interval is configured for {source_id!r}"
            ) from error
        return timedelta(seconds=seconds)


def load_settings() -> MakoletSettings:
    """Load settings while keeping validation inputs out of user-facing errors."""

    try:
        return MakoletSettings()
    except Exception as error:
        raise ConfigurationError(
            "Invalid runtime configuration; inspect the documented MAKOLET_* settings"
        ) from error


def validate_s3_endpoint_transport(
    value: str,
    *,
    environment: str,
    allow_insecure_local: bool,
    path_style: bool,
) -> str:
    """Validate an S3 endpoint and the production transport trust boundary."""

    if environment not in {"development", "test", "production"}:
        raise ValueError("environment must be development, test, or production")
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("S3 endpoint must be an absolute secret-free HTTP(S) URL")
    canonical = value.rstrip("/")
    if environment != "production" or parsed.scheme == "https":
        return canonical
    host = (parsed.hostname or "").casefold()
    if allow_insecure_local and path_style and _is_literal_loopback(host):
        return canonical
    raise ValueError(
        "production S3 connections require HTTPS; the explicit insecure-local "
        "exception requires path-style addressing and is limited to literal loopback "
        "IP endpoints"
    )


def _is_literal_loopback(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _database_query_pairs(query: str) -> tuple[tuple[str, str], ...]:
    try:
        return tuple(parse_qsl(query, keep_blank_values=True, max_num_fields=128))
    except ValueError as error:
        raise ValueError("database URL query is invalid or contains too many fields") from error


def _database_tls_mode(query: tuple[tuple[str, str], ...]) -> str | None:
    modes = tuple(
        value.strip().casefold()
        for key, value in query
        if key.strip().casefold() in _POSTGRES_TLS_QUERY_KEYS
    )
    if len(modes) > 1:
        raise ValueError("database URL must specify exactly one TLS mode")
    if not modes:
        return None
    mode = modes[0]
    if mode not in _POSTGRES_TLS_MODES:
        raise ValueError("database URL contains an unsupported TLS mode")
    return mode


def _canonical_database_url(value: str, *, default_tls_mode: str | None = None) -> str:
    parsed = urlsplit(value)
    query = _database_query_pairs(parsed.query)
    tls_mode = _database_tls_mode(query) or default_tls_mode
    canonical_query = [
        (key, item) for key, item in query if key.strip().casefold() not in _POSTGRES_TLS_QUERY_KEYS
    ]
    if tls_mode is not None:
        canonical_query.append(("ssl", tls_mode))
    return urlunsplit(parsed._replace(query=urlencode(canonical_query)))
