from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

import makolet.composition as composition
from makolet.adapters.archive.s3 import S3UploadProcessConfig
from makolet.adapters.parsers import RetailXmlParser, XmlParserLimits
from makolet.adapters.persistence.collection import PostgresCollectionLeaseManager
from makolet.adapters.persistence.database import Database
from makolet.application.ports import MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES
from makolet.composition import open_runtime
from makolet.config import ConfigurationError, MakoletSettings, load_settings
from makolet.domain.errors import NotFoundError

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_settings_load_secret_safe_defaults(tmp_path: Path) -> None:
    secret = "postgresql://makolet:private-password@localhost:5432/makolet"

    settings = MakoletSettings(
        _env_file=None,
        database_url=SecretStr(secret),
        archive_root=tmp_path / "archive",
    )

    assert settings.database_dsn() == secret
    assert "private-password" not in repr(settings)
    assert settings.archive_root == (tmp_path / "archive").resolve()
    assert settings.allow_insecure_ftp is False
    assert settings.api_http_maximum_concurrency == 100
    assert settings.database_allow_insecure_local is False
    assert settings.s3_allow_insecure_local is False


def test_settings_parse_source_intervals_and_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_SOURCE_INTERVALS_SECONDS", '{"zeta":120,"alpha":60}')
    monkeypatch.setenv("MAKOLET_ENABLED_SOURCES", '["zeta"]')
    monkeypatch.setenv("MAKOLET_MCP_ALLOWED_ORIGINS", '["https://example.test/"]')
    monkeypatch.setenv("MAKOLET_ALLOW_INSECURE_FTP", "true")

    settings = MakoletSettings(_env_file=None)

    assert settings.configured_source_ids() == ("zeta",)
    assert settings.source_interval("zeta").total_seconds() == 120
    assert settings.mcp_allowed_origins == ("https://example.test",)
    assert settings.allow_insecure_ftp is True
    assert tuple(settings.source_intervals_seconds) == ("alpha", "zeta")


def test_copied_example_environment_loads_as_application_settings(tmp_path: Path) -> None:
    environment_file = tmp_path / ".env"
    shutil.copyfile(REPOSITORY_ROOT / ".env.example", environment_file)

    settings = MakoletSettings(_env_file=environment_file)

    assert settings.environment == "development"
    assert settings.api_port == 8000
    assert settings.database_allow_insecure_local is True
    assert settings.s3_allow_insecure_local is True
    assert settings.configured_source_ids() == ("shufersal",)


@pytest.mark.parametrize(
    "password",
    [
        "makolet-development-only-change-me",
        "%6Dakolet-development-only-change-me",
    ],
)
def test_production_rejects_bundled_development_database_credential(password: str) -> None:
    database_url = f"postgresql://makolet:{password}@postgres:5432/makolet"  # secret-scan: allow

    with pytest.raises(ValidationError, match="bundled development database credential"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


def test_production_rejects_bundled_database_credential_in_query_parameter() -> None:
    database_url = (
        "postgresql://makolet@postgres:5432/makolet"
        "?password=makolet-development-only-change-me"  # secret-scan: allow
    )

    with pytest.raises(ValidationError, match="bundled development database credential"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


def test_database_url_rejects_fragment_query_parser_differential() -> None:
    database_url = (
        "postgresql://makolet@postgres:5432/makolet-fragment"
        "#?password=makolet-development-only-change-me"  # secret-scan: allow
    )

    with pytest.raises(ValidationError, match="without a fragment"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


def test_production_rejects_bundled_development_s3_credentials() -> None:
    with pytest.raises(ValidationError, match="bundled development S3 credentials"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            archive_backend="s3",
            s3_access_key=SecretStr("makolet-development"),  # secret-scan: allow
            s3_secret_key=SecretStr("makolet-development-only-change-me"),  # secret-scan: allow
        )


def test_production_accepts_operator_s3_credentials_over_authenticated_transports() -> None:
    database_url = "postgresql://makolet@database.example.test:5432/makolet?sslmode=verify-full"
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(database_url),
        archive_backend="s3",
        s3_endpoint="https://objects.example.test:8333",
        s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
        s3_secret_key=SecretStr("operator-configured"),  # secret-scan: allow
    )

    assert settings.environment == "production"
    assert settings.archive_backend == "s3"


@pytest.mark.parametrize("allow_insecure_local", [False, True])
@pytest.mark.parametrize(
    "endpoint",
    [
        "http://objects.example.test:9000",
        "http://seaweedfs.example.test:8333",
        "http://seaweedfs.:8333",
    ],
)
def test_production_remote_s3_rejects_plaintext_even_with_local_exception(
    endpoint: str,
    *,
    allow_insecure_local: bool,
) -> None:
    with pytest.raises(ValidationError, match="production S3 connections require HTTPS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(
                "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
            ),
            archive_backend="s3",
            s3_endpoint=endpoint,
            s3_allow_insecure_local=allow_insecure_local,
            s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
            s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        )


def test_production_remote_s3_accepts_https() -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
        ),
        archive_backend="s3",
        s3_endpoint="https://objects.example.test:9000/",
        s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
        s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
    )

    assert settings.s3_endpoint == "https://objects.example.test:9000"
    assert settings.s3_direct_connection_required() is False


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://127.0.0.1:8333",
        "http://[::1]:8333",
    ],
)
def test_production_s3_plaintext_exception_is_explicit_literal_loopback(endpoint: str) -> None:
    def create_settings(*, allow_insecure_local: bool) -> MakoletSettings:
        return MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(
                "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
            ),
            archive_backend="s3",
            s3_endpoint=endpoint,
            s3_allow_insecure_local=allow_insecure_local,
            s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
            s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        )

    with pytest.raises(ValidationError, match="production S3 connections require HTTPS"):
        create_settings(allow_insecure_local=False)

    settings = create_settings(allow_insecure_local=True)

    assert settings.s3_endpoint == endpoint
    assert settings.s3_direct_connection_required() is True


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://seaweedfs:8333",
        "http://localhost:8333",
    ],
)
def test_production_s3_plaintext_exception_rejects_resolvable_labels(endpoint: str) -> None:
    with pytest.raises(ValidationError, match="literal loopback"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(
                "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
            ),
            archive_backend="s3",
            s3_endpoint=endpoint,
            s3_allow_insecure_local=True,
            s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
            s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        )


def test_production_s3_plaintext_exception_requires_path_style_addressing() -> None:
    with pytest.raises(ValidationError, match="path-style addressing"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(
                "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
            ),
            archive_backend="s3",
            s3_endpoint="http://127.0.0.1:8333",
            s3_allow_insecure_local=True,
            s3_path_style=False,
            s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
            s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        )


def test_runtime_preserves_path_style_for_the_local_plaintext_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
        ),
        archive_root=tmp_path / "archive",
        archive_backend="s3",
        s3_endpoint="http://127.0.0.1:8333",
        s3_allow_insecure_local=True,
        s3_path_style=True,
        s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
        s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
    )

    def create_archive(client: object, bucket: str, **arguments: object) -> object:
        captured["client"] = client
        captured["bucket"] = bucket
        captured.update(arguments)
        return object()

    monkeypatch.setattr(composition, "S3ContentAddressedArchive", create_archive)

    archive = composition._create_archive(settings)

    assert archive is not None
    configuration = captured["upload_process_config"]
    assert isinstance(configuration, S3UploadProcessConfig)
    assert captured["client"] is None
    assert configuration.path_style is True
    assert configuration.direct_connection is True


@pytest.mark.parametrize("environment", ["development", "test"])
def test_nonproduction_s3_preserves_remote_http_compatibility(environment: str) -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment=environment,
        archive_backend="s3",
        s3_endpoint="http://objects.example.test:9000",
        s3_access_key=SecretStr("test-access-key"),  # secret-scan: allow
        s3_secret_key=SecretStr("test-secret-key"),  # secret-scan: allow
    )

    assert settings.s3_endpoint == "http://objects.example.test:9000"
    assert settings.s3_direct_connection_required() is False


def test_production_local_archive_does_not_require_dormant_s3_transport() -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
        ),
        archive_backend="local",
    )

    assert settings.archive_backend == "local"


@pytest.mark.parametrize(
    "query",
    [
        "",
        "?sslmode=disable",
        "?ssl=disable",
        "?%73slmode=disable",
    ],
)
def test_production_remote_database_rejects_missing_or_disabled_tls(query: str) -> None:
    database_url = f"postgresql://makolet@database.example.test:5432/makolet{query}"

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


@pytest.mark.parametrize("sslmode", ["allow", "prefer", "require", "verify-ca"])
def test_production_remote_database_rejects_unverified_tls(sslmode: str) -> None:
    database_url = f"postgresql://makolet@database.example.test:5432/makolet?sslmode={sslmode}"

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


@pytest.mark.parametrize(
    ("scheme", "query"),
    [
        ("postgresql", "sslmode=verify-full"),
        ("postgresql+asyncpg", "ssl=verify-full"),
        ("postgresql", "%73slmode=VERIFY-FULL"),
    ],
)
def test_production_remote_database_accepts_and_canonicalizes_verify_full(
    scheme: str,
    query: str,
) -> None:
    database_url = f"{scheme}://makolet@database.example.test:5432/makolet?{query}"

    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(database_url),
    )

    assert "ssl=verify-full" in settings.database_dsn()
    assert "sslmode=" not in settings.database_dsn().casefold()


def test_production_database_plaintext_exception_rejects_resolvable_service_label() -> None:
    database_url = "postgresql://makolet:operator-configured@postgres:5432/makolet"  # secret-scan: allow  # noqa: E501

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )

    with pytest.raises(ValidationError, match="literal loopback"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
            database_allow_insecure_local=True,
        )


@pytest.mark.parametrize(
    "database_url",
    [
        "postgresql://makolet@127.0.0.1:5432/makolet",
        "postgresql://makolet@[::1]:5432/makolet",
    ],
)
def test_production_database_plaintext_exception_accepts_literal_loopback(
    database_url: str,
) -> None:
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(database_url),
        database_allow_insecure_local=True,
    )

    assert settings.database_allow_insecure_local is True
    assert settings.database_dsn() == f"{database_url}?ssl=disable"


@pytest.mark.parametrize("sslmode", ["allow", "prefer", "require", "verify-ca"])
def test_production_trusted_local_exception_rejects_unverified_tls_modes(
    sslmode: str,
) -> None:
    database_url = f"postgresql://makolet@127.0.0.1:5432/makolet?sslmode={sslmode}"

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
            database_allow_insecure_local=True,
        )


def test_production_trusted_local_exception_cannot_authorize_remote_database() -> None:
    database_url = "postgresql://makolet@database.example.test:5432/makolet?sslmode=disable"

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
            database_allow_insecure_local=True,
        )


@pytest.mark.parametrize(
    "override",
    [
        "host=database.example.test:5432",
        "%68ost=database.example.test:5432",
        "service=remote-database",
        "servicefile=remote-services.conf",
    ],
)
def test_production_trusted_local_exception_rejects_host_overrides(override: str) -> None:
    database_url = f"postgresql://makolet@127.0.0.1:5432/makolet?{override}"

    with pytest.raises(ValidationError, match="authenticated TLS"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
            database_allow_insecure_local=True,
        )


def test_database_url_rejects_ambiguous_tls_query_variants() -> None:
    database_url = (
        "postgresql://makolet@database.example.test:5432/makolet?sslmode=verify-full&ssl=disable"
    )

    with pytest.raises(ValidationError, match="exactly one TLS mode"):
        MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(database_url),
        )


def test_compose_requires_credentials_and_keeps_state_ports_on_loopback() -> None:
    compose = (REPOSITORY_ROOT / "compose.yaml").read_text(encoding="utf-8")

    for variable in (
        "MAKOLET_DATABASE_URL",
        "MAKOLET_S3_ACCESS_KEY",
        "MAKOLET_S3_ALLOW_INSECURE_LOCAL",
        "MAKOLET_S3_SECRET_KEY",
        "POSTGRES_PASSWORD",
    ):
        assert f"${{{variable}:?" in compose
    assert "makolet-development" not in compose
    assert '"127.0.0.1:${MAKOLET_POSTGRES_PORT:-5432}:5432"' in compose
    assert '"127.0.0.1:${MAKOLET_S3_PORT:-8333}:8333"' in compose
    assert '"${MAKOLET_BIND_ADDRESS:-127.0.0.1}:${MAKOLET_API_PORT:-8000}:8000"' in compose


def test_dotenv_rejects_unknown_makolet_application_setting(tmp_path: Path) -> None:
    environment_file = tmp_path / ".env"
    environment_file.write_text("MAKOLET_WORKER_CONCURENCY=2\n", encoding="utf-8")

    with pytest.raises(ValidationError, match="worker_concurency"):
        MakoletSettings(_env_file=environment_file)


def test_settings_reject_unsafe_or_unbounded_values() -> None:
    with pytest.raises(ValidationError):
        MakoletSettings(_env_file=None, database_url=SecretStr("sqlite:///tmp/makolet.db"))
    with pytest.raises(ValidationError):
        MakoletSettings(_env_file=None, worker_concurrency=0)
    with pytest.raises(ValidationError):
        MakoletSettings(_env_file=None, source_intervals_seconds={"unsafe source": 60})
    with pytest.raises(ValidationError):
        MakoletSettings(_env_file=None, mcp_allowed_origins=("file:///tmp/socket",))
    for invalid_concurrency in (0, 10_001):
        with pytest.raises(ValidationError):
            MakoletSettings(
                _env_file=None,
                api_http_maximum_concurrency=invalid_concurrency,
            )
    with pytest.raises(
        ValidationError,
        match="archive object plus bounded protocol and transfer-frame headroom",
    ):
        MakoletSettings(
            _env_file=None,
            archive_maximum_object_bytes=4096,
            ingestion_maximum_charged_bytes_per_source_run=2048,
            ingestion_maximum_charged_bytes_per_source_day=8192,
        )
    with pytest.raises(
        ValidationError,
        match="archive object plus bounded protocol and transfer-frame headroom",
    ):
        MakoletSettings(
            _env_file=None,
            archive_maximum_object_bytes=2048,
            ingestion_maximum_charged_bytes_per_source_run=8192,
            ingestion_maximum_charged_bytes_per_source_day=4096,
        )
    for invalid_worker_id in ("worker one", "עובד", "-worker"):
        with pytest.raises(ValidationError, match="worker ID must use"):
            MakoletSettings(_env_file=None, worker_id=invalid_worker_id)


def test_charged_byte_budget_accepts_exact_protocol_and_frame_headroom() -> None:
    object_bytes = 1024
    exact_budget = object_bytes + MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES

    assert MAXIMUM_TRANSFER_RESERVATION_HEADROOM_BYTES == 97 * 1024 * 1024 + 64 * 1024

    settings = MakoletSettings(
        _env_file=None,
        archive_maximum_object_bytes=object_bytes,
        ingestion_maximum_charged_bytes_per_source_run=exact_budget,
        ingestion_maximum_charged_bytes_per_source_day=exact_budget,
    )

    assert settings.ingestion_maximum_charged_bytes_per_source_run == exact_budget
    with pytest.raises(ValidationError, match="bounded protocol and transfer-frame"):
        MakoletSettings(
            _env_file=None,
            archive_maximum_object_bytes=object_bytes,
            ingestion_maximum_charged_bytes_per_source_run=exact_budget - 1,
            ingestion_maximum_charged_bytes_per_source_day=exact_budget,
        )


def test_worker_id_matches_lifecycle_logging_identifier_contract() -> None:
    settings = MakoletSettings(_env_file=None, worker_id="worker.01:test_node")

    assert settings.worker_id == "worker.01:test_node"


def test_enabled_source_requires_an_interval() -> None:
    with pytest.raises(ValidationError, match="configured collection intervals"):
        MakoletSettings(_env_file=None, enabled_sources=("alpha",))


def test_missing_source_interval_raises_public_configuration_error() -> None:
    settings = MakoletSettings(_env_file=None)

    with pytest.raises(ConfigurationError, match="No collection interval"):
        settings.source_interval("alpha")


def test_load_settings_redacts_invalid_environment_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_DATABASE_URL", "not-a-database-secret-value")

    with pytest.raises(ConfigurationError) as captured:
        load_settings()

    assert "not-a-database-secret-value" not in str(captured.value)


@pytest.mark.asyncio
async def test_default_composition_builds_shared_api_query_and_mcp_services(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parser_limits: list[XmlParserLimits] = []
    lease_databases: list[object] = []
    create_collection_lease_manager = PostgresCollectionLeaseManager

    def create_parser(limits: XmlParserLimits | None = None) -> RetailXmlParser:
        assert limits is not None
        parser_limits.append(limits)
        return RetailXmlParser(limits)

    def capture_collection_lease_manager(database: object) -> object:
        lease_databases.append(database)
        return create_collection_lease_manager(database)  # type: ignore[arg-type]

    monkeypatch.setattr(composition, "RetailXmlParser", create_parser)
    monkeypatch.setattr(
        composition,
        "PostgresCollectionLeaseManager",
        capture_collection_lease_manager,
    )
    settings = MakoletSettings(
        _env_file=None,
        environment="test",
        archive_root=tmp_path / "archive",
        api_http_maximum_concurrency=37,
    )

    async with open_runtime(settings) as runtime:
        assert runtime.settings is settings
        assert runtime.api_app.title == "Makolet API"
        assert runtime.api_app.state.uvicorn_limit_concurrency == 37
        assert runtime.mcp_server is not None
        assert runtime.source_operations is not None
        sources = await runtime.source_operations.list_sources()
        assert len(sources) == 28
        blocked = await runtime.source_operations.inspect_source("netiv-hahesed")
        assert blocked["status"] == "externally_blocked"
        assert "reason" in blocked
        with pytest.raises(NotFoundError):
            await runtime.source_operations.inspect_source("missing-source")
        assert runtime.ingestion_operations is not None
        assert runtime.operational_operations is not None
        assert runtime.worker is not None
        assert runtime.exporter is not None
    assert parser_limits[0].temporary_directory == (tmp_path / "archive/.parser-spool").resolve()
    assert len(lease_databases) == 1
    assert isinstance(lease_databases[0], Database)


@pytest.mark.asyncio
async def test_runtime_setup_failure_closes_all_created_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanup_events: list[str] = []

    class FakeDatabase:
        engine = object()

        async def dispose(self) -> None:
            cleanup_events.append("database")

    class FakeS3Archive:
        async def close(self) -> None:
            cleanup_events.append("archive")

    class FakeHttpClient:
        async def aclose(self) -> None:
            cleanup_events.append("http")

    database = FakeDatabase()
    archive = FakeS3Archive()
    http_client = FakeHttpClient()

    def create_database(*_args: object, **_kwargs: object) -> FakeDatabase:
        return database

    def create_archive(_settings: MakoletSettings) -> FakeS3Archive:
        return archive

    def create_http_client(*_args: object, **_kwargs: object) -> FakeHttpClient:
        return http_client

    def fail_source_registry(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("deliberate setup failure")

    monkeypatch.setattr("makolet.composition.Database.from_url", staticmethod(create_database))
    monkeypatch.setattr(composition, "S3ContentAddressedArchive", FakeS3Archive)
    monkeypatch.setattr(composition, "_create_archive", create_archive)
    monkeypatch.setattr("makolet.composition.httpx.AsyncClient", create_http_client)
    monkeypatch.setattr(composition, "SourceRegistry", fail_source_registry)
    settings = MakoletSettings(
        _env_file=None,
        environment="test",
        archive_root=tmp_path / "archive",
    )

    with pytest.raises(RuntimeError, match="deliberate setup failure"):
        async with open_runtime(settings):
            pytest.fail("runtime yielded after setup failed")

    assert cleanup_events == ["http", "archive", "database"]
