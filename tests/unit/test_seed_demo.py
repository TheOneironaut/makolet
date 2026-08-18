from __future__ import annotations

import ssl
from typing import Any, cast

import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy.engine import URL

import deployment.seed_demo as seed_demo
import makolet.adapters.persistence.database as database_module
from makolet.adapters.archive.s3 import S3UploadProcessConfig
from makolet.config import MakoletSettings


class _FakeEngine:
    async def dispose(self) -> None:
        return None


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
def test_demo_seed_database_uses_validated_tls_factory(
    monkeypatch: pytest.MonkeyPatch,
    database_url: str,
    *,
    allow_insecure_local: bool,
    verified: bool,
) -> None:
    captured: dict[str, object] = {}
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(database_url),
        database_allow_insecure_local=allow_insecure_local,
    )

    def create_engine(url: URL, **kwargs: object) -> Any:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return _FakeEngine()

    monkeypatch.setattr(database_module, "create_async_engine", create_engine)
    monkeypatch.setattr(seed_demo, "load_settings", lambda: settings, raising=False)

    database = seed_demo._database()

    assert database.engine is not None
    url = cast(URL, captured["url"])
    assert "ssl" not in url.query
    kwargs = cast(dict[str, object], captured["kwargs"])
    connect_args = cast(dict[str, object], kwargs["connect_args"])
    tls_argument = connect_args["ssl"]
    if verified:
        assert isinstance(tls_argument, ssl.SSLContext)
        assert tls_argument.check_hostname is True
        assert tls_argument.verify_mode is ssl.CERT_REQUIRED
    else:
        assert tls_argument is False


def test_demo_seed_s3_uses_the_validated_application_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    settings = MakoletSettings(
        _env_file=None,
        environment="production",
        database_url=SecretStr(
            "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
        ),
        archive_backend="s3",
        s3_endpoint="http://127.0.0.1:8333",
        s3_allow_insecure_local=True,
        s3_bucket="demo-archive",
        s3_region="eu-west-1",
        s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
        s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        s3_key_prefix="evidence/raw",
    )

    def create_archive(client: object, bucket: str, **arguments: object) -> object:
        captured["client"] = client
        captured["bucket"] = bucket
        captured.update(arguments)
        return object()

    monkeypatch.setattr(seed_demo, "load_settings", lambda: settings)
    monkeypatch.setattr(seed_demo, "S3ContentAddressedArchive", create_archive)

    archive = seed_demo._s3_archive()

    assert archive is not None
    configuration = captured.pop("upload_process_config")
    assert isinstance(configuration, S3UploadProcessConfig)
    assert configuration.path_style is True
    assert configuration.direct_connection is True
    assert captured == {
        "client": None,
        "bucket": "demo-archive",
        "key_prefix": "evidence/raw",
        "maximum_object_bytes": settings.archive_maximum_object_bytes,
        "minimum_free_bytes": settings.archive_minimum_free_bytes,
        "temporary_directory": settings.archive_root,
    }
    assert configuration.endpoint_url == "http://127.0.0.1:8333"
    assert configuration.region_name == "eu-west-1"
    assert configuration.access_key_id == "operator-access-key"  # secret-scan: allow
    assert configuration.secret_access_key == "operator-secret-key"  # secret-scan: allow


def test_demo_seed_s3_does_not_bypass_production_transport_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unsafe_settings() -> MakoletSettings:
        return MakoletSettings(
            _env_file=None,
            environment="production",
            database_url=SecretStr(
                "postgresql://makolet@database.example.test/makolet?sslmode=verify-full"
            ),
            archive_backend="s3",
            s3_endpoint="http://objects.example.test:9000",
            s3_allow_insecure_local=True,
            s3_access_key=SecretStr("operator-access-key"),  # secret-scan: allow
            s3_secret_key=SecretStr("operator-secret-key"),  # secret-scan: allow
        )

    monkeypatch.setattr(seed_demo, "load_settings", unsafe_settings)

    with pytest.raises(ValidationError, match="production S3 connections require HTTPS"):
        seed_demo._s3_archive()
