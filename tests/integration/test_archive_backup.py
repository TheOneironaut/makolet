"""Raw-archive backup and restore against the configured S3 prefix."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config

from deployment import archive_backup
from makolet.adapters.archive.keys import key_for_digest

pytestmark = pytest.mark.integration


@pytest.mark.usefixtures("database")
@pytest.mark.asyncio
async def test_authoritative_inventory_uses_real_postgresql_deadline_transaction(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TestSettings:
        def database_dsn(self) -> str:
            return migrated_database_url

    monkeypatch.setattr(archive_backup, "load_settings", TestSettings)
    started_at = time.monotonic()
    deadline = archive_backup._OperationDeadline(
        work_deadline=started_at + 5,
        cleanup_deadline=started_at + 10,
    )

    rows = await archive_backup._query_authoritative_object_rows(
        key_prefix="raw",
        maximum_objects=10,
        deadline=deadline,
    )

    assert rows == ()


def _test_client(endpoint: str, access_key: str | None, secret_key: str | None) -> Any:
    arguments: dict[str, Any] = {
        "endpoint_url": endpoint,
        "region_name": "us-east-1",
        "config": Config(s3={"addressing_style": "path"}),
    }
    if access_key and secret_key:
        arguments.update(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
    else:
        arguments["config"] = Config(
            signature_version=UNSIGNED,
            s3={"addressing_style": "path"},
        )
    return boto3.client("s3", **arguments)


def _configure_backup_environment(
    monkeypatch: pytest.MonkeyPatch,
    *,
    endpoint: str,
    bucket: str,
    key_prefix: str,
    access_key: str | None,
    secret_key: str | None,
    authentication_key: Path,
) -> None:
    monkeypatch.setenv("MAKOLET_S3_ENDPOINT", endpoint)
    monkeypatch.setenv("MAKOLET_S3_REGION", "us-east-1")
    monkeypatch.setenv("MAKOLET_S3_BUCKET", bucket)
    monkeypatch.setenv("MAKOLET_S3_KEY_PREFIX", key_prefix)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE", str(authentication_key))
    if access_key and secret_key:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", access_key)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret_key)
    else:
        monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
        monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)


def test_nondefault_prefix_backup_restore_and_manifest_key_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = os.environ.get("MAKOLET_TEST_S3_ENDPOINT")
    if endpoint is None:
        pytest.skip("MAKOLET_TEST_S3_ENDPOINT is not configured")
    access_key = os.environ.get("MAKOLET_TEST_S3_ACCESS_KEY")
    secret_key = os.environ.get("MAKOLET_TEST_S3_SECRET_KEY")
    if bool(access_key) is not bool(secret_key):
        pytest.fail("S3 integration access key and secret key must be configured together")
    client = _test_client(endpoint, access_key, secret_key)
    bucket = os.environ.get("MAKOLET_TEST_S3_BUCKET", "makolet-raw")
    suffix = uuid4().hex
    key_prefix = f"regulated/archive/{suffix}"
    payload = "clean-room archive bytes \u05de\u05d7\u05d9\u05e8".encode()
    digest = hashlib.sha256(payload).hexdigest()
    service_key = f"{key_prefix}/{key_for_digest(digest)}"
    decoy = b"not part of the configured archive"
    decoy_digest = hashlib.sha256(decoy).hexdigest()
    decoy_key = f"integration-decoy/{suffix}/{key_for_digest(decoy_digest)}"

    try:
        client.head_bucket(Bucket=bucket)
        client.put_object(
            Bucket=bucket,
            Key=service_key,
            Body=payload,
            Metadata={"sha256": digest},
        )
        client.put_object(
            Bucket=bucket,
            Key=decoy_key,
            Body=decoy,
            Metadata={"sha256": decoy_digest},
        )
        authentication_key = tmp_path / "protected" / "archive-backup-auth.key"
        authentication_key.parent.mkdir()
        authentication_key.write_bytes(bytes(range(32)))
        authentication_key.chmod(0o600)
        _configure_backup_environment(
            monkeypatch,
            endpoint=endpoint,
            bucket=bucket,
            key_prefix=key_prefix,
            access_key=access_key,  # secret-scan: allow
            secret_key=secret_key,  # secret-scan: allow
            authentication_key=authentication_key,
        )
        destination = archive_backup._destination(str(tmp_path / "backup"))
        monkeypatch.setattr(
            archive_backup,
            "_authoritative_object_rows",
            lambda **_kwargs: iter(({"key": service_key, "sha256": digest, "size": len(payload)},)),
        )

        backed_up = archive_backup.backup(destination)
        verified = archive_backup.verify(destination)
        manifest_path = destination / "manifest.json"
        checksum_path = destination / "manifest.json.sha256"
        authentication_path = destination / "manifest.json.hmac-sha256"
        original_manifest = manifest_path.read_bytes()
        original_checksum = checksum_path.read_bytes()
        original_authentication = authentication_path.read_bytes()
        manifest = json.loads(original_manifest)

        assert backed_up["objects"] == verified["objects"] == 1
        assert manifest["key_prefix"] == key_prefix
        assert manifest["objects"] == [{"key": service_key, "sha256": digest, "size": len(payload)}]

        monkeypatch.setenv("MAKOLET_S3_KEY_PREFIX", "wrong/prefix")
        with pytest.raises(archive_backup.BackupError, match="key prefix"):
            archive_backup.verify(destination)

        monkeypatch.setenv("MAKOLET_S3_KEY_PREFIX", key_prefix)
        tampered = json.loads(original_manifest)
        tampered["objects"][0]["key"] = f"{key_prefix}/sha256/00/00/{digest}"
        tampered_payload = (
            json.dumps(tampered, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
        ).encode()
        manifest_path.chmod(stat.S_IWRITE | stat.S_IREAD)
        checksum_path.chmod(stat.S_IWRITE | stat.S_IREAD)
        authentication_path.chmod(stat.S_IWRITE | stat.S_IREAD)
        manifest_path.write_bytes(tampered_payload)
        checksum_path.write_bytes(
            f"{hashlib.sha256(tampered_payload).hexdigest()}  manifest.json\n".encode("ascii")
        )
        authentication_path.write_bytes(
            archive_backup._authentication_payload(
                authentication_key.read_bytes(),
                tampered_payload,
            )
        )
        with pytest.raises(archive_backup.BackupError, match="non-canonical"):
            archive_backup.verify(destination)

        manifest_path.write_bytes(original_manifest)
        checksum_path.write_bytes(original_checksum)
        authentication_path.write_bytes(original_authentication)
        client.delete_object(Bucket=bucket, Key=service_key)
        restored = archive_backup.restore(destination, confirmed_bucket=bucket)

        assert restored == {"status": "restored", "objects": 1, "objects_created": 1}
        restored_object = client.get_object(Bucket=bucket, Key=service_key)
        assert restored_object["Body"].read() == payload
    finally:
        client.delete_object(Bucket=bucket, Key=service_key)
        client.delete_object(Bucket=bucket, Key=decoy_key)
        client.close()
