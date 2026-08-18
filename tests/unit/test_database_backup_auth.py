from __future__ import annotations

import hashlib
import hmac
import os
import stat
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from makolet.adapters.filesystem_capacity import CAPACITY_LOCK_FILENAME
from makolet.interfaces import database_backup_auth

_DOMAIN = b"makolet-database-backup-hmac-sha256-v1\0"
_PREFIX = "makolet-database-backup-hmac-sha256-v1:"


def _backup_material(tmp_path: Path) -> tuple[Path, Path, Path, bytes]:
    backup_directory = tmp_path / "recovery-set"
    key_directory = tmp_path / "protected-key"
    backup_directory.mkdir()
    key_directory.mkdir()
    backup = backup_directory / "makolet.dump"
    authentication = backup.with_name(f"{backup.name}.hmac-sha256")
    key_file = key_directory / "database-backup-auth.key"
    payload = b"PGDMP\x01\x0f\x00\x04 independently authored test backup"
    key = bytes(range(32))
    backup.write_bytes(payload)
    key_file.write_bytes(key)
    key_file.chmod(0o600)
    return backup, authentication, key_file, payload


def test_authentication_round_trip_copies_only_exact_verified_bytes(tmp_path: Path) -> None:
    backup, authentication, key_file, payload = _backup_material(tmp_path)
    verified_copy = tmp_path / "verified" / "authenticated.dump"
    verified_copy.parent.mkdir()

    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    database_backup_auth.verify_backup_to_copy(
        backup,
        authentication,
        verified_copy,
        key_file,
    )

    expected = hmac.new(bytes(range(32)), _DOMAIN + payload, hashlib.sha256).hexdigest()
    assert authentication.read_text(encoding="ascii") == f"{_PREFIX}{expected}\n"
    assert verified_copy.read_bytes() == payload


def test_tampered_backup_with_recomputed_sha256_is_rejected_before_copy(
    tmp_path: Path,
) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    verified_copy = tmp_path / "authenticated.dump"
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    tampered = b"PGDMP attacker-controlled replacement"
    backup.write_bytes(tampered)
    backup.with_name(f"{backup.name}.sha256").write_text(
        f"{hashlib.sha256(tampered).hexdigest()}  {backup.name}\n",
        encoding="ascii",
    )

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="failed"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            verified_copy,
            key_file,
        )

    assert not verified_copy.exists()


def test_wrong_key_and_modified_authentication_sidecar_are_rejected(tmp_path: Path) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    wrong_key = key_file.with_name("wrong.key")
    wrong_key.write_bytes(bytes(reversed(range(32))))
    wrong_key.chmod(0o600)

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="failed"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            tmp_path / "wrong-key.dump",
            wrong_key,
        )

    authentication.write_bytes(f"{_PREFIX}{'0' * 64}\n".encode("ascii"))
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="failed"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            tmp_path / "wrong-sidecar.dump",
            key_file,
        )
    assert not (tmp_path / "wrong-key.dump").exists()
    assert not (tmp_path / "wrong-sidecar.dump").exists()


def test_authentication_key_must_be_exact_and_outside_backup_tree(tmp_path: Path) -> None:
    backup, authentication, _key_file, _payload = _backup_material(tmp_path)
    adjacent_key = backup.with_name("database-backup-auth.key")
    adjacent_key.write_bytes(bytes(range(32)))
    adjacent_key.chmod(0o600)

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="outside"):
        database_backup_auth.authenticate_backup(backup, authentication, adjacent_key)

    nested_key = backup.parent / "protected" / "database-backup-auth.key"
    nested_key.parent.mkdir()
    nested_key.write_bytes(bytes(range(32)))
    nested_key.chmod(0o600)
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="outside"):
        database_backup_auth.authenticate_backup(backup, authentication, nested_key)

    short_key = tmp_path / "short-key" / "short.key"
    short_key.parent.mkdir()
    short_key.write_bytes(b"not-a-raw-256-bit-key")
    short_key.chmod(0o600)
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="invalid"):
        database_backup_auth.authenticate_backup(backup, authentication, short_key)


def test_authentication_key_must_not_have_an_additional_hard_link(tmp_path: Path) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    linked_directory = tmp_path / "linked-key"
    linked_directory.mkdir()
    try:
        os.link(key_file, linked_directory / "linked.key")
    except OSError:
        pytest.skip("test filesystem does not support hard links")

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="link count"):
        database_backup_auth.authenticate_backup(backup, authentication, key_file)


def test_generated_key_is_exclusive_and_owner_only_on_posix(tmp_path: Path) -> None:
    key_file = tmp_path / "protected" / "database-backup-auth.key"
    key_file.parent.mkdir()

    database_backup_auth.generate_authentication_key(key_file)

    assert len(key_file.read_bytes()) == 32
    if os.name != "nt":
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="cannot be created"):
        database_backup_auth.generate_authentication_key(key_file)


def test_script_interface_reports_authentication_failure_without_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    backup.write_bytes(b"tampered after authentication")
    monkeypatch.setenv(database_backup_auth.AUTHENTICATION_KEY_ENVIRONMENT, str(key_file))

    result = database_backup_auth.main(
        ("verify-copy", str(backup), str(authentication), str(tmp_path / "verified.dump"))
    )

    output = capsys.readouterr()
    assert result == 1
    assert output.out == ""
    assert output.err == "Database backup authentication verification failed.\n"
    assert str(key_file) not in output.err
    assert bytes(range(32)).hex() not in output.err


def test_backup_authentication_rejects_oversized_input_before_retaining_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup, authentication, key_file, payload = _backup_material(tmp_path)
    monkeypatch.setenv(
        database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT,
        str(len(payload) - 1),
    )

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="byte limit"):
        database_backup_auth.authenticate_backup(backup, authentication, key_file)
    assert not authentication.exists()

    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, str(len(payload)))
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    verified_copy = tmp_path / "verified.dump"
    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, str(len(payload) - 1))
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="byte limit"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            verified_copy,
            key_file,
        )
    assert not verified_copy.exists()


def test_backup_capture_enforces_byte_limit_while_command_is_streaming(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "captured.dump"
    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, "4")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "0")

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="byte limit"):
        database_backup_auth.capture_command_to_backup(
            destination,
            (sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'12345')"),
        )

    assert not destination.exists()


def test_backup_capture_preserves_exact_bounded_command_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "captured.dump"
    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, "5")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "0")

    database_backup_auth.capture_command_to_backup(
        destination,
        (sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'12345')"),
    )

    assert destination.read_bytes() == b"12345"


def test_backup_validation_streams_exact_host_bytes_to_command_stdin(tmp_path: Path) -> None:
    backup = tmp_path / "captured.dump"
    payload = b"PGDMP\x00\xff independently authored binary backup"
    backup.write_bytes(payload)

    database_backup_auth.validate_backup_with_command(
        backup,
        (
            sys.executable,
            "-c",
            "import sys; raise SystemExit(sys.stdin.buffer.read() != " + repr(payload) + ")",
        ),
    )


def test_backup_sidecars_share_capacity_coordination_and_preserve_exact_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup, authentication, key_file, payload = _backup_material(tmp_path)
    checksum = backup.with_name(f"{backup.name}.sha256")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "1")

    database_backup_auth.create_backup_sidecars(
        backup,
        checksum,
        authentication,
        "published.dump",
        key_file,
        tmp_path,
    )

    digest = hashlib.sha256(payload).hexdigest()
    assert checksum.read_bytes() == f"{digest}  published.dump\n".encode("ascii")
    expected_hmac = hmac.new(key_file.read_bytes(), _DOMAIN + payload, hashlib.sha256).hexdigest()
    assert authentication.read_bytes() == _PREFIX.encode("ascii") + expected_hmac.encode() + b"\n"
    private_directories = list(tmp_path.glob(".makolet-capacity-*"))
    assert len(private_directories) == 1
    assert (private_directories[0] / CAPACITY_LOCK_FILENAME).is_file()


def test_backup_sidecars_fail_closed_when_capacity_cannot_cover_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    checksum = backup.with_name(f"{backup.name}.sha256")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "1")
    monkeypatch.setattr(
        "makolet.adapters.filesystem_capacity.shutil.disk_usage",
        lambda _path: SimpleNamespace(total=1, used=0, free=1),
    )

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="free-space"):
        database_backup_auth.create_backup_sidecars(
            backup,
            checksum,
            authentication,
            "published.dump",
            key_file,
            tmp_path,
        )

    assert not checksum.exists()
    assert not authentication.exists()


def test_backup_capture_script_interface_uses_the_bounded_binary_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "captured.dump"
    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, "5")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "0")

    result = database_backup_auth.main(
        (
            "capture-command",
            str(destination),
            str(tmp_path),
            "--",
            sys.executable,
            "-c",
            "import sys; sys.stdout.buffer.write(b'12345')",
        )
    )

    assert result == 0
    assert destination.read_bytes() == b"12345"


def test_backup_capture_checks_capacity_before_writing_command_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination = tmp_path / "captured.dump"
    monkeypatch.setenv(database_backup_auth.MAXIMUM_BACKUP_BYTES_ENVIRONMENT, "5")
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "1")
    monkeypatch.setattr(
        "makolet.adapters.filesystem_capacity.shutil.disk_usage",
        lambda _path: SimpleNamespace(total=5, used=0, free=5),
    )

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="free-space"):
        database_backup_auth.capture_command_to_backup(
            destination,
            (sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'12345')"),
        )

    assert not destination.exists()


def test_backup_verification_preserves_configured_free_space_reserve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "0")
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    verified_copy = tmp_path / "verified.dump"
    capacity_directory = tmp_path / "capacity-locks"
    capacity_directory.mkdir()
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, str(2**63))

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="free-space reserve"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            verified_copy,
            key_file,
            capacity_directory,
        )

    assert not verified_copy.exists()
    private_directories = list(capacity_directory.glob(".makolet-capacity-*"))
    assert len(private_directories) == 1
    assert (private_directories[0] / CAPACITY_LOCK_FILENAME).is_file()
    assert not (tmp_path / CAPACITY_LOCK_FILENAME).exists()


def test_backup_verification_rejects_a_precreated_non_private_lock_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup, authentication, key_file, _payload = _backup_material(tmp_path)
    monkeypatch.setenv(database_backup_auth.MINIMUM_FREE_BYTES_ENVIRONMENT, "0")
    database_backup_auth.authenticate_backup(backup, authentication, key_file)
    capacity_root = tmp_path / "capacity-root"
    capacity_root.mkdir()
    private = database_backup_auth._private_capacity_directory(capacity_root)
    private.rmdir()
    private.write_text("attacker-controlled path", encoding="ascii")
    verified_copy = tmp_path / "verified.dump"

    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="capacity directory"):
        database_backup_auth.verify_backup_to_copy(
            backup,
            authentication,
            verified_copy,
            key_file,
            capacity_root,
        )

    assert not verified_copy.exists()


def test_checksum_sidecar_reader_is_bounded_and_accepts_generated_shape(tmp_path: Path) -> None:
    checksum = tmp_path / "backup.dump.sha256"
    digest = "a" * 64
    checksum.write_text(f"{digest}  backup.dump\n", encoding="ascii")

    assert database_backup_auth.read_checksum(checksum) == digest

    checksum.write_bytes(f"{digest}  ".encode("ascii") + b"x" * 4096 + b"\n")
    with pytest.raises(database_backup_auth.BackupAuthenticationError, match="invalid"):
        database_backup_auth.read_checksum(checksum)


def test_checksum_script_rejects_unbounded_first_line_without_echoing_it(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checksum = tmp_path / "backup.dump.sha256"
    checksum.write_bytes(b"a" * 8192)

    assert database_backup_auth.main(("read-checksum", str(checksum))) == 1

    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "Database backup checksum validation failed.\n"
