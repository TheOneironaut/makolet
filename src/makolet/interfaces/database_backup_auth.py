"""Authenticate PostgreSQL backup bytes with a separately stored protected key."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import stat
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import BinaryIO

from makolet.adapters.filesystem_capacity import (
    FileSystemCapacityGuard,
    FileSystemCapacityUnavailableError,
)

AUTHENTICATION_KEY_ENVIRONMENT = "MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE"
MAXIMUM_BACKUP_BYTES_ENVIRONMENT = "MAKOLET_DATABASE_BACKUP_MAXIMUM_BYTES"
MINIMUM_FREE_BYTES_ENVIRONMENT = "MAKOLET_DATABASE_BACKUP_MINIMUM_FREE_BYTES"
_AUTHENTICATION_DOMAIN = b"makolet-database-backup-hmac-sha256-v1\0"
_AUTHENTICATION_PREFIX = b"makolet-database-backup-hmac-sha256-v1:"
_AUTHENTICATION_LINE_BYTES = len(_AUTHENTICATION_PREFIX) + 64 + 1
_MAXIMUM_CHECKSUM_SIDECAR_BYTES = 4096
_KEY_BYTES = 32
_STREAM_CHUNK_BYTES = 1024 * 1024
_DEFAULT_MAXIMUM_BACKUP_BYTES = 128 * 1024 * 1024 * 1024
_DEFAULT_MINIMUM_FREE_BYTES = 1024 * 1024 * 1024


class BackupAuthenticationError(RuntimeError):
    """A database backup or its protected authentication material is invalid."""


def generate_authentication_key(destination: Path) -> None:
    """Create a new 256-bit authentication key without overwriting any path."""
    _write_new_file(destination, secrets.token_bytes(_KEY_BYTES))


def authenticate_backup(backup: Path, authentication: Path, key_file: Path) -> None:
    """Write a versioned HMAC sidecar for one exact database backup."""
    _require_separate_key_directory(backup, key_file)
    key = _read_key(key_file)
    maximum_bytes, _ = _configured_capacity_limits()
    with _open_regular_file(backup) as source:
        digest = _hmac_stream(key, source, maximum_bytes=maximum_bytes)
    payload = _AUTHENTICATION_PREFIX + digest.encode("ascii") + b"\n"
    _write_new_file(authentication, payload)


def verify_backup_to_copy(
    backup: Path,
    authentication: Path,
    verified_copy: Path,
    key_file: Path,
    capacity_directory: Path | None = None,
) -> None:
    """Authenticate a backup while copying the exact verified bytes to a new file."""
    _require_separate_key_directory(backup, key_file)
    key = _read_key(key_file)
    expected = _read_authentication(authentication)
    maximum_bytes, minimum_free_bytes = _configured_capacity_limits()
    capacity_root = capacity_directory or verified_copy.parent
    private_capacity_directory = _private_capacity_directory(capacity_root)
    capacity = FileSystemCapacityGuard(
        verified_copy.parent,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=private_capacity_directory,
    )
    copy_created = False
    authenticated = False
    try:
        with (
            _open_regular_file(backup) as source,
            _open_new_file(verified_copy) as destination,
        ):
            copy_created = True
            source_size = os.fstat(source.fileno()).st_size
            if source_size > maximum_bytes:
                raise BackupAuthenticationError("database backup exceeds the configured byte limit")
            actual = _hmac_stream(
                key,
                source,
                destination,
                maximum_bytes=maximum_bytes,
                capacity=capacity,
            )
            try:
                with capacity.reserve(0):
                    destination.flush()
                    os.fsync(destination.fileno())
            except FileSystemCapacityUnavailableError as error:
                raise BackupAuthenticationError(
                    "database backup verification reached its configured free-space reserve"
                ) from error
        if not hmac.compare_digest(expected, actual):
            raise BackupAuthenticationError("database backup authentication failed")
        authenticated = True
    finally:
        if copy_created and not authenticated:
            verified_copy.unlink(missing_ok=True)


def capture_command_to_backup(
    destination: Path,
    command: Sequence[str],
    capacity_directory: Path | None = None,
) -> None:
    """Capture one command's stdout without exceeding backup storage limits."""

    if not command or not command[0] or any("\0" in argument for argument in command):
        raise BackupAuthenticationError("database backup capture command is invalid")
    maximum_bytes, minimum_free_bytes = _configured_capacity_limits()
    capacity_root = capacity_directory or destination.parent
    private_capacity_directory = _private_capacity_directory(capacity_root)
    capacity = FileSystemCapacityGuard(
        destination.parent,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=private_capacity_directory,
    )
    process: subprocess.Popen[bytes] | None = None
    output_created = False
    complete = False
    try:
        with _open_new_file(destination) as output:
            output_created = True
            try:
                process = subprocess.Popen(  # noqa: S603 - exact operator-supplied argv
                    tuple(command),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                )
            except OSError as error:
                raise BackupAuthenticationError(
                    "database backup capture command could not start"
                ) from error
            if process.stdout is None:
                raise BackupAuthenticationError("database backup capture command is invalid")
            total = 0
            try:
                while chunk := process.stdout.read(_STREAM_CHUNK_BYTES):
                    total += len(chunk)
                    if total > maximum_bytes:
                        raise BackupAuthenticationError(
                            "database backup exceeds the configured byte limit"
                        )
                    try:
                        with capacity.reserve(len(chunk)):
                            written = output.write(chunk)
                            output.flush()
                    except FileSystemCapacityUnavailableError as error:
                        raise BackupAuthenticationError(
                            "database backup creation reached its configured free-space reserve"
                        ) from error
                    if written != len(chunk):
                        raise BackupAuthenticationError(
                            "database backup capture write was incomplete"
                        )
            finally:
                process.stdout.close()
            if process.wait() != 0:
                raise BackupAuthenticationError("database backup capture command failed")
            try:
                with capacity.reserve(0):
                    output.flush()
                    os.fsync(output.fileno())
            except FileSystemCapacityUnavailableError as error:
                raise BackupAuthenticationError(
                    "database backup creation reached its configured free-space reserve"
                ) from error
        complete = True
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if output_created and not complete:
            destination.unlink(missing_ok=True)


def validate_backup_with_command(backup: Path, command: Sequence[str]) -> None:
    """Stream one bounded host backup directly to a validator command's stdin."""

    if not command or not command[0] or any("\0" in argument for argument in command):
        raise BackupAuthenticationError("database backup validation command is invalid")
    maximum_bytes, _minimum_free_bytes = _configured_capacity_limits()
    try:
        with _open_regular_file(backup) as source:
            if os.fstat(source.fileno()).st_size > maximum_bytes:
                raise BackupAuthenticationError("database backup exceeds the configured byte limit")
            completed = subprocess.run(  # noqa: S603 - exact operator-supplied argv
                tuple(command),
                stdin=source,
                stdout=subprocess.DEVNULL,
                check=False,
            )
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup validation command could not run"
        ) from error
    if completed.returncode != 0:
        raise BackupAuthenticationError("database backup validation command failed")


def create_backup_sidecars(
    backup: Path,
    checksum: Path,
    authentication: Path,
    published_filename: str,
    key_file: Path,
    capacity_directory: Path | None = None,
) -> str:
    """Create checksum and HMAC sidecars under one filesystem-capacity lock."""

    _require_separate_key_directory(backup, key_file)
    _require_adjacent_sidecars(backup, checksum, authentication)
    if (
        not published_filename
        or published_filename in {".", ".."}
        or any(character in "/\\" or ord(character) < 32 for character in published_filename)
    ):
        raise BackupAuthenticationError("database backup published filename is invalid")
    key = _read_key(key_file)
    maximum_bytes, minimum_free_bytes = _configured_capacity_limits()
    with _open_regular_file(backup) as source:
        checksum_digest, authentication_digest = _backup_digests(
            key,
            source,
            maximum_bytes=maximum_bytes,
        )
    checksum_payload = f"{checksum_digest}  {published_filename}\n".encode()
    if len(checksum_payload) > _MAXIMUM_CHECKSUM_SIDECAR_BYTES:
        raise BackupAuthenticationError("database backup published filename is invalid")
    authentication_payload = _AUTHENTICATION_PREFIX + authentication_digest.encode("ascii") + b"\n"
    capacity_root = capacity_directory or backup.parent
    capacity = FileSystemCapacityGuard(
        checksum.parent,
        minimum_free_bytes=minimum_free_bytes,
        coordination_directory=_private_capacity_directory(capacity_root),
    )
    created: list[Path] = []
    try:
        _write_new_file(checksum, checksum_payload, capacity=capacity)
        created.append(checksum)
        _write_new_file(authentication, authentication_payload, capacity=capacity)
        created.append(authentication)
    except Exception:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return checksum_digest


def read_checksum(checksum: Path) -> str:
    """Read one bounded sha256sum-compatible sidecar without trusting its filename."""

    with _open_regular_file(checksum) as source:
        payload = source.read(_MAXIMUM_CHECKSUM_SIDECAR_BYTES + 1)
    if (
        not payload.endswith(b"\n")
        or len(payload) > _MAXIMUM_CHECKSUM_SIDECAR_BYTES
        or b"\n" in payload[:-1]
    ):
        raise BackupAuthenticationError("database backup checksum sidecar is invalid")
    line = payload[:-1]
    encoded_digest = line[:64]
    suffix = line[64:]
    if (
        len(encoded_digest) != 64
        or any(byte not in b"0123456789abcdef" for byte in encoded_digest)
        or (suffix and suffix[:1] not in {b" ", b"\t"})
    ):
        raise BackupAuthenticationError("database backup checksum sidecar is invalid")
    return encoded_digest.decode("ascii")


def _hmac_stream(
    key: bytes,
    source: BinaryIO,
    destination: BinaryIO | None = None,
    *,
    maximum_bytes: int,
    capacity: FileSystemCapacityGuard | None = None,
) -> str:
    authenticator = hmac.new(key, digestmod=hashlib.sha256)
    authenticator.update(_AUTHENTICATION_DOMAIN)
    total = 0
    while chunk := source.read(_STREAM_CHUNK_BYTES):
        total += len(chunk)
        if total > maximum_bytes:
            raise BackupAuthenticationError("database backup exceeds the configured byte limit")
        authenticator.update(chunk)
        if destination is not None:
            if capacity is None:
                raise BackupAuthenticationError(
                    "database backup verification destination is invalid"
                )
            try:
                with capacity.reserve(len(chunk)):
                    destination.write(chunk)
                    destination.flush()
            except FileSystemCapacityUnavailableError as error:
                raise BackupAuthenticationError(
                    "database backup verification reached its configured free-space reserve"
                ) from error
    return authenticator.hexdigest()


def _backup_digests(
    key: bytes,
    source: BinaryIO,
    *,
    maximum_bytes: int,
) -> tuple[str, str]:
    checksum = hashlib.sha256()
    authenticator = hmac.new(key, digestmod=hashlib.sha256)
    authenticator.update(_AUTHENTICATION_DOMAIN)
    total = 0
    while chunk := source.read(_STREAM_CHUNK_BYTES):
        total += len(chunk)
        if total > maximum_bytes:
            raise BackupAuthenticationError("database backup exceeds the configured byte limit")
        checksum.update(chunk)
        authenticator.update(chunk)
    return checksum.hexdigest(), authenticator.hexdigest()


def _read_key(key_file: Path) -> bytes:
    with _open_regular_file(key_file) as source:
        status = os.fstat(source.fileno())
        if status.st_nlink != 1:
            raise BackupAuthenticationError(
                "database backup authentication key has an unsafe link count"
            )
        if os.name != "nt" and stat.S_IMODE(status.st_mode) & 0o077:
            raise BackupAuthenticationError("database backup authentication key is not protected")
        key = source.read(_KEY_BYTES + 1)
    if len(key) != _KEY_BYTES:
        raise BackupAuthenticationError("database backup authentication key is invalid")
    return key


def _read_authentication(authentication: Path) -> str:
    with _open_regular_file(authentication) as source:
        payload = source.read(_AUTHENTICATION_LINE_BYTES + 1)
    if len(payload) != _AUTHENTICATION_LINE_BYTES or not payload.endswith(b"\n"):
        raise BackupAuthenticationError("database backup authentication sidecar is invalid")
    prefix, separator, encoded_digest = payload[:-1].partition(b":")
    if prefix + separator != _AUTHENTICATION_PREFIX:
        raise BackupAuthenticationError("database backup authentication sidecar is invalid")
    if len(encoded_digest) != 64 or any(byte not in b"0123456789abcdef" for byte in encoded_digest):
        raise BackupAuthenticationError("database backup authentication sidecar is invalid")
    return encoded_digest.decode("ascii")


def _require_separate_key_directory(backup: Path, key_file: Path) -> None:
    _regular_file_status(backup)
    _regular_file_status(key_file)
    try:
        backup_parent = backup.resolve(strict=True).parent
        key_parent = key_file.resolve(strict=True).parent
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup authentication paths are invalid"
        ) from error
    if (
        backup_parent == key_parent
        or backup_parent in key_parent.parents
        or key_parent in backup_parent.parents
    ):
        raise BackupAuthenticationError(
            "database backup authentication key must be outside the backup tree"
        )


def _require_adjacent_sidecars(
    backup: Path,
    checksum: Path,
    authentication: Path,
) -> None:
    try:
        backup_parent = backup.resolve(strict=True).parent
        checksum_parent = checksum.parent.resolve(strict=True)
        authentication_parent = authentication.parent.resolve(strict=True)
    except OSError as error:
        raise BackupAuthenticationError("database backup sidecar paths are invalid") from error
    if (
        checksum == authentication
        or checksum in {backup, authentication}
        or authentication == backup
        or checksum_parent != backup_parent
        or authentication_parent != backup_parent
    ):
        raise BackupAuthenticationError("database backup sidecar paths are invalid")


def _regular_file_status(path: Path) -> os.stat_result:
    try:
        status = path.lstat()
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup authentication file is unavailable"
        ) from error
    attributes = getattr(status, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        not stat.S_ISREG(status.st_mode)
        or path.is_symlink()
        or (isinstance(attributes, int) and bool(attributes & reparse_flag))
    ):
        raise BackupAuthenticationError("database backup authentication path is not a regular file")
    return status


def _open_regular_file(path: Path) -> BinaryIO:
    before = _regular_file_status(path)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup authentication file cannot be opened"
        ) from error
    try:
        after = os.fstat(descriptor)
    except OSError as error:
        os.close(descriptor)
        raise BackupAuthenticationError(
            "database backup authentication file cannot be inspected"
        ) from error
    if (
        not stat.S_ISREG(after.st_mode)
        or before.st_dev != after.st_dev
        or before.st_ino != after.st_ino
    ):
        os.close(descriptor)
        raise BackupAuthenticationError("database backup authentication file changed")
    try:
        return os.fdopen(descriptor, "rb")
    except OSError as error:
        os.close(descriptor)
        raise BackupAuthenticationError(
            "database backup authentication file cannot be opened"
        ) from error


def _open_new_file(path: Path) -> BinaryIO:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup authentication output cannot be created"
        ) from error
    return os.fdopen(descriptor, "wb")


def _write_new_file(
    path: Path,
    payload: bytes,
    *,
    capacity: FileSystemCapacityGuard | None = None,
) -> None:
    created = False
    complete = False
    try:
        with _open_new_file(path) as destination:
            created = True
            try:
                if capacity is None:
                    destination.write(payload)
                    destination.flush()
                    os.fsync(destination.fileno())
                else:
                    with capacity.reserve(len(payload)):
                        destination.write(payload)
                        destination.flush()
                        os.fsync(destination.fileno())
            except FileSystemCapacityUnavailableError as error:
                raise BackupAuthenticationError(
                    "database backup metadata reached its configured free-space reserve"
                ) from error
        complete = True
    finally:
        if created and not complete:
            path.unlink(missing_ok=True)


def _configured_key_file() -> Path:
    value = os.environ.get(AUTHENTICATION_KEY_ENVIRONMENT, "")
    if not value:
        raise BackupAuthenticationError("database backup authentication key is not configured")
    return Path(value)


def _configured_capacity_limits() -> tuple[int, int]:
    maximum_bytes = _environment_byte_limit(
        MAXIMUM_BACKUP_BYTES_ENVIRONMENT,
        _DEFAULT_MAXIMUM_BACKUP_BYTES,
        positive=True,
    )
    minimum_free_bytes = _environment_byte_limit(
        MINIMUM_FREE_BYTES_ENVIRONMENT,
        _DEFAULT_MINIMUM_FREE_BYTES,
        positive=False,
    )
    return maximum_bytes, minimum_free_bytes


def _private_capacity_directory(root: Path) -> Path:
    try:
        resolved_root = root.resolve(strict=True)
        root_status = resolved_root.lstat()
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup capacity directory is unavailable"
        ) from error
    root_attributes = getattr(root_status, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        not stat.S_ISDIR(root_status.st_mode)
        or resolved_root.is_symlink()
        or (isinstance(root_attributes, int) and bool(root_attributes & reparse_flag))
    ):
        raise BackupAuthenticationError("database backup capacity directory is unsafe")

    user_id: int | None = None
    if os.name == "nt":
        identity_source = os.fsencode(str(Path.home().resolve(strict=False)).casefold())
        identity = hashlib.sha256(identity_source).hexdigest()[:16]
    else:
        # ``getuid`` is intentionally absent from the Windows typeshed module.
        user_id = os.getuid()  # type: ignore[attr-defined]
        mode = stat.S_IMODE(root_status.st_mode)
        if mode & 0o022 and not mode & stat.S_ISVTX:
            raise BackupAuthenticationError("database backup capacity directory is unsafe")
        identity = str(user_id)

    private = resolved_root / f".makolet-capacity-{identity}"
    try:
        private.mkdir(mode=0o700, exist_ok=True)
        private_status = private.lstat()
    except OSError as error:
        raise BackupAuthenticationError(
            "database backup capacity directory is unavailable"
        ) from error
    private_attributes = getattr(private_status, "st_file_attributes", 0)
    if (
        not stat.S_ISDIR(private_status.st_mode)
        or private.is_symlink()
        or (isinstance(private_attributes, int) and bool(private_attributes & reparse_flag))
    ):
        raise BackupAuthenticationError("database backup capacity directory is unsafe")
    if user_id is not None and (
        private_status.st_uid != user_id or stat.S_IMODE(private_status.st_mode) & 0o077
    ):
        raise BackupAuthenticationError("database backup capacity directory is unsafe")
    return private


def _environment_byte_limit(name: str, default: int, *, positive: bool) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise BackupAuthenticationError("database backup capacity setting is invalid") from error
    if value < int(positive):
        raise BackupAuthenticationError("database backup capacity setting is invalid")
    return value


def main(arguments: Sequence[str] | None = None) -> int:
    """Run the small script-facing authentication interface with secret-safe errors."""
    selected = tuple(sys.argv[1:] if arguments is None else arguments)
    command = selected[0] if selected else ""
    try:
        _run_command(selected)
    except Exception:
        messages = {
            "capture-command": "Database backup capture failed.\n",
            "generate-key": "Database backup authentication key generation failed.\n",
            "sign": "Database backup authentication signing failed.\n",
            "validate-command": "Database backup validation failed.\n",
            "write-sidecars": "Database backup sidecar creation failed.\n",
            "verify-copy": "Database backup authentication verification failed.\n",
            "read-checksum": "Database backup checksum validation failed.\n",
        }
        sys.stderr.write(messages.get(command, "Database backup authentication command failed.\n"))
        return 1
    return 0


def _run_command(selected: tuple[str, ...]) -> None:
    command = selected[0] if selected else ""
    if command == "generate-key" and len(selected) == 2:
        generate_authentication_key(Path(selected[1]))
    elif command == "capture-command" and len(selected) >= 5 and selected[3] == "--":
        capture_command_to_backup(
            Path(selected[1]),
            selected[4:],
            Path(selected[2]),
        )
    elif command == "validate-command" and len(selected) >= 4 and selected[2] == "--":
        validate_backup_with_command(Path(selected[1]), selected[3:])
    elif command == "write-sidecars" and len(selected) == 6:
        digest = create_backup_sidecars(
            Path(selected[1]),
            Path(selected[2]),
            Path(selected[3]),
            selected[4],
            _configured_key_file(),
            Path(selected[5]),
        )
        sys.stdout.write(f"{digest}\n")
    elif command == "sign" and len(selected) == 3:
        authenticate_backup(Path(selected[1]), Path(selected[2]), _configured_key_file())
    elif command == "verify-copy" and len(selected) in {4, 5}:
        verify_backup_to_copy(
            Path(selected[1]),
            Path(selected[2]),
            Path(selected[3]),
            _configured_key_file(),
            Path(selected[4]) if len(selected) == 5 else None,
        )
    elif command == "read-checksum" and len(selected) == 2:
        sys.stdout.write(f"{read_checksum(Path(selected[1]))}\n")
    else:
        raise BackupAuthenticationError("database backup authentication command is invalid")


if __name__ == "__main__":
    raise SystemExit(main())
