from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
import textwrap
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from makolet.adapters.archive.local import LocalContentAddressedArchive
from makolet.domain.errors import ArchiveCapacityError, ArchiveIntegrityError, DownloadLimitError


async def byte_stream(*chunks: bytes) -> AsyncIterator[bytes]:
    for chunk in chunks:
        yield chunk


@pytest.mark.asyncio
async def test_local_archive_round_trip_is_content_addressed(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path)
    payload = b"exact\x00source\nbytes"

    key, length, created = await archive.put(
        byte_stream(payload[:5], payload[5:]), original_filename="x.gz"
    )

    expected = hashlib.sha256(payload).hexdigest()
    assert key == f"sha256/{expected[:2]}/{expected[2:4]}/{expected}"
    assert (length, created) == (len(payload), True)
    assert await archive.exists(key)
    assert await archive.verify(key, expected) == len(payload)
    collected = bytearray()
    async with archive.open(key) as chunks:
        async for chunk in chunks:
            collected.extend(chunk)
    assert bytes(collected) == payload


@pytest.mark.asyncio
async def test_local_archive_never_replaces_duplicate_content(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path)

    first = await archive.put(byte_stream(b"same"), original_filename="first.xml")
    second = await archive.put(byte_stream(b"sa", b"me"), original_filename="second.xml")

    assert first[:2] == second[:2]
    assert first[2] is True
    assert second[2] is False


@pytest.mark.asyncio
async def test_local_archive_removes_partial_file_after_limit_failure(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path, maximum_object_bytes=3)

    with pytest.raises(DownloadLimitError):
        await archive.put(byte_stream(b"four"), original_filename="too-large.xml")

    assert not list((tmp_path / ".incoming").glob("*.part"))


@pytest.mark.asyncio
async def test_local_archive_preserves_configured_free_space_reserve(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path, minimum_free_bytes=2**63)

    with pytest.raises(ArchiveCapacityError, match="free-space reserve"):
        await archive.put(byte_stream(b"bounded"), original_filename="bounded.xml")

    assert not list((tmp_path / ".incoming").glob("*.part"))


@pytest.mark.asyncio
async def test_local_archive_rejects_noncanonical_key(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path)

    with pytest.raises(ArchiveIntegrityError, match="canonical"):
        await archive.exists("../../outside")


@pytest.mark.asyncio
async def test_local_archive_detects_expected_digest_mismatch(tmp_path: Path) -> None:
    archive = LocalContentAddressedArchive(tmp_path)
    key, _, _ = await archive.put(byte_stream(b"payload"), original_filename="x.xml")

    with pytest.raises(ArchiveIntegrityError, match="disagree"):
        await archive.verify(key, "0" * 64)


@pytest.mark.asyncio
async def test_local_archive_post_commit_failure_reports_committed_byte_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path)
    payload = b"committed-before-protection"

    async def fail_protection(_object_path: Path) -> None:
        raise OSError("simulated protection failure")

    monkeypatch.setattr(archive, "_protect_read_only", fail_protection)

    with pytest.raises(ArchiveIntegrityError, match="commit could not be confirmed") as caught:
        await archive.put(byte_stream(payload), original_filename="source.xml")

    digest = hashlib.sha256(payload).hexdigest()
    assert caught.value.transferred_bytes == len(payload)
    assert await archive.exists(archive.key_for_digest(digest))


@pytest.mark.asyncio
async def test_local_archive_rejects_tampered_bytes_before_exposing_parser_stream(
    tmp_path: Path,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path)
    key, _, _ = await archive.put(byte_stream(b"trusted"), original_filename="x.xml")
    assert await archive.verify(key, key.rsplit("/", 1)[-1]) == len(b"trusted")
    object_path = tmp_path.joinpath(*key.split("/"))
    object_path.chmod(stat.S_IREAD | stat.S_IWRITE)
    object_path.write_bytes(b"hostile")
    parser_stream_opened = False

    async def consume() -> None:
        nonlocal parser_stream_opened
        async with archive.open(key) as chunks:
            parser_stream_opened = True
            _ = [chunk async for chunk in chunks]

    with pytest.raises(ArchiveIntegrityError, match="SHA-256"):
        await consume()

    assert not parser_stream_opened


@pytest.mark.asyncio
async def test_local_archive_parser_reads_verified_private_spool_after_path_replacement(
    tmp_path: Path,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path, chunk_size=3)
    payload = b"verified-local-object"
    key, _, _ = await archive.put(byte_stream(payload), original_filename="x.xml")
    object_path = tmp_path.joinpath(*key.split("/"))

    async with archive.open(key) as chunks:
        object_path.chmod(stat.S_IREAD | stat.S_IWRITE)
        object_path.unlink()
        object_path.write_bytes(b"replacement-is-hostile")
        received = b"".join([chunk async for chunk in chunks])

    assert received == payload


@pytest.mark.asyncio
async def test_local_archive_rejects_oversized_existing_object_before_yield(
    tmp_path: Path,
) -> None:
    payload = b"12345"
    digest = hashlib.sha256(payload).hexdigest()
    key = LocalContentAddressedArchive.key_for_digest(digest)
    object_path = tmp_path.joinpath(*key.split("/"))
    object_path.parent.mkdir(parents=True)
    object_path.write_bytes(payload)
    archive = LocalContentAddressedArchive(tmp_path, maximum_object_bytes=4)

    with pytest.raises(ArchiveIntegrityError, match="configured bounds"):
        async with archive.open(key):
            pass


@pytest.mark.asyncio
async def test_local_archive_rejects_symlink_object_without_following_it(
    tmp_path: Path,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path / "archive")
    payload = b"trusted"
    key, _, _ = await archive.put(byte_stream(payload), original_filename="x.xml")
    object_path = (tmp_path / "archive").joinpath(*key.split("/"))
    target = tmp_path / "outside"
    target.write_bytes(payload)
    object_path.chmod(stat.S_IREAD | stat.S_IWRITE)
    object_path.unlink()
    try:
        object_path.symlink_to(target)
    except OSError as error:
        pytest.skip(f"host cannot create a test symlink: {error}")

    with pytest.raises(ArchiveIntegrityError, match="regular non-linked"):
        async with archive.open(key):
            pass


@pytest.mark.asyncio
async def test_local_archive_rejects_an_object_with_a_second_hard_link(
    tmp_path: Path,
) -> None:
    archive = LocalContentAddressedArchive(tmp_path / "archive")
    key, _, _ = await archive.put(byte_stream(b"trusted"), original_filename="x.xml")
    object_path = (tmp_path / "archive").joinpath(*key.split("/"))
    (tmp_path / "writable-alias").hardlink_to(object_path)

    with pytest.raises(ArchiveIntegrityError, match="regular non-linked"):
        async with archive.open(key):
            pass


def test_local_stalled_verifier_cannot_keep_python_process_alive(tmp_path: Path) -> None:
    support = tmp_path / "support"
    support.mkdir()
    (support / "stall_target.py").write_text(
        textwrap.dedent(
            """
            import os
            import threading

            def stall(duplicated_spool, *args):
                descriptor = duplicated_spool.detach()
                with os.fdopen(descriptor, "w+b", closefd=True):
                    threading.Event().wait()
            """
        ),
        encoding="utf-8",
    )
    script = textwrap.dedent(
        f"""
        import asyncio
        import hashlib
        from pathlib import Path

        import stall_target
        import makolet.adapters.archive.local as local_module
        from makolet.domain.errors import ArchiveIntegrityError

        root = Path({str(tmp_path / "archive")!r})
        payload = b"verified bytes"
        digest = hashlib.sha256(payload).hexdigest()
        key = local_module.LocalContentAddressedArchive.key_for_digest(digest)
        path = root.joinpath(*key.split("/"))
        path.parent.mkdir(parents=True)
        path.write_bytes(payload)
        local_module._verified_spool_in_child = stall_target.stall

        async def main():
            archive = local_module.LocalContentAddressedArchive(
                root,
                verify_timeout_seconds=0.1,
            )
            try:
                async with archive.open(key):
                    pass
            except ArchiveIntegrityError as error:
                assert "deadline" in str(error)
            else:
                raise AssertionError("stalled verifier unexpectedly completed")

        asyncio.run(main())
        print("bounded-exit")
        """
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join((str(support), environment.get("PYTHONPATH", "")))

    completed = subprocess.run(  # noqa: S603 - fixed interpreter and authored test script
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        cwd=tmp_path,
        env=environment,
        text=True,
        timeout=5,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "bounded-exit"
