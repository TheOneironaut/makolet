"""Behavior checks against a real S3-compatible object store."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from makolet.adapters.archive.s3 import (
    S3ContentAddressedArchive,
    S3UploadProcessConfig,
)

pytestmark = pytest.mark.integration


async def _chunks(payload: bytes) -> AsyncIterator[bytes]:
    midpoint = len(payload) // 2
    yield payload[:midpoint]
    yield payload[midpoint:]


async def test_s3_archive_is_exact_conditional_and_concurrency_safe() -> None:
    endpoint = os.environ.get("MAKOLET_TEST_S3_ENDPOINT")
    if endpoint is None:
        pytest.skip("MAKOLET_TEST_S3_ENDPOINT is not configured")
    bucket = os.environ.get("MAKOLET_TEST_S3_BUCKET", "makolet-raw")
    access_key = os.environ.get("MAKOLET_TEST_S3_ACCESS_KEY")
    secret_key = os.environ.get("MAKOLET_TEST_S3_SECRET_KEY")
    if bool(access_key) is not bool(secret_key):
        pytest.fail("S3 integration access key and secret key must be configured together")
    archive = S3ContentAddressedArchive(
        None,
        bucket,
        key_prefix=f"integration/{uuid4().hex}",
        upload_process_config=S3UploadProcessConfig(
            endpoint_url=endpoint,
            region_name="us-east-1",
            access_key_id=access_key,
            secret_access_key=secret_key,
            path_style=True,
            unsigned=not bool(access_key),
        ),
    )
    payload = "מחיר exact wire bytes\x00\xff".encode()

    first, second = await asyncio.gather(
        archive.put(_chunks(payload), original_filename="PriceFull.xml.gz"),
        archive.put(_chunks(payload), original_filename="PRICEFULL.XML.GZ"),
    )

    assert first[0] == second[0]
    assert first[1] == second[1] == len(payload)
    assert sorted((first[2], second[2])) == [False, True]
    received = bytearray()
    async with archive.open(first[0]) as chunks:
        async for chunk in chunks:
            received.extend(chunk)
    assert bytes(received) == payload
