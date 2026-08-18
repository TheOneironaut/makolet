from __future__ import annotations

import os
from datetime import UTC, datetime

import httpx
import pytest

from makolet.adapters.sources.bina import DEFAULT_PARTITIONS
from makolet.adapters.sources.common import encode_cursor
from makolet.adapters.sources.http import SafeHttpListingClient
from makolet.adapters.sources.ncr import EnvironmentCredentialProvider, StdlibFtpCatalogClient
from makolet.adapters.sources.registry import RETAILER_REGISTRY, SourceRegistry
from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.domain.enums import DocumentType
from makolet.domain.errors import SourceAccessError, SourceBlockedError, SourceResponseError
from makolet.domain.normalization import ISRAEL_TIMEZONE


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@pytest.mark.live
async def test_selected_source_has_a_low_volume_read_only_discovery_smoke() -> None:
    """Opt in with MAKOLET_LIVE_SOURCE=<retailer-id>; this performs no writes."""

    retailer_id = os.environ.get("MAKOLET_LIVE_SOURCE")
    if retailer_id is None:
        pytest.skip("set MAKOLET_LIVE_SOURCE to opt into one publisher listing smoke test")
    known_ids = {definition.retailer_id for definition in RETAILER_REGISTRY}
    if retailer_id not in known_ids:
        pytest.fail(f"MAKOLET_LIVE_SOURCE is not a configured retailer ID: {retailer_id}")
    allow_insecure_ftp = (
        os.environ.get("MAKOLET_ALLOW_INSECURE_FTP", "false").strip().casefold() == "true"
    )
    async with httpx.AsyncClient(timeout=20.0, trust_env=False, follow_redirects=False) as client:
        registry = SourceRegistry(
            SafeHttpListingClient(client),
            StdlibFtpCatalogClient(
                EnvironmentCredentialProvider(),
                allow_insecure_ftp=allow_insecure_ftp,
            ),
            SystemClock(),
        )
        try:
            pages = await _bounded_live_pages(registry, retailer_id)
        except SourceAccessError as error:
            pytest.fail(f"EXTERNAL_SOURCE_UNAVAILABLE: {error}", pytrace=False)
        except SourceBlockedError as error:
            pytest.fail(f"EXTERNAL_SOURCE_BLOCKED: {error}", pytrace=False)
        except SourceResponseError as error:
            pytest.fail(f"EXTERNAL_SOURCE_INVALID_RESPONSE: {error}", pytrace=False)

    assert all(len(page.files) <= 1 for page in pages)
    assert any(page.files for page in pages)
    assert all(page.files or page.next_cursor is None for page in pages)
    if retailer_id == "maayan-2000":
        assert len(pages) == len(DEFAULT_PARTITIONS)
        observed_types = [page.files[0].document_type for page in pages if page.files]
        expected_order = (
            DocumentType.STORES,
            DocumentType.PRICE_DELTA,
            DocumentType.PROMOTION_DELTA,
            DocumentType.PRICE_FULL,
            DocumentType.PROMOTION_FULL,
        )
        assert observed_types
        observed_index = -1
        for document_type in observed_types:
            next_index = expected_order.index(document_type)
            assert next_index >= observed_index
            observed_index = next_index


async def _bounded_live_pages(
    registry: SourceRegistry,
    retailer_id: str,
) -> tuple[DiscoveryPage, ...]:
    adapter = registry.create(retailer_id)
    if retailer_id != "maayan-2000":
        return (await adapter.discover(None, limit=1),)

    discovery_date = SystemClock().now().astimezone(ISRAEL_TIMEZONE).date().isoformat()
    pages = []
    budget = DiscoveryRunBudget()
    for partition_index, _partition in enumerate(DEFAULT_PARTITIONS):
        cursor = (
            None
            if partition_index == 0
            else DiscoveryCursor(
                encode_cursor(
                    retailer_id,
                    {"date": discovery_date, "partition": partition_index, "offset": 0},
                )
            )
        )
        pages.append(await adapter.discover(cursor, limit=1, budget=budget))
    return tuple(pages)
