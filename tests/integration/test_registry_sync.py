from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from sqlalchemy import select

from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.registry import PostgresRegistryRepository
from makolet.adapters.persistence.schema import portals, retailers
from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.ncr import FtpCatalogClient
from makolet.adapters.sources.registry import SourceRegistry

pytestmark = pytest.mark.integration


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 8, 11, tzinfo=UTC)


@pytest.mark.asyncio
async def test_official_registry_sync_is_exact_and_idempotent(database: Database) -> None:
    registry = SourceRegistry(
        cast(HttpListingClient, object()),
        cast(FtpCatalogClient, object()),
        FixedClock(),
    )
    retailer_values = registry.retailer_registrations()
    portal_values = registry.portal_registrations()
    subject = PostgresRegistryRepository(database.engine)

    first = await subject.synchronize(retailer_values, portal_values)
    second = await subject.synchronize(retailer_values, portal_values)

    assert first == second == {"retailers": 28, "portals": 30}
    async with database.engine.connect() as connection:
        retailer_rows = (
            (
                await connection.execute(
                    select(
                        retailers.c.source_key,
                        retailers.c.legal_name,
                        retailers.c.display_name,
                        retailers.c.edi,
                        retailers.c.is_active,
                    )
                )
            )
            .mappings()
            .all()
        )
        portal_rows = (
            (
                await connection.execute(
                    select(
                        retailers.c.source_key.label("retailer_source_key"),
                        portals.c.source_key,
                        portals.c.family,
                        portals.c.protocol,
                        portals.c.base_url,
                        portals.c.is_active,
                    ).join(retailers, portals.c.retailer_id == retailers.c.id)
                )
            )
            .mappings()
            .all()
        )

    expected_retailers = {
        value.source_key: (
            value.legal_name,
            value.display_name,
            value.edi,
            value.is_active,
        )
        for value in retailer_values
    }
    assert {
        str(row["source_key"]): (
            row["legal_name"],
            row["display_name"],
            row["edi"],
            row["is_active"],
        )
        for row in retailer_rows
    } == expected_retailers
    assert {
        (
            str(row["retailer_source_key"]),
            str(row["source_key"]),
            str(row["family"]),
            str(row["protocol"]),
            str(row["base_url"]),
            bool(row["is_active"]),
        )
        for row in portal_rows
    } == {
        (
            value.retailer_source_key,
            value.source_key,
            value.family,
            value.protocol.value,
            value.base_url,
            value.is_active,
        )
        for value in portal_values
    }
    assert all("@" not in str(row["base_url"]) for row in portal_rows)
