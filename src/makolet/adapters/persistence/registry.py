"""Idempotent synchronization of the researched source registry into PostgreSQL."""

from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.adapters.persistence.schema import portals, retailers
from makolet.application.models import PortalRegistration, RetailerRegistration


class PostgresRegistryRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def synchronize(
        self,
        retailer_values: Sequence[RetailerRegistration],
        portal_values: Sequence[PortalRegistration],
    ) -> dict[str, int]:
        _validate_registrations(retailer_values, portal_values)
        async with self._engine.begin() as connection:
            retailer_ids: dict[str, object] = {}
            for retailer_registration in retailer_values:
                retailer_id = (
                    await connection.execute(
                        pg_insert(retailers)
                        .values(
                            source_key=retailer_registration.source_key,
                            legal_name=retailer_registration.legal_name,
                            display_name=retailer_registration.display_name,
                            edi=retailer_registration.edi,
                            is_active=retailer_registration.is_active,
                        )
                        .on_conflict_do_update(
                            index_elements=[retailers.c.source_key],
                            set_={
                                "legal_name": retailer_registration.legal_name,
                                "display_name": retailer_registration.display_name,
                                "edi": retailer_registration.edi,
                                "is_active": retailer_registration.is_active,
                                "updated_at": func.clock_timestamp(),
                            },
                        )
                        .returning(retailers.c.id)
                    )
                ).scalar_one()
                retailer_ids[retailer_registration.source_key] = retailer_id
            for portal_registration in portal_values:
                await connection.execute(
                    pg_insert(portals)
                    .values(
                        retailer_id=retailer_ids[portal_registration.retailer_source_key],
                        source_key=portal_registration.source_key,
                        family=portal_registration.family,
                        protocol=portal_registration.protocol.value,
                        base_url=portal_registration.base_url,
                        is_active=portal_registration.is_active,
                    )
                    .on_conflict_do_update(
                        index_elements=[portals.c.retailer_id, portals.c.source_key],
                        set_={
                            "family": portal_registration.family,
                            "protocol": portal_registration.protocol.value,
                            "base_url": portal_registration.base_url,
                            "is_active": portal_registration.is_active,
                            "updated_at": func.clock_timestamp(),
                        },
                    )
                )
            retailer_count = int(
                (await connection.execute(select(func.count()).select_from(retailers))).scalar_one()
            )
            portal_count = int(
                (await connection.execute(select(func.count()).select_from(portals))).scalar_one()
            )
        return {"retailers": retailer_count, "portals": portal_count}


def _validate_registrations(
    retailer_values: Sequence[RetailerRegistration],
    portal_values: Sequence[PortalRegistration],
) -> None:
    retailer_keys = tuple(value.source_key for value in retailer_values)
    if not retailer_keys or len(retailer_keys) != len(set(retailer_keys)):
        raise ValueError("Retailer registrations must have unique source keys")
    if len({value.edi for value in retailer_values if value.edi is not None}) != sum(
        value.edi is not None for value in retailer_values
    ):
        raise ValueError("Retailer registrations must have unique non-null EDI values")
    portal_keys = tuple((value.retailer_source_key, value.source_key) for value in portal_values)
    if len(portal_keys) != len(set(portal_keys)):
        raise ValueError("Portal registrations must be unique within a retailer")
    known = set(retailer_keys)
    if any(value.retailer_source_key not in known for value in portal_values):
        raise ValueError("Portal registration refers to an unknown retailer")


__all__ = ["PostgresRegistryRepository"]
