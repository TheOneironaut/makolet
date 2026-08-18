"""Insert a deterministic, provenance-preserving clean-room demo dataset."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import Executable

from makolet.adapters.archive.s3 import (
    S3ContentAddressedArchive,
    S3UploadProcessConfig,
)
from makolet.adapters.persistence import Database, schema
from makolet.config import load_settings

_OBSERVED_AT = datetime(2026, 1, 1, tzinfo=UTC)
_DEMO_PAYLOAD = b'{"fixture":"makolet-clean-room-demo-v1","license":"CC0-1.0","records":1}\n'
_PRODUCT_ID = UUID("77777777-7777-7777-7777-777777777777")


async def _payload() -> AsyncIterator[bytes]:
    yield _DEMO_PAYLOAD


def _database() -> Database:
    settings = load_settings()
    return Database.from_url(
        settings.database_dsn(),
        pool_size=1,
        max_overflow=0,
        application_name="makolet-demo-seed",
    )


def _s3_archive() -> S3ContentAddressedArchive:
    settings = load_settings()
    if settings.archive_backend != "s3":
        raise RuntimeError("Demo seed requires the S3 archive backend")
    access_key, secret_key = settings.s3_credentials()
    return S3ContentAddressedArchive(
        None,
        settings.s3_bucket,
        key_prefix=settings.s3_key_prefix,
        maximum_object_bytes=settings.archive_maximum_object_bytes,
        minimum_free_bytes=settings.archive_minimum_free_bytes,
        temporary_directory=settings.archive_root,
        upload_process_config=S3UploadProcessConfig(
            endpoint_url=settings.s3_endpoint,
            region_name=settings.s3_region,
            access_key_id=access_key,
            secret_access_key=secret_key,
            path_style=settings.s3_path_style,
            direct_connection=settings.s3_direct_connection_required(),
        ),
    )


async def _scalar_uuid(connection: AsyncConnection, statement: Executable) -> UUID:
    result = await connection.execute(statement)
    value = result.scalar_one()
    if not isinstance(value, UUID):
        raise TypeError("Demo seed did not receive a UUID from PostgreSQL")
    return value


async def _seed_database(object_key: str, digest: str) -> dict[str, str]:
    database = _database()
    try:
        async with database.engine.begin() as connection:
            retailer_id = await _scalar_uuid(
                connection,
                insert(schema.retailers)
                .values(
                    id=UUID("11111111-1111-1111-1111-111111111111"),
                    source_key="makolet-clean-room-demo",
                    legal_name="Makolet clean-room demo",
                    display_name="מכולת הדגמה",
                    edi="demo-0001",
                    is_active=True,
                )
                .on_conflict_do_update(
                    index_elements=[schema.retailers.c.source_key],
                    set_={
                        "legal_name": "Makolet clean-room demo",
                        "display_name": "מכולת הדגמה",
                        "is_active": True,
                    },
                )
                .returning(schema.retailers.c.id),
            )
            portal_id = await _scalar_uuid(
                connection,
                insert(schema.portals)
                .values(
                    id=UUID("22222222-2222-2222-2222-222222222222"),
                    retailer_id=retailer_id,
                    source_key="clean-room-fixture",
                    family="fixture",
                    protocol="fixture",
                    base_url="fixture://makolet-clean-room-demo/",
                    is_active=True,
                )
                .on_conflict_do_update(
                    index_elements=[schema.portals.c.retailer_id, schema.portals.c.source_key],
                    set_={"is_active": True},
                )
                .returning(schema.portals.c.id),
            )
            archive_id = await _scalar_uuid(
                connection,
                insert(schema.raw_archive_objects)
                .values(
                    id=UUID("33333333-3333-3333-3333-333333333333"),
                    content_sha256=digest,
                    object_key=object_key,
                    content_length=len(_DEMO_PAYLOAD),
                    archived_at=_OBSERVED_AT,
                    verified_at=_OBSERVED_AT,
                )
                .on_conflict_do_update(
                    index_elements=[schema.raw_archive_objects.c.content_sha256],
                    set_={"verified_at": _OBSERVED_AT},
                )
                .returning(schema.raw_archive_objects.c.id),
            )
            source_file_id = await _scalar_uuid(
                connection,
                insert(schema.source_files)
                .values(
                    id=UUID("44444444-4444-4444-4444-444444444444"),
                    retailer_id=retailer_id,
                    portal_id=portal_id,
                    remote_id="clean-room-demo-v1",
                    download_url="fixture://makolet-clean-room-demo/demo-v1.json",
                    original_filename="makolet-clean-room-demo-v1.json",
                    document_type="unknown",
                    compression="none",
                    protocol="fixture",
                    status="completed",
                    discovered_at=_OBSERVED_AT,
                    source_timestamp=_OBSERVED_AT,
                    declared_content_length=len(_DEMO_PAYLOAD),
                    media_type="application/json",
                    raw_archive_object_id=archive_id,
                    parser_version="clean-room-demo-v1",
                    download_started_at=_OBSERVED_AT,
                    download_finished_at=_OBSERVED_AT,
                    download_status_code=200,
                    download_content_length=len(_DEMO_PAYLOAD),
                )
                .on_conflict_do_update(
                    index_elements=[
                        schema.source_files.c.portal_id,
                        schema.source_files.c.remote_id,
                    ],
                    set_={
                        "raw_archive_object_id": archive_id,
                        "status": "completed",
                        "parser_version": "clean-room-demo-v1",
                    },
                )
                .returning(schema.source_files.c.id),
            )
            store_id = await _scalar_uuid(
                connection,
                insert(schema.stores)
                .values(
                    id=UUID("55555555-5555-5555-5555-555555555555"),
                    retailer_id=retailer_id,
                    portal_id=portal_id,
                    chain_code="demo-chain",
                    subchain_code="",
                    source_store_code="demo-store-1",
                    chain_name="Makolet clean-room demo",
                    name="סניף הדגמה תל אביב",
                    address="רחוב הדוגמה 1",
                    city="תל אביב-יפו",
                    is_active=True,
                    first_seen_at=_OBSERVED_AT,
                    last_seen_at=_OBSERVED_AT,
                    last_source_file_id=source_file_id,
                )
                .on_conflict_do_update(
                    index_elements=[
                        schema.stores.c.retailer_id,
                        schema.stores.c.portal_id,
                        schema.stores.c.subchain_code,
                        schema.stores.c.source_store_code,
                    ],
                    set_={
                        "name": "סניף הדגמה תל אביב",
                        "is_active": True,
                        "last_source_file_id": source_file_id,
                    },
                )
                .returning(schema.stores.c.id),
            )
            retailer_item_id = await _scalar_uuid(
                connection,
                insert(schema.retailer_items)
                .values(
                    id=UUID("66666666-6666-6666-6666-666666666666"),
                    retailer_id=retailer_id,
                    portal_id=portal_id,
                    source_item_code="demo-item-1",
                    gtin="7290000000015",
                    name="טחינה גולמית להדגמה",
                    manufacturer_name="יצרן הדגמה",
                    unit_quantity="500 גרם",
                    quantity=Decimal("500"),
                    unit_of_measure="גרם",
                    is_weighted=False,
                    quantity_in_package=Decimal("1"),
                    first_seen_at=_OBSERVED_AT,
                    last_seen_at=_OBSERVED_AT,
                    last_source_file_id=source_file_id,
                )
                .on_conflict_do_update(
                    index_elements=[
                        schema.retailer_items.c.retailer_id,
                        schema.retailer_items.c.portal_id,
                        schema.retailer_items.c.source_item_code,
                    ],
                    set_={
                        "name": "טחינה גולמית להדגמה",
                        "last_source_file_id": source_file_id,
                    },
                )
                .returning(schema.retailer_items.c.id),
            )
            await connection.execute(
                insert(schema.canonical_products)
                .values(
                    id=_PRODUCT_ID,
                    name="טחינה גולמית להדגמה 500 גרם",
                    brand="מותג הדגמה",
                    manufacturer="יצרן הדגמה",
                    quantity=Decimal("500"),
                    unit_of_measure="גרם",
                    status="active",
                )
                .on_conflict_do_update(
                    index_elements=[schema.canonical_products.c.id],
                    set_={"name": "טחינה גולמית להדגמה 500 גרם", "status": "active"},
                )
            )
            await connection.execute(
                insert(schema.product_identifiers)
                .values(
                    id=UUID("88888888-8888-8888-8888-888888888888"),
                    product_id=_PRODUCT_ID,
                    kind="gtin",
                    value="7290000000015",
                    normalized_value="7290000000015",
                    issuer_retailer_id=None,
                    is_validated=True,
                )
                .on_conflict_do_nothing()
            )
            await connection.execute(
                insert(schema.confirmed_product_matches)
                .values(
                    id=UUID("99999999-9999-9999-9999-999999999999"),
                    retailer_item_id=retailer_item_id,
                    canonical_product_id=_PRODUCT_ID,
                    method="validated_gtin",
                    evidence={"fixture": "makolet-clean-room-demo-v1"},
                    confirmed_at=_OBSERVED_AT,
                    confirmed_by="makolet-demo-seed",
                )
                .on_conflict_do_update(
                    index_elements=[schema.confirmed_product_matches.c.retailer_item_id],
                    set_={"canonical_product_id": _PRODUCT_ID},
                )
            )
            await connection.execute(
                insert(schema.current_prices)
                .values(
                    id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                    retailer_item_id=retailer_item_id,
                    store_id=store_id,
                    item_price=Decimal("18.90"),
                    unit_of_measure_price=Decimal("3.78"),
                    allow_discount=True,
                    source_updated_at=_OBSERVED_AT,
                    source_file_id=source_file_id,
                    first_observed_at=_OBSERVED_AT,
                    last_observed_at=_OBSERVED_AT,
                )
                .on_conflict_do_update(
                    index_elements=[
                        schema.current_prices.c.retailer_item_id,
                        schema.current_prices.c.store_id,
                    ],
                    set_={"item_price": Decimal("18.90"), "source_file_id": source_file_id},
                )
            )
            await connection.execute(
                insert(schema.current_availability)
                .values(
                    id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                    retailer_item_id=retailer_item_id,
                    store_id=store_id,
                    is_available=True,
                    item_status=1,
                    source_file_id=source_file_id,
                    first_observed_at=_OBSERVED_AT,
                    last_observed_at=_OBSERVED_AT,
                )
                .on_conflict_do_update(
                    index_elements=[
                        schema.current_availability.c.retailer_item_id,
                        schema.current_availability.c.store_id,
                    ],
                    set_={"is_available": True, "source_file_id": source_file_id},
                )
            )
            await connection.execute(
                insert(schema.applied_source_contents)
                .values(
                    id=UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
                    retailer_id=retailer_id,
                    portal_id=portal_id,
                    document_type="unknown",
                    content_sha256=digest,
                    source_file_id=source_file_id,
                    applied_at=_OBSERVED_AT,
                )
                .on_conflict_do_nothing()
            )
        return {
            "retailer_id": str(retailer_id),
            "store_id": str(store_id),
            "product_id": str(_PRODUCT_ID),
            "source_file_id": str(source_file_id),
        }
    finally:
        await database.dispose()


async def _main() -> None:
    digest = hashlib.sha256(_DEMO_PAYLOAD).hexdigest()
    object_key, content_length, created = await _s3_archive().put(
        _payload(), original_filename="makolet-clean-room-demo-v1.json"
    )
    if content_length != len(_DEMO_PAYLOAD):
        raise RuntimeError("Demo archive byte count did not match the authored fixture")
    identifiers = await _seed_database(object_key, digest)
    result = {
        "status": "seeded",
        "archive_created": created,
        "archive_object_key": object_key,
        "archive_sha256": digest,
        **identifiers,
    }
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    asyncio.run(_main())
