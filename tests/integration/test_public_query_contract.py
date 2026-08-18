"""Real-PostgreSQL proof for the shared public query contract."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from sqlalchemy import insert, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection
from typer.testing import CliRunner, Result

from makolet.adapters.persistence import queries as persistence_queries
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.queries import (
    _ACTIVE_PROMOTIONS_PRODUCT_QUERY,
    _ACTIVE_PROMOTIONS_TIME_PROBE_QUERY,
    _CITY_STORES_CURSOR_QUERY,
    _CITY_STORES_FIRST_PAGE_QUERY,
    _CURRENT_PRICES_CURSOR_QUERY,
    _CURRENT_PRICES_FIRST_PAGE_QUERY,
    _FRESHNESS_QUERY,
    _FUZZY_STORES_CURSOR_QUERY,
    _FUZZY_STORES_FIRST_PAGE_QUERY,
    _ITEM_AVAILABILITY_CURSOR_QUERY,
    _ITEM_AVAILABILITY_FIRST_PAGE_QUERY,
    _PRICE_HISTORY_CURSOR_QUERY,
    _PRICE_HISTORY_FIRST_PAGE_QUERY,
    _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY,
    _PROMOTION_HISTORY_PRODUCT_QUERY,
    _PROMOTION_HISTORY_QUERY,
    _PROMOTION_HISTORY_TIME_PROBE_QUERY,
    MAXIMUM_HISTORY_PROBE_RESULTS,
    MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
    MAXIMUM_PROMOTION_PROBE_RESULTS,
    MAXIMUM_PROMOTION_RELATIONS,
    PostgresQueryRepository,
)
from makolet.adapters.persistence.schema import (
    canonical_products,
    confirmed_product_matches,
    current_availability,
    current_prices,
    portals,
    price_history,
    product_identifiers,
    promotion_clubs,
    promotion_items,
    promotion_stores,
    promotions,
    raw_archive_objects,
    retailer_items,
    retailers,
    source_files,
    stores,
)
from makolet.application.models import MAXIMUM_FRESHNESS_ITEMS_PER_STORE
from makolet.application.queries import QueryService
from makolet.domain.errors import DomainValidationError, QueryLimitError
from makolet.interfaces.api import create_app
from makolet.interfaces.cli import build_cli
from makolet.interfaces.mcp import LATEST_PROTOCOL_VERSION, MakoletMcpServer

pytestmark = pytest.mark.integration

RETAILER_ID = UUID("81000000-0000-0000-0000-000000000001")
PORTAL_A_ID = UUID("82000000-0000-0000-0000-000000000001")
PORTAL_B_ID = UUID("82000000-0000-0000-0000-000000000002")
SOURCE_OLD_ID = UUID("83000000-0000-0000-0000-000000000001")
SOURCE_CURRENT_ID = UUID("83000000-0000-0000-0000-000000000002")
SOURCE_PORTAL_B_ID = UUID("83000000-0000-0000-0000-000000000003")
STORE_A_ID = UUID("84000000-0000-0000-0000-000000000001")
STORE_A_SECOND_ID = UUID("84000000-0000-0000-0000-000000000002")
STORE_B_ID = UUID("84000000-0000-0000-0000-000000000003")
ITEM_A_ID = UUID("85000000-0000-0000-0000-000000000001")
ITEM_A_SECOND_ID = UUID("85000000-0000-0000-0000-000000000002")
ITEM_B_ID = UUID("85000000-0000-0000-0000-000000000003")
PRODUCT_A_ID = UUID("86000000-0000-0000-0000-000000000001")
PRODUCT_B_ID = UUID("86000000-0000-0000-0000-000000000002")
PROMOTION_OLD_ID = UUID("87000000-0000-0000-0000-000000000001")
PROMOTION_CURRENT_ID = UUID("87000000-0000-0000-0000-000000000002")

START = datetime(2026, 8, 1, tzinfo=UTC)
VERSION_CHANGE = datetime(2026, 8, 5, tzinfo=UTC)
NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
END = datetime(2026, 8, 31, 23, 59, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class QueryClock:
    def now(self) -> datetime:
        return NOW


async def _seed_public_contract(database: Database) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(
            insert(retailers).values(
                id=RETAILER_ID,
                source_key="public-query-retailer",
                legal_name="Public Query Retailer Ltd",
                display_name="Public Query Retailer",
            )
        )
        await connection.execute(
            insert(portals),
            (
                {
                    "id": PORTAL_A_ID,
                    "retailer_id": RETAILER_ID,
                    "source_key": "public-query-portal-a",
                    "family": "fixture",
                    "protocol": "fixture",
                },
                {
                    "id": PORTAL_B_ID,
                    "retailer_id": RETAILER_ID,
                    "source_key": "public-query-portal-b",
                    "family": "fixture",
                    "protocol": "fixture",
                },
            ),
        )
        archive_rows = (
            {
                "id": UUID("88000000-0000-0000-0000-000000000001"),
                "content_sha256": "1" * 64,
                "object_key": "sha256/" + "1" * 64,
                "content_length": 101,
                "archived_at": START,
            },
            {
                "id": UUID("88000000-0000-0000-0000-000000000002"),
                "content_sha256": "2" * 64,
                "object_key": "sha256/" + "2" * 64,
                "content_length": 202,
                "archived_at": VERSION_CHANGE,
            },
            {
                "id": UUID("88000000-0000-0000-0000-000000000003"),
                "content_sha256": "3" * 64,
                "object_key": "sha256/" + "3" * 64,
                "content_length": 303,
                "archived_at": NOW,
            },
        )
        await connection.execute(insert(raw_archive_objects), archive_rows)
        await connection.execute(
            insert(source_files),
            (
                _source_values(
                    SOURCE_OLD_ID,
                    PORTAL_A_ID,
                    "promotion-old",
                    "promotion_full",
                    archive_rows[0]["id"],
                    START,
                ),
                _source_values(
                    SOURCE_CURRENT_ID,
                    PORTAL_A_ID,
                    "promotion-current",
                    "promotion_full",
                    archive_rows[1]["id"],
                    VERSION_CHANGE,
                ),
                _source_values(
                    SOURCE_PORTAL_B_ID,
                    PORTAL_B_ID,
                    "price-portal-b",
                    "price_full",
                    archive_rows[2]["id"],
                    NOW,
                ),
            ),
        )
        await connection.execute(
            insert(stores),
            (
                _store_values(STORE_A_ID, PORTAL_A_ID, "001", "Alpha"),
                _store_values(STORE_A_SECOND_ID, PORTAL_A_ID, "002", "Beta"),
                _store_values(STORE_B_ID, PORTAL_B_ID, "001", "Portal B Alpha"),
            ),
        )
        await connection.execute(
            insert(retailer_items),
            (
                _item_values(ITEM_A_ID, PORTAL_A_ID, "COLLIDE-1", "Portal A Product"),
                _item_values(ITEM_A_SECOND_ID, PORTAL_A_ID, "EXTRA-2", "אורז"),
                _item_values(ITEM_B_ID, PORTAL_B_ID, "COLLIDE-1", "Portal B Product"),
            ),
        )
        await connection.execute(
            insert(canonical_products),
            (
                {"id": PRODUCT_A_ID, "name": "Portal A Product", "status": "active"},
                {"id": PRODUCT_B_ID, "name": "Portal B Product", "status": "active"},
            ),
        )
        await connection.execute(
            insert(confirmed_product_matches),
            (
                _match_values(ITEM_A_ID, PRODUCT_A_ID),
                _match_values(ITEM_A_SECOND_ID, PRODUCT_A_ID),
                _match_values(ITEM_B_ID, PRODUCT_B_ID),
            ),
        )
        await connection.execute(
            insert(product_identifiers),
            (
                {
                    "product_id": PRODUCT_A_ID,
                    "kind": "retailer_item",
                    "value": "COLLIDE-1",
                    "normalized_value": "COLLIDE-1",
                    "issuer_retailer_id": RETAILER_ID,
                    "issuer_portal_id": PORTAL_A_ID,
                    "is_validated": True,
                    "validation_method": "source_assertion",
                    "validation_evidence": {"source_file_id": str(SOURCE_CURRENT_ID)},
                },
                {
                    "product_id": PRODUCT_A_ID,
                    "kind": "gtin",
                    "value": "7290000000015",
                    "normalized_value": "7290000000015",
                    "issuer_retailer_id": None,
                    "issuer_portal_id": None,
                    "is_validated": True,
                    "validation_method": "checksum_and_corroboration",
                    "validation_evidence": {"fixture": True},
                },
            ),
        )
        await connection.execute(
            insert(current_prices).values(
                retailer_item_id=ITEM_A_ID,
                store_id=STORE_A_ID,
                item_price=Decimal("8.90"),
                unit_of_measure_price=Decimal("8.90"),
                allow_discount=True,
                source_updated_at=VERSION_CHANGE,
                source_file_id=SOURCE_CURRENT_ID,
                first_observed_at=START,
                last_observed_at=VERSION_CHANGE,
            )
        )
        await connection.execute(
            insert(current_availability).values(
                retailer_item_id=ITEM_A_ID,
                store_id=STORE_A_ID,
                is_available=True,
                item_status=1,
                source_file_id=SOURCE_CURRENT_ID,
                first_observed_at=START,
                last_observed_at=VERSION_CHANGE,
            )
        )
        await connection.execute(
            insert(price_history),
            (
                {
                    "retailer_item_id": ITEM_A_ID,
                    "store_id": STORE_A_ID,
                    "item_price": Decimal("9.90"),
                    "source_file_id": SOURCE_OLD_ID,
                    "valid_from": START,
                    "valid_to": VERSION_CHANGE,
                },
                {
                    "retailer_item_id": ITEM_A_ID,
                    "store_id": STORE_A_ID,
                    "item_price": Decimal("8.90"),
                    "source_file_id": SOURCE_CURRENT_ID,
                    "valid_from": VERSION_CHANGE,
                    "valid_to": None,
                },
            ),
        )
        await connection.execute(
            insert(promotions),
            (
                _promotion_values(
                    PROMOTION_OLD_ID,
                    SOURCE_OLD_ID,
                    Decimal("18.00"),
                    START,
                    VERSION_CHANGE,
                    "a" * 64,
                    reward_type=1,
                ),
                _promotion_values(
                    PROMOTION_CURRENT_ID,
                    SOURCE_CURRENT_ID,
                    Decimal("16.00"),
                    VERSION_CHANGE,
                    None,
                    "b" * 64,
                    reward_type=2,
                ),
            ),
        )
        await connection.execute(
            insert(promotion_items),
            (
                {"promotion_id": PROMOTION_OLD_ID, "retailer_item_id": ITEM_A_ID},
                {"promotion_id": PROMOTION_OLD_ID, "retailer_item_id": ITEM_A_SECOND_ID},
                {"promotion_id": PROMOTION_CURRENT_ID, "retailer_item_id": ITEM_A_ID},
            ),
        )
        await connection.execute(
            insert(promotion_stores),
            (
                {"promotion_id": PROMOTION_OLD_ID, "store_id": STORE_A_ID},
                {"promotion_id": PROMOTION_OLD_ID, "store_id": STORE_A_SECOND_ID},
                {"promotion_id": PROMOTION_CURRENT_ID, "store_id": STORE_A_ID},
            ),
        )
        await connection.execute(
            insert(promotion_clubs),
            (
                {"promotion_id": PROMOTION_OLD_ID, "club_id": "ALPHA"},
                {"promotion_id": PROMOTION_OLD_ID, "club_id": "BETA"},
            ),
        )
        await _seed_oversized_current_relations(connection)


def _source_values(
    source_id: UUID,
    portal_id: UUID,
    remote_id: str,
    document_type: str,
    archive_id: object,
    observed_at: datetime,
) -> dict[str, object]:
    return {
        "id": source_id,
        "retailer_id": RETAILER_ID,
        "portal_id": portal_id,
        "remote_id": remote_id,
        "download_url": f"https://fixtures.invalid/{remote_id}",
        "original_filename": f"{remote_id}.xml",
        "document_type": document_type,
        "compression": "none",
        "protocol": "fixture",
        "status": "completed",
        "discovered_at": observed_at,
        "source_timestamp": observed_at,
        "raw_archive_object_id": archive_id,
    }


def _store_values(store_id: UUID, portal_id: UUID, code: str, name: str) -> dict[str, object]:
    return {
        "id": store_id,
        "retailer_id": RETAILER_ID,
        "portal_id": portal_id,
        "chain_code": "chain",
        "subchain_code": "subchain",
        "source_store_code": code,
        "name": name,
        "city": "Jerusalem",
        "last_source_file_id": SOURCE_CURRENT_ID,
    }


def _item_values(item_id: UUID, portal_id: UUID, code: str, name: str) -> dict[str, object]:
    source_id = SOURCE_CURRENT_ID if portal_id == PORTAL_A_ID else SOURCE_PORTAL_B_ID
    if item_id == ITEM_A_ID:
        quantity, unit = Decimal("1"), "l"
    elif item_id == ITEM_A_SECOND_ID:
        quantity, unit = Decimal("1"), "kg"
    else:
        quantity, unit = Decimal("10"), "l"
    return {
        "id": item_id,
        "retailer_id": RETAILER_ID,
        "portal_id": portal_id,
        "source_item_code": code,
        "name": name,
        "quantity": quantity,
        "unit_of_measure": unit,
        "first_seen_at": START,
        "last_seen_at": NOW,
        "last_source_file_id": source_id,
    }


def _match_values(item_id: UUID, product_id: UUID) -> dict[str, object]:
    return {
        "retailer_item_id": item_id,
        "canonical_product_id": product_id,
        "method": "operator_review",
        "evidence": {"fixture": True},
        "confirmed_by": "integration-test",
    }


def _promotion_values(
    promotion_id: UUID,
    source_id: UUID,
    discounted_price: Decimal,
    valid_from: datetime,
    valid_to: datetime | None,
    fingerprint: str,
    *,
    reward_type: int,
) -> dict[str, object]:
    return {
        "id": promotion_id,
        "retailer_id": RETAILER_ID,
        "portal_id": PORTAL_A_ID,
        "subchain_code": "subchain",
        "source_promotion_id": "PROMO-1",
        "source_scope_store_code": "",
        "description": "Clean-room promotion",
        "discount_kind": "quantity",
        "starts_at": START,
        "ends_at": END,
        "reward_type": reward_type,
        "allows_multiple_discounts": False,
        "minimum_quantity": Decimal("2"),
        "maximum_quantity": Decimal("4"),
        "discount_rate": Decimal("0.10"),
        "minimum_purchase": Decimal("20.00"),
        "discounted_price": discounted_price,
        "discounted_unit_price": discounted_price / 2,
        "minimum_items_offered": 2,
        "additional_restrictions": "Members only",
        "remarks": "Synthetic fixture",
        "is_active": True,
        "fingerprint_sha256": fingerprint,
        "source_file_id": source_id,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "last_observed_at": valid_from,
    }


async def _seed_oversized_current_relations(connection: AsyncConnection) -> None:
    execute = connection.execute
    relation_count = MAXIMUM_PROMOTION_RELATIONS + 5
    await execute(
        text(
            """
            INSERT INTO retailer_items (
                id, retailer_id, portal_id, source_item_code, name,
                first_seen_at, last_seen_at, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id,
                   'BULK-ITEM-' || lpad(series.value::text, 4, '0'),
                   'Bulk item ' || series.value,
                   :observed_at, :observed_at, :source_file_id
              FROM generate_series(1, :relation_count) AS series(value)
            """
        ),
        {
            "retailer_id": RETAILER_ID,
            "portal_id": PORTAL_A_ID,
            "observed_at": NOW,
            "source_file_id": SOURCE_CURRENT_ID,
            "relation_count": relation_count,
        },
    )
    await execute(
        text(
            """
            INSERT INTO stores (
                id, retailer_id, portal_id, chain_code, subchain_code,
                source_store_code, name, last_source_file_id
            )
            SELECT uuidv7(), :retailer_id, :portal_id, 'chain', 'subchain',
                   'BULK-STORE-' || lpad(series.value::text, 4, '0'),
                   'Bulk store ' || series.value, :source_file_id
              FROM generate_series(1, :relation_count) AS series(value)
            """
        ),
        {
            "retailer_id": RETAILER_ID,
            "portal_id": PORTAL_A_ID,
            "source_file_id": SOURCE_CURRENT_ID,
            "relation_count": relation_count,
        },
    )
    await execute(
        text(
            """
            INSERT INTO promotion_items (promotion_id, retailer_item_id)
            SELECT :promotion_id, item.id
              FROM retailer_items item
             WHERE item.portal_id = :portal_id
               AND item.source_item_code LIKE 'BULK-ITEM-%'
            """
        ),
        {"promotion_id": PROMOTION_CURRENT_ID, "portal_id": PORTAL_A_ID},
    )
    await execute(
        text(
            """
            INSERT INTO promotion_stores (promotion_id, store_id)
            SELECT :promotion_id, store.id
              FROM stores store
             WHERE store.portal_id = :portal_id
               AND store.source_store_code LIKE 'BULK-STORE-%'
            """
        ),
        {"promotion_id": PROMOTION_CURRENT_ID, "portal_id": PORTAL_A_ID},
    )
    await execute(
        text(
            """
            INSERT INTO promotion_clubs (promotion_id, club_id)
            SELECT :promotion_id,
                   'BULK-CLUB-' || lpad(series.value::text, 4, '0')
              FROM generate_series(1, :relation_count) AS series(value)
            """
        ),
        {"promotion_id": PROMOTION_CURRENT_ID, "relation_count": relation_count},
    )


async def _seed_current_head_plan_evidence(database: Database) -> None:
    item_count = MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 5
    filler_store_count = 50
    history_row_count = 1_005
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO promotions (
                    id, retailer_id, portal_id, subchain_code,
                    source_promotion_id, source_scope_store_code,
                    description, discount_kind, fingerprint_sha256,
                    source_file_id, valid_from, valid_to, last_observed_at,
                    is_active
                )
                SELECT uuidv7(), :retailer_id, :portal_id, 'subchain',
                       'PLAN-HISTORY-' || lpad(series.value::text, 4, '0'), '',
                       'Plan history row ' || series.value, 'unknown',
                       repeat('c', 64), :source_file_id,
                       CAST(:history_end AS timestamptz)
                           - make_interval(days => series.value::integer),
                       CAST(:history_end AS timestamptz),
                       CAST(:history_end AS timestamptz)
                           - make_interval(days => series.value::integer),
                       false
                   FROM generate_series(1, :history_row_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_OLD_ID,
                "history_end": START,
                "history_row_count": history_row_count,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    id, retailer_id, portal_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                )
                SELECT uuidv7(), :retailer_id, :portal_id,
                       'PLAN-ITEM-' || lpad(series.value::text, 4, '0'),
                       'Plan item ' || series.value,
                       :first_seen_at, :last_seen_at, :source_file_id
                  FROM generate_series(1, :item_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "first_seen_at": START,
                "last_seen_at": NOW,
                "source_file_id": SOURCE_CURRENT_ID,
                "item_count": item_count,
            },
        )
        await connection.execute(
            text(
                """
                WITH filler_items AS MATERIALIZED (
                    SELECT item.id
                      FROM retailer_items item
                     WHERE item.portal_id = :portal_id
                       AND item.source_item_code LIKE 'PLAN-ITEM-%'
                     ORDER BY item.id
                     LIMIT :filler_item_count
                ), filler_stores AS MATERIALIZED (
                    SELECT store.id
                      FROM stores store
                     WHERE store.portal_id = :portal_id
                       AND store.source_store_code LIKE 'BULK-STORE-%'
                     ORDER BY store.id
                     LIMIT :filler_store_count
                     OFFSET 2
                )
                INSERT INTO current_availability (
                    retailer_item_id, store_id, is_available, item_status,
                    source_file_id, first_observed_at, last_observed_at
                )
                SELECT item.id, store.id, true, 1,
                       :source_file_id,
                       CAST(:first_observed_at AS timestamptz),
                       CAST(:last_observed_at AS timestamptz)
                  FROM filler_items item
                 CROSS JOIN filler_stores store
                """
            ),
            {
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
                "first_observed_at": START,
                "last_observed_at": VERSION_CHANGE,
                "filler_item_count": MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
                "filler_store_count": filler_store_count,
            },
        )
        await connection.execute(
            text(
                """
                WITH selected_items AS MATERIALIZED (
                    SELECT item.id,
                           row_number() OVER (ORDER BY item.id) AS item_number
                      FROM retailer_items item
                     WHERE item.portal_id = :portal_id
                       AND item.source_item_code LIKE 'PLAN-ITEM-%'
                ), selected_stores AS MATERIALIZED (
                    SELECT selected.id
                      FROM (
                            SELECT store.id
                              FROM stores store
                             WHERE store.portal_id = :portal_id
                               AND store.source_store_code LIKE 'BULK-STORE-%'
                             ORDER BY store.id
                             LIMIT 2
                           ) selected
                    UNION ALL
                    SELECT CAST(:fixed_store_id AS uuid)
                )
                INSERT INTO current_availability (
                    retailer_item_id, store_id, is_available, item_status,
                    source_file_id, first_observed_at, last_observed_at
                )
                SELECT item.id, store.id, true, 1,
                       CASE WHEN item.item_number = 1
                            THEN CAST(:latest_source_file_id AS uuid)
                            ELSE CAST(:ordinary_source_file_id AS uuid)
                       END,
                       CAST(:first_observed_at AS timestamptz),
                       CASE WHEN item.item_number = 1
                            THEN CAST(:latest_observed_at AS timestamptz)
                            ELSE CAST(:ordinary_observed_at AS timestamptz)
                       END
                  FROM selected_items item
                 CROSS JOIN selected_stores store
                """
            ),
            {
                "portal_id": PORTAL_A_ID,
                "fixed_store_id": STORE_A_ID,
                "latest_source_file_id": SOURCE_PORTAL_B_ID,
                "ordinary_source_file_id": SOURCE_CURRENT_ID,
                "first_observed_at": START,
                "latest_observed_at": NOW,
                "ordinary_observed_at": VERSION_CHANGE,
            },
        )
        await connection.execute(
            text(
                """
                ANALYZE promotions, promotion_items, promotion_stores,
                        promotion_clubs, stores, retailer_items,
                        current_availability, source_files, raw_archive_objects
                """
            )
        )


async def test_public_query_contract_is_portal_scoped_provenanced_and_bounded(
    database: Database,
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    await _seed_public_contract(database)
    await _assert_promotion_relation_plans_are_keyset_bounded(database)
    queries = QueryService(PostgresQueryRepository(database.engine), QueryClock())

    with pytest.raises(DomainValidationError, match="ambiguous across source portals"):
        await queries.find_product_by_retailer_item_code(RETAILER_ID, "COLLIDE-1")
    portal_product = await queries.find_product_by_retailer_item_code(
        RETAILER_ID,
        "COLLIDE-1",
        portal_id=PORTAL_A_ID,
    )
    assert portal_product is not None
    assert portal_product["id"] == PRODUCT_A_ID
    assert portal_product["portal_key"] == "public-query-portal-a"
    assert portal_product["retailer_item_source_file_id"] == SOURCE_CURRENT_ID

    product = await queries.get_product(PRODUCT_A_ID)
    assert product is not None
    identifiers = product["identifiers"]
    assert isinstance(identifiers, list)
    identifier = next(item for item in identifiers if item["kind"] == "retailer_item")
    assert isinstance(identifier, dict)
    assert identifier["issuer_portal_id"] == str(PORTAL_A_ID)
    assert identifier["issuer_portal_key"] == "public-query-portal-a"
    assert identifier["validation_evidence"] == {"source_file_id": str(SOURCE_CURRENT_ID)}

    prices = await queries.current_prices(PRODUCT_A_ID, limit=10)
    assert prices.items[0]["source_file_id"] == SOURCE_CURRENT_ID
    assert prices.items[0]["portal_key"] == "public-query-portal-a"
    assert prices.items[0]["content_sha256"] == "2" * 64

    liter_search = await queries.search_products("Portal A Product 1000 ml", limit=10)
    kilogram_search = await queries.search_products("אורז 1000 גרם", limit=10)
    ten_liter_search = await queries.search_products("Portal B Product 10 l", limit=10)
    assert liter_search.items[0]["id"] == PRODUCT_A_ID
    assert kilogram_search.items[0]["id"] == PRODUCT_A_ID
    assert ten_liter_search.items[0]["id"] == PRODUCT_B_ID

    price_versions = await queries.price_history(PRODUCT_A_ID, limit=10)
    assert [row["source_file_id"] for row in price_versions.items] == [
        SOURCE_CURRENT_ID,
        SOURCE_OLD_ID,
    ]
    assert [row["content_sha256"] for row in price_versions.items] == ["2" * 64, "1" * 64]
    first_price_page = await queries.price_history(PRODUCT_A_ID, limit=1)
    assert first_price_page.next_cursor is not None
    second_price_page = await queries.price_history(
        PRODUCT_A_ID,
        limit=1,
        cursor=first_price_page.next_cursor,
    )
    assert [
        first_price_page.items[0]["source_file_id"],
        second_price_page.items[0]["source_file_id"],
    ] == [
        SOURCE_CURRENT_ID,
        SOURCE_OLD_ID,
    ]
    assert second_price_page.next_cursor is None

    active = await queries.promotions(product_id=PRODUCT_A_ID, at=NOW, limit=10)
    assert len(active.items) == 1
    assert active.items[0]["id"] == PROMOTION_CURRENT_ID
    assert active.items[0]["reward_type"] == 2
    assert active.items[0]["source_file_id"] == SOURCE_CURRENT_ID
    assert active.items[0]["content_sha256"] == "2" * 64

    first_promotion_page = await queries.promotion_history(product_id=PRODUCT_A_ID, limit=1)
    assert first_promotion_page.next_cursor is not None
    assert first_promotion_page.next_cursor != str(PROMOTION_CURRENT_ID)
    second_promotion_page = await queries.promotion_history(
        product_id=PRODUCT_A_ID,
        limit=1,
        cursor=first_promotion_page.next_cursor,
    )
    assert second_promotion_page.items[0]["id"] == PROMOTION_OLD_ID
    assert second_promotion_page.next_cursor is None
    history_items = first_promotion_page.items + second_promotion_page.items
    assert [row["id"] for row in history_items] == [PROMOTION_CURRENT_ID, PROMOTION_OLD_ID]
    old_version = history_items[1]
    assert [item["source_item_code"] for item in old_version["items"]] == [
        "COLLIDE-1",
        "EXTRA-2",
    ]
    assert [store["source_store_code"] for store in old_version["stores"]] == ["001", "002"]
    assert old_version["clubs"] == ["ALPHA", "BETA"]
    assert old_version["reward_type"] == 1
    assert old_version["allows_multiple_discounts"] is False
    assert old_version["minimum_items_offered"] == 2
    assert old_version["source_file_id"] == SOURCE_OLD_ID
    assert old_version["content_sha256"] == "1" * 64

    current_version = history_items[0]
    for relation_name, count_name, truncated_name in (
        ("items", "returned_item_count", "items_truncated"),
        ("stores", "returned_store_count", "stores_truncated"),
        ("clubs", "returned_club_count", "clubs_truncated"),
    ):
        assert len(current_version[relation_name]) == MAXIMUM_PROMOTION_RELATIONS
        assert current_version[count_name] == MAXIMUM_PROMOTION_RELATIONS
        assert current_version[truncated_name] is True
    assert current_version["items"] == sorted(
        current_version["items"],
        key=lambda item: item["retailer_item_id"],
    )
    assert current_version["stores"] == sorted(
        current_version["stores"],
        key=lambda store: store["store_id"],
    )
    assert current_version["clubs"] == sorted(current_version["clubs"])

    await _assert_http_contract(queries)
    await _assert_mcp_contract(queries)
    await _assert_cli_contract(migrated_database_url, tmp_path, monkeypatch)


async def test_current_head_promotion_history_and_freshness_plans_are_bounded(
    database: Database,
) -> None:
    await _seed_public_contract(database)
    await _seed_current_head_plan_evidence(database)
    await _assert_promotion_relation_plans_are_keyset_bounded(database)
    queries = QueryService(PostgresQueryRepository(database.engine), QueryClock())

    promotion_full = await queries.promotion_history(limit=10)
    promotion_first = await queries.promotion_history(limit=1)
    maximum_promotion_page = await queries.promotion_history(
        since=START - timedelta(days=1_006),
        until=NOW + timedelta(days=1),
        limit=1_000,
    )
    assert (
        sum(
            int(row[count_name])
            for row in promotion_full.items
            for count_name in (
                "returned_item_count",
                "returned_store_count",
                "returned_club_count",
            )
        )
        <= MAXIMUM_PROMOTION_CHILDREN_PER_PAGE
    )
    assert promotion_first.items[0]["returned_item_count"] == MAXIMUM_PROMOTION_RELATIONS
    assert promotion_full.items[0]["returned_item_count"] == MAXIMUM_PROMOTION_RELATIONS
    assert len(promotion_full.items) == 1
    assert len(maximum_promotion_page.items) == 1
    assert maximum_promotion_page.next_cursor is not None
    assert (
        sum(
            int(row[count_name])
            for row in maximum_promotion_page.items
            for count_name in (
                "returned_item_count",
                "returned_store_count",
                "returned_club_count",
            )
        )
        <= MAXIMUM_PROMOTION_CHILDREN_PER_PAGE
    )
    assert maximum_promotion_page.items[0]["returned_item_count"] == MAXIMUM_PROMOTION_RELATIONS
    assert maximum_promotion_page.items[0]["items_truncated"] is True
    assert promotion_first.next_cursor is not None
    assert promotion_first.next_cursor != str(promotion_first.items[0]["id"])
    promotion_cursor = await queries.promotion_history(
        limit=1,
        cursor=promotion_first.next_cursor,
    )
    assert promotion_cursor.next_cursor is not None
    assert [
        promotion_first.items[0]["id"],
        promotion_cursor.items[0]["id"],
    ] == [PROMOTION_CURRENT_ID, PROMOTION_OLD_ID]

    freshness_full = await queries.freshness(limit=10)
    freshness_first = await queries.freshness(limit=1)
    assert freshness_first.next_cursor is not None
    assert freshness_first.next_cursor != str(freshness_first.items[0]["store_id"])
    freshness_cursor = await queries.freshness(
        limit=1,
        cursor=freshness_first.next_cursor,
    )
    assert freshness_cursor.next_cursor is not None
    selected_freshness = [freshness_first.items[0], freshness_cursor.items[0]]
    assert [row["store_id"] for row in selected_freshness] == [
        row["store_id"] for row in freshness_full.items[:2]
    ]

    async with database.engine.connect() as connection:
        for row in selected_freshness:
            store_id = row["store_id"]
            exact_counts = (
                (
                    await connection.execute(
                        text(
                            """
                        SELECT count(*) AS observed_items,
                               count(*) FILTER (WHERE is_available) AS available_items
                          FROM current_availability
                         WHERE store_id = :store_id
                        """
                        ),
                        {"store_id": store_id},
                    )
                )
                .mappings()
                .one()
            )
            latest = (
                (
                    await connection.execute(
                        text(
                            """
                        SELECT current.id, current.last_observed_at,
                               current.source_file_id, archive.content_sha256
                          FROM current_availability current
                          JOIN source_files source
                            ON source.id = current.source_file_id
                          LEFT JOIN raw_archive_objects archive
                            ON archive.id = source.raw_archive_object_id
                         WHERE current.store_id = :store_id
                         ORDER BY current.last_observed_at DESC,
                                  current.source_file_id DESC,
                                  current.id DESC
                         LIMIT 1
                        """
                        ),
                        {"store_id": store_id},
                    )
                )
                .mappings()
                .one()
            )
            latest_is_in_count_probe = (
                await connection.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                              FROM (
                                    SELECT current.id
                                      FROM current_availability current
                                     WHERE current.store_id = :store_id
                                     ORDER BY current.is_available DESC,
                                              current.retailer_item_id DESC
                                     LIMIT :probe_limit
                                   ) bounded
                             WHERE bounded.id = :latest_id
                        )
                        """
                    ),
                    {
                        "store_id": store_id,
                        "probe_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1,
                        "latest_id": latest["id"],
                    },
                )
            ).scalar_one()

            assert exact_counts["observed_items"] == MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 5
            assert exact_counts["available_items"] == MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 5
            assert row["observed_items"] == MAXIMUM_FRESHNESS_ITEMS_PER_STORE
            assert row["available_items"] == MAXIMUM_FRESHNESS_ITEMS_PER_STORE
            assert row["item_probe_limit"] == MAXIMUM_FRESHNESS_ITEMS_PER_STORE
            assert row["items_truncated"] is True
            assert latest_is_in_count_probe is False
            assert row["last_observed_at"] == latest["last_observed_at"] == NOW
            assert row["source_file_id"] == latest["source_file_id"] == SOURCE_PORTAL_B_ID
            assert row["content_sha256"] == latest["content_sha256"] == "3" * 64

        promotion_plans = {
            "first": await _explain_query(
                connection,
                _PROMOTION_HISTORY_QUERY,
                _promotion_history_plan_parameters(cursor_id=None),
            ),
            "cursor": await _explain_query(
                connection,
                _PROMOTION_HISTORY_QUERY,
                _promotion_history_plan_parameters(cursor_id=PROMOTION_CURRENT_ID),
            ),
        }
        freshness_plans = {
            "first": await _explain_query(
                connection,
                _FRESHNESS_QUERY,
                _freshness_plan_parameters(cursor_id=None),
            ),
            "cursor": await _explain_query(
                connection,
                _FRESHNESS_QUERY,
                _freshness_plan_parameters(
                    cursor_id=freshness_first.items[0]["store_id"],
                ),
            ),
        }

    for page_name, plan in promotion_plans.items():
        _assert_buffer_metrics(plan)
        candidate = _cte_plan_node(plan, "candidate_promotions")
        assert _plan_number(candidate, "Actual Rows") == 2, page_name
        assert _plan_number(candidate, "Actual Rows") <= 1 + 1, page_name
        page = _cte_plan_node(plan, "page_promotions")
        assert _plan_number(page, "Actual Rows") == 1, page_name
        time_probe = _cte_plan_node(plan, "time_promotions")
        assert _plan_number(time_probe, "Actual Rows") <= MAXIMUM_PROMOTION_PROBE_RESULTS, page_name

    for page_name, plan in freshness_plans.items():
        _assert_buffer_metrics(plan)
        candidate = _cte_plan_node(plan, "candidate_stores")
        candidate_count = _plan_number(candidate, "Actual Rows")
        assert candidate_count == 2, page_name
        assert candidate_count <= 1 + 1, page_name

        bounded = _cte_plan_node(plan, "bounded_availability")
        assert _plan_number(bounded, "Actual Rows") <= (
            candidate_count * (MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1)
        ), page_name

        nodes = _plan_nodes(plan)
        count_probe_limits = [
            node
            for node in _plan_nodes(bounded)
            if node.get("Node Type") == "Limit"
            and _plan_number(node, "Actual Rows") <= MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1
            and _plan_number(node, "Actual Loops") == candidate_count
        ]
        contributor_scans = [
            node
            for node in nodes
            if node.get("Index Name") == "ix_current_availability_store_latest"
            and _plan_number(node, "Actual Rows") == 1
            and _plan_number(node, "Actual Loops") == candidate_count
        ]
        assert count_probe_limits, page_name
        assert "ix_current_availability_store_available_item" in _plan_index_names(bounded), (
            page_name
        )
        assert contributor_scans, page_name


async def test_fuzzy_store_search_has_an_indexed_window_and_preserves_later_matches(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_public_contract(database)
    candidate_limit = 100
    target_store_id = UUID("ffffffff-ffff-ffff-ffff-fffffffffff1")
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO stores (
                    id, retailer_id, portal_id, chain_code, subchain_code,
                    source_store_code, name, last_source_file_id
                )
                SELECT uuidv7(), :retailer_id, :portal_id, 'chain', 'subchain',
                       'SECURITY-FILLER-' || lpad(series.value::text, 5, '0'),
                       'Unrelated store ' || series.value, :source_file_id
                  FROM generate_series(1, :filler_count) AS series(value)
                """
            ),
            {
                "filler_count": candidate_limit + 1,
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
            },
        )
        await connection.execute(
            insert(stores).values(
                _store_values(
                    target_store_id,
                    PORTAL_A_ID,
                    "SECURITY-TARGET",
                    "Needle Xylophone",
                )
            )
        )
        await connection.execute(text("ANALYZE stores"))

    monkeypatch.setattr(
        "makolet.adapters.persistence.queries.MAXIMUM_SEARCH_CANDIDATES",
        candidate_limit,
    )
    queries = QueryService(PostgresQueryRepository(database.engine), QueryClock())
    cursor: str | None = None
    matched_ids: list[UUID] = []
    saw_empty_continuation = False
    for _ in range(5):
        page = await queries.find_stores(query="xylophone", limit=10, cursor=cursor)
        matched_ids.extend(row["id"] for row in page.items)
        saw_empty_continuation |= not page.items and page.next_cursor is not None
        cursor = page.next_cursor
        if cursor is None:
            break

    assert cursor is None
    assert matched_ids == [target_store_id]
    assert saw_empty_continuation is True

    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO stores (
                    id, retailer_id, portal_id, chain_code, subchain_code,
                    source_store_code, name, last_source_file_id
                )
                SELECT uuidv7(), :retailer_id, :portal_id, 'chain', 'subchain',
                       'FUZZY-PLAN-' || lpad(series.value::text, 5, '0'),
                       'Fuzzy plan store ' || series.value, :source_file_id
                  FROM generate_series(1, 10000) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
            },
        )
        await connection.execute(text("ANALYZE stores"))
        cursor_store_id = (
            await connection.execute(text("SELECT id FROM stores ORDER BY id OFFSET 5000 LIMIT 1"))
        ).scalar_one()
        plans = {
            "first": await _explain_forced_generic_query(
                connection,
                prepared_name="makolet_fuzzy_first_page",
                query=_FUZZY_STORES_FIRST_PAGE_QUERY,
                parameters={
                    "candidate_limit": candidate_limit,
                    "candidate_probe_limit": candidate_limit + 1,
                    "city": None,
                    "page_limit": 11,
                    "query": "xylophone",
                    "retailer_id": None,
                },
            ),
            "cursor": await _explain_forced_generic_query(
                connection,
                prepared_name="makolet_fuzzy_cursor_page",
                query=_FUZZY_STORES_CURSOR_QUERY,
                parameters={
                    "candidate_limit": candidate_limit,
                    "candidate_probe_limit": candidate_limit + 1,
                    "city": None,
                    "cursor_id": cursor_store_id,
                    "page_limit": 11,
                    "query": "xylophone",
                    "retailer_id": None,
                },
            ),
        }

    for page_name, plan in plans.items():
        candidate = _cte_plan_node(plan, "candidate_stores")
        bounded = _cte_plan_node(plan, "bounded_candidates")
        store_scans = [
            node for node in _plan_nodes(candidate) if node.get("Relation Name") == "stores"
        ]
        assert len(store_scans) == 1, page_name
        store_scan = store_scans[0]
        rows_removed = store_scan.get("Rows Removed by Filter", 0)
        assert isinstance(rows_removed, (int, float)), page_name
        visits = (_plan_number(store_scan, "Actual Rows") + float(rows_removed)) * _plan_number(
            store_scan, "Actual Loops"
        )
        assert _plan_number(candidate, "Actual Rows") == candidate_limit + 1, page_name
        assert _plan_number(bounded, "Actual Rows") == candidate_limit, page_name
        assert store_scan.get("Index Name") == "pk_stores", page_name
        assert visits <= candidate_limit + 1, page_name

    cursor_scan = next(
        node
        for node in _plan_nodes(_cte_plan_node(plans["cursor"], "candidate_stores"))
        if node.get("Relation Name") == "stores"
    )
    assert "id > $1" in str(cursor_scan.get("Index Cond"))


async def test_product_projection_remap_is_exact_for_all_public_state_queries(
    database: Database,
) -> None:
    await _seed_public_contract(database)
    repository = PostgresQueryRepository(database.engine)
    since = START - timedelta(days=1)
    until = NOW + timedelta(days=1)

    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE confirmed_product_matches
                   SET canonical_product_id = :product_id,
                       method = 'manual_review',
                       confirmed_by = 'projection-remap-test'
                 WHERE retailer_item_id = :item_id
                """
            ),
            {"product_id": PRODUCT_B_ID, "item_id": ITEM_A_ID},
        )

    old_prices = await repository.current_prices(
        PRODUCT_A_ID,
        retailer_id=None,
        store_id=None,
        limit=10,
        cursor=None,
    )
    new_prices = await repository.current_prices(
        PRODUCT_B_ID,
        retailer_id=None,
        store_id=None,
        limit=10,
        cursor=None,
    )
    old_history = await repository.price_history(
        PRODUCT_A_ID,
        store_id=None,
        since=since,
        until=until,
        limit=10,
        cursor=None,
    )
    new_history = await repository.price_history(
        PRODUCT_B_ID,
        store_id=None,
        since=since,
        until=until,
        limit=10,
        cursor=None,
    )
    old_availability = await repository.item_availability(
        PRODUCT_A_ID,
        store_id=None,
        limit=10,
        cursor=None,
    )
    new_availability = await repository.item_availability(
        PRODUCT_B_ID,
        store_id=None,
        limit=10,
        cursor=None,
    )

    assert not any(row["retailer_item_id"] == ITEM_A_ID for row in old_prices.items)
    assert any(row["retailer_item_id"] == ITEM_A_ID for row in new_prices.items)
    assert not any(row["retailer_item_id"] == ITEM_A_ID for row in old_history.items)
    assert {row["retailer_item_id"] for row in new_history.items} == {ITEM_A_ID}
    assert not any(row["retailer_item_id"] == ITEM_A_ID for row in old_availability.items)
    assert any(row["retailer_item_id"] == ITEM_A_ID for row in new_availability.items)


async def test_product_state_history_and_city_first_cursor_plans_bound_candidates(
    database: Database,
) -> None:
    await _seed_public_contract(database)
    row_count = 5_000
    # Keep the requested product sparse so an id-only scan would do adversarial
    # filter work and the canonical-product response-order indexes are necessary.
    target_stride = 100
    city_stride = 100
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    id, retailer_id, portal_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                )
                SELECT uuidv7(), :retailer_id, :portal_id,
                       'BOUNDED-ITEM-' || lpad(series.value::text, 5, '0'),
                       'Bounded item ' || series.value,
                       :observed_at, :observed_at, :source_file_id
                  FROM generate_series(1, :row_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "observed_at": NOW,
                "source_file_id": SOURCE_CURRENT_ID,
                "row_count": row_count,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO confirmed_product_matches (
                    id, retailer_item_id, canonical_product_id,
                    method, confirmed_by
                )
                SELECT uuidv7(), item.id,
                       CASE WHEN right(item.source_item_code, 5)::integer
                                      % :target_stride = 0
                            THEN CAST(:target_product_id AS uuid)
                            ELSE CAST(:other_product_id AS uuid)
                       END,
                       'manual_review', 'bounded-plan-test'
                  FROM retailer_items item
                 WHERE item.source_item_code LIKE 'BOUNDED-ITEM-%'
                """
            ),
            {
                "target_product_id": PRODUCT_A_ID,
                "other_product_id": PRODUCT_B_ID,
                "target_stride": target_stride,
            },
        )
        await connection.execute(
            text(
                """
                WITH selected AS MATERIALIZED (
                    SELECT item.id,
                           row_number() OVER (ORDER BY item.id) AS item_number
                      FROM retailer_items item
                     WHERE item.source_item_code LIKE 'BOUNDED-ITEM-%'
                )
                INSERT INTO current_prices (
                    retailer_item_id, store_id, item_price, source_file_id,
                    first_observed_at, last_observed_at
                )
                SELECT selected.id, :store_id,
                       20 + selected.item_number::numeric / 100,
                       :source_file_id, :observed_at, :observed_at
                  FROM selected
                """
            ),
            {
                "store_id": STORE_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
                "observed_at": NOW,
            },
        )
        await connection.execute(
            text(
                """
                WITH selected AS MATERIALIZED (
                    SELECT item.id,
                           row_number() OVER (ORDER BY item.id) AS item_number
                      FROM retailer_items item
                     WHERE item.source_item_code LIKE 'BOUNDED-ITEM-%'
                )
                INSERT INTO price_history (
                    retailer_item_id, store_id, item_price, source_file_id,
                    valid_from, valid_to
                )
                SELECT selected.id, :store_id,
                       20 + selected.item_number::numeric / 100,
                       :source_file_id,
                       CAST(:range_start AS timestamptz)
                           + make_interval(secs => selected.item_number::double precision),
                       NULL
                  FROM selected
                """
            ),
            {
                "store_id": STORE_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
                "range_start": START,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO current_availability (
                    retailer_item_id, store_id, is_available, item_status,
                    source_file_id, first_observed_at, last_observed_at
                )
                SELECT item.id, :store_id, true, 1,
                       :source_file_id, :observed_at, :observed_at
                  FROM retailer_items item
                 WHERE item.source_item_code LIKE 'BOUNDED-ITEM-%'
                """
            ),
            {
                "store_id": STORE_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
                "observed_at": NOW,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO stores (
                    id, retailer_id, portal_id, chain_code, subchain_code,
                    source_store_code, name, city, last_source_file_id
                )
                SELECT uuidv7(), :retailer_id, :portal_id, 'chain', 'subchain',
                       'CITY-PLAN-' || lpad(series.value::text, 5, '0'),
                       'City plan store ' || series.value,
                       CASE WHEN series.value % :city_stride = 0
                            THEN 'Jerusalem' ELSE 'Tel Aviv' END,
                       :source_file_id
                  FROM generate_series(1, :row_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_CURRENT_ID,
                "row_count": row_count,
                "city_stride": city_stride,
            },
        )
        await connection.execute(
            text(
                """
                ANALYZE current_prices, price_history, current_availability,
                        stores, retailer_items, confirmed_product_matches
                """
            )
        )

    repository = PostgresQueryRepository(database.engine)
    since = START - timedelta(days=1)
    until = NOW + timedelta(days=1)
    current_full = await repository.current_prices(
        PRODUCT_A_ID,
        retailer_id=None,
        store_id=None,
        limit=10,
        cursor=None,
    )
    current_first = await repository.current_prices(
        PRODUCT_A_ID,
        retailer_id=None,
        store_id=None,
        limit=1,
        cursor=None,
    )
    assert current_first.next_cursor is not None
    current_cursor = await repository.current_prices(
        PRODUCT_A_ID,
        retailer_id=None,
        store_id=None,
        limit=1,
        cursor=current_first.next_cursor,
    )
    history_full = await repository.price_history(
        PRODUCT_A_ID,
        store_id=None,
        since=since,
        until=until,
        limit=10,
        cursor=None,
    )
    history_first = await repository.price_history(
        PRODUCT_A_ID,
        store_id=None,
        since=since,
        until=until,
        limit=1,
        cursor=None,
    )
    assert history_first.next_cursor is not None
    history_cursor = await repository.price_history(
        PRODUCT_A_ID,
        store_id=None,
        since=since,
        until=until,
        limit=1,
        cursor=history_first.next_cursor,
    )
    availability_full = await repository.item_availability(
        PRODUCT_A_ID,
        store_id=None,
        limit=10,
        cursor=None,
    )
    availability_first = await repository.item_availability(
        PRODUCT_A_ID,
        store_id=None,
        limit=1,
        cursor=None,
    )
    assert availability_first.next_cursor is not None
    availability_cursor = await repository.item_availability(
        PRODUCT_A_ID,
        store_id=None,
        limit=1,
        cursor=availability_first.next_cursor,
    )
    city_full = await repository.find_stores(
        query=None,
        retailer_id=None,
        city="Jerusalem",
        limit=10,
        cursor=None,
    )
    city_first = await repository.find_stores(
        query=None,
        retailer_id=None,
        city="Jerusalem",
        limit=1,
        cursor=None,
    )
    assert city_first.next_cursor is not None
    city_cursor = await repository.find_stores(
        query=None,
        retailer_id=None,
        city="Jerusalem",
        limit=1,
        cursor=city_first.next_cursor,
    )

    assert [row["id"] for row in (*current_first.items, *current_cursor.items)] == [
        row["id"] for row in current_full.items[:2]
    ]
    assert [row["id"] for row in (*history_first.items, *history_cursor.items)] == [
        row["id"] for row in history_full.items[:2]
    ]
    assert [row["id"] for row in (*availability_first.items, *availability_cursor.items)] == [
        row["id"] for row in availability_full.items[:2]
    ]
    assert [row["id"] for row in (*city_first.items, *city_cursor.items)] == [
        row["id"] for row in city_full.items[:2]
    ]

    async with database.engine.connect() as connection:
        plans = {
            "current_first": await _explain_query(
                connection,
                _CURRENT_PRICES_FIRST_PAGE_QUERY,
                {"product_id": PRODUCT_A_ID, "candidate_limit": 2},
            ),
            "current_cursor": await _explain_query(
                connection,
                _CURRENT_PRICES_CURSOR_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "cursor_id": current_first.items[0]["id"],
                    "candidate_limit": 2,
                },
            ),
            "history_first": await _explain_query(
                connection,
                _PRICE_HISTORY_FIRST_PAGE_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "since": since,
                    "until": until,
                    "candidate_limit": 2,
                    "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
                },
            ),
            "history_cursor": await _explain_query(
                connection,
                _PRICE_HISTORY_CURSOR_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "since": since,
                    "until": until,
                    "cursor_id": history_first.items[0]["id"],
                    "candidate_limit": 2,
                    "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
                },
            ),
            "availability_first": await _explain_query(
                connection,
                _ITEM_AVAILABILITY_FIRST_PAGE_QUERY,
                {"product_id": PRODUCT_A_ID, "candidate_limit": 2},
            ),
            "availability_cursor": await _explain_query(
                connection,
                _ITEM_AVAILABILITY_CURSOR_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "cursor_id": availability_first.items[0]["id"],
                    "candidate_limit": 2,
                },
            ),
            "city_first": await _explain_query(
                connection,
                _CITY_STORES_FIRST_PAGE_QUERY,
                {"city": "jerusalem", "limit": 2},
            ),
            "city_cursor": await _explain_query(
                connection,
                _CITY_STORES_CURSOR_QUERY,
                {"city": "jerusalem", "cursor_id": city_first.items[0]["id"], "limit": 2},
            ),
        }

    candidate_contracts = {
        "current_first": ("candidate_prices", "ix_current_prices_product_price_id"),
        "current_cursor": ("candidate_prices", "ix_current_prices_product_price_id"),
        "availability_first": (
            "candidate_availability",
            "ix_current_availability_product_id",
        ),
        "availability_cursor": (
            "candidate_availability",
            "ix_current_availability_product_id",
        ),
    }
    for plan_name, (cte_name, index_name) in candidate_contracts.items():
        plan = plans[plan_name]
        _assert_buffer_metrics(plan)
        candidate = _cte_plan_node(plan, cte_name)
        assert _plan_number(candidate, "Actual Rows") == 2, plan_name
        assert index_name in _plan_index_names(candidate), plan_name
        assert not any(node.get("Node Type") == "Sort" for node in _plan_nodes(candidate)), (
            plan_name
        )

    for plan_name in ("history_first", "history_cursor"):
        plan = plans[plan_name]
        _assert_buffer_metrics(plan)
        bounded = _cte_plan_node(plan, "bounded_history")
        candidate = _cte_plan_node(plan, "candidate_history")
        assert _plan_number(bounded, "Actual Rows") <= MAXIMUM_HISTORY_PROBE_RESULTS, plan_name
        assert _plan_number(candidate, "Actual Rows") == 2, plan_name

    for plan_name in ("city_first", "city_cursor"):
        plan = plans[plan_name]
        _assert_buffer_metrics(plan)
        assert "ix_stores_city_search_id" in _plan_index_names(plan), plan_name
        assert not any(
            node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == "stores"
            for node in _plan_nodes(plan)
        ), plan_name


async def test_sparse_temporal_and_relationship_led_paths_are_indexed_and_exact(
    database: Database,
) -> None:
    await _seed_public_contract(database)
    row_count = 12_000
    window_start = VERSION_CHANGE + timedelta(days=1)
    change_at = window_start + timedelta(hours=12)
    window_end = window_start + timedelta(days=1)

    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE price_history
                   SET valid_to = :change_at
                 WHERE retailer_item_id = :item_id
                   AND store_id = :store_id
                   AND valid_to IS NULL
                """
            ),
            {"change_at": change_at, "item_id": ITEM_A_ID, "store_id": STORE_A_ID},
        )
        await connection.execute(
            insert(price_history).values(
                retailer_item_id=ITEM_A_ID,
                store_id=STORE_A_ID,
                canonical_product_id=PRODUCT_A_ID,
                item_price=Decimal("7.90"),
                source_file_id=SOURCE_CURRENT_ID,
                valid_from=change_at,
                valid_to=None,
            )
        )
        await connection.execute(
            text(
                """
                INSERT INTO price_history (
                    retailer_item_id, store_id, canonical_product_id,
                    item_price, source_file_id, valid_from, valid_to
                )
                WITH candidate_stores AS MATERIALIZED (
                    SELECT store.id,
                           row_number() OVER (ORDER BY store.id) AS position,
                           count(*) OVER () AS store_count
                      FROM stores store
                     WHERE store.source_store_code LIKE 'BULK-STORE-%'
                )
                SELECT :item_id, store.id, :product_id, 50.00,
                       :source_file_id,
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2)::integer),
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2 + 1)::integer)
                  FROM generate_series(1, :row_count) AS series(value)
                  JOIN candidate_stores store
                    ON store.position = 1 + ((series.value - 1) % store.store_count)
                """
            ),
            {
                "item_id": ITEM_A_ID,
                "product_id": PRODUCT_A_ID,
                "source_file_id": SOURCE_OLD_ID,
                "row_count": row_count,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO promotions (
                    retailer_id, portal_id, subchain_code,
                    source_promotion_id, source_scope_store_code,
                    discount_kind, starts_at, ends_at, is_active,
                    fingerprint_sha256, source_file_id,
                    valid_from, valid_to, last_observed_at
                )
                SELECT :retailer_id, :portal_id, 'subchain',
                       'SPARSE-PROMO-' || lpad(series.value::text, 5, '0'),
                       '', 'quantity',
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2)::integer),
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2 + 1)::integer),
                       true, lpad(to_hex(series.value::bigint), 64, '0'),
                       :source_file_id,
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2)::integer),
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2 + 1)::integer),
                       TIMESTAMPTZ '1990-01-01 00:00:00+00'
                           + make_interval(hours => (series.value * 2)::integer)
                  FROM generate_series(1, :row_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "source_file_id": SOURCE_OLD_ID,
                "row_count": row_count,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    retailer_id, portal_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                )
                SELECT :retailer_id, :portal_id,
                       'SPARSE-REL-' || lpad(series.value::text, 5, '0'),
                       'Sparse relation ' || series.value,
                       :observed_at, :observed_at, :source_file_id
                  FROM generate_series(1, :row_count) AS series(value)
                """
            ),
            {
                "retailer_id": RETAILER_ID,
                "portal_id": PORTAL_A_ID,
                "observed_at": NOW,
                "source_file_id": SOURCE_CURRENT_ID,
                "row_count": row_count,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO confirmed_product_matches (
                    retailer_item_id, canonical_product_id, method, confirmed_by
                )
                SELECT item.id, :product_id, 'manual_review', 'sparse-plan-test'
                  FROM retailer_items item
                 WHERE item.source_item_code LIKE 'SPARSE-REL-%'
                """
            ),
            {"product_id": PRODUCT_B_ID},
        )
        await connection.execute(
            text(
                """
                WITH related_items AS MATERIALIZED (
                    SELECT item.id,
                           row_number() OVER (ORDER BY item.source_item_code) AS position
                      FROM retailer_items item
                     WHERE item.source_item_code LIKE 'SPARSE-REL-%'
                ), related_promotions AS MATERIALIZED (
                    SELECT promotion.id,
                           row_number() OVER (ORDER BY promotion.source_promotion_id) AS position
                      FROM promotions promotion
                     WHERE promotion.source_promotion_id LIKE 'SPARSE-PROMO-%'
                )
                INSERT INTO promotion_items (promotion_id, retailer_item_id)
                SELECT promotion.id, item.id
                  FROM related_items item
                  JOIN related_promotions promotion USING (position)
                """
            )
        )
        await connection.execute(
            text(
                """
                ANALYZE price_history, promotions, promotion_items,
                        retailer_items, confirmed_product_matches
                """
            )
        )

    repository = PostgresQueryRepository(database.engine)
    history = await repository.price_history(
        PRODUCT_A_ID,
        store_id=STORE_A_ID,
        since=window_start,
        until=window_end,
        limit=10,
        cursor=None,
    )
    assert [row["valid_from"] for row in history.items] == [change_at, VERSION_CHANGE]
    promotion_history = await repository.promotion_history(
        product_id=PRODUCT_A_ID,
        store_id=STORE_A_ID,
        since=window_start,
        until=window_end,
        limit=10,
        cursor=None,
    )
    active_promotions = await repository.active_promotions(
        product_id=PRODUCT_A_ID,
        store_id=STORE_A_ID,
        at=window_start,
        limit=10,
        cursor=None,
    )
    assert [row["id"] for row in promotion_history.items] == [PROMOTION_CURRENT_ID]
    assert [row["id"] for row in active_promotions.items] == [PROMOTION_CURRENT_ID]

    promotion_parameters = {
        "product_id": PRODUCT_A_ID,
        "store_id": STORE_A_ID,
        "since": window_start,
        "until": window_end,
        "at": window_start,
        "cursor_id": None,
        "candidate_limit": 11,
        "page_limit": 10,
        "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
        "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
        "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
        "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS,
    }
    async with database.engine.connect() as connection:
        plans = {
            "price": await _explain_query(
                connection,
                _PRICE_HISTORY_FIRST_PAGE_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "since": window_start,
                    "until": window_end,
                    "candidate_limit": 11,
                    "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
                },
            ),
            "price_store": await _explain_query(
                connection,
                _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY,
                {
                    "product_id": PRODUCT_A_ID,
                    "store_id": STORE_A_ID,
                    "since": window_start,
                    "until": window_end,
                    "candidate_limit": 11,
                    "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
                },
            ),
            "promotion_history_time": await _explain_query(
                connection,
                _PROMOTION_HISTORY_TIME_PROBE_QUERY,
                {
                    "since": window_start,
                    "until": window_end,
                    "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS + 1,
                },
            ),
            "promotion_active_time": await _explain_query(
                connection,
                _ACTIVE_PROMOTIONS_TIME_PROBE_QUERY,
                {"at": window_start, "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS + 1},
            ),
            "promotion_history_product": await _explain_query(
                connection,
                _PROMOTION_HISTORY_PRODUCT_QUERY,
                promotion_parameters,
            ),
            "promotion_active_product": await _explain_query(
                connection,
                _ACTIVE_PROMOTIONS_PRODUCT_QUERY,
                promotion_parameters,
            ),
        }

    price_probe = _cte_plan_node(plans["price"], "bounded_history")
    price_store_probe = _cte_plan_node(plans["price_store"], "bounded_history")
    assert "ix_price_history_product_period_gist" in _plan_index_names(price_probe)
    assert "ix_price_history_product_store_period_gist" in _plan_index_names(price_store_probe)
    assert _plan_number(price_probe, "Actual Rows") == 2
    assert _plan_number(price_store_probe, "Actual Rows") == 2
    assert "ix_promotions_valid_period_gist" in _plan_index_names(plans["promotion_history_time"])
    assert "ix_promotions_active_period_gist" in _plan_index_names(plans["promotion_active_time"])
    for plan_name in ("promotion_history_product", "promotion_active_product"):
        relationships = _cte_plan_node(plans[plan_name], "product_relationships")
        relationship_indexes = _plan_index_names(relationships)
        assert "ix_confirmed_product_matches_product_item" in relationship_indexes, plan_name
        assert "ix_promotion_items_item_promotion" in relationship_indexes, plan_name
        assert _plan_number(relationships, "Actual Rows") <= MAXIMUM_PROMOTION_PROBE_RESULTS


async def test_temporal_and_relationship_probes_fail_closed_before_decoration(
    database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_public_contract(database)
    repository = PostgresQueryRepository(database.engine)
    monkeypatch.setattr(persistence_queries, "MAXIMUM_HISTORY_PROBE_RESULTS", 1)

    with pytest.raises(QueryLimitError, match="price history scope"):
        await repository.price_history(
            PRODUCT_A_ID,
            store_id=None,
            since=START - timedelta(days=1),
            until=NOW + timedelta(days=1),
            limit=10,
            cursor=None,
        )

    monkeypatch.setattr(persistence_queries, "MAXIMUM_PROMOTION_PROBE_RESULTS", 1)
    with pytest.raises(QueryLimitError, match="promotion history scope"):
        await repository.promotion_history(
            product_id=None,
            store_id=None,
            since=START - timedelta(days=1),
            until=NOW + timedelta(days=1),
            limit=10,
            cursor=None,
        )
    with pytest.raises(QueryLimitError, match="active promotion scope"):
        await repository.active_promotions(
            product_id=PRODUCT_A_ID,
            store_id=STORE_A_ID,
            at=VERSION_CHANGE + timedelta(days=1),
            limit=10,
            cursor=None,
        )


def _promotion_history_plan_parameters(*, cursor_id: UUID | None) -> dict[str, object]:
    return {
        "product_id": None,
        "store_id": None,
        "since": START - timedelta(days=1_006),
        "until": NOW + timedelta(days=1),
        "cursor_id": cursor_id,
        "candidate_limit": 2,
        "page_limit": 1,
        "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
        "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
        "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
        "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS + 1,
    }


def _freshness_plan_parameters(*, cursor_id: object | None) -> dict[str, object]:
    return {
        "cursor_id": cursor_id,
        "limit": 2,
        "item_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
        "item_probe_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1,
    }


async def _explain_query(
    connection: AsyncConnection,
    query: str,
    parameters: Mapping[str, object],
) -> Mapping[str, object]:
    raw = (
        await connection.execute(
            text(
                # Query is one of the module-level production constants imported above.
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
            ),
            parameters,
        )
    ).scalar_one()
    payload = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(payload, list)
    assert len(payload) == 1
    root = payload[0]
    assert isinstance(root, Mapping)
    plan = root.get("Plan")
    assert isinstance(plan, Mapping)
    return plan


async def _explain_forced_generic_query(
    connection: AsyncConnection,
    *,
    prepared_name: str,
    query: str,
    parameters: Mapping[str, object],
) -> Mapping[str, object]:
    assert prepared_name.replace("_", "").isalnum()
    await connection.execute(text("SET LOCAL plan_cache_mode = force_generic_plan"))
    assert (await connection.execute(text("SHOW plan_cache_mode"))).scalar_one() == (
        "force_generic_plan"
    )
    assert (await connection.execute(text("SHOW enable_seqscan"))).scalar_one() == "on"

    dialect = postgresql.dialect(  # type: ignore[no-untyped-call]
        paramstyle="numeric_dollar"
    )
    compiled = text(query).compile(dialect=dialect)
    position = compiled.positiontup
    assert position is not None
    parameter_types = {
        "candidate_limit": "integer",
        "candidate_probe_limit": "integer",
        "city": "text",
        "cursor_id": "uuid",
        "page_limit": "integer",
        "query": "text",
        "retailer_id": "uuid",
    }
    declarations = ", ".join(parameter_types[name] for name in position)
    arguments = ", ".join(_prepared_argument(parameters[name]) for name in position)
    prepared = False
    try:
        await connection.execute(text(f"PREPARE {prepared_name} ({declarations}) AS {compiled}"))
        prepared = True
        raw = (
            await connection.execute(
                text(
                    "EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON) "
                    f"EXECUTE {prepared_name} ({arguments})"
                )
            )
        ).scalar_one()
    finally:
        if prepared:
            await connection.execute(text(f"DEALLOCATE {prepared_name}"))

    payload = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(payload, list)
    assert len(payload) == 1
    root = payload[0]
    assert isinstance(root, Mapping)
    settings = root.get("Settings")
    assert isinstance(settings, Mapping)
    assert settings.get("plan_cache_mode") == "force_generic_plan"
    plan = root.get("Plan")
    assert isinstance(plan, Mapping)
    return plan


def _prepared_argument(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, UUID):
        return f"'{value}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported prepared-statement value: {type(value).__name__}")


def _plan_nodes(plan: Mapping[str, object]) -> list[Mapping[str, object]]:
    nodes: list[Mapping[str, object]] = []
    pending = [plan]
    while pending:
        node = pending.pop()
        nodes.append(node)
        children = node.get("Plans")
        if isinstance(children, list):
            pending.extend(child for child in children if isinstance(child, Mapping))
    return nodes


def _cte_plan_node(plan: Mapping[str, object], name: str) -> Mapping[str, object]:
    matches = [node for node in _plan_nodes(plan) if node.get("Subplan Name") == f"CTE {name}"]
    assert len(matches) == 1, f"expected one CTE {name} node, found {len(matches)}"
    return matches[0]


def _plan_index_names(plan: Mapping[str, object]) -> set[str]:
    return {
        index_name
        for node in _plan_nodes(plan)
        if isinstance(index_name := node.get("Index Name"), str)
    }


def _plan_number(node: Mapping[str, object], key: str) -> float:
    value = node.get(key)
    assert isinstance(value, (int, float)), f"{key} is missing from plan node"
    return float(value)


def _assert_buffer_metrics(plan: Mapping[str, object]) -> None:
    assert isinstance(plan.get("Shared Hit Blocks"), int)
    assert isinstance(plan.get("Shared Read Blocks"), int)


async def _assert_promotion_relation_plans_are_keyset_bounded(database: Database) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(text("SET LOCAL enable_seqscan = off"))
        item_plan = (
            await connection.execute(
                text(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT relation.retailer_item_id
                      FROM promotion_items relation
                     WHERE relation.promotion_id = :promotion_id
                     ORDER BY relation.retailer_item_id
                     LIMIT :limit
                    """
                ),
                {
                    "promotion_id": PROMOTION_CURRENT_ID,
                    "limit": MAXIMUM_PROMOTION_RELATIONS + 1,
                },
            )
        ).scalar_one()
        store_plan = (
            await connection.execute(
                text(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT relation.store_id
                      FROM promotion_stores relation
                     WHERE relation.promotion_id = :promotion_id
                     ORDER BY relation.store_id
                     LIMIT :limit
                    """
                ),
                {
                    "promotion_id": PROMOTION_CURRENT_ID,
                    "limit": MAXIMUM_PROMOTION_RELATIONS + 1,
                },
            )
        ).scalar_one()
        club_plan = (
            await connection.execute(
                text(
                    """
                    EXPLAIN (FORMAT JSON)
                    SELECT relation.club_id
                      FROM promotion_clubs relation
                     WHERE relation.promotion_id = :promotion_id
                     ORDER BY relation.club_id
                     LIMIT :limit
                    """
                ),
                {
                    "promotion_id": PROMOTION_CURRENT_ID,
                    "limit": MAXIMUM_PROMOTION_RELATIONS + 1,
                },
            )
        ).scalar_one()

    assert "pk_promotion_items" in str(item_plan)
    assert "pk_promotion_stores" in str(store_plan)
    assert "pk_promotion_clubs" in str(club_plan)


async def _assert_http_contract(queries: QueryService) -> None:
    async def ready() -> bool:
        return True

    app = create_app(queries, ready)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/healthz")
        readiness = await client.get("/readyz")
        retailer_page = await client.get("/api/v1/retailers", params={"limit": 10})
        store_page = await client.get(
            "/api/v1/stores",
            params={"city": "Jerusalem", "limit": 10},
        )
        search = await client.get(
            "/api/v1/products/search",
            params={"query": "portal", "limit": 10},
        )
        litre_search = await client.get(
            "/api/v1/products/search",
            params={"query": "portal 1000 ml", "limit": 10},
        )
        ten_litre_search = await client.get(
            "/api/v1/products/search",
            params={"query": "portal 10 l", "limit": 10},
        )
        hebrew_kg_search = await client.get(
            "/api/v1/products/search",
            params={"query": "אורז 1000 גרם", "limit": 10},
        )
        barcode = await client.get("/api/v1/barcodes/7290000000015")
        ambiguous = await client.get(
            "/api/v1/retailer-items/lookup",
            params={"retailer_id": str(RETAILER_ID), "item_code": "COLLIDE-1"},
        )
        scoped = await client.get(
            "/api/v1/retailer-items/lookup",
            params={
                "retailer_id": str(RETAILER_ID),
                "portal_id": str(PORTAL_A_ID),
                "item_code": "COLLIDE-1",
            },
        )
        product = await client.get(f"/api/v1/products/{PRODUCT_A_ID}")
        current_prices_response = await client.get(f"/api/v1/products/{PRODUCT_A_ID}/prices")
        comparison = await client.get(f"/api/v1/products/{PRODUCT_A_ID}/compare")
        price_history_response = await client.get(f"/api/v1/products/{PRODUCT_A_ID}/history")
        availability = await client.get(f"/api/v1/products/{PRODUCT_A_ID}/availability")
        active_promotions_response = await client.get(
            "/api/v1/promotions",
            params={"product_id": str(PRODUCT_A_ID), "at": NOW.isoformat(), "limit": 10},
        )
        promotion_history_response = await client.get(
            "/api/v1/promotions/history",
            params={"product_id": str(PRODUCT_A_ID), "limit": 10},
        )
        promotion_history_cursor = promotion_history_response.json()["next_cursor"]
        promotion_history_second_response = await client.get(
            "/api/v1/promotions/history",
            params={
                "product_id": str(PRODUCT_A_ID),
                "limit": 10,
                "cursor": promotion_history_cursor,
            },
        )
        invalid_history = await client.get(
            f"/api/v1/products/{PRODUCT_A_ID}/history",
            params={"since": NOW.isoformat(), "until": NOW.isoformat()},
        )
        invalid_cursor = await client.get(
            "/api/v1/retailers",
            params={"cursor": "not-a-uuid"},
        )
        bounded_history = await client.get(
            f"/api/v1/products/{PRODUCT_A_ID}/history",
            params={"limit": 1},
        )
        mismatched_history_cursor = await client.get(
            f"/api/v1/products/{PRODUCT_A_ID}/history",
            params={
                "store_id": str(STORE_A_ID),
                "limit": 1,
                "cursor": bounded_history.json()["next_cursor"],
            },
        )
        freshness = await client.get("/api/v1/freshness", params={"limit": 10})
        source_status = await client.get("/api/v1/source-status", params={"limit": 10})
        platform_status = await client.get("/api/v1/status", params={"limit": 10})

    assert health.json() == {"status": "ok"}
    assert readiness.json() == {"status": "ready"}
    assert retailer_page.status_code == store_page.status_code == 200
    assert retailer_page.json()["items"][0]["id"] == str(RETAILER_ID)
    assert {store["portal_id"] for store in store_page.json()["items"]} == {
        str(PORTAL_A_ID),
        str(PORTAL_B_ID),
    }
    assert search.status_code == barcode.status_code == 200
    assert any(item["id"] == str(PRODUCT_A_ID) for item in search.json()["items"])
    assert litre_search.json()["items"][0]["id"] == str(PRODUCT_A_ID)
    assert ten_litre_search.json()["items"][0]["id"] == str(PRODUCT_B_ID)
    assert hebrew_kg_search.json()["items"][0]["id"] == str(PRODUCT_A_ID)
    assert barcode.json()["data"]["id"] == str(PRODUCT_A_ID)
    assert ambiguous.status_code == 422
    assert ambiguous.json()["error"]["code"] == "domain_validation_error"
    assert scoped.status_code == 200
    assert scoped.json()["data"]["portal_id"] == str(PORTAL_A_ID)
    assert product.status_code == 200
    retailer_identifier = next(
        identifier
        for identifier in product.json()["data"]["identifiers"]
        if identifier["kind"] == "retailer_item"
    )
    assert retailer_identifier["issuer_portal_id"] == str(PORTAL_A_ID)
    assert current_prices_response.status_code == 200
    assert current_prices_response.json()["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert price_history_response.status_code == 200
    assert comparison.json()["items"] == current_prices_response.json()["items"]
    assert [row["source_file_id"] for row in price_history_response.json()["items"]] == [
        str(SOURCE_CURRENT_ID),
        str(SOURCE_OLD_ID),
    ]
    assert active_promotions_response.status_code == 200
    active_version = active_promotions_response.json()["items"][0]
    assert active_version["id"] == str(PROMOTION_CURRENT_ID)
    assert active_version["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert active_version["reward_type"] == 2
    assert len(active_version["stores"]) == MAXIMUM_PROMOTION_RELATIONS
    assert availability.status_code == 200
    assert availability.json()["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert availability.json()["items"][0]["portal_id"] == str(PORTAL_A_ID)
    assert promotion_history_response.status_code == 200
    assert promotion_history_response.json()["next_cursor"] is not None
    assert promotion_history_second_response.status_code == 200
    assert promotion_history_second_response.json()["next_cursor"] is None
    versions = (
        promotion_history_response.json()["items"]
        + promotion_history_second_response.json()["items"]
    )
    assert [version["id"] for version in versions] == [
        str(PROMOTION_CURRENT_ID),
        str(PROMOTION_OLD_ID),
    ]
    assert versions[0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert len(versions[0]["items"]) == MAXIMUM_PROMOTION_RELATIONS
    assert invalid_history.status_code == 422
    assert invalid_history.json()["error"]["code"] == "domain_validation_error"
    assert invalid_cursor.status_code == 422
    assert invalid_cursor.json()["error"]["code"] == "domain_validation_error"
    assert bounded_history.status_code == 200
    assert mismatched_history_cursor.status_code == 422
    assert mismatched_history_cursor.json()["error"]["code"] == "domain_validation_error"
    assert freshness.status_code == source_status.status_code == platform_status.status_code == 200
    assert freshness.json()["items"][0]["portal_id"] == str(PORTAL_A_ID)
    assert freshness.json()["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert freshness.json()["items"][0]["content_sha256"] == "2" * 64
    assert {item["portal_key"] for item in source_status.json()["items"]} == {
        "public-query-portal-a",
        "public-query-portal-b",
    }
    assert platform_status.json()["maintenance"] == {"active": False, "mode": "normal"}


async def _assert_mcp_contract(queries: QueryService) -> None:
    server = MakoletMcpServer(queries)
    search = await server.handle(
        _mcp_request("search_products", {"query": "portal", "limit": 10}, request_id=1)
    )
    litre_search = await server.handle(
        _mcp_request("search_products", {"query": "portal 1000 ml", "limit": 10}, request_id=20)
    )
    ten_litre_search = await server.handle(
        _mcp_request("search_products", {"query": "portal 10 l", "limit": 10}, request_id=21)
    )
    hebrew_kg_search = await server.handle(
        _mcp_request("search_products", {"query": 'אורז 1 ק"ג', "limit": 10}, request_id=22)
    )
    barcode = await server.handle(
        _mcp_request(
            "find_product_by_barcode",
            {"barcode": "7290000000015"},
            request_id=2,
        )
    )
    retailer_page = await server.handle(_mcp_request("list_retailers", {"limit": 10}, request_id=3))
    store_page = await server.handle(
        _mcp_request(
            "find_stores",
            {"city": "Jerusalem", "limit": 10},
            request_id=4,
        )
    )
    ambiguous = await server.handle(
        _mcp_request(
            "find_product_by_retailer_item_code",
            {"retailer_id": str(RETAILER_ID), "item_code": "COLLIDE-1"},
            request_id=5,
        )
    )
    scoped = await server.handle(
        _mcp_request(
            "find_product_by_retailer_item_code",
            {
                "retailer_id": str(RETAILER_ID),
                "portal_id": str(PORTAL_A_ID),
                "item_code": "COLLIDE-1",
            },
            request_id=6,
        )
    )
    product = await server.handle(
        _mcp_request(
            "get_product",
            {"product_id": str(PRODUCT_A_ID)},
            request_id=7,
        )
    )
    current_prices_response = await server.handle(
        _mcp_request(
            "get_current_prices",
            {"product_id": str(PRODUCT_A_ID)},
            request_id=8,
        )
    )
    comparison = await server.handle(
        _mcp_request(
            "compare_product_prices",
            {"product_id": str(PRODUCT_A_ID)},
            request_id=9,
        )
    )
    price_history_response = await server.handle(
        _mcp_request(
            "get_price_history",
            {"product_id": str(PRODUCT_A_ID)},
            request_id=10,
        )
    )
    active = await server.handle(
        _mcp_request(
            "get_active_promotions",
            {"product_id": str(PRODUCT_A_ID), "at": NOW.isoformat(), "limit": 10},
            request_id=11,
        )
    )
    history = await server.handle(
        _mcp_request(
            "get_promotion_history",
            {"product_id": str(PRODUCT_A_ID), "limit": 10},
            request_id=12,
        )
    )
    assert history is not None
    promotion_history_cursor = history["result"]["structuredContent"]["nextCursor"]
    history_second = await server.handle(
        _mcp_request(
            "get_promotion_history",
            {
                "product_id": str(PRODUCT_A_ID),
                "limit": 10,
                "cursor": promotion_history_cursor,
            },
            request_id=23,
        )
    )
    availability = await server.handle(
        _mcp_request(
            "get_item_availability",
            {"product_id": str(PRODUCT_A_ID)},
            request_id=13,
        )
    )
    freshness = await server.handle(
        _mcp_request("get_data_freshness", {"limit": 10}, request_id=14)
    )
    source_status = await server.handle(
        _mcp_request("get_source_status", {"limit": 10}, request_id=15)
    )
    invalid_history = await server.handle(
        _mcp_request(
            "get_price_history",
            {
                "product_id": str(PRODUCT_A_ID),
                "since": NOW.isoformat(),
                "until": NOW.isoformat(),
            },
            request_id=16,
        )
    )
    invalid_cursor = await server.handle(
        _mcp_request(
            "list_retailers",
            {"cursor": "not-a-uuid"},
            request_id=17,
        )
    )
    bounded_history = await server.handle(
        _mcp_request(
            "get_price_history",
            {"product_id": str(PRODUCT_A_ID), "limit": 1},
            request_id=18,
        )
    )
    assert bounded_history is not None
    history_cursor = bounded_history["result"]["structuredContent"]["nextCursor"]
    mismatched_history_cursor = await server.handle(
        _mcp_request(
            "get_price_history",
            {
                "product_id": str(PRODUCT_A_ID),
                "store_id": str(STORE_A_ID),
                "limit": 1,
                "cursor": history_cursor,
            },
            request_id=19,
        )
    )

    for response in (
        search,
        litre_search,
        ten_litre_search,
        hebrew_kg_search,
        barcode,
        retailer_page,
        store_page,
        scoped,
        product,
        current_prices_response,
        comparison,
        price_history_response,
        active,
        history,
        history_second,
        availability,
        freshness,
        source_status,
    ):
        assert response is not None
        assert response["result"]["isError"] is False
    assert search is not None
    assert barcode is not None
    assert retailer_page is not None
    assert store_page is not None
    assert comparison is not None
    assert availability is not None
    assert freshness is not None
    assert source_status is not None
    assert any(
        item["id"] == str(PRODUCT_A_ID) for item in search["result"]["structuredContent"]["items"]
    )
    assert litre_search is not None
    assert ten_litre_search is not None
    assert hebrew_kg_search is not None
    assert litre_search["result"]["structuredContent"]["items"][0]["id"] == str(PRODUCT_A_ID)
    assert ten_litre_search["result"]["structuredContent"]["items"][0]["id"] == str(PRODUCT_B_ID)
    assert hebrew_kg_search["result"]["structuredContent"]["items"][0]["id"] == str(PRODUCT_A_ID)
    assert barcode["result"]["structuredContent"]["data"]["id"] == str(PRODUCT_A_ID)
    assert retailer_page["result"]["structuredContent"]["items"][0]["id"] == str(RETAILER_ID)
    assert {item["portal_id"] for item in store_page["result"]["structuredContent"]["items"]} == {
        str(PORTAL_A_ID),
        str(PORTAL_B_ID),
    }
    assert ambiguous is not None
    assert ambiguous["result"]["isError"] is True
    assert ambiguous["result"]["structuredContent"]["error"]["code"] == ("domain_validation_error")
    assert scoped is not None
    assert scoped["result"]["structuredContent"]["data"]["portal_id"] == str(PORTAL_A_ID)
    assert product is not None
    retailer_identifier = next(
        identifier
        for identifier in product["result"]["structuredContent"]["data"]["identifiers"]
        if identifier["kind"] == "retailer_item"
    )
    assert retailer_identifier["issuer_portal_id"] == str(PORTAL_A_ID)
    assert current_prices_response is not None
    assert current_prices_response["result"]["structuredContent"]["items"][0][
        "source_file_id"
    ] == str(SOURCE_CURRENT_ID)
    assert (
        comparison["result"]["structuredContent"]
        == current_prices_response["result"]["structuredContent"]
    )
    assert price_history_response is not None
    assert [
        row["source_file_id"]
        for row in price_history_response["result"]["structuredContent"]["items"]
    ] == [str(SOURCE_CURRENT_ID), str(SOURCE_OLD_ID)]
    assert active is not None
    active_version = active["result"]["structuredContent"]["items"][0]
    assert active_version["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert len(active_version["items"]) == MAXIMUM_PROMOTION_RELATIONS
    assert history_second is not None
    assert history["result"]["structuredContent"]["nextCursor"] is not None
    assert history_second["result"]["structuredContent"]["nextCursor"] is None
    versions = (
        history["result"]["structuredContent"]["items"]
        + history_second["result"]["structuredContent"]["items"]
    )
    assert [version["id"] for version in versions] == [
        str(PROMOTION_CURRENT_ID),
        str(PROMOTION_OLD_ID),
    ]
    assert len(versions[0]["clubs"]) == MAXIMUM_PROMOTION_RELATIONS
    assert availability["result"]["structuredContent"]["items"][0]["source_file_id"] == str(
        SOURCE_CURRENT_ID
    )
    assert freshness["result"]["structuredContent"]["items"][0]["portal_id"] == str(PORTAL_A_ID)
    assert freshness["result"]["structuredContent"]["items"][0]["source_file_id"] == str(
        SOURCE_CURRENT_ID
    )
    assert freshness["result"]["structuredContent"]["items"][0]["content_sha256"] == "2" * 64
    assert source_status["result"]["structuredContent"]["maintenance"] == {
        "active": False,
        "mode": "normal",
    }
    assert invalid_history is not None
    assert invalid_history["result"]["structuredContent"]["error"]["code"] == (
        "domain_validation_error"
    )
    assert invalid_cursor is not None
    assert invalid_cursor["result"]["structuredContent"]["error"]["code"] == (
        "domain_validation_error"
    )
    assert mismatched_history_cursor is not None
    assert mismatched_history_cursor["result"]["structuredContent"]["error"]["code"] == (
        "domain_validation_error"
    )


async def _assert_cli_contract(
    database_url: str,
    archive_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MAKOLET_ENVIRONMENT", "test")
    monkeypatch.setenv("MAKOLET_DATABASE_URL", database_url)
    monkeypatch.setenv("MAKOLET_ARCHIVE_BACKEND", "local")
    monkeypatch.setenv("MAKOLET_ARCHIVE_ROOT", str(archive_root))
    monkeypatch.delenv("MAKOLET_ENABLED_SOURCES", raising=False)
    monkeypatch.delenv("MAKOLET_SOURCE_INTERVALS_SECONDS", raising=False)

    def invoke() -> tuple[Result, ...]:
        application = build_cli()
        runner = CliRunner()
        results = (
            runner.invoke(
                application,
                [
                    "products",
                    "find-retailer-item",
                    str(RETAILER_ID),
                    "COLLIDE-1",
                    "--json",
                ],
            ),
            runner.invoke(
                application,
                [
                    "products",
                    "find-retailer-item",
                    str(RETAILER_ID),
                    "COLLIDE-1",
                    "--portal-id",
                    str(PORTAL_A_ID),
                    "--json",
                ],
            ),
            runner.invoke(
                application,
                ["products", "search", "portal", "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["products", "search", "portal 1000 ml", "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["products", "search", "portal 10 l", "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["products", "search", "אורז 1000 גרם", "--limit", "10", "--json"],
            ),
            runner.invoke(application, ["products", "get", str(PRODUCT_A_ID), "--json"]),
            runner.invoke(application, ["retailers", "list", "--limit", "10", "--json"]),
            runner.invoke(
                application,
                ["stores", "find", "--city", "Jerusalem", "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["products", "find-barcode", "7290000000015", "--json"],
            ),
            runner.invoke(
                application,
                ["prices", "current", str(PRODUCT_A_ID), "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["prices", "compare", str(PRODUCT_A_ID), "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                ["prices", "history", str(PRODUCT_A_ID), "--limit", "10", "--json"],
            ),
            runner.invoke(
                application,
                [
                    "promotions",
                    "active",
                    "--product-id",
                    str(PRODUCT_A_ID),
                    "--at",
                    NOW.isoformat(),
                    "--limit",
                    "10",
                    "--json",
                ],
            ),
            runner.invoke(
                application,
                [
                    "promotions",
                    "history",
                    "--product-id",
                    str(PRODUCT_A_ID),
                    "--limit",
                    "10",
                    "--json",
                ],
            ),
            runner.invoke(
                application,
                ["availability", "current", str(PRODUCT_A_ID), "--limit", "10", "--json"],
            ),
            runner.invoke(application, ["freshness", "--limit", "10", "--json"]),
            runner.invoke(application, ["source-status", "--limit", "10", "--json"]),
        )
        bounded_history = runner.invoke(
            application,
            ["prices", "history", str(PRODUCT_A_ID), "--limit", "1", "--json"],
        )
        promotion_history_cursor = json.loads(results[14].stdout)["next_cursor"]
        promotion_history_second = runner.invoke(
            application,
            [
                "promotions",
                "history",
                "--product-id",
                str(PRODUCT_A_ID),
                "--limit",
                "10",
                "--cursor",
                promotion_history_cursor,
                "--json",
            ],
        )
        history_cursor = json.loads(bounded_history.stdout)["next_cursor"]
        mismatched_history_cursor = runner.invoke(
            application,
            [
                "prices",
                "history",
                str(PRODUCT_A_ID),
                "--store-id",
                str(STORE_A_ID),
                "--limit",
                "1",
                "--cursor",
                history_cursor,
                "--json",
            ],
        )
        return (
            *results,
            promotion_history_second,
            bounded_history,
            mismatched_history_cursor,
        )

    (
        ambiguous,
        scoped,
        search,
        litre_search,
        ten_litre_search,
        hebrew_kg_search,
        product,
        retailer_page,
        store_page,
        barcode,
        prices,
        comparison,
        price_versions,
        active,
        history,
        availability,
        freshness,
        source_status,
        promotion_history_second,
        bounded_history,
        mismatched_history_cursor,
    ) = await asyncio.to_thread(invoke)
    assert ambiguous.exit_code == 1
    assert '"code":"domain_validation_error"' in ambiguous.output
    assert bounded_history.exit_code == 0
    assert mismatched_history_cursor.exit_code == 1
    assert '"code":"domain_validation_error"' in mismatched_history_cursor.output
    for result in (
        scoped,
        search,
        litre_search,
        ten_litre_search,
        hebrew_kg_search,
        product,
        retailer_page,
        store_page,
        barcode,
        prices,
        comparison,
        price_versions,
        active,
        history,
        availability,
        freshness,
        source_status,
        promotion_history_second,
    ):
        assert result.exit_code == 0, result.output

    scoped_body = json.loads(scoped.stdout)
    search_body = json.loads(search.stdout)
    litre_search_body = json.loads(litre_search.stdout)
    ten_litre_search_body = json.loads(ten_litre_search.stdout)
    hebrew_kg_search_body = json.loads(hebrew_kg_search.stdout)
    product_body = json.loads(product.stdout)
    retailer_page_body = json.loads(retailer_page.stdout)
    store_page_body = json.loads(store_page.stdout)
    barcode_body = json.loads(barcode.stdout)
    prices_body = json.loads(prices.stdout)
    comparison_body = json.loads(comparison.stdout)
    price_versions_body = json.loads(price_versions.stdout)
    active_body = json.loads(active.stdout)
    history_body = json.loads(history.stdout)
    history_second_body = json.loads(promotion_history_second.stdout)
    availability_body = json.loads(availability.stdout)
    freshness_body = json.loads(freshness.stdout)
    source_status_body = json.loads(source_status.stdout)
    assert scoped_body["data"]["portal_id"] == str(PORTAL_A_ID)
    assert any(item["id"] == str(PRODUCT_A_ID) for item in search_body["items"])
    assert litre_search_body["items"][0]["id"] == str(PRODUCT_A_ID)
    assert ten_litre_search_body["items"][0]["id"] == str(PRODUCT_B_ID)
    assert hebrew_kg_search_body["items"][0]["id"] == str(PRODUCT_A_ID)
    retailer_identifier = next(
        identifier
        for identifier in product_body["data"]["identifiers"]
        if identifier["kind"] == "retailer_item"
    )
    assert retailer_identifier["issuer_portal_id"] == str(PORTAL_A_ID)
    assert retailer_page_body["items"][0]["id"] == str(RETAILER_ID)
    assert {store["portal_id"] for store in store_page_body["items"]} == {
        str(PORTAL_A_ID),
        str(PORTAL_B_ID),
    }
    assert barcode_body["data"]["id"] == str(PRODUCT_A_ID)
    assert prices_body["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert comparison_body == prices_body
    assert [row["source_file_id"] for row in price_versions_body["items"]] == [
        str(SOURCE_CURRENT_ID),
        str(SOURCE_OLD_ID),
    ]
    assert active_body["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert len(active_body["items"][0]["clubs"]) == MAXIMUM_PROMOTION_RELATIONS
    assert history_body["next_cursor"] is not None
    assert history_second_body["next_cursor"] is None
    assert [version["id"] for version in history_body["items"] + history_second_body["items"]] == [
        str(PROMOTION_CURRENT_ID),
        str(PROMOTION_OLD_ID),
    ]
    assert availability_body["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert freshness_body["items"][0]["portal_id"] == str(PORTAL_A_ID)
    assert freshness_body["items"][0]["source_file_id"] == str(SOURCE_CURRENT_ID)
    assert freshness_body["items"][0]["content_sha256"] == "2" * 64
    assert {item["portal_key"] for item in source_status_body["items"]} == {
        "public-query-portal-a",
        "public-query-portal-b",
    }


def _mcp_request(
    name: str,
    arguments: dict[str, object],
    *,
    request_id: int,
) -> dict[str, object]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": LATEST_PROTOCOL_VERSION,
                "io.modelcontextprotocol/clientCapabilities": {},
                "io.modelcontextprotocol/clientInfo": {
                    "name": "public-query-integration",
                    "version": "1",
                },
            },
            "name": name,
            "arguments": arguments,
        },
    }
