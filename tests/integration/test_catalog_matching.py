"""Behavior-level staged catalog matching checks against PostgreSQL 18."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest
from sqlalchemy import text

from makolet.adapters.persistence.catalog_matching import PostgresCatalogMatchingRepository
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.application.catalog_matching import CandidateStatus, CatalogMatchingService
from makolet.domain.errors import CatalogMatchConflictError

pytestmark = pytest.mark.integration

RETAILER_A = UUID("00000000-0000-0000-0000-00000000000a")
RETAILER_B = UUID("00000000-0000-0000-0000-00000000000b")
PORTAL_A = UUID("10000000-0000-0000-0000-00000000000a")
PORTAL_B = UUID("10000000-0000-0000-0000-00000000000b")
SOURCE_A = UUID("20000000-0000-0000-0000-00000000000a")
SOURCE_B = UUID("20000000-0000-0000-0000-00000000000b")
ITEM_A = UUID("30000000-0000-0000-0000-00000000000a")
ITEM_B = UUID("30000000-0000-0000-0000-00000000000b")
ITEM_C = UUID("30000000-0000-0000-0000-00000000000c")
PORTAL_A_SECOND = UUID("10000000-0000-0000-0000-0000000000aa")
SOURCE_A_SECOND = UUID("20000000-0000-0000-0000-0000000000aa")
ITEM_A_SECOND = UUID("30000000-0000-0000-0000-0000000000aa")


async def _seed_non_gtin_items(database: Database) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO retailers (id, source_key, legal_name, display_name)
                VALUES (:retailer_a, 'catalog-a', 'Catalog A Ltd', 'Catalog A'),
                       (:retailer_b, 'catalog-b', 'Catalog B Ltd', 'Catalog B')
                """
            ),
            {"retailer_a": RETAILER_A, "retailer_b": RETAILER_B},
        )
        await connection.execute(
            text(
                """
                INSERT INTO portals (id, retailer_id, source_key, family, protocol)
                VALUES (:portal_a, :retailer_a, 'catalog-a', 'fixture', 'fixture'),
                       (:portal_b, :retailer_b, 'catalog-b', 'fixture', 'fixture')
                """
            ),
            {
                "portal_a": PORTAL_A,
                "portal_b": PORTAL_B,
                "retailer_a": RETAILER_A,
                "retailer_b": RETAILER_B,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO source_files (
                    id, retailer_id, portal_id, remote_id, download_url,
                    original_filename, document_type, compression, protocol,
                    status, discovered_at, source_timestamp
                ) VALUES (
                    :source_a, :retailer_a, :portal_a, 'catalog-a.xml',
                    'https://fixtures.invalid/catalog-a.xml', 'catalog-a.xml',
                    'price_full', 'none', 'fixture', 'completed',
                    '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z'
                ), (
                    :source_b, :retailer_b, :portal_b, 'catalog-b.xml',
                    'https://fixtures.invalid/catalog-b.xml', 'catalog-b.xml',
                    'price_full', 'none', 'fixture', 'completed',
                    '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z'
                )
                """
            ),
            {
                "source_a": SOURCE_A,
                "source_b": SOURCE_B,
                "retailer_a": RETAILER_A,
                "retailer_b": RETAILER_B,
                "portal_a": PORTAL_A,
                "portal_b": PORTAL_B,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    id, retailer_id, portal_id, source_item_code, name,
                    manufacturer_name, unit_quantity, quantity,
                    unit_of_measure, quantity_in_package,
                    first_seen_at, last_seen_at, last_source_file_id
                ) VALUES (
                    :item_a, :retailer_a, :portal_a, 'MILK-A', 'Organic whole milk',
                    'Clean Room Dairy', '1 litre', 1, 'litre', 1,
                    '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', :source_a
                ), (
                    :item_b, :retailer_b, :portal_b, 'MILK-B', 'Organic whole milk',
                    'Clean Room Dairy', '1 litre', 1, 'litre', 1,
                    '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', :source_b
                ), (
                    :item_c, :retailer_b, :portal_b, 'MILK-A', 'Organic whole milk',
                    'Clean Room Dairy', '1 litre', 1, 'litre', 1,
                    '2026-08-12T00:00:00Z', '2026-08-12T00:00:00Z', :source_b
                )
                """
            ),
            {
                "item_a": ITEM_A,
                "item_b": ITEM_B,
                "item_c": ITEM_C,
                "retailer_a": RETAILER_A,
                "retailer_b": RETAILER_B,
                "portal_a": PORTAL_A,
                "portal_b": PORTAL_B,
                "source_a": SOURCE_A,
                "source_b": SOURCE_B,
            },
        )


async def test_non_gtin_bootstrap_generation_and_review_are_auditable(
    database: Database,
) -> None:
    await _seed_non_gtin_items(database)
    repository = PostgresCatalogMatchingRepository(database.engine)
    service = CatalogMatchingService(repository)
    queries = PostgresQueryRepository(database.engine)

    first_bootstrap = await service.bootstrap_source_file(SOURCE_A)
    second_bootstrap = await service.bootstrap_source_file(SOURCE_B)
    repeated_bootstrap = await service.bootstrap_source_file(SOURCE_B)

    assert first_bootstrap["bootstrapped_items"] == 1
    assert second_bootstrap["bootstrapped_items"] == 2
    assert repeated_bootstrap["bootstrapped_items"] == 0
    isolated_a = await queries.find_product_by_retailer_item_code(RETAILER_A, "MILK-A")
    isolated_b = await queries.find_product_by_retailer_item_code(RETAILER_B, "MILK-B")
    assert isolated_a is not None
    assert isolated_b is not None
    assert isolated_a["id"] != isolated_b["id"]
    assert isolated_a["match_method"] == "isolated_retailer_item"

    generated = await service.generate_candidates(item_limit=3, candidate_limit=10)
    pending = await service.list_candidates(status=CandidateStatus.PENDING, limit=20)

    assert generated.processed_items == 3
    assert generated.bootstrapped_items == 0
    assert generated.candidates_written >= 3
    assert pending.items
    exact_non_gtin = next(
        item
        for item in pending.items
        if item["retailer_item_id"] == ITEM_C and item["canonical_product_id"] == isolated_a["id"]
    )
    assert exact_non_gtin["method"] == "normalized_identifier"
    assert Decimal(str(exact_non_gtin["score"])) == Decimal("0.95")
    candidate_a = next(
        item
        for item in pending.items
        if item["retailer_item_id"] == ITEM_A and item["canonical_product_id"] == isolated_b["id"]
    )
    inspected = await service.inspect_candidate(UUID(str(candidate_a["id"])))
    assert inspected["status"] == "pending"
    evidence = cast(dict[str, object], inspected["evidence"])
    explanations = cast(list[str], evidence["explanations"])
    assert Decimal(str(inspected["score"])) >= Decimal("0.75")
    assert evidence["disposition"] == "review"
    assert any("quantity" in value for value in explanations)

    accepted = await service.accept_candidate(
        UUID(str(candidate_a["id"])),
        reviewed_by="catalog-reviewer@example.test",
    )
    merged_a = await queries.find_product_by_retailer_item_code(RETAILER_A, "MILK-A")
    merged_b = await queries.find_product_by_retailer_item_code(RETAILER_B, "MILK-B")

    assert accepted["status"] == "accepted"
    assert accepted["reviewed_by"] == "catalog-reviewer@example.test"
    assert accepted["reviewed_at"] is not None
    assert accepted["effective_match_method"] == "manual_review"
    assert merged_a is not None
    assert merged_b is not None
    assert merged_a["id"] == merged_b["id"] == isolated_b["id"]
    async with database.engine.connect() as connection:
        isolated_status = await connection.scalar(
            text("SELECT status FROM canonical_products WHERE id = :product_id"),
            {"product_id": isolated_a["id"]},
        )
        superseded_count = await connection.scalar(
            text(
                """
                SELECT count(*) FROM product_match_candidates
                 WHERE canonical_product_id = :product_id
                   AND status = 'superseded'
                   AND reviewed_by = :reviewed_by
                """
            ),
            {
                "product_id": isolated_a["id"],
                "reviewed_by": "catalog-reviewer@example.test",
            },
        )
    assert isolated_status == "retired"
    assert superseded_count >= 1


async def test_reject_and_conflicting_manual_match_never_overwrite(
    database: Database,
) -> None:
    await _seed_non_gtin_items(database)
    repository = PostgresCatalogMatchingRepository(database.engine)
    service = CatalogMatchingService(repository)
    await service.bootstrap_source_file(SOURCE_A)
    await service.bootstrap_source_file(SOURCE_B)
    await service.generate_candidates(item_limit=3, candidate_limit=10)
    pending = await service.list_candidates(status=CandidateStatus.PENDING, limit=20)
    candidate_b = next(item for item in pending.items if item["retailer_item_id"] == ITEM_B)
    candidate_c = next(item for item in pending.items if item["retailer_item_id"] == ITEM_C)

    rejected = await service.reject_candidate(
        UUID(str(candidate_b["id"])),
        reviewed_by="rejector@example.test",
    )
    assert rejected["status"] == "rejected"
    assert rejected["reviewed_at"] is not None

    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE confirmed_product_matches
                   SET method = 'manual_review', confirmed_by = 'prior-reviewer'
                 WHERE retailer_item_id = :item_id
                """
            ),
            {"item_id": ITEM_C},
        )
    with pytest.raises(CatalogMatchConflictError, match="manual or non-isolated"):
        await service.accept_candidate(
            UUID(str(candidate_c["id"])),
            reviewed_by="second-reviewer@example.test",
        )
    unchanged = await service.inspect_candidate(UUID(str(candidate_c["id"])))
    assert unchanged["status"] == "pending"
    assert unchanged["effective_match_method"] == "manual_review"


async def test_post_ingestion_bootstrap_plan_uses_source_item_index(
    database: Database,
) -> None:
    await _seed_non_gtin_items(database)

    async with database.engine.connect() as connection:
        await connection.execute(text("SET LOCAL enable_seqscan = off"))
        plan = await connection.scalar(
            text(
                """
                EXPLAIN (FORMAT JSON, COSTS FALSE)
                SELECT item.id
                  FROM retailer_items item
                  LEFT JOIN confirmed_product_matches confirmed
                    ON confirmed.retailer_item_id = item.id
                 WHERE item.last_source_file_id = :source_file_id
                   AND confirmed.id IS NULL
                 ORDER BY item.id
                 LIMIT 501
                """
            ),
            {"source_file_id": SOURCE_B},
        )
        candidate_plan = await connection.scalar(
            text(
                """
                EXPLAIN (FORMAT JSON, COSTS FALSE)
                SELECT product.id
                  FROM canonical_products product
                 WHERE product.status = 'active'
                 ORDER BY product.name_search <-> 'organic whole milk', product.id
                 LIMIT 10
                """
            )
        )
        review_plan = await connection.scalar(
            text(
                """
                EXPLAIN (FORMAT JSON, COSTS FALSE)
                SELECT candidate.id
                  FROM product_match_candidates candidate
                 WHERE candidate.status = 'pending'
                 ORDER BY candidate.score DESC, candidate.id
                 LIMIT 10
                """
            )
        )

    rendered = json.dumps(plan)
    assert "ix_retailer_items_last_source_file_id_id" in rendered
    assert "ix_canonical_products_active_name_trgm_gist" in json.dumps(candidate_plan)
    assert "ix_match_candidates_status_score" in json.dumps(review_plan)


async def test_same_retailer_item_code_isolated_per_portal(database: Database) -> None:
    await _seed_non_gtin_items(database)
    async with database.engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO portals (id, retailer_id, source_key, family, protocol)
                VALUES (:portal_id, :retailer_id, 'catalog-a-second', 'fixture', 'fixture')
                """
            ),
            {"portal_id": PORTAL_A_SECOND, "retailer_id": RETAILER_A},
        )
        await connection.execute(
            text(
                """
                INSERT INTO source_files (
                    id, retailer_id, portal_id, remote_id, download_url,
                    original_filename, document_type, compression, protocol,
                    status, discovered_at
                ) VALUES (
                    :source_id, :retailer_id, :portal_id, 'catalog-a-second.xml',
                    'https://fixtures.invalid/catalog-a-second.xml',
                    'catalog-a-second.xml', 'price_full', 'none', 'fixture',
                    'completed', '2026-08-12T00:00:00Z'
                )
                """
            ),
            {
                "portal_id": PORTAL_A_SECOND,
                "retailer_id": RETAILER_A,
                "source_id": SOURCE_A_SECOND,
            },
        )
        await connection.execute(
            text(
                """
                INSERT INTO retailer_items (
                    id, retailer_id, portal_id, source_item_code, name,
                    first_seen_at, last_seen_at, last_source_file_id
                ) VALUES (
                    :item_id, :retailer_id, :portal_id, 'MILK-A',
                    'Independent portal milk', '2026-08-12T00:00:00Z',
                    '2026-08-12T00:00:00Z', :source_id
                )
                """
            ),
            {
                "portal_id": PORTAL_A_SECOND,
                "retailer_id": RETAILER_A,
                "source_id": SOURCE_A_SECOND,
                "item_id": ITEM_A_SECOND,
            },
        )

    service = CatalogMatchingService(PostgresCatalogMatchingRepository(database.engine))
    await service.bootstrap_source_file(SOURCE_A)
    await service.bootstrap_source_file(SOURCE_A_SECOND)

    async with database.engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    """
                    SELECT identifier.issuer_portal_id, identifier.product_id
                      FROM product_identifiers identifier
                     WHERE identifier.kind = 'retailer_item'
                       AND identifier.issuer_retailer_id = :retailer_id
                       AND identifier.normalized_value = 'MILK-A'
                     ORDER BY identifier.issuer_portal_id
                    """
                ),
                {"retailer_id": RETAILER_A},
            )
        ).all()

    assert [row.issuer_portal_id for row in rows] == [PORTAL_A, PORTAL_A_SECOND]
    assert rows[0].product_id != rows[1].product_id
