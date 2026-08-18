"""Set-bounded PostgreSQL catalog candidate generation and operator review."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from makolet.application.models import CatalogCandidateGenerationResult, Page
from makolet.domain.catalog import (
    CanonicalMatchCandidate,
    MatchRule,
    ProductIdentifier,
    canonical_product_descriptor,
    product_descriptor,
    score_catalog_candidate,
)
from makolet.domain.enums import IdentifierKind
from makolet.domain.errors import CatalogMatchConflictError, NotFoundError
from makolet.domain.normalization import is_valid_gtin

_ISOLATED_METHOD = "isolated_retailer_item"
_ISOLATED_ACTOR = "system:isolated-catalog"
_BOOTSTRAP_BATCH_SIZE = 500


class PostgresCatalogMatchingRepository:
    """Keep isolated catalog representation separate from reviewed equivalence."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def bootstrap_source_file(self, source_file_id: UUID) -> dict[str, object]:
        """Create isolated identities in fixed transactions until the file is covered."""

        cursor_id: UUID | None = None
        bootstrapped = 0
        while True:
            try:
                async with self._engine.begin() as connection:
                    await self._select_source_subjects(
                        connection,
                        source_file_id=source_file_id,
                        cursor_id=cursor_id,
                    )
                    subject_rows = await self._subject_rows(
                        connection,
                        item_limit=_BOOTSTRAP_BATCH_SIZE,
                    )
                    if not subject_rows:
                        break
                    bootstrapped += await self._bootstrap_isolated_products(
                        connection,
                        _BOOTSTRAP_BATCH_SIZE,
                    )
                    cursor_id = UUID(str(subject_rows[-1]["retailer_item_id"]))
                    has_more = await self._has_more_subjects(
                        connection,
                        item_limit=_BOOTSTRAP_BATCH_SIZE,
                    )
                if not has_more:
                    break
            except IntegrityError as error:
                raise CatalogMatchConflictError(
                    "Catalog bootstrap encountered a concurrent identity decision; retry ingestion"
                ) from error
        return {
            "source_file_id": source_file_id,
            "bootstrapped_items": bootstrapped,
            "complete": True,
        }

    async def generate_candidates(
        self,
        *,
        cursor: str | None,
        item_limit: int,
        candidate_limit: int,
        review_threshold: str,
    ) -> CatalogCandidateGenerationResult:
        cursor_id = _uuid_cursor(cursor)
        threshold = Decimal(review_threshold)
        try:
            async with self._engine.begin() as connection:
                await self._select_subjects(
                    connection,
                    cursor_id=cursor_id,
                    item_limit=item_limit,
                )
                subject_rows = await self._subject_rows(connection, item_limit=item_limit)
                if not subject_rows:
                    return CatalogCandidateGenerationResult(0, 0, 0, None)
                bootstrapped = await self._bootstrap_isolated_products(connection, item_limit)
                block_rows = await self._candidate_block_rows(
                    connection,
                    item_limit=item_limit,
                    candidate_limit=candidate_limit,
                )
                proposals = _score_rows(
                    block_rows,
                    threshold=threshold,
                    candidate_limit=candidate_limit,
                )
                written = await self._write_candidates(connection, proposals)
                has_more = await self._has_more_subjects(connection, item_limit=item_limit)
                next_cursor = str(subject_rows[-1]["retailer_item_id"]) if has_more else None
                return CatalogCandidateGenerationResult(
                    processed_items=len(subject_rows),
                    bootstrapped_items=bootstrapped,
                    candidates_written=written,
                    next_cursor=next_cursor,
                )
        except IntegrityError as error:
            raise CatalogMatchConflictError(
                "Catalog generation encountered a concurrent identity decision; rerun the page"
            ) from error

    async def list_candidates(
        self,
        *,
        status: str,
        retailer_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            """
            WITH cursor_row AS (
                SELECT score, id
                  FROM product_match_candidates
                 WHERE id = CAST(:cursor_id AS uuid)
            )
            SELECT candidate.id, candidate.method, candidate.score,
                   candidate.status, candidate.evidence, candidate.created_at,
                   candidate.reviewed_at, candidate.reviewed_by,
                   item.id AS retailer_item_id,
                   item.source_item_code, item.name AS retailer_item_name,
                   retailer.id AS retailer_id,
                   retailer.display_name AS retailer_name,
                   product.id AS canonical_product_id,
                   product.name AS canonical_product_name,
                   product.status AS canonical_product_status
              FROM product_match_candidates candidate
              JOIN retailer_items item ON item.id = candidate.retailer_item_id
              JOIN retailers retailer ON retailer.id = item.retailer_id
              JOIN canonical_products product
                ON product.id = candidate.canonical_product_id
             WHERE candidate.status = :status
               AND (
                    CAST(:retailer_id AS uuid) IS NULL
                    OR item.retailer_id = CAST(:retailer_id AS uuid)
               )
               AND (
                    CAST(:cursor_id AS uuid) IS NULL
                    OR candidate.score < (SELECT score FROM cursor_row)
                    OR (
                        candidate.score = (SELECT score FROM cursor_row)
                        AND candidate.id > CAST(:cursor_id AS uuid)
                    )
               )
             ORDER BY candidate.score DESC, candidate.id
             LIMIT :limit
            """,
            {
                "status": status,
                "retailer_id": retailer_id,
                "cursor_id": cursor_id,
                "limit": limit + 1,
            },
        )
        items = rows[:limit]
        next_cursor = str(items[-1]["id"]) if len(rows) > limit and items else None
        return Page(items=items, next_cursor=next_cursor)

    async def inspect_candidate(self, candidate_id: UUID) -> dict[str, object]:
        rows = await self._rows(
            """
            SELECT candidate.id, candidate.method, candidate.score,
                   candidate.status, candidate.evidence, candidate.created_at,
                   candidate.reviewed_at, candidate.reviewed_by,
                   item.id AS retailer_item_id,
                   item.source_item_code, item.gtin,
                   item.name AS retailer_item_name,
                   item.manufacturer_name, item.quantity,
                   item.unit_of_measure, item.unit_quantity,
                   item.quantity_in_package,
                   retailer.id AS retailer_id,
                   retailer.display_name AS retailer_name,
                   product.id AS canonical_product_id,
                   product.name AS canonical_product_name,
                   product.brand AS canonical_product_brand,
                   product.manufacturer AS canonical_product_manufacturer,
                   product.quantity AS canonical_product_quantity,
                   product.unit_of_measure AS canonical_product_unit,
                   product.status AS canonical_product_status,
                   confirmed.canonical_product_id AS effective_product_id,
                   confirmed.method AS effective_match_method,
                   confirmed.confirmed_at, confirmed.confirmed_by
              FROM product_match_candidates candidate
              JOIN retailer_items item ON item.id = candidate.retailer_item_id
              JOIN retailers retailer ON retailer.id = item.retailer_id
              JOIN canonical_products product
                ON product.id = candidate.canonical_product_id
              LEFT JOIN confirmed_product_matches confirmed
                ON confirmed.retailer_item_id = item.id
             WHERE candidate.id = :candidate_id
            """,
            {"candidate_id": candidate_id},
        )
        if not rows:
            raise NotFoundError("Catalog match candidate was not found")
        return rows[0]

    async def accept_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        try:
            async with self._engine.begin() as connection:
                candidate = await _locked_candidate(connection, candidate_id)
                _require_pending(candidate)
                existing = await _locked_match(connection, candidate["retailer_item_id"])
                if (
                    existing is None
                    or existing["method"] != _ISOLATED_METHOD
                    or existing["confirmed_by"] != _ISOLATED_ACTOR
                ):
                    raise CatalogMatchConflictError(
                        "Candidate cannot replace a manual or non-isolated catalog match"
                    )
                isolated_product_id = UUID(str(existing["canonical_product_id"]))
                target_product_id = UUID(str(candidate["canonical_product_id"]))
                if isolated_product_id == target_product_id:
                    raise CatalogMatchConflictError(
                        "Candidate target is the item's existing isolated product"
                    )
                products = await _lock_products(
                    connection,
                    (isolated_product_id, target_product_id),
                )
                target = products.get(target_product_id)
                if target is None or target["status"] != "active":
                    raise CatalogMatchConflictError("Candidate target product is not active")
                await _validate_identifier_move(
                    connection,
                    retailer_id=UUID(str(candidate["retailer_id"])),
                    portal_id=UUID(str(candidate["portal_id"])),
                    source_item_code=str(candidate["source_item_code"]),
                    isolated_product_id=isolated_product_id,
                    target_product_id=target_product_id,
                )
                decision_evidence = json.dumps(
                    {
                        "candidate_id": str(candidate_id),
                        "candidate_method": str(candidate["method"]),
                        "candidate_score": str(candidate["score"]),
                        "candidate_evidence": candidate["evidence"],
                        "superseded_isolated_product_id": str(isolated_product_id),
                    },
                    ensure_ascii=False,
                )
                await connection.execute(
                    text(
                        """
                        UPDATE confirmed_product_matches
                           SET canonical_product_id = :target_product_id,
                               method = 'manual_review',
                               evidence = CAST(:evidence AS jsonb),
                               confirmed_at = clock_timestamp(),
                               confirmed_by = :reviewed_by
                         WHERE retailer_item_id = :retailer_item_id
                           AND canonical_product_id = :isolated_product_id
                           AND method = :isolated_method
                           AND confirmed_by = :isolated_actor
                        """
                    ),
                    {
                        "target_product_id": target_product_id,
                        "evidence": decision_evidence,
                        "reviewed_by": reviewed_by,
                        "retailer_item_id": candidate["retailer_item_id"],
                        "isolated_product_id": isolated_product_id,
                        "isolated_method": _ISOLATED_METHOD,
                        "isolated_actor": _ISOLATED_ACTOR,
                    },
                )
                await _move_retailer_identifier(
                    connection,
                    retailer_id=UUID(str(candidate["retailer_id"])),
                    portal_id=UUID(str(candidate["portal_id"])),
                    source_item_code=str(candidate["source_item_code"]),
                    isolated_product_id=isolated_product_id,
                    target_product_id=target_product_id,
                    candidate_id=candidate_id,
                    reviewed_by=reviewed_by,
                )
                await connection.execute(
                    text(
                        """
                        UPDATE product_match_candidates
                           SET status = CASE WHEN id = :candidate_id
                                             THEN 'accepted'
                                             ELSE 'superseded'
                                        END,
                               reviewed_at = clock_timestamp(),
                               reviewed_by = :reviewed_by
                         WHERE retailer_item_id = :retailer_item_id
                           AND status = 'pending'
                        """
                    ),
                    {
                        "candidate_id": candidate_id,
                        "reviewed_by": reviewed_by,
                        "retailer_item_id": candidate["retailer_item_id"],
                    },
                )
                retired = (
                    await connection.execute(
                        text(
                            """
                            UPDATE canonical_products product
                               SET status = 'retired', updated_at = clock_timestamp()
                             WHERE product.id = :isolated_product_id
                               AND product.status = 'active'
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM confirmed_product_matches confirmed
                                    WHERE confirmed.canonical_product_id = product.id
                               )
                            RETURNING product.id
                            """
                        ),
                        {"isolated_product_id": isolated_product_id},
                    )
                ).first()
                if retired is not None:
                    await connection.execute(
                        text(
                            """
                            UPDATE product_match_candidates
                               SET status = 'superseded',
                                   reviewed_at = clock_timestamp(),
                                   reviewed_by = :reviewed_by
                             WHERE canonical_product_id = :isolated_product_id
                               AND status = 'pending'
                            """
                        ),
                        {
                            "isolated_product_id": isolated_product_id,
                            "reviewed_by": reviewed_by,
                        },
                    )
        except IntegrityError as error:
            raise CatalogMatchConflictError(
                "Candidate acceptance conflicts with an established catalog identifier"
            ) from error
        return await self.inspect_candidate(candidate_id)

    async def reject_candidate(
        self,
        candidate_id: UUID,
        *,
        reviewed_by: str,
    ) -> dict[str, object]:
        async with self._engine.begin() as connection:
            candidate = await _locked_candidate(connection, candidate_id)
            _require_pending(candidate)
            await connection.execute(
                text(
                    """
                    UPDATE product_match_candidates
                       SET status = 'rejected',
                           reviewed_at = clock_timestamp(),
                           reviewed_by = :reviewed_by
                     WHERE id = :candidate_id
                       AND status = 'pending'
                    """
                ),
                {"candidate_id": candidate_id, "reviewed_by": reviewed_by},
            )
        return await self.inspect_candidate(candidate_id)

    async def _select_subjects(
        self,
        connection: AsyncConnection,
        *,
        cursor_id: UUID | None,
        item_limit: int,
    ) -> None:
        await connection.execute(
            text(
                """
                CREATE TEMP TABLE makolet_catalog_subjects ON COMMIT DROP AS
                WITH selected AS MATERIALIZED (
                    SELECT item.id
                      FROM retailer_items item
                      LEFT JOIN confirmed_product_matches confirmed
                        ON confirmed.retailer_item_id = item.id
                     WHERE (confirmed.id IS NULL OR confirmed.method = :isolated_method)
                       AND (
                            CAST(:cursor_id AS uuid) IS NULL
                            OR item.id > CAST(:cursor_id AS uuid)
                       )
                     ORDER BY item.id
                     LIMIT :selection_limit
                       FOR UPDATE OF item
                )
                SELECT row_number() OVER (ORDER BY item.id) AS sequence,
                       item.id AS retailer_item_id, item.retailer_id,
                       item.portal_id,
                       item.source_item_code, item.gtin, item.name,
                       item.name_search, item.manufacturer_name,
                       item.quantity, item.unit_of_measure,
                       item.unit_quantity, item.quantity_in_package,
                       confirmed.method AS current_match_method,
                       COALESCE(confirmed.canonical_product_id, uuidv7())
                           AS isolated_product_id
                  FROM selected
                  JOIN retailer_items item ON item.id = selected.id
                  LEFT JOIN confirmed_product_matches confirmed
                    ON confirmed.retailer_item_id = item.id
                 ORDER BY item.id
                """
            ),
            {
                "isolated_method": _ISOLATED_METHOD,
                "cursor_id": cursor_id,
                "selection_limit": item_limit + 1,
            },
        )
        await connection.execute(
            text(
                """
                CREATE UNIQUE INDEX makolet_catalog_subjects_item_idx
                    ON makolet_catalog_subjects (retailer_item_id)
                """
            )
        )
        await connection.execute(text("ANALYZE makolet_catalog_subjects"))

    async def _select_source_subjects(
        self,
        connection: AsyncConnection,
        *,
        source_file_id: UUID,
        cursor_id: UUID | None,
    ) -> None:
        await connection.execute(
            text(
                """
                CREATE TEMP TABLE makolet_catalog_subjects ON COMMIT DROP AS
                WITH selected AS MATERIALIZED (
                    SELECT item.id
                      FROM retailer_items item
                      LEFT JOIN confirmed_product_matches confirmed
                        ON confirmed.retailer_item_id = item.id
                     WHERE item.last_source_file_id = :source_file_id
                       AND confirmed.id IS NULL
                       AND (
                            CAST(:cursor_id AS uuid) IS NULL
                            OR item.id > CAST(:cursor_id AS uuid)
                       )
                     ORDER BY item.id
                     LIMIT :selection_limit
                       FOR UPDATE OF item
                )
                SELECT row_number() OVER (ORDER BY item.id) AS sequence,
                       item.id AS retailer_item_id, item.retailer_id,
                       item.portal_id,
                       item.source_item_code, item.gtin, item.name,
                       item.name_search, item.manufacturer_name,
                       item.quantity, item.unit_of_measure,
                       item.unit_quantity, item.quantity_in_package,
                       NULL::text AS current_match_method,
                       uuidv7() AS isolated_product_id
                  FROM selected
                  JOIN retailer_items item ON item.id = selected.id
                 ORDER BY item.id
                """
            ),
            {
                "source_file_id": source_file_id,
                "cursor_id": cursor_id,
                "selection_limit": _BOOTSTRAP_BATCH_SIZE + 1,
            },
        )
        await connection.execute(
            text(
                """
                CREATE UNIQUE INDEX makolet_catalog_subjects_item_idx
                    ON makolet_catalog_subjects (retailer_item_id)
                """
            )
        )
        await connection.execute(text("ANALYZE makolet_catalog_subjects"))

    async def _subject_rows(
        self,
        connection: AsyncConnection,
        *,
        item_limit: int,
    ) -> tuple[dict[str, object], ...]:
        values = (
            await connection.execute(
                text(
                    """
                    SELECT retailer_item_id
                      FROM makolet_catalog_subjects
                     WHERE sequence <= :item_limit
                     ORDER BY retailer_item_id
                    """
                ),
                {"item_limit": item_limit},
            )
        ).mappings()
        return tuple(dict(value) for value in values)

    async def _has_more_subjects(
        self,
        connection: AsyncConnection,
        *,
        item_limit: int,
    ) -> bool:
        return bool(
            await connection.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM makolet_catalog_subjects
                         WHERE sequence > :item_limit
                    )
                    """
                ),
                {"item_limit": item_limit},
            )
        )

    async def _bootstrap_isolated_products(
        self,
        connection: AsyncConnection,
        item_limit: int,
    ) -> int:
        await connection.execute(
            text(
                """
                INSERT INTO canonical_products (
                    id, name, brand, manufacturer, quantity,
                    unit_of_measure, status, created_at, updated_at
                )
                SELECT isolated_product_id, name, NULL, manufacturer_name,
                       quantity, unit_of_measure, 'active',
                       clock_timestamp(), clock_timestamp()
                  FROM makolet_catalog_subjects
                 WHERE sequence <= :item_limit
                   AND current_match_method IS NULL
                 ORDER BY retailer_item_id
                """
            ),
            {"item_limit": item_limit},
        )
        inserted = (
            await connection.execute(
                text(
                    """
                    INSERT INTO confirmed_product_matches (
                        id, retailer_item_id, canonical_product_id,
                        method, evidence, confirmed_at, confirmed_by
                    )
                    SELECT uuidv7(), retailer_item_id, isolated_product_id,
                           :isolated_method,
                           jsonb_build_object(
                               'scope', 'single_retailer_item',
                               'retailer_id', retailer_id,
                               'portal_id', portal_id,
                               'source_item_code', source_item_code,
                               'reason', 'queryable representation without cross-item merge'
                           ),
                           clock_timestamp(), :isolated_actor
                      FROM makolet_catalog_subjects
                     WHERE sequence <= :item_limit
                       AND current_match_method IS NULL
                     ORDER BY retailer_item_id
                    ON CONFLICT (retailer_item_id) DO NOTHING
                    RETURNING id
                    """
                ),
                {
                    "item_limit": item_limit,
                    "isolated_method": _ISOLATED_METHOD,
                    "isolated_actor": _ISOLATED_ACTOR,
                },
            )
        ).all()
        await connection.execute(
            text(
                """
                INSERT INTO product_identifiers (
                    id, product_id, kind, value, normalized_value,
                    issuer_retailer_id, issuer_portal_id,
                    is_validated, validation_method,
                    validation_evidence, created_at
                )
                SELECT uuidv7(), subject.isolated_product_id, 'retailer_item',
                       subject.source_item_code, subject.source_item_code,
                       subject.retailer_id, subject.portal_id,
                       true, :isolated_method,
                       jsonb_build_object(
                           'scope', 'retailer',
                           'retailer_item_id', subject.retailer_item_id,
                           'portal_id', subject.portal_id
                       ),
                       clock_timestamp()
                  FROM makolet_catalog_subjects subject
                  JOIN confirmed_product_matches confirmed
                    ON confirmed.retailer_item_id = subject.retailer_item_id
                   AND confirmed.canonical_product_id = subject.isolated_product_id
                   AND confirmed.method = :isolated_method
                 WHERE subject.sequence <= :item_limit
                ON CONFLICT (
                    kind, normalized_value, issuer_retailer_id, issuer_portal_id
                ) DO NOTHING
                """
            ),
            {"item_limit": item_limit, "isolated_method": _ISOLATED_METHOD},
        )
        await connection.execute(
            text(
                """
                DELETE FROM canonical_products product
                 USING makolet_catalog_subjects subject
                 WHERE subject.sequence <= :item_limit
                   AND subject.current_match_method IS NULL
                   AND product.id = subject.isolated_product_id
                   AND NOT EXISTS (
                       SELECT 1 FROM confirmed_product_matches confirmed
                        WHERE confirmed.canonical_product_id = product.id
                   )
                """
            ),
            {"item_limit": item_limit},
        )
        return len(inserted)

    async def _candidate_block_rows(
        self,
        connection: AsyncConnection,
        *,
        item_limit: int,
        candidate_limit: int,
    ) -> tuple[dict[str, Any], ...]:
        values = (
            await connection.execute(
                text(
                    """
                    WITH subjects AS MATERIALIZED (
                        SELECT subject.*
                          FROM makolet_catalog_subjects subject
                          JOIN confirmed_product_matches confirmed
                            ON confirmed.retailer_item_id = subject.retailer_item_id
                           AND confirmed.canonical_product_id = subject.isolated_product_id
                           AND confirmed.method = :isolated_method
                         WHERE subject.sequence <= :item_limit
                    ),
                    exact_candidates AS (
                        SELECT subject.retailer_item_id,
                               exact.product_id AS canonical_product_id,
                               0 AS block_priority
                          FROM subjects subject
                          CROSS JOIN LATERAL (
                              SELECT DISTINCT identifier.product_id
                                FROM product_identifiers identifier
                                JOIN canonical_products product
                                  ON product.id = identifier.product_id
                                 AND product.status = 'active'
                               WHERE (
                                   (
                                       identifier.kind = 'retailer_item'
                                       AND identifier.normalized_value =
                                           subject.source_item_code
                                   ) OR (
                                       subject.gtin IS NOT NULL
                                       AND identifier.kind = 'gtin'
                                       AND identifier.normalized_value = subject.gtin
                                   )
                               )
                                 AND identifier.product_id <>
                                     subject.isolated_product_id
                               ORDER BY identifier.product_id
                               LIMIT :candidate_limit
                          ) exact
                    ),
                    nearest_candidates AS (
                        SELECT subject.retailer_item_id,
                               nearest.id AS canonical_product_id,
                               1 AS block_priority
                          FROM subjects subject
                          CROSS JOIN LATERAL (
                              SELECT product.id
                                FROM canonical_products product
                               WHERE product.status = 'active'
                                 AND product.id <> subject.isolated_product_id
                               ORDER BY product.name_search <-> subject.name_search,
                                        product.id
                               LIMIT :candidate_limit
                          ) nearest
                    ),
                    blocked AS MATERIALIZED (
                        SELECT retailer_item_id, canonical_product_id,
                               min(block_priority) AS block_priority
                          FROM (
                              SELECT * FROM exact_candidates
                              UNION ALL
                              SELECT * FROM nearest_candidates
                          ) candidates
                         GROUP BY retailer_item_id, canonical_product_id
                    ),
                    bounded AS MATERIALIZED (
                        SELECT blocked.*,
                               row_number() OVER (
                                   PARTITION BY blocked.retailer_item_id
                                   ORDER BY blocked.block_priority,
                                            blocked.canonical_product_id
                               ) AS candidate_sequence
                          FROM blocked
                    )
                    SELECT subject.retailer_item_id, subject.retailer_id,
                           subject.portal_id,
                           subject.source_item_code, subject.gtin AS subject_gtin,
                           subject.name AS subject_name,
                           subject.manufacturer_name AS subject_manufacturer,
                           subject.quantity AS subject_quantity,
                           subject.unit_of_measure AS subject_unit,
                           subject.unit_quantity AS subject_unit_quantity,
                           subject.quantity_in_package AS subject_package_quantity,
                           product.id AS canonical_product_id,
                           product.name AS canonical_name,
                           product.brand AS canonical_brand,
                           product.manufacturer AS canonical_manufacturer,
                           product.quantity AS canonical_quantity,
                           product.unit_of_measure AS canonical_unit,
                           representative.unit_quantity AS canonical_unit_quantity,
                           representative.quantity_in_package
                               AS canonical_package_quantity,
                           COALESCE(identifiers.values, '[]'::jsonb)
                               AS canonical_identifiers
                      FROM bounded
                      JOIN subjects subject
                        ON subject.retailer_item_id = bounded.retailer_item_id
                      JOIN canonical_products product
                        ON product.id = bounded.canonical_product_id
                       AND product.status = 'active'
                      LEFT JOIN LATERAL (
                          SELECT item.unit_quantity, item.quantity_in_package
                            FROM confirmed_product_matches confirmed
                            JOIN retailer_items item
                              ON item.id = confirmed.retailer_item_id
                           WHERE confirmed.canonical_product_id = product.id
                           ORDER BY item.id
                           LIMIT 1
                      ) representative ON true
                      LEFT JOIN LATERAL (
                          SELECT jsonb_agg(
                                     jsonb_build_object(
                                         'kind', selected.kind,
                                         'value', selected.value,
                                         'issuer_retailer_id',
                                             selected.issuer_retailer_id,
                                         'issuer_portal_id',
                                             selected.issuer_portal_id,
                                         'is_validated', selected.is_validated,
                                         'validation_method',
                                             selected.validation_method
                                     ) ORDER BY selected.kind,
                                                selected.normalized_value,
                                                selected.id
                                 ) AS values
                            FROM (
                                SELECT identifier.id, identifier.kind,
                                       identifier.value,
                                       identifier.normalized_value,
                                       identifier.issuer_retailer_id,
                                       identifier.issuer_portal_id,
                                       identifier.is_validated,
                                       identifier.validation_method
                                  FROM product_identifiers identifier
                                 WHERE identifier.product_id = product.id
                                 ORDER BY identifier.kind,
                                          identifier.normalized_value,
                                          identifier.id
                                 LIMIT 32
                            ) selected
                      ) identifiers ON true
                     WHERE bounded.candidate_sequence <= :candidate_limit
                     ORDER BY subject.retailer_item_id,
                              bounded.block_priority,
                              product.id
                    """
                ),
                {
                    "isolated_method": _ISOLATED_METHOD,
                    "item_limit": item_limit,
                    "candidate_limit": candidate_limit,
                },
            )
        ).mappings()
        return tuple(dict(value) for value in values)

    async def _write_candidates(
        self,
        connection: AsyncConnection,
        proposals: tuple[dict[str, object], ...],
    ) -> int:
        if not proposals:
            return 0
        result = await connection.execute(
            text(
                """
                WITH proposed AS MATERIALIZED (
                    SELECT CAST(value->>'retailer_item_id' AS uuid)
                               AS retailer_item_id,
                           CAST(value->>'canonical_product_id' AS uuid)
                               AS canonical_product_id,
                           value->>'method' AS method,
                           CAST(value->>'score' AS numeric) AS score,
                           value->'evidence' AS evidence
                      FROM jsonb_array_elements(CAST(:proposals AS jsonb)) value
                )
                INSERT INTO product_match_candidates (
                    id, retailer_item_id, canonical_product_id, method,
                    score, status, evidence, created_at
                )
                SELECT uuidv7(), retailer_item_id, canonical_product_id,
                       method, score, 'pending', evidence, clock_timestamp()
                  FROM proposed
                 ORDER BY retailer_item_id, canonical_product_id, method
                ON CONFLICT (retailer_item_id, canonical_product_id, method)
                DO UPDATE SET score = excluded.score, evidence = excluded.evidence
                      WHERE product_match_candidates.status = 'pending'
                RETURNING id
                """
            ),
            {"proposals": json.dumps(proposals, ensure_ascii=False)},
        )
        return len(result.all())

    async def _rows(
        self,
        statement: str,
        parameters: dict[str, object],
    ) -> tuple[dict[str, object], ...]:
        async with self._engine.connect() as connection:
            values = (await connection.execute(text(statement), parameters)).mappings()
            return tuple(dict(value) for value in values)


def _score_rows(
    rows: tuple[dict[str, Any], ...],
    *,
    threshold: Decimal,
    candidate_limit: int,
) -> tuple[dict[str, object], ...]:
    proposals: list[dict[str, object]] = []
    per_item: dict[UUID, int] = {}
    generated_at = datetime.now(UTC).isoformat()
    for row in rows:
        item_id = UUID(str(row["retailer_item_id"]))
        if per_item.get(item_id, 0) >= candidate_limit:
            continue
        subject_identifiers = [
            ProductIdentifier(
                IdentifierKind.RETAILER_ITEM,
                str(row["source_item_code"]),
                issuer_retailer_id=UUID(str(row["retailer_id"])),
                is_validated=True,
                validation_method=_ISOLATED_METHOD,
            )
        ]
        subject_gtin = row.get("subject_gtin")
        if subject_gtin and is_valid_gtin(str(subject_gtin)):
            subject_identifiers.append(ProductIdentifier(IdentifierKind.GTIN, str(subject_gtin)))
        subject = product_descriptor(
            retailer_item_id=item_id,
            retailer_id=UUID(str(row["retailer_id"])),
            name=str(row["subject_name"]),
            identifiers=tuple(subject_identifiers),
            manufacturer=_optional_string(row.get("subject_manufacturer")),
            quantity=_optional_decimal(row.get("subject_quantity")),
            unit=_optional_string(row.get("subject_unit")),
            packaging=_packaging(
                row.get("subject_unit_quantity"),
                row.get("subject_package_quantity"),
            ),
        )
        candidate_identifiers = _identifiers(row.get("canonical_identifiers"))
        candidate = canonical_product_descriptor(
            canonical_product_id=UUID(str(row["canonical_product_id"])),
            name=str(row["canonical_name"]),
            identifiers=candidate_identifiers,
            brand=_optional_string(row.get("canonical_brand")),
            manufacturer=_optional_string(row.get("canonical_manufacturer")),
            quantity=_optional_decimal(row.get("canonical_quantity")),
            unit=_optional_string(row.get("canonical_unit")),
            packaging=_packaging(
                row.get("canonical_unit_quantity"),
                row.get("canonical_package_quantity"),
            ),
        )
        scored = score_catalog_candidate(subject, candidate, review_threshold=threshold)
        if scored is None:
            continue
        proposals.append(_proposal(row, scored, generated_at=generated_at))
        per_item[item_id] = per_item.get(item_id, 0) + 1
    return tuple(proposals)


def _proposal(
    row: dict[str, Any],
    scored: CanonicalMatchCandidate,
    *,
    generated_at: str,
) -> dict[str, object]:
    method = {
        MatchRule.EXACT_GTIN: "staged_exact_gtin",
        MatchRule.EXACT_NORMALIZED_IDENTIFIER: "normalized_identifier",
        MatchRule.STRUCTURED_CANDIDATE: "structured_catalog",
    }[scored.rule]
    return {
        "retailer_item_id": str(scored.subject_retailer_item_id),
        "canonical_product_id": str(scored.canonical_product_id),
        "method": method,
        "score": str(scored.confidence),
        "evidence": {
            "version": 1,
            "rule": scored.rule.value,
            "disposition": scored.disposition.value,
            "explanations": list(scored.explanations),
            "generated_at": generated_at,
            "subject": {
                "retailer_id": str(row["retailer_id"]),
                "portal_id": str(row["portal_id"]),
                "retailer_item_id": str(row["retailer_item_id"]),
                "source_item_code": str(row["source_item_code"]),
                "name": str(row["subject_name"]),
                "manufacturer": _optional_string(row.get("subject_manufacturer")),
                "quantity": _json_decimal(row.get("subject_quantity")),
                "unit": _optional_string(row.get("subject_unit")),
                "packaging": _packaging(
                    row.get("subject_unit_quantity"),
                    row.get("subject_package_quantity"),
                ),
            },
            "candidate": {
                "canonical_product_id": str(row["canonical_product_id"]),
                "name": str(row["canonical_name"]),
                "brand": _optional_string(row.get("canonical_brand")),
                "manufacturer": _optional_string(row.get("canonical_manufacturer")),
                "quantity": _json_decimal(row.get("canonical_quantity")),
                "unit": _optional_string(row.get("canonical_unit")),
                "packaging": _packaging(
                    row.get("canonical_unit_quantity"),
                    row.get("canonical_package_quantity"),
                ),
            },
        },
    }


async def _locked_candidate(
    connection: AsyncConnection,
    candidate_id: UUID,
) -> dict[str, object]:
    row = (
        (
            await connection.execute(
                text(
                    """
                SELECT candidate.id, candidate.retailer_item_id,
                       candidate.canonical_product_id, candidate.method,
                       candidate.score, candidate.status, candidate.evidence,
                       item.retailer_id, item.portal_id, item.source_item_code
                  FROM product_match_candidates candidate
                  JOIN retailer_items item
                    ON item.id = candidate.retailer_item_id
                 WHERE candidate.id = :candidate_id
                   FOR UPDATE OF candidate, item
                """
                ),
                {"candidate_id": candidate_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise NotFoundError("Catalog match candidate was not found")
    return dict(row)


async def _locked_match(
    connection: AsyncConnection,
    retailer_item_id: object,
) -> dict[str, object] | None:
    row = (
        (
            await connection.execute(
                text(
                    """
                SELECT canonical_product_id, method, confirmed_by
                  FROM confirmed_product_matches
                 WHERE retailer_item_id = :retailer_item_id
                   FOR UPDATE
                """
                ),
                {"retailer_item_id": retailer_item_id},
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row is not None else None


async def _lock_products(
    connection: AsyncConnection,
    product_ids: tuple[UUID, UUID],
) -> dict[UUID, dict[str, object]]:
    rows = (
        await connection.execute(
            text(
                """
                SELECT id, status
                  FROM canonical_products
                 WHERE id = ANY(CAST(:product_ids AS uuid[]))
                 ORDER BY id
                   FOR UPDATE
                """
            ),
            {"product_ids": list(product_ids)},
        )
    ).mappings()
    return {UUID(str(row["id"])): dict(row) for row in rows}


async def _validate_identifier_move(
    connection: AsyncConnection,
    *,
    retailer_id: UUID,
    portal_id: UUID,
    source_item_code: str,
    isolated_product_id: UUID,
    target_product_id: UUID,
) -> None:
    row = (
        await connection.execute(
            text(
                """
                SELECT product_id
                  FROM product_identifiers
                 WHERE kind = 'retailer_item'
                   AND normalized_value = :source_item_code
                   AND issuer_retailer_id = :retailer_id
                   AND issuer_portal_id = :portal_id
                   FOR UPDATE
                """
            ),
            {
                "source_item_code": source_item_code,
                "retailer_id": retailer_id,
                "portal_id": portal_id,
            },
        )
    ).first()
    if row is not None and UUID(str(row.product_id)) not in {
        isolated_product_id,
        target_product_id,
    }:
        raise CatalogMatchConflictError(
            "Retailer-scoped identifier already belongs to another canonical product"
        )


async def _move_retailer_identifier(
    connection: AsyncConnection,
    *,
    retailer_id: UUID,
    portal_id: UUID,
    source_item_code: str,
    isolated_product_id: UUID,
    target_product_id: UUID,
    candidate_id: UUID,
    reviewed_by: str,
) -> None:
    evidence = json.dumps(
        {
            "scope": "retailer",
            "candidate_id": str(candidate_id),
            "reviewed_by": reviewed_by,
            "superseded_isolated_product_id": str(isolated_product_id),
        },
        ensure_ascii=False,
    )
    updated = await connection.execute(
        text(
            """
            UPDATE product_identifiers
               SET product_id = :target_product_id,
                   is_validated = true,
                   validation_method = 'operator_review',
                   validation_evidence = CAST(:evidence AS jsonb)
             WHERE product_id = :isolated_product_id
               AND kind = 'retailer_item'
               AND normalized_value = :source_item_code
               AND issuer_retailer_id = :retailer_id
               AND issuer_portal_id = :portal_id
            """
        ),
        {
            "target_product_id": target_product_id,
            "isolated_product_id": isolated_product_id,
            "source_item_code": source_item_code,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "evidence": evidence,
        },
    )
    if updated.rowcount:
        return
    await connection.execute(
        text(
            """
            INSERT INTO product_identifiers (
                id, product_id, kind, value, normalized_value,
                issuer_retailer_id, issuer_portal_id,
                is_validated, validation_method,
                validation_evidence, created_at
            ) VALUES (
                uuidv7(), :target_product_id, 'retailer_item',
                :source_item_code, :source_item_code, :retailer_id, :portal_id,
                true, 'operator_review', CAST(:evidence AS jsonb),
                clock_timestamp()
            )
            ON CONFLICT (
                kind, normalized_value, issuer_retailer_id, issuer_portal_id
            ) DO NOTHING
            """
        ),
        {
            "target_product_id": target_product_id,
            "source_item_code": source_item_code,
            "retailer_id": retailer_id,
            "portal_id": portal_id,
            "evidence": evidence,
        },
    )


def _require_pending(candidate: dict[str, object]) -> None:
    if candidate["status"] != "pending":
        raise CatalogMatchConflictError("Catalog match candidate is no longer pending")


def _uuid_cursor(value: str | None) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(value)
    except ValueError as error:
        raise ValueError("Catalog cursor is not a valid UUID") from error


def _identifiers(value: object) -> tuple[ProductIdentifier, ...]:
    if not isinstance(value, list):
        return ()
    identifiers: list[ProductIdentifier] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        try:
            kind = IdentifierKind(str(item.get("kind")))
            identifier_value = str(item.get("value"))
            if kind is IdentifierKind.GTIN and not is_valid_gtin(identifier_value):
                continue
            issuer_value = item.get("issuer_retailer_id")
            identifiers.append(
                ProductIdentifier(
                    kind,
                    identifier_value,
                    issuer_retailer_id=(UUID(str(issuer_value)) if issuer_value else None),
                    is_validated=bool(item.get("is_validated")),
                    validation_method=_optional_string(item.get("validation_method")),
                )
            )
        except ValueError, TypeError:
            continue
    return tuple(identifiers)


def _packaging(unit_quantity: object, package_quantity: object) -> str | None:
    components: list[str] = []
    if unit_quantity is not None and str(unit_quantity).strip():
        components.append(str(unit_quantity).strip())
    if package_quantity is not None:
        components.append(f"package {package_quantity}")
    return " | ".join(components) or None


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _json_decimal(value: object) -> str | None:
    selected = _optional_decimal(value)
    return str(selected) if selected is not None else None


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    selected = str(value).strip()
    return selected or None


__all__ = ["PostgresCatalogMatchingRepository"]
