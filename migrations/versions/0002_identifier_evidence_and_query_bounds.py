"""Separate retailer identifier evidence from globally validated identity.

Revision ID: 0002_identifier_evidence
Revises: 0001_initial
Created: 2026-08-12

The backfill is set based. Existing checksum-only GTINs become retailer-scoped
assertions. A global identifier remains validated only when at least two independent
retailers currently corroborate it.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002_identifier_evidence"
down_revision: str | None = "0001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE product_identifiers
            ADD COLUMN validation_method VARCHAR(64),
            ADD COLUMN validation_evidence JSONB NOT NULL DEFAULT '{}'::jsonb
        """
    )
    op.execute(
        """
        CREATE TABLE identifier_match_groups (
            id UUID DEFAULT uuidv7() NOT NULL,
            kind VARCHAR(32) NOT NULL,
            normalized_value VARCHAR(128) NOT NULL,
            canonical_product_id UUID NOT NULL,
            created_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_identifier_match_groups PRIMARY KEY (id),
            CONSTRAINT uq_identifier_match_groups_kind_normalized_value
                UNIQUE (kind, normalized_value),
            CONSTRAINT ck_identifier_match_groups_kind_value
                CHECK (kind IN ('gtin', 'retailer_item', 'manufacturer', 'unknown')),
            CONSTRAINT ck_identifier_match_groups_normalized_value_length
                CHECK (length(normalized_value) BETWEEN 1 AND 128),
            CONSTRAINT fk_identifier_match_groups_canonical_product_id_canonical_products
                FOREIGN KEY (canonical_product_id)
                REFERENCES canonical_products (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        """
        CREATE TABLE retailer_identifier_assertions (
            id UUID DEFAULT uuidv7() NOT NULL,
            retailer_item_id UUID NOT NULL,
            kind VARCHAR(32) NOT NULL,
            value VARCHAR(128) NOT NULL,
            normalized_value VARCHAR(128) NOT NULL,
            source_file_id UUID NOT NULL,
            validation_method VARCHAR(64) NOT NULL,
            asserted_at TIMESTAMPTZ NOT NULL,
            superseded_at TIMESTAMPTZ,
            CONSTRAINT pk_retailer_identifier_assertions PRIMARY KEY (id),
            CONSTRAINT uq_retailer_identifier_assertion_item_kind_source
                UNIQUE (retailer_item_id, kind, source_file_id),
            CONSTRAINT ck_retailer_identifier_assertions_kind_value
                CHECK (kind IN ('gtin', 'retailer_item', 'manufacturer', 'unknown')),
            CONSTRAINT ck_retailer_identifier_assertions_value_length
                CHECK (length(value) BETWEEN 1 AND 128),
            CONSTRAINT ck_retailer_identifier_assertions_normalized_value_length
                CHECK (length(normalized_value) BETWEEN 1 AND 128),
            CONSTRAINT ck_retailer_identifier_assertions_superseded_after_assertion
                CHECK (superseded_at IS NULL OR superseded_at >= asserted_at),
            CONSTRAINT fk_retailer_identifier_assertions_retailer_item_id_retailer_items
                FOREIGN KEY (retailer_item_id)
                REFERENCES retailer_items (id) ON DELETE CASCADE,
            CONSTRAINT fk_retailer_identifier_assertions_source_file_id_source_files
                FOREIGN KEY (source_file_id)
                REFERENCES source_files (id) ON DELETE RESTRICT
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_identifier_match_groups_product
            ON identifier_match_groups (canonical_product_id)
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_retailer_identifier_assertions_current
            ON retailer_identifier_assertions (retailer_item_id, kind)
            WHERE superseded_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_retailer_identifier_assertions_active_value
            ON retailer_identifier_assertions
                (kind, normalized_value, retailer_item_id)
            WHERE superseded_at IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_retailer_identifier_assertions_source
            ON retailer_identifier_assertions (source_file_id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_source_files_portal_latest
            ON source_files (portal_id, discovered_at DESC, id DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_canonical_products_active_name_prefix
            ON canonical_products (name_search text_pattern_ops, id)
            WHERE status = 'active'
        """
    )
    op.execute(
        """
        CREATE INDEX ix_canonical_products_active_name_trgm_gist
            ON canonical_products USING gist (name_search gist_trgm_ops)
            WHERE status = 'active'
        """
    )

    op.execute(
        """
        INSERT INTO identifier_match_groups (
            id, kind, normalized_value, canonical_product_id, created_at, updated_at
        )
        SELECT uuidv7(), identifier.kind, identifier.normalized_value,
               min(identifier.product_id::text)::uuid,
               min(identifier.created_at), clock_timestamp()
          FROM product_identifiers identifier
         WHERE identifier.kind = 'gtin'
           AND identifier.issuer_retailer_id IS NULL
         GROUP BY identifier.kind, identifier.normalized_value
        ON CONFLICT (kind, normalized_value) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO identifier_match_groups (
            id, kind, normalized_value, canonical_product_id, created_at, updated_at
        )
        SELECT uuidv7(), 'gtin', item.gtin,
               min(match.canonical_product_id::text)::uuid,
               min(item.first_seen_at), clock_timestamp()
          FROM retailer_items item
          JOIN confirmed_product_matches match
            ON match.retailer_item_id = item.id
         WHERE item.gtin IS NOT NULL
         GROUP BY item.gtin
        ON CONFLICT (kind, normalized_value) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO retailer_identifier_assertions (
            id, retailer_item_id, kind, value, normalized_value,
            source_file_id, validation_method, asserted_at, superseded_at
        )
        SELECT uuidv7(), item.id, 'gtin', item.gtin, item.gtin,
               item.last_source_file_id, 'gtin_checksum', item.last_seen_at, NULL
          FROM retailer_items item
         WHERE item.gtin IS NOT NULL
        ON CONFLICT (retailer_item_id, kind, source_file_id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO product_identifiers (
            id, product_id, kind, value, normalized_value,
            issuer_retailer_id, is_validated, validation_method,
            validation_evidence, created_at
        )
        SELECT uuidv7(), group_row.canonical_product_id, 'gtin', item.gtin,
               item.gtin, item.retailer_id, false, 'retailer_assertion',
               jsonb_build_object(
                   'scope', 'retailer',
                   'retailer_id', item.retailer_id,
                   'source_file_id', max(item.last_source_file_id::text)
               ),
               min(item.first_seen_at)
          FROM retailer_items item
          JOIN identifier_match_groups group_row
            ON group_row.kind = 'gtin'
           AND group_row.normalized_value = item.gtin
         WHERE item.gtin IS NOT NULL
         GROUP BY group_row.canonical_product_id, item.retailer_id, item.gtin
        ON CONFLICT (kind, normalized_value, issuer_retailer_id) DO NOTHING
        """
    )
    op.execute(
        """
        WITH corroboration AS (
            SELECT assertion.normalized_value,
                   count(DISTINCT item.retailer_id) AS retailer_count
              FROM retailer_identifier_assertions assertion
              JOIN retailer_items item ON item.id = assertion.retailer_item_id
             WHERE assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
             GROUP BY assertion.normalized_value
        )
        UPDATE product_identifiers identifier
           SET is_validated = corroboration.retailer_count >= 2,
               validation_method = CASE
                   WHEN corroboration.retailer_count >= 2
                   THEN 'independent_retailer_corroboration'
                   ELSE 'legacy_retailer_assertion'
               END,
               validation_evidence = jsonb_build_object(
                   'retailer_count', corroboration.retailer_count,
                   'migrated_from', 'checksum_only_global_identifier'
               )
          FROM corroboration
         WHERE identifier.kind = 'gtin'
           AND identifier.issuer_retailer_id IS NULL
           AND identifier.normalized_value = corroboration.normalized_value
        """
    )
    op.execute(
        """
        DELETE FROM product_identifiers identifier
         WHERE identifier.kind = 'gtin'
           AND identifier.issuer_retailer_id IS NULL
           AND NOT identifier.is_validated
        """
    )
    op.execute(
        """
        WITH corroboration AS (
            SELECT assertion.retailer_item_id,
                   count(DISTINCT peer_item.retailer_id) AS retailer_count
              FROM retailer_identifier_assertions assertion
              JOIN retailer_identifier_assertions peer
                ON peer.kind = assertion.kind
               AND peer.normalized_value = assertion.normalized_value
               AND peer.superseded_at IS NULL
              JOIN retailer_items peer_item ON peer_item.id = peer.retailer_item_id
             WHERE assertion.kind = 'gtin'
               AND assertion.superseded_at IS NULL
             GROUP BY assertion.retailer_item_id
        )
        UPDATE confirmed_product_matches match
           SET method = CASE
                   WHEN corroboration.retailer_count >= 2
                   THEN 'exact_validated_gtin'
                   ELSE 'exact_provisional_gtin'
               END,
               evidence = match.evidence || jsonb_build_object(
                   'evidence_scope', 'retailer_assertion',
                   'retailer_count', corroboration.retailer_count,
                   'migrated_from', 'checksum_only_exact_gtin'
               ),
               confirmed_by = 'system:exact-gtin-evidence'
          FROM corroboration
         WHERE match.retailer_item_id = corroboration.retailer_item_id
           AND match.method = 'exact_gtin'
           AND match.confirmed_by = 'system:exact-gtin'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        INSERT INTO product_identifiers (
            id, product_id, kind, value, normalized_value,
            issuer_retailer_id, is_validated, created_at
        )
        SELECT uuidv7(), group_row.canonical_product_id, group_row.kind,
               group_row.normalized_value, group_row.normalized_value,
               NULL, true, min(group_row.created_at)
          FROM identifier_match_groups group_row
         WHERE group_row.kind = 'gtin'
         GROUP BY group_row.canonical_product_id, group_row.kind,
                  group_row.normalized_value
        ON CONFLICT (kind, normalized_value, issuer_retailer_id) DO NOTHING
        """
    )
    op.execute(
        """
        UPDATE confirmed_product_matches
           SET method = 'exact_gtin',
               confirmed_by = 'system:exact-gtin'
         WHERE method IN ('exact_provisional_gtin', 'exact_validated_gtin')
           AND confirmed_by = 'system:exact-gtin-evidence'
        """
    )
    op.execute("DROP INDEX ix_canonical_products_active_name_trgm_gist")
    op.execute("DROP INDEX ix_canonical_products_active_name_prefix")
    op.execute("DROP INDEX ix_source_files_portal_latest")
    op.execute("DROP TABLE retailer_identifier_assertions")
    op.execute("DROP TABLE identifier_match_groups")
    op.execute(
        """
        DELETE FROM product_identifiers
         WHERE issuer_retailer_id IS NOT NULL
           AND validation_method = 'retailer_assertion'
        """
    )
    op.execute(
        """
        ALTER TABLE product_identifiers
            DROP COLUMN validation_evidence,
            DROP COLUMN validation_method
        """
    )
