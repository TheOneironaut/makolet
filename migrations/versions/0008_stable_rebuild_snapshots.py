"""Preserve stable identities and temporal rows across normalized rebuilds.

Revision ID: 0008_stable_rebuild_snapshots
Revises: 0007_collection_traversal
Created: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0008_stable_rebuild_snapshots"
down_revision: str | None = "0007_collection_traversal"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ENTITIES = (
    "stores",
    "store_aliases",
    "retailer_items",
    "canonical_products",
    "product_identifiers",
    "identifier_match_groups",
    "retailer_identifier_assertions",
    "product_match_candidates",
    "confirmed_product_matches",
    "current_prices",
    "price_history",
    "current_availability",
    "availability_history",
    "promotions",
    "promotion_items",
    "promotion_stores",
    "promotion_clubs",
    "applied_source_contents",
    "source_scope_watermarks",
)


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE normalized_rebuild_files
            ADD COLUMN original_parser_version VARCHAR(128),
            ADD CONSTRAINT ck_normalized_rebuild_files_original_parser_version_length
                CHECK (original_parser_version IS NULL
                       OR length(original_parser_version) BETWEEN 1 AND 128)
        """
    )
    entity_values = ", ".join(f"'{entity}'" for entity in _ENTITIES)
    op.execute(
        f"""
        CREATE TABLE normalized_rebuild_snapshots (
            rebuild_run_id UUID NOT NULL,
            phase VARCHAR(16) NOT NULL,
            entity VARCHAR(64) NOT NULL,
            row_key VARCHAR(512) NOT NULL,
            payload JSONB NOT NULL,
            outcome VARCHAR(16),
            captured_at TIMESTAMP WITH TIME ZONE
                DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_normalized_rebuild_snapshots
                PRIMARY KEY (rebuild_run_id, phase, entity, row_key),
            CONSTRAINT fk_normalized_rebuild_snapshots_run
                FOREIGN KEY (rebuild_run_id)
                REFERENCES normalized_rebuild_runs (id) ON DELETE CASCADE,
            CONSTRAINT ck_normalized_rebuild_snapshots_phase_value
                CHECK (phase IN ('original', 'rebuilt')),
            CONSTRAINT ck_normalized_rebuild_snapshots_entity_value
                CHECK (entity IN ({entity_values})),
            CONSTRAINT ck_normalized_rebuild_snapshots_outcome_value
                CHECK (outcome IN ('preserved', 'superseded')),
            CONSTRAINT ck_normalized_rebuild_snapshots_row_key_length
                CHECK (octet_length(row_key) BETWEEN 1 AND 512),
            CONSTRAINT ck_normalized_rebuild_snapshots_payload_object
                CHECK (jsonb_typeof(payload) = 'object'),
            CONSTRAINT ck_normalized_rebuild_snapshots_payload_size
                CHECK (octet_length(payload::text) <= 1048576)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_normalized_rebuild_snapshots_run_entity
            ON normalized_rebuild_snapshots (rebuild_run_id, entity)
        """
    )


def downgrade() -> None:
    connection = op.get_bind()
    retained = connection.execute(
        text("SELECT EXISTS (SELECT 1 FROM normalized_rebuild_snapshots)")
    ).scalar_one()
    if retained:
        raise RuntimeError(
            "Refusing to drop retained normalized-rebuild audit snapshots; "
            "preserve or explicitly retire their rebuild runs first"
        )
    op.drop_table("normalized_rebuild_snapshots")
    op.execute(
        """
        ALTER TABLE normalized_rebuild_files
            DROP CONSTRAINT ck_normalized_rebuild_files_original_parser_version_length,
            DROP COLUMN original_parser_version
        """
    )
