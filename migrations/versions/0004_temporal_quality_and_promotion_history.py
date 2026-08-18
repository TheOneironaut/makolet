"""Add source-scope ordering watermarks and promotion-history access support.

Revision ID: 0004_temporal_quality
Revises: 0003_normalized_rebuilds
Created: 2026-08-12

Watermarks fail closed before an older or conflicting source can mutate normalized
current state.  They are raw-derived coordination state and are rebuilt from the
archive during an explicit normalized rebuild.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_temporal_quality"
down_revision: str | None = "0003_normalized_rebuilds"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE normalized_rebuild_files
            ALTER COLUMN original_applied_at DROP NOT NULL,
            ADD COLUMN effective_source_timestamp TIMESTAMPTZ
        """
    )
    op.execute(
        """
        UPDATE normalized_rebuild_files rebuild_file
           SET effective_source_timestamp = COALESCE(
                   source.source_timestamp,
                   rebuild_file.archived_at
               )
          FROM source_files source
         WHERE source.id = rebuild_file.source_file_id
        """
    )
    op.execute(
        """
        ALTER TABLE normalized_rebuild_files
            ALTER COLUMN effective_source_timestamp SET NOT NULL
        """
    )
    op.execute(
        """
        CREATE TABLE source_scope_watermarks (
            id UUID DEFAULT uuidv7() NOT NULL,
            retailer_id UUID NOT NULL,
            document_family VARCHAR(16) NOT NULL,
            subchain_code VARCHAR(128) DEFAULT '' NOT NULL,
            source_scope_code VARCHAR(128) DEFAULT '' NOT NULL,
            effective_source_timestamp TIMESTAMPTZ NOT NULL,
            source_content_sha256 VARCHAR(64) NOT NULL,
            source_file_id UUID NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_source_scope_watermarks PRIMARY KEY (id),
            CONSTRAINT uq_source_scope_watermarks_retailer_family_scope
                UNIQUE (
                    retailer_id, document_family,
                    subchain_code, source_scope_code
                ),
            CONSTRAINT ck_source_scope_watermarks_document_family_value
                CHECK (document_family IN ('stores', 'prices', 'promotions')),
            CONSTRAINT ck_source_scope_watermarks_content_sha256_format
                CHECK (source_content_sha256 ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_source_scope_watermarks_subchain_code_length
                CHECK (length(subchain_code) <= 128),
            CONSTRAINT ck_source_scope_watermarks_source_scope_code_length
                CHECK (length(source_scope_code) <= 128),
            CONSTRAINT fk_source_scope_watermarks_retailer_id_retailers
                FOREIGN KEY (retailer_id)
                REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT fk_source_scope_watermarks_source_file_id_source_files
                FOREIGN KEY (source_file_id)
                REFERENCES source_files (id) ON DELETE RESTRICT
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_source_scope_watermarks_source_file
            ON source_scope_watermarks (source_file_id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_promotions_history_from_id
            ON promotions (valid_from DESC, id)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX ix_promotions_history_from_id")
    op.execute("DROP TABLE source_scope_watermarks")
    op.execute(
        """
        DELETE FROM normalized_rebuild_files
         WHERE original_applied_at IS NULL
        """
    )
    op.execute(
        """
        ALTER TABLE normalized_rebuild_files
            DROP COLUMN effective_source_timestamp,
            ALTER COLUMN original_applied_at SET NOT NULL
        """
    )
