"""Persist bounded source collection traversals and audited attempts.

Revision ID: 0007_collection_traversal
Revises: 0006_portal_scoped_identity
Created: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007_collection_traversal"
down_revision: str | None = "0006_portal_scoped_identity"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE collection_checkpoints (
            id UUID DEFAULT uuidv7() NOT NULL,
            retailer_id UUID NOT NULL,
            portal_ids JSONB NOT NULL,
            portal_generation VARCHAR(64) NOT NULL,
            operation VARCHAR(16) NOT NULL,
            range_since TIMESTAMP WITH TIME ZONE,
            range_until TIMESTAMP WITH TIME ZONE,
            archive_only BOOLEAN DEFAULT false NOT NULL,
            generation BIGINT DEFAULT 1 NOT NULL,
            publisher_cursor TEXT,
            page_offset INTEGER DEFAULT 0 NOT NULL,
            generation_recognized_count BIGINT DEFAULT 0 NOT NULL,
            generation_unknown_count BIGINT DEFAULT 0 NOT NULL,
            traversal_complete BOOLEAN DEFAULT false NOT NULL,
            last_completed_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_collection_checkpoints PRIMARY KEY (id),
            CONSTRAINT fk_collection_checkpoints_retailer_id_retailers
                FOREIGN KEY(retailer_id) REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT ck_collection_checkpoints_operation_value
                CHECK (operation IN ('ordinary', 'backfill')),
            CONSTRAINT ck_collection_checkpoints_portal_ids_array
                CHECK (jsonb_typeof(portal_ids) = 'array'
                       AND jsonb_array_length(portal_ids) BETWEEN 1 AND 64),
            CONSTRAINT ck_collection_checkpoints_portal_generation_format
                CHECK (portal_generation ~ '^[0-9a-f]{64}$'),
            CONSTRAINT ck_collection_checkpoints_range_order
                CHECK (range_since IS NULL OR range_until IS NULL
                       OR range_until >= range_since),
            CONSTRAINT ck_collection_checkpoints_operation_scope
                CHECK ((operation = 'ordinary' AND range_since IS NULL
                        AND range_until IS NULL AND NOT archive_only)
                    OR (operation = 'backfill' AND range_since IS NOT NULL
                        AND range_until IS NOT NULL)),
            CONSTRAINT ck_collection_checkpoints_generation_positive
                CHECK (generation > 0),
            CONSTRAINT ck_collection_checkpoints_page_offset_nonnegative
                CHECK (page_offset >= 0),
            CONSTRAINT ck_collection_checkpoints_publisher_cursor_length
                CHECK (publisher_cursor IS NULL
                       OR octet_length(publisher_cursor) BETWEEN 1 AND 8192),
            CONSTRAINT ck_collection_checkpoints_generation_counts_nonnegative
                CHECK (generation_recognized_count >= 0
                       AND generation_unknown_count >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_collection_checkpoints_scope
            ON collection_checkpoints (
                retailer_id, portal_generation, operation,
                range_since, range_until, archive_only
            ) NULLS NOT DISTINCT
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_checkpoints_portal_ids
            ON collection_checkpoints USING gin (portal_ids)
        """
    )
    op.execute(
        """
        CREATE TABLE collection_attempts (
            id UUID DEFAULT uuidv7() NOT NULL,
            checkpoint_id UUID NOT NULL,
            generation BIGINT NOT NULL,
            status VARCHAR(16) DEFAULT 'running' NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE,
            start_cursor TEXT,
            start_page_offset INTEGER NOT NULL,
            checkpoint_cursor TEXT,
            checkpoint_page_offset INTEGER NOT NULL,
            discovered_count BIGINT DEFAULT 0 NOT NULL,
            processed_count BIGINT DEFAULT 0 NOT NULL,
            duplicate_count BIGINT DEFAULT 0 NOT NULL,
            skipped_unknown_count BIGINT DEFAULT 0 NOT NULL,
            warning_count BIGINT DEFAULT 0 NOT NULL,
            truncated BOOLEAN DEFAULT false NOT NULL,
            error_code VARCHAR(128),
            error_message TEXT,
            CONSTRAINT pk_collection_attempts PRIMARY KEY (id),
            CONSTRAINT fk_collection_attempts_checkpoint_id_collection_checkpoints
                FOREIGN KEY(checkpoint_id) REFERENCES collection_checkpoints (id)
                ON DELETE CASCADE,
            CONSTRAINT ck_collection_attempts_status_value
                CHECK (status IN ('running', 'completed', 'bounded', 'failed')),
            CONSTRAINT ck_collection_attempts_generation_positive
                CHECK (generation > 0),
            CONSTRAINT ck_collection_attempts_page_offsets_nonnegative
                CHECK (start_page_offset >= 0 AND checkpoint_page_offset >= 0),
            CONSTRAINT ck_collection_attempts_start_cursor_length
                CHECK (start_cursor IS NULL
                       OR octet_length(start_cursor) BETWEEN 1 AND 8192),
            CONSTRAINT ck_collection_attempts_checkpoint_cursor_length
                CHECK (checkpoint_cursor IS NULL
                       OR octet_length(checkpoint_cursor) BETWEEN 1 AND 8192),
            CONSTRAINT ck_collection_attempts_counts_nonnegative
                CHECK (discovered_count >= 0 AND processed_count >= 0
                       AND duplicate_count >= 0 AND skipped_unknown_count >= 0
                       AND warning_count >= 0),
            CONSTRAINT ck_collection_attempts_finish_state
                CHECK ((status = 'running' AND finished_at IS NULL)
                    OR (status <> 'running' AND finished_at IS NOT NULL)),
            CONSTRAINT ck_collection_attempts_attempt_time_order
                CHECK (finished_at IS NULL OR finished_at >= started_at)
        )
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_collection_attempts_running_checkpoint
            ON collection_attempts (checkpoint_id)
            WHERE status = 'running'
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_attempts_checkpoint_started
            ON collection_attempts (checkpoint_id, started_at DESC, id DESC)
        """
    )


def downgrade() -> None:
    op.drop_table("collection_attempts")
    op.drop_table("collection_checkpoints")
