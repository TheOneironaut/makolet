"""Persist bounded per-run and per-day collection charged-byte accounting.

Revision ID: 0009_collection_charge_budgets
Revises: 0008_stable_rebuild_snapshots
Created: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0009_collection_charge_budgets"
down_revision: str | None = "0008_stable_rebuild_snapshots"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE collection_attempts
            ADD COLUMN charged_bytes BIGINT,
            ADD COLUMN truncation_reason VARCHAR(64)
        """
    )
    op.execute(
        """
        ALTER TABLE collection_attempts
            ALTER COLUMN charged_bytes SET DEFAULT 0
        """
    )
    op.execute(
        """
        UPDATE collection_attempts
           SET truncation_reason = 'legacy_limit'
         WHERE truncated
        """
    )
    op.execute(
        """
        ALTER TABLE collection_attempts
            DROP CONSTRAINT ck_collection_attempts_counts_nonnegative,
            ADD CONSTRAINT ck_collection_attempts_counts_nonnegative
                CHECK (discovered_count >= 0 AND processed_count >= 0
                       AND duplicate_count >= 0 AND skipped_unknown_count >= 0
                       AND warning_count >= 0
                       AND (charged_bytes IS NULL OR charged_bytes >= 0)),
            ADD CONSTRAINT ck_collection_attempts_truncation_reason_value
                CHECK (truncation_reason IS NULL OR truncation_reason IN
                       ('file_limit', 'discovery_limit', 'charged_byte_run_limit',
                        'charged_byte_day_limit', 'legacy_limit')),
            ADD CONSTRAINT ck_collection_attempts_truncation_reason_state
                CHECK ((truncated AND truncation_reason IS NOT NULL)
                    OR (NOT truncated AND truncation_reason IS NULL))
        """
    )
    op.execute(
        """
        CREATE TABLE collection_charge_budgets (
            retailer_id UUID NOT NULL,
            window_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
            charged_bytes BIGINT DEFAULT 0 NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_collection_charge_budgets PRIMARY KEY (retailer_id),
            CONSTRAINT fk_collection_charge_budgets_retailer_id_retailers
                FOREIGN KEY(retailer_id) REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT ck_collection_charge_budgets_charged_bytes_nonnegative
                CHECK (charged_bytes >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE TABLE collection_archive_charges (
            source_file_id UUID NOT NULL,
            retailer_id UUID NOT NULL,
            attempt_id UUID,
            content_length BIGINT NOT NULL,
            charged_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_collection_archive_charges PRIMARY KEY (source_file_id),
            CONSTRAINT fk_collection_archive_charges_source_file_id_source_files
                FOREIGN KEY(source_file_id) REFERENCES source_files (id) ON DELETE RESTRICT,
            CONSTRAINT fk_collection_archive_charges_retailer_id_retailers
                FOREIGN KEY(retailer_id) REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT fk_collection_archive_charges_attempt_id_collection_attempts
                FOREIGN KEY(attempt_id) REFERENCES collection_attempts (id) ON DELETE SET NULL,
            CONSTRAINT ck_collection_archive_charges_content_length_nonnegative
                CHECK (content_length >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_archive_charges_retailer_charged
            ON collection_archive_charges
                (retailer_id, charged_at, source_file_id)
        """
    )
    op.execute(
        """
        CREATE TABLE collection_transfer_charges (
            attempt_id UUID NOT NULL,
            source_file_id UUID NOT NULL,
            retailer_id UUID NOT NULL,
            content_length BIGINT NOT NULL,
            charged_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            settled BOOLEAN DEFAULT false NOT NULL,
            archive_attached BOOLEAN DEFAULT false NOT NULL,
            CONSTRAINT pk_collection_transfer_charges
                PRIMARY KEY (attempt_id, source_file_id),
            CONSTRAINT fk_collection_transfer_charges_attempt_id_collection_attempts
                FOREIGN KEY(attempt_id) REFERENCES collection_attempts (id) ON DELETE RESTRICT,
            CONSTRAINT fk_collection_transfer_charges_source_file_id_source_files
                FOREIGN KEY(source_file_id) REFERENCES source_files (id) ON DELETE RESTRICT,
            CONSTRAINT fk_collection_transfer_charges_retailer_id_retailers
                FOREIGN KEY(retailer_id) REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT ck_collection_transfer_charges_content_length_nonnegative
                CHECK (content_length >= 0)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_availability_store_latest
            ON current_availability
                (store_id, last_observed_at DESC, source_file_id DESC, id DESC)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_raw_archive_objects_archived_id
            ON raw_archive_objects (archived_at, id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_transfer_charges_retailer_charged
            ON collection_transfer_charges
                (retailer_id, charged_at, source_file_id)
        """
    )
    # Capture one trailing-window candidate set. The 100,001-row sentinel makes the
    # transform fail closed before changing durable charge state when an unusually
    # busy installation requires an operator-planned successor migration.
    op.execute(
        """
        CREATE TEMPORARY TABLE makolet_0009_archive_charge_candidates
        ON COMMIT DROP AS
        WITH migration_cutoff AS MATERIALIZED (
            SELECT clock_timestamp() AS captured_at
        ), download_candidates AS MATERIALIZED (
            SELECT bounded.source_file_id,
                   bounded.retailer_id,
                   bounded.content_length,
                   bounded.charged_at
              FROM migration_cutoff cutoff
              JOIN LATERAL (
                    SELECT source.id AS source_file_id,
                           source.retailer_id,
                           archive.content_length,
                           source.download_finished_at AS charged_at
                      FROM source_files source
                      JOIN raw_archive_objects archive
                        ON archive.id = source.raw_archive_object_id
                     WHERE source.download_finished_at
                               > cutoff.captured_at - INTERVAL '24 hours'
                       AND source.download_finished_at <= cutoff.captured_at
                       AND source.raw_archive_object_id IS NOT NULL
                     ORDER BY source.download_finished_at, source.id
                     LIMIT 100001
              ) bounded ON true
        ), archive_fallback_candidates AS MATERIALIZED (
            SELECT bounded.source_file_id,
                   bounded.retailer_id,
                   bounded.content_length,
                   bounded.charged_at
              FROM migration_cutoff cutoff
              JOIN LATERAL (
                    SELECT source.id AS source_file_id,
                           source.retailer_id,
                           archive.content_length,
                           archive.archived_at AS charged_at
                      FROM raw_archive_objects archive
                      JOIN source_files source
                        ON source.raw_archive_object_id = archive.id
                     WHERE source.download_finished_at IS NULL
                       AND archive.archived_at
                               > cutoff.captured_at - INTERVAL '24 hours'
                       AND archive.archived_at <= cutoff.captured_at
                     ORDER BY archive.archived_at, archive.id, source.id
                     LIMIT 100001
              ) bounded ON true
        ), candidates AS MATERIALIZED (
            SELECT source_file_id, retailer_id, content_length, charged_at
              FROM download_candidates
            UNION ALL
            SELECT source_file_id, retailer_id, content_length, charged_at
              FROM archive_fallback_candidates
        )
        SELECT source_file_id, retailer_id, content_length, charged_at
          FROM candidates
         ORDER BY charged_at, source_file_id
         LIMIT 100001
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF (SELECT count(*) FROM makolet_0009_archive_charge_candidates) > 100000 THEN
                RAISE EXCEPTION
                    '0009 archive-charge seed exceeds its 100000-row migration bound';
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        INSERT INTO collection_archive_charges (
            source_file_id, retailer_id, attempt_id, content_length, charged_at
        )
        SELECT source_file_id, retailer_id, NULL, content_length, charged_at
          FROM makolet_0009_archive_charge_candidates
         ORDER BY charged_at, source_file_id
        ON CONFLICT (source_file_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM collection_archive_charges)
               OR EXISTS (SELECT 1 FROM collection_transfer_charges)
               OR EXISTS (SELECT 1 FROM collection_charge_budgets)
               OR EXISTS (
                    SELECT 1 FROM collection_attempts WHERE charged_bytes > 0
               ) THEN
                RAISE EXCEPTION
                    '0009 downgrade requires an explicit charged-byte retention decision';
            END IF;
        END
        $$
        """
    )
    op.drop_table("collection_transfer_charges")
    op.drop_index("ix_current_availability_store_latest", table_name="current_availability")
    op.drop_index("ix_raw_archive_objects_archived_id", table_name="raw_archive_objects")
    op.drop_table("collection_archive_charges")
    op.drop_table("collection_charge_budgets")
    op.execute(
        """
        ALTER TABLE collection_attempts
            DROP CONSTRAINT ck_collection_attempts_truncation_reason_state,
            DROP CONSTRAINT ck_collection_attempts_truncation_reason_value,
            DROP CONSTRAINT ck_collection_attempts_counts_nonnegative,
            ADD CONSTRAINT ck_collection_attempts_counts_nonnegative
                CHECK (discovered_count >= 0 AND processed_count >= 0
                       AND duplicate_count >= 0 AND skipped_unknown_count >= 0
                       AND warning_count >= 0),
            DROP COLUMN truncation_reason,
            DROP COLUMN charged_bytes
        """
    )
