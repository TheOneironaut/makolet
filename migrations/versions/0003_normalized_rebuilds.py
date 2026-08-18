"""Add bounded archive replay order and auditable normalized rebuild state.

Revision ID: 0003_normalized_rebuilds
Revises: 0002_identifier_evidence
Created: 2026-08-12

The migration only adds coordination and audit structures. Rebuild data deletion is
an explicit operator command, never a migration side effect.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_normalized_rebuilds"
down_revision: str | None = "0002_identifier_evidence"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE normalized_rebuild_runs (
            id UUID DEFAULT uuidv7() NOT NULL,
            status VARCHAR(32) NOT NULL,
            requested_by TEXT NOT NULL,
            requested_parser_version VARCHAR(128) NOT NULL,
            archive_cutoff_at TIMESTAMPTZ NOT NULL,
            source_files_total BIGINT DEFAULT 0 NOT NULL,
            source_files_completed BIGINT DEFAULT 0 NOT NULL,
            last_sequence BIGINT,
            last_source_file_id UUID,
            last_archived_at TIMESTAMPTZ,
            started_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            finished_at TIMESTAMPTZ,
            error_code VARCHAR(128),
            error_message TEXT,
            CONSTRAINT pk_normalized_rebuild_runs PRIMARY KEY (id),
            CONSTRAINT ck_normalized_rebuild_runs_status_value
                CHECK (status IN ('running', 'failed', 'completed')),
            CONSTRAINT ck_normalized_rebuild_runs_requested_by_length
                CHECK (length(requested_by) BETWEEN 1 AND 128),
            CONSTRAINT ck_normalized_rebuild_runs_requested_parser_version_length
                CHECK (length(requested_parser_version) BETWEEN 1 AND 128),
            CONSTRAINT ck_normalized_rebuild_runs_source_file_counts_valid
                CHECK (
                    source_files_total >= 0
                    AND source_files_completed >= 0
                    AND source_files_completed <= source_files_total
                ),
            CONSTRAINT ck_normalized_rebuild_runs_last_sequence_positive
                CHECK (last_sequence IS NULL OR last_sequence > 0),
            CONSTRAINT ck_normalized_rebuild_runs_run_time_order
                CHECK (finished_at IS NULL OR finished_at >= started_at),
            CONSTRAINT fk_normalized_rebuild_runs_last_source_file_id_source_files
                FOREIGN KEY (last_source_file_id)
                REFERENCES source_files (id) ON DELETE RESTRICT
        )
        """
    )
    op.execute(
        """
        CREATE TABLE normalized_rebuild_files (
            rebuild_run_id UUID NOT NULL,
            sequence BIGINT NOT NULL,
            source_file_id UUID NOT NULL,
            archived_at TIMESTAMPTZ NOT NULL,
            original_applied_at TIMESTAMPTZ NOT NULL,
            status VARCHAR(32) DEFAULT 'pending' NOT NULL,
            completed_at TIMESTAMPTZ,
            CONSTRAINT pk_normalized_rebuild_files
                PRIMARY KEY (rebuild_run_id, sequence),
            CONSTRAINT uq_normalized_rebuild_files_rebuild_run_id_source_file_id
                UNIQUE (rebuild_run_id, source_file_id),
            CONSTRAINT ck_normalized_rebuild_files_status_value
                CHECK (status IN ('pending', 'completed')),
            CONSTRAINT ck_normalized_rebuild_files_sequence_positive
                CHECK (sequence > 0),
            CONSTRAINT ck_normalized_rebuild_files_completion_state_consistent
                CHECK (
                    (status = 'pending' AND completed_at IS NULL)
                    OR (status = 'completed' AND completed_at IS NOT NULL)
                ),
            CONSTRAINT fk_normalized_rebuild_files_run
                FOREIGN KEY (rebuild_run_id)
                REFERENCES normalized_rebuild_runs (id) ON DELETE CASCADE,
            CONSTRAINT fk_normalized_rebuild_files_source_file_id_source_files
                FOREIGN KEY (source_file_id)
                REFERENCES source_files (id) ON DELETE RESTRICT
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_normalized_rebuild_files_pending
            ON normalized_rebuild_files (rebuild_run_id, sequence)
            WHERE status = 'pending'
        """
    )
    op.execute(
        """
        CREATE TABLE normalized_rebuild_control (
            singleton_id INTEGER NOT NULL,
            active_rebuild_run_id UUID,
            updated_at TIMESTAMPTZ DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_normalized_rebuild_control PRIMARY KEY (singleton_id),
            CONSTRAINT ck_normalized_rebuild_control_singleton_id_value
                CHECK (singleton_id = 1),
            CONSTRAINT fk_normalized_rebuild_control_active
                FOREIGN KEY (active_rebuild_run_id)
                REFERENCES normalized_rebuild_runs (id) ON DELETE RESTRICT
        )
        """
    )
    op.execute(
        """
        INSERT INTO normalized_rebuild_control (singleton_id, active_rebuild_run_id)
        VALUES (1, NULL)
        """
    )
    op.execute(
        """
        ALTER TABLE replay_runs
            ADD COLUMN rebuild_run_id UUID,
            ADD CONSTRAINT fk_replay_runs_rebuild_run_id_normalized_rebuild_runs
                FOREIGN KEY (rebuild_run_id)
                REFERENCES normalized_rebuild_runs (id) ON DELETE RESTRICT
        """
    )
    op.execute(
        """
        CREATE INDEX ix_replay_runs_rebuild_started
            ON replay_runs (rebuild_run_id, started_at)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_source_files_archived_id
            ON source_files (download_finished_at, id)
            WHERE raw_archive_object_id IS NOT NULL
        """
    )


def downgrade() -> None:
    # The migration was exercised locally before the archive timestamp was moved
    # from the deduplicated raw object to the source-file attachment. No released
    # schema used the former name, but tolerating it keeps development downgrades
    # recoverable while the unreleased migration is being validated.
    op.execute("DROP INDEX IF EXISTS ix_source_files_archived_id")
    op.execute("DROP INDEX IF EXISTS ix_raw_archive_objects_archived_id")
    op.execute("DROP INDEX ix_replay_runs_rebuild_started")
    op.execute(
        """
        ALTER TABLE replay_runs
            DROP CONSTRAINT fk_replay_runs_rebuild_run_id_normalized_rebuild_runs,
            DROP COLUMN rebuild_run_id
        """
    )
    op.execute("DROP TABLE normalized_rebuild_control")
    op.execute("DROP TABLE normalized_rebuild_files")
    op.execute("DROP TABLE normalized_rebuild_runs")
