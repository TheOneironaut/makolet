"""Add indexed temporal probes and fixed-bucket hostile-cardinality budgets.

Revision ID: 0011_resource_probe_budgets
Revises: 0010_bounded_query_paths
Created: 2026-08-17

The stored range columns rewrite ``price_history`` and ``promotions`` once and the
index builds take ordinary PostgreSQL DDL locks. Operators must preflight both table
sizes and schedule a maintenance window with enough temporary/WAL capacity. Legacy
validation evidence and charge audit rows are summarized or trimmed in 10,000-row
batches inside the migration transaction. An interruption rolls the revision back
completely; cancel, allow rollback to finish, restore capacity, and rerun the
ordinary upgrade.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0011_resource_probe_budgets"
down_revision: str | None = "0010_bounded_query_paths"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute(
        """
        ALTER TABLE price_history
            ADD COLUMN valid_period TSTZRANGE GENERATED ALWAYS AS (
                tstzrange(valid_from, valid_to, '[)')
            ) STORED NOT NULL
        """
    )
    op.execute(
        """
        ALTER TABLE promotions
            ADD COLUMN valid_period TSTZRANGE GENERATED ALWAYS AS (
                tstzrange(valid_from, valid_to, '[)')
            ) STORED NOT NULL,
            ADD COLUMN active_period TSTZRANGE GENERATED ALWAYS AS (
                tstzrange(valid_from, valid_to, '[)')
                * tstzrange(starts_at, ends_at, '[]')
            ) STORED NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_price_history_product_period_gist
            ON price_history USING gist (canonical_product_id, valid_period)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_price_history_product_store_period_gist
            ON price_history USING gist
                (canonical_product_id, store_id, valid_period)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_promotions_valid_period_gist
            ON promotions USING gist (valid_period)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_promotions_active_period_gist
            ON promotions USING gist (active_period)
            WHERE COALESCE(is_active, true)
        """
    )

    op.execute(
        """
        ALTER TABLE ingestion_runs
            ADD COLUMN file_quarantine_issues BIGINT DEFAULT 0 NOT NULL,
            ADD COLUMN validation_issue_bytes BIGINT DEFAULT 0 NOT NULL,
            ADD COLUMN validation_issue_samples BIGINT DEFAULT 0 NOT NULL,
            DROP CONSTRAINT ck_ingestion_runs_counts_nonnegative,
            ADD CONSTRAINT ck_ingestion_runs_counts_nonnegative CHECK (
                metadata_records >= 0 AND store_records >= 0
                AND price_records >= 0 AND promotion_records >= 0
                AND warnings >= 0 AND rejected_records >= 0
                AND file_quarantine_issues >= 0
                AND validation_issue_bytes >= 0
                AND validation_issue_samples >= 0
                AND inserted_records >= 0 AND updated_records >= 0
                AND unchanged_records >= 0 AND unavailable_records >= 0
                AND history_events >= 0
            )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_validation_issues_ingestion_evidence
            ON validation_issues (ingestion_run_id, created_at, id)
            WHERE ingestion_run_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_validation_issues_replay_evidence
            ON validation_issues (replay_run_id, created_at, id)
            WHERE replay_run_id IS NOT NULL
        """
    )
    _backfill_validation_summaries()
    _trim_legacy_validation_evidence()

    op.execute(
        """
        ALTER TABLE collection_charge_budgets
            ADD COLUMN identity_count BIGINT DEFAULT 0 NOT NULL,
            ADD COLUMN attempt_count BIGINT DEFAULT 0 NOT NULL,
            ADD COLUMN success_count BIGINT DEFAULT 0 NOT NULL,
            DROP CONSTRAINT ck_collection_charge_budgets_charged_bytes_nonnegative,
            ADD CONSTRAINT ck_collection_charge_budgets_counts_nonnegative CHECK (
                charged_bytes >= 0 AND identity_count >= 0
                AND attempt_count >= 0 AND success_count >= 0
            )
        """
    )
    op.execute(
        """
        CREATE TABLE collection_budget_buckets (
            retailer_id UUID NOT NULL,
            bucket_started_at TIMESTAMP WITH TIME ZONE NOT NULL,
            charged_bytes BIGINT DEFAULT 0 NOT NULL,
            identity_count BIGINT DEFAULT 0 NOT NULL,
            attempt_count BIGINT DEFAULT 0 NOT NULL,
            success_count BIGINT DEFAULT 0 NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_collection_budget_buckets
                PRIMARY KEY (retailer_id, bucket_started_at),
            CONSTRAINT fk_collection_budget_buckets_retailer_id_retailers
                FOREIGN KEY (retailer_id) REFERENCES retailers (id) ON DELETE CASCADE,
            CONSTRAINT ck_collection_budget_buckets_counts_nonnegative CHECK (
                charged_bytes >= 0 AND identity_count >= 0
                AND attempt_count >= 0 AND success_count >= 0
            )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE collection_identity_observations (
            source_file_id UUID NOT NULL,
            retailer_id UUID NOT NULL,
            observed_at TIMESTAMP WITH TIME ZONE DEFAULT clock_timestamp() NOT NULL,
            CONSTRAINT pk_collection_identity_observations PRIMARY KEY (source_file_id),
            CONSTRAINT fk_collection_identity_observations_source_file_id_source_files
                FOREIGN KEY (source_file_id) REFERENCES source_files (id) ON DELETE RESTRICT,
            CONSTRAINT fk_collection_identity_observations_retailer_id_retailers
                FOREIGN KEY (retailer_id) REFERENCES retailers (id) ON DELETE CASCADE
        )
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_identity_observations_retailer_observed
            ON collection_identity_observations
                (retailer_id, observed_at, source_file_id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_collection_transfer_charges_source_charged
            ON collection_transfer_charges (source_file_id, charged_at, attempt_id)
        """
    )
    op.execute(
        """
        ALTER TABLE collection_attempts
            DROP CONSTRAINT ck_collection_attempts_truncation_reason_value,
            ADD CONSTRAINT ck_collection_attempts_truncation_reason_value CHECK (
                truncation_reason IS NULL OR truncation_reason IN (
                    'file_limit', 'discovery_limit',
                    'charged_byte_run_limit', 'charged_byte_day_limit',
                    'identity_day_limit', 'attempt_day_limit',
                    'success_day_limit', 'legacy_limit'
                )
            )
        """
    )

    _backfill_collection_buckets()


def _backfill_validation_summaries() -> None:
    op.execute(
        """
        CREATE TEMP TABLE makolet_0011_validation_summaries (
            ingestion_run_id UUID PRIMARY KEY,
            file_quarantine_issues BIGINT NOT NULL,
            validation_issue_bytes BIGINT NOT NULL,
            validation_issue_samples BIGINT NOT NULL
        ) ON COMMIT DROP
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            last_id UUID := NULL;
            batch_last UUID;
        BEGIN
            LOOP
                CREATE TEMP TABLE makolet_0011_validation_batch ON COMMIT DROP AS
                SELECT issue.id, issue.ingestion_run_id, issue.severity,
                       issue.code, issue.message, issue.field_name,
                       issue.rejected_value
                  FROM validation_issues issue
                 WHERE issue.ingestion_run_id IS NOT NULL
                   AND (last_id IS NULL OR issue.id > last_id)
                 ORDER BY issue.id
                 LIMIT 10000;
                EXIT WHEN NOT EXISTS (SELECT 1 FROM makolet_0011_validation_batch);

                INSERT INTO makolet_0011_validation_summaries (
                    ingestion_run_id, file_quarantine_issues,
                    validation_issue_bytes, validation_issue_samples
                )
                SELECT ingestion_run_id,
                       count(*) FILTER (WHERE severity = 'file_quarantine'),
                       sum(
                           64 + octet_length(code) + octet_length(message)
                           + COALESCE(octet_length(field_name), 0)
                           + COALESCE(octet_length(rejected_value), 0)
                       ),
                       count(*)
                  FROM makolet_0011_validation_batch
                 GROUP BY ingestion_run_id
                ON CONFLICT (ingestion_run_id) DO UPDATE
                    SET file_quarantine_issues =
                            makolet_0011_validation_summaries.file_quarantine_issues
                            + EXCLUDED.file_quarantine_issues,
                        validation_issue_bytes =
                            makolet_0011_validation_summaries.validation_issue_bytes
                            + EXCLUDED.validation_issue_bytes,
                        validation_issue_samples =
                            makolet_0011_validation_summaries.validation_issue_samples
                            + EXCLUDED.validation_issue_samples;

                SELECT id INTO batch_last
                  FROM makolet_0011_validation_batch
                 ORDER BY id DESC LIMIT 1;
                last_id := batch_last;
                DROP TABLE makolet_0011_validation_batch;
            END LOOP;
            DROP TABLE IF EXISTS makolet_0011_validation_batch;
        END
        $$
        """
    )
    op.execute(
        """
        UPDATE ingestion_runs run
           SET file_quarantine_issues = summary.file_quarantine_issues,
               validation_issue_bytes = summary.validation_issue_bytes,
               validation_issue_samples = LEAST(summary.validation_issue_samples, 1000)
          FROM makolet_0011_validation_summaries summary
         WHERE run.id = summary.ingestion_run_id
        """
    )


def _trim_legacy_validation_evidence() -> None:
    for run_column in ("ingestion_run_id", "replay_run_id"):
        op.execute(
            f"""
            DO $$
            DECLARE
                selected_run UUID;
                next_run UUID;
                deleted_count INTEGER;
            BEGIN
                selected_run := NULL;
                LOOP
                    SELECT issue.{run_column} INTO next_run
                      FROM validation_issues issue
                     WHERE issue.{run_column} IS NOT NULL
                       AND (
                           selected_run IS NULL
                           OR issue.{run_column} > selected_run
                       )
                     ORDER BY issue.{run_column}
                     LIMIT 1;
                    EXIT WHEN NOT FOUND;
                    selected_run := next_run;
                    LOOP
                        DELETE FROM validation_issues issue
                         WHERE issue.id IN (
                             SELECT candidate.id
                               FROM validation_issues candidate
                              WHERE candidate.{run_column} = selected_run
                              ORDER BY candidate.created_at, candidate.id
                              OFFSET 1000
                              LIMIT 10000
                         );
                        GET DIAGNOSTICS deleted_count = ROW_COUNT;
                        EXIT WHEN deleted_count = 0;
                    END LOOP;
                END LOOP;
            END
            $$
            """  # noqa: S608 - run_column is selected from two fixed identifiers.
        )


def _backfill_collection_buckets() -> None:
    op.execute(
        """
        CREATE TEMP TABLE makolet_0011_cutoff (
            captured_at TIMESTAMP WITH TIME ZONE NOT NULL
        ) ON COMMIT DROP
        """
    )
    op.execute(
        """
        INSERT INTO makolet_0011_cutoff VALUES (clock_timestamp())
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            last_source UUID := NULL;
            batch_last UUID;
        BEGIN
            LOOP
                CREATE TEMP TABLE makolet_0011_identity_batch ON COMMIT DROP AS
                SELECT source.id AS source_file_id,
                       source.retailer_id,
                       first_charge.charged_at AS observed_at
                  FROM source_files source
                  JOIN LATERAL (
                        SELECT charge.charged_at
                          FROM collection_transfer_charges charge
                         WHERE charge.source_file_id = source.id
                         ORDER BY charge.charged_at, charge.attempt_id
                         LIMIT 1
                       ) first_charge ON true
                 WHERE last_source IS NULL OR source.id > last_source
                 ORDER BY source.id
                 LIMIT 10000;
                EXIT WHEN NOT EXISTS (SELECT 1 FROM makolet_0011_identity_batch);

                INSERT INTO collection_identity_observations
                    (source_file_id, retailer_id, observed_at)
                SELECT source_file_id, retailer_id, observed_at
                  FROM makolet_0011_identity_batch
                 ORDER BY source_file_id
                ON CONFLICT (source_file_id) DO NOTHING;

                SELECT source_file_id INTO batch_last
                  FROM makolet_0011_identity_batch
                 ORDER BY source_file_id DESC LIMIT 1;
                last_source := batch_last;
                DROP TABLE makolet_0011_identity_batch;
            END LOOP;
            DROP TABLE IF EXISTS makolet_0011_identity_batch;
        END
        $$
        """
    )
    op.execute(
        """
        INSERT INTO collection_budget_buckets (
            retailer_id, bucket_started_at, identity_count
        )
        SELECT observation.retailer_id,
               date_bin(
                   INTERVAL '5 minutes', observation.observed_at,
                   TIMESTAMPTZ '1970-01-01 00:00:00+00'
               ),
               count(*)
          FROM collection_identity_observations observation
          CROSS JOIN makolet_0011_cutoff cutoff
         WHERE observation.observed_at
                   >= cutoff.captured_at - INTERVAL '24 hours 5 minutes'
         GROUP BY observation.retailer_id,
                  date_bin(
                      INTERVAL '5 minutes', observation.observed_at,
                      TIMESTAMPTZ '1970-01-01 00:00:00+00'
                  )
        ON CONFLICT (retailer_id, bucket_started_at) DO UPDATE
            SET identity_count = EXCLUDED.identity_count,
                updated_at = clock_timestamp()
        """
    )
    _backfill_charge_table(
        table_name="collection_transfer_charges",
        order_columns="attempt_id, source_file_id",
        cursor_declaration="last_attempt UUID := NULL; last_source UUID := NULL;",
        cursor_filter=(
            "last_attempt IS NULL OR (attempt_id, source_file_id) > (last_attempt, last_source)"
        ),
        cursor_select=(
            "SELECT attempt_id, source_file_id INTO batch_attempt, batch_source "
            "FROM makolet_0011_charge_batch ORDER BY attempt_id DESC, "
            "source_file_id DESC LIMIT 1; last_attempt := batch_attempt; "
            "last_source := batch_source;"
        ),
        extra_declarations="batch_attempt UUID; batch_source UUID;",
        attempt_expression="count(*)",
        success_expression="0",
    )
    _backfill_charge_table(
        table_name="collection_archive_charges",
        order_columns="source_file_id",
        cursor_declaration="last_source UUID := NULL;",
        cursor_filter="last_source IS NULL OR source_file_id > last_source",
        cursor_select=(
            "SELECT source_file_id INTO batch_source "
            "FROM makolet_0011_charge_batch ORDER BY source_file_id DESC LIMIT 1; "
            "last_source := batch_source;"
        ),
        extra_declarations="batch_source UUID;",
        attempt_expression="0",
        success_expression="count(*)",
    )
    op.execute(
        """
        WITH current_bucket AS (
            SELECT date_bin(
                       INTERVAL '5 minutes', captured_at,
                       TIMESTAMPTZ '1970-01-01 00:00:00+00'
                   ) AS value
              FROM makolet_0011_cutoff
        )
        UPDATE collection_charge_budgets budget
           SET window_started_at = current.value - INTERVAL '24 hours',
               charged_bytes = 0,
               identity_count = 0,
               attempt_count = 0,
                success_count = 0,
                updated_at = (SELECT captured_at FROM makolet_0011_cutoff)
          FROM current_bucket current
        """
    )
    op.execute(
        """
        WITH current_bucket AS (
            SELECT date_bin(
                       INTERVAL '5 minutes', captured_at,
                       TIMESTAMPTZ '1970-01-01 00:00:00+00'
                   ) AS value
              FROM makolet_0011_cutoff
        ), totals AS (
            SELECT bucket.retailer_id,
                   sum(bucket.charged_bytes) AS charged_bytes,
                   sum(bucket.identity_count) AS identity_count,
                   sum(bucket.attempt_count) AS attempt_count,
                   sum(bucket.success_count) AS success_count
              FROM collection_budget_buckets bucket
              CROSS JOIN current_bucket current
             WHERE bucket.bucket_started_at >= current.value - INTERVAL '24 hours'
               AND bucket.bucket_started_at <= current.value
             GROUP BY bucket.retailer_id
        )
        UPDATE collection_charge_budgets budget
           SET window_started_at = current.value - INTERVAL '24 hours',
               charged_bytes = COALESCE(totals.charged_bytes, 0),
               identity_count = COALESCE(totals.identity_count, 0),
               attempt_count = COALESCE(totals.attempt_count, 0),
               success_count = COALESCE(totals.success_count, 0),
               updated_at = (SELECT captured_at FROM makolet_0011_cutoff)
          FROM current_bucket current, totals
         WHERE totals.retailer_id = budget.retailer_id
        """
    )


def _backfill_charge_table(
    *,
    table_name: str,
    order_columns: str,
    cursor_declaration: str,
    cursor_filter: str,
    cursor_select: str,
    extra_declarations: str,
    attempt_expression: str,
    success_expression: str,
) -> None:
    op.execute(
        f"""
        DO $$
        DECLARE
            {cursor_declaration}
            {extra_declarations}
        BEGIN
            LOOP
                CREATE TEMP TABLE makolet_0011_charge_batch ON COMMIT DROP AS
                SELECT *
                  FROM {table_name}
                 WHERE ({cursor_filter})
                   AND charged_at >= (
                       SELECT captured_at - INTERVAL '24 hours 5 minutes'
                         FROM makolet_0011_cutoff
                   )
                 ORDER BY {order_columns}
                 LIMIT 10000;
                EXIT WHEN NOT EXISTS (SELECT 1 FROM makolet_0011_charge_batch);

                INSERT INTO collection_budget_buckets (
                    retailer_id, bucket_started_at, charged_bytes,
                    attempt_count, success_count
                )
                SELECT retailer_id,
                       date_bin(
                           INTERVAL '5 minutes', charged_at,
                           TIMESTAMPTZ '1970-01-01 00:00:00+00'
                       ),
                       sum(content_length),
                       {attempt_expression},
                       {success_expression}
                  FROM makolet_0011_charge_batch
                 GROUP BY retailer_id,
                          date_bin(
                              INTERVAL '5 minutes', charged_at,
                              TIMESTAMPTZ '1970-01-01 00:00:00+00'
                          )
                ON CONFLICT (retailer_id, bucket_started_at) DO UPDATE
                    SET charged_bytes = collection_budget_buckets.charged_bytes
                            + EXCLUDED.charged_bytes,
                        attempt_count = collection_budget_buckets.attempt_count
                            + EXCLUDED.attempt_count,
                        success_count = collection_budget_buckets.success_count
                            + EXCLUDED.success_count,
                        updated_at = clock_timestamp();

                {cursor_select}
                DROP TABLE makolet_0011_charge_batch;
            END LOOP;
            DROP TABLE IF EXISTS makolet_0011_charge_batch;
        END
        $$
        """  # noqa: S608 - all fragments are fixed migration-owned identifiers/expressions.
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM ingestion_runs
                 WHERE file_quarantine_issues > 0
                    OR validation_issue_bytes > 0
                    OR validation_issue_samples > 0
            ) THEN
                RAISE EXCEPTION
                    '0011 downgrade requires an explicit validation-summary retention decision';
            END IF;
        END
        $$
        """
    )
    op.execute(
        """
        ALTER TABLE collection_attempts
            DROP CONSTRAINT ck_collection_attempts_truncation_reason_value,
            ADD CONSTRAINT ck_collection_attempts_truncation_reason_value CHECK (
                truncation_reason IS NULL OR truncation_reason IN (
                    'file_limit', 'discovery_limit',
                    'charged_byte_run_limit', 'charged_byte_day_limit',
                    'legacy_limit'
                )
            )
        """
    )
    op.drop_index(
        "ix_collection_transfer_charges_source_charged",
        table_name="collection_transfer_charges",
    )
    op.drop_table("collection_identity_observations")
    op.drop_table("collection_budget_buckets")
    op.execute(
        """
        ALTER TABLE collection_charge_budgets
            DROP CONSTRAINT ck_collection_charge_budgets_counts_nonnegative,
            DROP COLUMN success_count,
            DROP COLUMN attempt_count,
            DROP COLUMN identity_count,
            ADD CONSTRAINT ck_collection_charge_budgets_charged_bytes_nonnegative
                CHECK (charged_bytes >= 0)
        """
    )
    op.execute(
        """
        ALTER TABLE ingestion_runs
            DROP CONSTRAINT ck_ingestion_runs_counts_nonnegative,
            DROP COLUMN validation_issue_samples,
            DROP COLUMN validation_issue_bytes,
            DROP COLUMN file_quarantine_issues,
            ADD CONSTRAINT ck_ingestion_runs_counts_nonnegative CHECK (
                metadata_records >= 0 AND store_records >= 0
                AND price_records >= 0 AND promotion_records >= 0
                AND warnings >= 0 AND rejected_records >= 0
                AND inserted_records >= 0 AND updated_records >= 0
                AND unchanged_records >= 0 AND unavailable_records >= 0
                AND history_events >= 0
            )
        """
    )
    op.drop_index("ix_validation_issues_replay_evidence", table_name="validation_issues")
    op.drop_index("ix_validation_issues_ingestion_evidence", table_name="validation_issues")
    op.drop_index("ix_promotions_active_period_gist", table_name="promotions")
    op.drop_index("ix_promotions_valid_period_gist", table_name="promotions")
    op.drop_index(
        "ix_price_history_product_store_period_gist",
        table_name="price_history",
    )
    op.drop_index("ix_price_history_product_period_gist", table_name="price_history")
    op.execute("ALTER TABLE promotions DROP COLUMN active_period, DROP COLUMN valid_period")
    op.execute("ALTER TABLE price_history DROP COLUMN valid_period")
