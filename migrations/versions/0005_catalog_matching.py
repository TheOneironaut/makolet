"""Index bounded post-ingestion catalog bootstrap.

Revision ID: 0005_catalog_matching
Revises: 0004_temporal_quality
Created: 2026-08-12

The index build takes a SHARE lock while PostgreSQL scans retailer_items. Operators
upgrading an unusually large existing catalog should schedule the migration during
the documented maintenance window and retry the idempotent Alembic upgrade if the
transaction is interrupted before commit.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005_catalog_matching"
down_revision: str | None = "0004_temporal_quality"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE INDEX ix_retailer_items_last_source_file_id_id
            ON retailer_items (last_source_file_id, id)
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX ix_retailer_items_last_source_file_id_id")
