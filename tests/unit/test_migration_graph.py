"""Offline invariants for the append-only Alembic revision graph."""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

WORKSPACE = Path(__file__).parents[2]


def test_migration_graph_has_one_head_and_revision_ids_fit_alembic_storage() -> None:
    configuration = Config(str(WORKSPACE / "alembic.ini"))
    configuration.set_main_option("script_location", str(WORKSPACE / "migrations"))
    scripts = ScriptDirectory.from_config(configuration)
    revisions = tuple(scripts.walk_revisions())

    assert scripts.get_heads() == ["0011_resource_probe_budgets"]
    assert all(len(revision.revision) <= 32 for revision in revisions)


def test_charge_seed_captures_one_bounded_trailing_window() -> None:
    migration = (
        WORKSPACE / "migrations" / "versions" / "0009_collection_charge_budgets.py"
    ).read_text(encoding="utf-8")

    assert "migration_cutoff AS MATERIALIZED" in migration
    assert "download_candidates AS MATERIALIZED" in migration
    assert "archive_fallback_candidates AS MATERIALIZED" in migration
    assert "ORDER BY source.download_finished_at, source.id" in migration
    assert "ORDER BY archive.archived_at, archive.id, source.id" in migration
    assert "ix_raw_archive_objects_archived_id" in migration
    assert "INTERVAL '24 hours'" in migration
    assert migration.count("LIMIT 100001") >= 3
    assert "> 100000" in migration
    assert "makolet_0009_archive_charge_candidates" in migration
    assert "ADD COLUMN charged_bytes BIGINT," in migration
    assert "ALTER COLUMN charged_bytes SET DEFAULT 0" in migration
    assert "ADD COLUMN charged_bytes BIGINT DEFAULT 0 NOT NULL" not in migration


def test_bounded_query_projection_migration_has_batched_backfill_and_maintenance() -> None:
    migration = (WORKSPACE / "migrations" / "versions" / "0010_bounded_query_paths.py").read_text(
        encoding="utf-8"
    )

    assert 'down_revision: str | None = "0009_collection_charge_budgets"' in migration
    assert migration.count("LIMIT 10000") >= 3
    assert "max(bounded.id)" not in migration.casefold()
    assert migration.count("ORDER BY bounded.id DESC") == 3
    assert "canonical_product_id UUID" in migration
    assert "query_retailer_id UUID" in migration
    assert "city_search TEXT GENERATED ALWAYS AS" in migration
    assert "CREATE TRIGGER trg_current_prices_project_insert" in migration
    assert "CREATE TRIGGER trg_confirmed_matches_refresh_query_projection" in migration
    assert "REFERENCING NEW TABLE AS makolet_inserted_confirmed_matches" in migration
    assert "CREATE CONSTRAINT TRIGGER trg_confirmed_matches_clear_query_projection" in migration
    assert "ix_current_prices_product_price_id" in migration
    assert "ix_price_history_product_from_id" in migration
    assert "ix_current_availability_product_id" in migration
    assert "ix_stores_city_search_id" in migration


def test_resource_probe_migration_is_batched_and_extends_the_current_head() -> None:
    migration = (
        WORKSPACE / "migrations" / "versions" / "0011_resource_probe_budgets.py"
    ).read_text(encoding="utf-8")

    assert 'down_revision: str | None = "0010_bounded_query_paths"' in migration
    assert "ix_price_history_product_period_gist" in migration
    assert "ix_promotions_active_period_gist" in migration
    assert "collection_budget_buckets" in migration
    assert migration.count("LIMIT 10000") >= 4
