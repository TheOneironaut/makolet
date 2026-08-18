"""Static PostgreSQL proofs for archive-budget atomicity while PG is unavailable."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from sqlalchemy import DefaultClause

from makolet.adapters.persistence import collection as collection_repository
from makolet.adapters.persistence.schema import (
    collection_archive_charges,
    collection_attempts,
    collection_budget_buckets,
    collection_charge_budgets,
    collection_identity_observations,
    collection_transfer_charges,
    raw_archive_objects,
)


def _source(value: Callable[..., Any]) -> str:
    return " ".join(inspect.getsource(value).split())


def test_transfer_settlement_uses_persisted_archive_bytes_atomically() -> None:
    source = _source(collection_repository.PostgresCollectionRepository.settle_transfer)

    transaction = source.index("async with self._engine.begin() as connection")
    attempt_lock = source.index("self._locked_attempt(connection, attempt_id)")
    budget_lock = source.index("self._locked_charge_budget(connection, retailer_id)")
    source_lock = source.index(".with_for_update(of=source_files)")
    persisted_length = source.index("select(raw_archive_objects.c.content_length)")
    reservation_lock = source.index("select(collection_transfer_charges)", source_lock)
    charge_insert = source.index("pg_insert(collection_archive_charges)", reservation_lock)
    idempotence = source.index("collection_archive_charges.c.source_file_id", charge_insert)
    reservation_guard = source.index(
        "exact_attempt_charge > int(reservation.content_length)",
        idempotence,
    )
    settlement_update = source.index("update(collection_transfer_charges)", idempotence)
    attempt_update = source.index("update(collection_attempts)", settlement_update)
    exact_day_recompute = source.index("self._locked_charge_budget", attempt_update)

    assert transaction < attempt_lock < budget_lock < source_lock
    assert source_lock < reservation_lock < persisted_length < charge_insert < idempotence
    assert "on_conflict_do_nothing" in source[charge_insert:attempt_update]
    assert reservation_guard < settlement_update < attempt_update < exact_day_recompute


def test_transfer_reservation_is_durable_before_network_io() -> None:
    source = _source(collection_repository.PostgresCollectionRepository.reserve_transfer)

    reservation = source.index("pg_insert(collection_transfer_charges)")
    composite_key = source.index("collection_transfer_charges.c.attempt_id", reservation)
    identity_observation = source.index("pg_insert(collection_identity_observations)")
    run_charge = source.index("update(collection_attempts)", composite_key)
    bucket_charge = source.index("self._adjust_budget_bucket", run_charge)
    bounded_summary = source.index("self._locked_charge_budget", bucket_charge)

    assert reservation < composite_key < identity_observation < run_charge
    assert run_charge < bucket_charge < bounded_summary
    assert "on_conflict_do_nothing" in source[reservation:run_charge]


def test_transfer_ledger_supports_multiple_sources_and_unsettled_reservations() -> None:
    primary_key = tuple(collection_transfer_charges.primary_key.columns.keys())
    window_source = _source(
        collection_repository.PostgresCollectionRepository._locked_charge_budget
    )

    assert primary_key == ("attempt_id", "source_file_id")
    assert "collection_transfer_charges" not in window_source
    assert "collection_archive_charges" not in window_source
    assert "collection_budget_buckets.c.charged_bytes" in window_source


def test_archive_day_budget_reads_only_a_fixed_five_minute_bucket_window() -> None:
    source = _source(collection_repository.PostgresCollectionRepository._locked_charge_budget)

    row_select = source.index("select(collection_charge_budgets)")
    row_lock = source.index(".with_for_update()", row_select)
    bucket_floor = source.index("_budget_bucket_start(connection, now)", row_lock)
    window_start = source.index("current_bucket - _ARCHIVE_BUDGET_WINDOW", bucket_floor)
    expired_cleanup = source.index("delete(collection_budget_buckets)", window_start)
    fixed_bucket_sum = source.index(
        "func.sum(collection_budget_buckets.c.charged_bytes)", expired_cleanup
    )
    persisted_summary = source.index("update(collection_charge_budgets)", fixed_bucket_sum)
    final_return = source.rindex("return")

    assert timedelta(hours=24) == collection_repository._ARCHIVE_BUDGET_WINDOW
    assert timedelta(minutes=5) == collection_repository._BUDGET_BUCKET_WIDTH
    assert row_select < row_lock < bucket_floor < window_start < expired_cleanup
    assert expired_cleanup < fixed_bucket_sum < persisted_summary < final_return
    assert "union_all" not in source
    assert source.count(" return (") == 1


def test_bucket_adjustment_is_atomic_and_updates_all_independent_counters() -> None:
    source = _source(collection_repository.PostgresCollectionRepository._adjust_budget_bucket)

    assert "pg_insert(collection_budget_buckets)" in source
    assert "on_conflict_do_update" in source
    for counter in ("charged_bytes", "identity_count", "attempt_count", "success_count"):
        assert f'"{counter}"' in source


def test_archive_charge_schema_keys_make_retry_accounting_idempotent_and_scoped() -> None:
    assert collection_attempts.c.charged_bytes.nullable is True
    charged_bytes_default = collection_attempts.c.charged_bytes.server_default
    assert isinstance(charged_bytes_default, DefaultClause)
    assert str(charged_bytes_default.arg) == "0"
    assert "ix_raw_archive_objects_archived_id" in {
        index.name for index in raw_archive_objects.indexes
    }
    assert tuple(collection_charge_budgets.primary_key.columns.keys()) == ("retailer_id",)
    assert tuple(collection_budget_buckets.primary_key.columns.keys()) == (
        "retailer_id",
        "bucket_started_at",
    )
    assert tuple(collection_identity_observations.primary_key.columns.keys()) == ("source_file_id",)
    assert tuple(collection_archive_charges.primary_key.columns.keys()) == ("source_file_id",)

    source_file_fk = next(iter(collection_archive_charges.c.source_file_id.foreign_keys))
    retailer_fk = next(iter(collection_archive_charges.c.retailer_id.foreign_keys))
    attempt_fk = next(iter(collection_archive_charges.c.attempt_id.foreign_keys))
    assert source_file_fk.target_fullname == "source_files.id"
    assert source_file_fk.ondelete == "RESTRICT"
    assert retailer_fk.target_fullname == "retailers.id"
    assert retailer_fk.ondelete == "CASCADE"
    assert attempt_fk.target_fullname == "collection_attempts.id"
    assert attempt_fk.ondelete == "SET NULL"
    assert collection_archive_charges.c.content_length.nullable is False

    assert tuple(collection_transfer_charges.primary_key.columns.keys()) == (
        "attempt_id",
        "source_file_id",
    )
    transfer_attempt_fk = next(iter(collection_transfer_charges.c.attempt_id.foreign_keys))
    assert transfer_attempt_fk.ondelete == "RESTRICT"
    assert collection_transfer_charges.c.retailer_id.nullable is False
    assert collection_transfer_charges.c.content_length.nullable is False
