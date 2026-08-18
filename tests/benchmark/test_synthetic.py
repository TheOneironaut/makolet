"""Fast determinism checks; these are not performance assertions."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from benchmarks.database import (
    _EXPECTED_QUERY_PLANS,
    _INITIAL_SNAPSHOT_TIMESTAMP,
    _RECONCILIATION_TIMESTAMP,
    BenchmarkPlanError,
    _amplify_current_prices,
    _apply_plan_statements,
    _create_archived_source,
    _enforce_plan_gate,
    _is_standard_scale,
    _join_nodes,
    _measure_duplicate_detection,
    _plan_gate_summary,
    _query_plan_statements,
    _stage_and_apply,
    run_database_benchmark,
)
from benchmarks.run import (
    PROFILES,
    BenchmarkUsageError,
    _acceptance_metadata,
    _arguments,
    _run,
    _source_tree_digest,
)
from benchmarks.synthetic import (
    XmlGenerationStats,
    gtin14,
    price_full_xml_chunks,
    price_grid_record_batches,
    price_record_batches,
    price_text,
    product_name,
)
from makolet.adapters.persistence.queries import (
    _CURRENT_PRICES_FIRST_PAGE_QUERY,
    _FRESHNESS_QUERY,
    _FUZZY_STORES_CURSOR_QUERY,
    _FUZZY_STORES_FIRST_PAGE_QUERY,
    _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY,
    _PRODUCT_SEARCH_QUERY,
    _PROMOTION_HISTORY_QUERY,
    MAXIMUM_HISTORY_PROBE_RESULTS,
    MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
    MAXIMUM_PROMOTION_PROBE_RESULTS,
    MAXIMUM_PROMOTION_RELATIONS,
    MAXIMUM_SEARCH_CANDIDATES,
)
from makolet.application.models import MAXIMUM_FRESHNESS_ITEMS_PER_STORE
from makolet.domain.enums import IngestionStatus
from makolet.domain.models import DocumentMetadata, RemoteFile
from makolet.domain.normalization import is_valid_gtin


def test_gtins_are_unique_valid_and_reproducible() -> None:
    values = [gtin14(index) for index in range(1_000)]

    assert len(set(values)) == len(values)
    assert all(is_valid_gtin(value) for value in values)
    assert gtin14(42) == "72900000000420"


def test_normalized_record_batches_are_bounded_and_deterministic() -> None:
    source_file_id = UUID("018f0000-0000-7000-8000-000000000002")
    batches = list(price_record_batches(source_file_id, 12, batch_size=5))

    assert [len(batch) for batch in batches] == [5, 5, 2]
    assert batches[0][0].item_code == gtin14(0)
    assert batches[-1][-1].record_index == 12
    assert batches[0][0].item_price == batches[0][0].unit_of_measure_price
    assert product_name(0).startswith("קפה")
    assert price_text(0) == "1.00"


def test_grid_records_reuse_products_across_stores_with_unique_event_indexes() -> None:
    source_file_id = UUID("018f0000-0000-7000-8000-000000000003")
    records = [
        record
        for batch in price_grid_record_batches(
            source_file_id,
            12,
            unique_products=4,
            batch_size=5,
        )
        for record in batch
    ]

    assert [record.record_index for record in records] == list(range(1, 13))
    assert len({record.item_code for record in records}) == 4
    assert [record.store_id for record in records[::4]] == [
        "store-0001",
        "store-0002",
        "store-0003",
    ]


async def test_xml_generator_reports_exact_stable_bytes() -> None:
    first_stats = XmlGenerationStats()
    first = b"".join(
        [chunk async for chunk in price_full_xml_chunks(3, chunk_size=1_024, stats=first_stats)]
    )
    second_stats = XmlGenerationStats()
    second = b"".join(
        [chunk async for chunk in price_full_xml_chunks(3, chunk_size=1_024, stats=second_stats)]
    )

    assert first == second
    assert first_stats == second_stats
    assert first_stats.records == 3
    assert first_stats.bytes_emitted == len(first)
    assert first.count(b"<Item>") == 3
    assert first.endswith(b"</Items></Root>")


def test_standard_profile_is_the_only_scale_acceptance_profile() -> None:
    standard = PROFILES["standard"]

    assert standard.acceptance_evidence
    assert standard.parser_records == 1_000_000
    assert standard.database.normalized_records == 1_000_000
    assert standard.database.unique_products == 100_000
    assert standard.database.expected_current_prices == 10_000_000
    assert not PROFILES["quick"].acceptance_evidence
    assert _is_standard_scale(standard.database)
    assert not _is_standard_scale(PROFILES["quick"].database)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "confirmation"),
    [
        ("postgresql://u@db.example.test/makolet_benchmark", "makolet_benchmark"),
        ("postgresql://u@127.0.0.1/contest", "contest"),
        ("postgresql://u@127.0.0.1/makolet_benchmark", None),
    ],
)
async def test_database_benchmark_refuses_unproved_destructive_target(
    url: str,
    confirmation: str | None,
) -> None:
    arguments = _arguments(
        [
            "--profile",
            "smoke",
            "--scenario",
            "database",
            "--database-url",
            url,
            *(["--database-confirmation", confirmation] if confirmation is not None else []),
        ]
    )

    with pytest.raises(BenchmarkUsageError):
        await _run(arguments)


@pytest.mark.asyncio
async def test_database_benchmark_sink_revalidates_target_before_connecting() -> None:
    with pytest.raises(ValueError, match="loopback"):
        await run_database_benchmark(
            "postgresql://u@db.example.test/makolet_benchmark",
            PROFILES["smoke"].database,
            database_confirmation="makolet_benchmark",
        )


def test_product_search_plan_uses_exact_production_statement_and_parameters() -> None:
    product_id = UUID("018f0000-0000-7000-8000-000000000010")
    store_id = UUID("018f0000-0000-7000-8000-000000000011")

    statement, parameters, important_relations = _query_plan_statements(
        product_id=product_id,
        store_id=store_id,
        barcode=gtin14(42),
        search_query="  Mixed   Search  ",
    )["product_search"]

    assert statement is _PRODUCT_SEARCH_QUERY
    assert parameters == {
        "query": "mixed search",
        "query_quantity": None,
        "query_unit": None,
        "cursor_id": None,
        "limit": 51,
        "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
    }
    assert important_relations == {
        "canonical_products",
        "product_identifiers",
        "retailer_items",
        "confirmed_product_matches",
    }


def test_bounded_public_query_plans_use_exact_production_statements() -> None:
    statements = _query_plan_statements(
        product_id=UUID("018f0000-0000-7000-8000-000000000010"),
        store_id=UUID("018f0000-0000-7000-8000-000000000011"),
        barcode=gtin14(42),
        search_query="mixed search",
    )
    assert set(statements) == _EXPECTED_QUERY_PLANS

    promotion, promotion_parameters, promotion_relations = statements[
        "promotion_history_bounded_page"
    ]
    comparison, comparison_parameters, comparison_relations = statements[
        "cross_store_price_comparison"
    ]
    history, history_parameters, history_relations = statements["price_history"]
    freshness, freshness_parameters, freshness_relations = statements["freshness_bounded_page"]
    fuzzy_first, fuzzy_first_parameters, fuzzy_first_relations = statements[
        "fuzzy_store_first_page"
    ]
    fuzzy_cursor, fuzzy_cursor_parameters, fuzzy_cursor_relations = statements[
        "fuzzy_store_cursor_page"
    ]

    assert promotion is _PROMOTION_HISTORY_QUERY
    assert promotion_parameters == {
        "product_id": None,
        "store_id": None,
        "since": datetime(2024, 1, 1, tzinfo=UTC),
        "until": datetime(2027, 1, 1, tzinfo=UTC),
        "cursor_id": None,
        "candidate_limit": 2,
        "page_limit": 1,
        "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
        "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
        "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
        "probe_limit": MAXIMUM_PROMOTION_PROBE_RESULTS,
    }
    assert promotion_relations == set()
    assert comparison is _CURRENT_PRICES_FIRST_PAGE_QUERY
    assert comparison_parameters == {
        "product_id": UUID("018f0000-0000-7000-8000-000000000010"),
        "candidate_limit": 51,
    }
    assert comparison_relations == {"current_prices"}
    assert history is _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY
    assert history_parameters == {
        "product_id": UUID("018f0000-0000-7000-8000-000000000010"),
        "store_id": UUID("018f0000-0000-7000-8000-000000000011"),
        "since": datetime(2024, 1, 1, tzinfo=UTC),
        "until": datetime(2027, 1, 1, tzinfo=UTC),
        "candidate_limit": 51,
        "probe_limit": MAXIMUM_HISTORY_PROBE_RESULTS,
    }
    assert history_relations == {"price_history"}
    assert freshness is _FRESHNESS_QUERY
    assert freshness_parameters == {
        "cursor_id": None,
        "limit": 2,
        "item_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
        "item_probe_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1,
    }
    assert freshness_relations == {"current_availability"}
    assert fuzzy_first is _FUZZY_STORES_FIRST_PAGE_QUERY
    assert fuzzy_first_parameters == {
        "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
        "candidate_probe_limit": MAXIMUM_SEARCH_CANDIDATES + 1,
        "city": None,
        "page_limit": 51,
        "query": "mixed search",
        "retailer_id": None,
    }
    assert fuzzy_first_relations == set()
    assert fuzzy_cursor is _FUZZY_STORES_CURSOR_QUERY
    assert fuzzy_cursor_parameters == {
        "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
        "candidate_probe_limit": MAXIMUM_SEARCH_CANDIDATES + 1,
        "city": None,
        "cursor_id": UUID("018f0000-0000-7000-8000-000000000011"),
        "page_limit": 51,
        "query": "mixed search",
        "retailer_id": None,
    }
    assert fuzzy_cursor_relations == set()


def test_apply_plan_set_includes_availability_insert_that_regressed() -> None:
    statements = _apply_plan_statements()

    assert set(statements) == {
        "incoming_availability_change_detection",
        "incoming_availability_history_close_update",
        "incoming_availability_history_insert",
        "full_snapshot_missing_detection",
    }
    assert (
        "pg_temp.makolet_mapped_price_incoming"
        in statements["incoming_availability_history_insert"]
    )


def test_join_summary_distinguishes_index_lookup_from_repeated_large_inner_scan() -> None:
    indexed = _join_nodes(
        {
            "Node Type": "Nested Loop",
            "Plans": [
                {"Node Type": "Seq Scan", "Actual Rows": 1_000_000, "Actual Loops": 1},
                {"Node Type": "Index Scan", "Actual Rows": 1, "Actual Loops": 1_000_000},
            ],
        }
    )[0]
    repeated = _join_nodes(
        {
            "Node Type": "Nested Loop",
            "Plans": [
                {"Node Type": "Seq Scan", "Actual Rows": 100_001, "Actual Loops": 1},
                {"Node Type": "Materialize", "Actual Rows": 101, "Actual Loops": 100_001},
            ],
        }
    )[0]

    assert indexed["pathological_nested_loop"] is False
    assert repeated["pathological_nested_loop"] is True


def test_standard_plan_gate_fails_closed_on_missing_scan_and_nested_loop() -> None:
    query_plans: dict[str, object] = {
        name: {
            "sequential_scans_on_important_relations": (
                ["current_prices"] if name == "cross_store_price_comparison" else []
            )
        }
        for name in (
            "product_search",
            "barcode_lookup",
            "cross_store_price_comparison",
            "price_history",
            "promotion_history_bounded_page",
            "freshness_bounded_page",
            "fuzzy_store_cursor_page",
            "fuzzy_store_first_page",
        )
    }
    apply_plans: dict[str, object] = {
        "incoming_availability_change_detection": {
            "join_nodes": [
                {
                    "node_type": "Nested Loop",
                    "inner_actual_loops": 1_000_000,
                    "inner_actual_rows_per_loop": 100,
                    "inner_tuple_visits": 100_000_000,
                    "pathological_nested_loop": True,
                }
            ]
        },
        "incoming_availability_history_close_update": {"join_nodes": []},
        "incoming_availability_history_insert": {"join_nodes": []},
    }

    summary = _plan_gate_summary(
        query_plans=query_plans,
        apply_plans=apply_plans,
        enforced=True,
    )

    assert summary["passed"] is False
    assert summary["failure_count"] == 3
    failures = summary["failures"]
    assert isinstance(failures, list)
    assert [failure["kind"] for failure in failures] == [
        "missing_apply_plans",
        "important_permanent_relation_sequential_scan",
        "pathological_nested_loop",
    ]
    with pytest.raises(BenchmarkPlanError) as failure:
        _enforce_plan_gate(summary)
    assert failure.value.summary is summary


def test_quick_plan_gate_reports_failure_without_enforcing_it() -> None:
    summary = _plan_gate_summary(query_plans={}, apply_plans={}, enforced=False)

    assert summary["passed"] is False
    _enforce_plan_gate(summary)


async def test_cas_duplicate_probe_leaves_distinct_source_archived() -> None:
    source_file_id = UUID("018f0000-0000-7000-8000-000000000012")

    class DuplicateRepository:
        def __init__(self) -> None:
            self.transitions: list[tuple[object, object, object]] = []

        async def register_discovery(self, _remote_file: object) -> object:
            return SimpleNamespace(source_file_id=source_file_id)

        async def transition(
            self,
            observed_source_file_id: object,
            expected: object,
            target: object,
        ) -> None:
            self.transitions.append((observed_source_file_id, expected, target))

        async def record_archive(self, *args: object, **kwargs: object) -> bool:
            return True

    repository = DuplicateRepository()

    result = await _measure_duplicate_detection(
        cast(Any, repository),
        digest="a" * 64,
        logical_bytes=586,
    )

    assert repository.transitions == [
        (
            source_file_id,
            (IngestionStatus.DISCOVERED,),
            IngestionStatus.DOWNLOADING,
        )
    ]
    assert result["archive_object_reused"] is True
    assert result["source_status_after"] == "archived"
    assert "not measured" in str(result["normalized_processing"])


async def test_standard_snapshots_advance_together_and_use_per_store_minimum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_ids = [
        UUID("018f0000-0000-7000-8000-000000000020"),
        UUID("018f0000-0000-7000-8000-000000000021"),
    ]

    class SnapshotRepository:
        def __init__(self) -> None:
            self.remote_files: list[RemoteFile] = []
            self.documents: list[DocumentMetadata] = []
            self.record_counts: dict[UUID, int] = {}
            self.apply_parameters: list[dict[str, object]] = []

        async def register_discovery(self, remote_file: RemoteFile) -> object:
            source_file_id = source_ids[len(self.remote_files)]
            self.remote_files.append(remote_file)
            return SimpleNamespace(source_file_id=source_file_id)

        async def transition(self, *args: object, **kwargs: object) -> None:
            return None

        async def record_archive(self, *args: object, **kwargs: object) -> bool:
            return False

        async def clear_staging(self, _source_file_id: UUID) -> None:
            return None

        async def stage(self, _source_file_id: UUID, records: object) -> object:
            staged = tuple(cast(Any, records))
            if staged and isinstance(staged[0], DocumentMetadata):
                self.documents.append(staged[0])
                return SimpleNamespace(price_records=0)
            return SimpleNamespace(price_records=self.record_counts[_source_file_id])

        async def apply(self, *args: object, **kwargs: object) -> object:
            self.apply_parameters.append(kwargs)
            return SimpleNamespace(
                inserted=1,
                updated=0,
                unchanged=0,
                unavailable=0,
                history_events=2,
            )

    monkeypatch.setattr("benchmarks.database._require_disk_headroom", lambda: None)
    monkeypatch.setattr(
        "benchmarks.database.price_grid_record_batches",
        lambda *args, **kwargs: iter(([object()],)),
    )
    repository = SnapshotRepository()
    snapshots = (
        ("initial", _INITIAL_SNAPSHOT_TIMESTAMP, 1_000_000),
        ("reconciliation", _RECONCILIATION_TIMESTAMP, 990_000),
    )

    for label, timestamp, record_count in snapshots:
        source_file_id, _ = await _create_archived_source(
            cast(Any, repository),
            remote_id=f"price-full-{label}",
            digest_seed=label,
            logical_bytes=586,
            source_timestamp=timestamp,
        )
        repository.record_counts[source_file_id] = record_count
        await _stage_and_apply(
            cast(Any, repository),
            source_file_id,
            record_count,
            unique_products=100_000,
            maximum_drop_fraction=0.05,
            phase=label,
            source_updated_at=timestamp,
        )

    assert _INITIAL_SNAPSHOT_TIMESTAMP + timedelta(hours=1) == _RECONCILIATION_TIMESTAMP
    assert [remote.source_timestamp for remote in repository.remote_files] == [
        _INITIAL_SNAPSHOT_TIMESTAMP,
        _RECONCILIATION_TIMESTAMP,
    ]
    assert [document.source_updated_at for document in repository.documents] == [
        _INITIAL_SNAPSHOT_TIMESTAMP,
        _RECONCILIATION_TIMESTAMP,
    ]
    assert repository.apply_parameters == [
        {"minimum_full_records": 100_000, "maximum_drop_fraction": 0.05},
        {"minimum_full_records": 100_000, "maximum_drop_fraction": 0.05},
    ]


async def test_amplification_carries_and_joins_on_portal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retailer_id = UUID("018f0000-0000-7000-8000-000000000030")
    portal_id = UUID("018f0000-0000-7000-8000-000000000031")
    base_store_id = UUID("018f0000-0000-7000-8000-000000000032")
    source_file_id = UUID("018f0000-0000-7000-8000-000000000033")

    class Result:
        def __init__(
            self,
            *,
            row: tuple[UUID, UUID, UUID] | None = None,
            scalar: int | None = None,
            rowcount: int = 0,
        ) -> None:
            self._row = row
            self._scalar = scalar
            self.rowcount = rowcount

        def one(self) -> tuple[UUID, UUID, UUID]:
            assert self._row is not None
            return self._row

        def scalar_one(self) -> int:
            assert self._scalar is not None
            return self._scalar

    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        async def execute(
            self,
            statement: object,
            parameters: dict[str, object] | None = None,
        ) -> Result:
            sql = " ".join(str(statement).split())
            self.calls.append((sql, parameters or {}))
            if "SELECT retailer.id AS retailer_id" in sql:
                return Result(row=(retailer_id, portal_id, base_store_id))
            if "INSERT INTO current_prices" in sql:
                return Result(rowcount=2)
            if "SELECT count(*) FROM current_prices" in sql:
                return Result(scalar=22)
            return Result()

    class Context:
        def __init__(self, connection: Connection) -> None:
            self.connection = connection

        async def __aenter__(self) -> Connection:
            return self.connection

        async def __aexit__(self, *exc_info: object) -> None:
            return None

    class Engine:
        def __init__(self) -> None:
            self.connection = Connection()

        def begin(self) -> Context:
            return Context(self.connection)

        def connect(self) -> Context:
            return Context(self.connection)

    monkeypatch.setattr("benchmarks.database._require_disk_headroom", lambda: None)
    engine = Engine()

    result = await _amplify_current_prices(
        cast(Any, engine),
        source_file_id=source_file_id,
        store_count=11,
        ingestion_store_count=10,
        unique_products=2,
    )

    assert result["rows"] == 22
    identity_sql, identity_parameters = engine.connection.calls[0]
    assert "JOIN retailers retailer" in identity_sql
    assert "JOIN portals portal" in identity_sql
    assert identity_parameters == {
        "retailer_source_key": "benchmark-retailer",
        "portal_source_key": "benchmark-portal",
    }
    store_sql, store_parameters = engine.connection.calls[1]
    assert "INSERT INTO stores ( retailer_id, portal_id," in store_sql
    assert "SELECT :retailer_id, :portal_id," in store_sql
    assert store_parameters["portal_id"] == portal_id
    price_sql, price_parameters = engine.connection.calls[2]
    assert "target.retailer_id = base.retailer_id" in price_sql
    assert "target.portal_id = base.portal_id" in price_sql
    assert price_parameters["base_store_id"] == base_store_id


def test_isolated_standard_scenario_does_not_claim_the_unrun_workload() -> None:
    parser_only = _acceptance_metadata(PROFILES["standard"], "parser")
    full_profile = _acceptance_metadata(PROFILES["standard"], "all")

    assert parser_only["acceptance_evidence"] is False
    assert parser_only["scenario_acceptance_evidence"] == {
        "parser": True,
        "database": False,
    }
    assert full_profile["acceptance_evidence"] is True


def test_source_tree_digest_is_stable_and_excludes_generated_results(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "module.py").write_text("value = 1\n", encoding="utf-8")
    (tmp_path / "benchmarks" / "results").mkdir(parents=True)
    generated = tmp_path / "benchmarks" / "results" / "measurement.json"
    generated.write_text('{"duration": 1}\n', encoding="utf-8")

    first = _source_tree_digest(tmp_path)
    generated.write_text('{"duration": 2}\n', encoding="utf-8")
    second = _source_tree_digest(tmp_path)
    (tmp_path / "src" / "module.py").write_text("value = 2\n", encoding="utf-8")

    assert first == second
    assert _source_tree_digest(tmp_path) != first


@pytest.mark.parametrize("value", [-1, 10_000_000_000])
def test_gtin_generator_rejects_out_of_range_indexes(value: int) -> None:
    with pytest.raises(ValueError, match="GTIN synthetic index"):
        gtin14(value)
