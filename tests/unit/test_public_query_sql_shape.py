"""Static regression checks for bounded public-query SQL work."""

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.adapters.persistence import queries
from makolet.adapters.persistence.schema import (
    QUERY_PROJECTION_MAINTENANCE_DDL,
    current_availability,
    current_prices,
    price_history,
    promotions,
    stores,
)
from makolet.domain.errors import DomainValidationError, QueryLimitError


def _normalized(statement: str) -> str:
    return " ".join(statement.split())


def test_product_detail_limits_identifiers_before_json_aggregation() -> None:
    statement = _normalized(queries._PRODUCT_DETAIL_QUERY)
    bounded_start = statement.index("bounded_identifiers AS MATERIALIZED")
    bounded_limit = statement.index("LIMIT :identifier_probe_limit", bounded_start)
    summary_start = statement.index("identifier_summary AS MATERIALIZED", bounded_limit)
    aggregation = statement.index("jsonb_agg", summary_start)

    bounded_sql = statement[bounded_start:bounded_limit]
    assert "WHERE identifier.product_id = :product_id" in bounded_sql
    assert (
        "ORDER BY identifier.kind, identifier.normalized_value, "
        "identifier.issuer_retailer_id, identifier.issuer_portal_id, identifier.id" in bounded_sql
    )
    assert "count(*) > :identifier_limit AS _identifiers_overflow" in statement
    assert bounded_start < bounded_limit < summary_start < aggregation
    assert set(text(queries._PRODUCT_DETAIL_QUERY).compile().params) == {
        "identifier_limit",
        "identifier_probe_limit",
        "product_id",
    }


@pytest.mark.asyncio
async def test_product_detail_preserves_two_hundred_identifiers_and_strips_probe_state() -> None:
    repository = queries.PostgresQueryRepository(cast(AsyncEngine, object()))
    identifiers = [{"kind": "retailer_item", "value": str(index)} for index in range(200)]
    captured: list[tuple[str, dict[str, object]]] = []

    async def rows(statement: str, parameters: dict[str, object]) -> tuple[dict[str, Any], ...]:
        captured.append((statement, parameters))
        return (
            {
                "id": UUID("10000000-0000-0000-0000-000000000001"),
                "identifiers": identifiers,
                "_identifiers_overflow": False,
            },
        )

    repository._rows = rows  # type: ignore[method-assign]

    product = await repository.get_product(UUID("10000000-0000-0000-0000-000000000001"))

    assert product == {
        "id": UUID("10000000-0000-0000-0000-000000000001"),
        "identifiers": identifiers,
    }
    assert captured == [
        (
            queries._PRODUCT_DETAIL_QUERY,
            {
                "identifier_limit": 200,
                "identifier_probe_limit": 201,
                "product_id": UUID("10000000-0000-0000-0000-000000000001"),
            },
        )
    ]


@pytest.mark.asyncio
async def test_product_detail_rejects_identifier_overflow_before_returning_row() -> None:
    repository = queries.PostgresQueryRepository(cast(AsyncEngine, object()))

    async def rows(
        _statement: str,
        _parameters: dict[str, object],
    ) -> tuple[dict[str, Any], ...]:
        return (
            {
                "id": UUID("10000000-0000-0000-0000-000000000001"),
                "identifiers": [{}] * 201,
                "_identifiers_overflow": True,
            },
        )

    repository._rows = rows  # type: ignore[assignment]

    with pytest.raises(
        QueryLimitError,
        match="Product identifier count exceeds the 200-item public query limit",
    ):
        await repository.get_product(UUID("10000000-0000-0000-0000-000000000001"))


def test_promotion_history_limits_candidates_before_relation_aggregation() -> None:
    statement = _normalized(queries._PROMOTION_HISTORY_QUERY)
    candidate_start = statement.index("candidate_promotions AS MATERIALIZED")
    candidate_limit = statement.index("LIMIT :candidate_limit", candidate_start)
    page_start = statement.index("page_promotions AS MATERIALIZED", candidate_limit)
    page_limit = statement.index("LIMIT :page_limit", page_start)
    budget_start = statement.index("promotion_page_state AS MATERIALIZED", page_limit)
    relation_aggregation = statement.index("jsonb_agg", budget_start)

    time_probe = statement.index("time_promotions AS MATERIALIZED")
    probe_limit = statement.index("LIMIT :probe_limit", time_probe)
    filtered = statement.index("filtered_promotions AS MATERIALIZED", probe_limit)
    assert (
        "promotion.valid_period && tstzrange(:since, :until, '[)')"
        in statement[time_probe:probe_limit]
    )
    assert time_probe < probe_limit < filtered < candidate_start
    assert "SELECT history.id, history.valid_from" in statement[candidate_start:candidate_limit]
    assert "FROM candidate_promotions" in statement[page_start:page_limit]
    assert ":relation_page_limit" in statement[budget_start:relation_aggregation]
    assert "LEAST(count(*), :page_limit)" in statement[budget_start:relation_aggregation]
    assert candidate_limit < page_limit < budget_start < relation_aggregation
    assert statement.count("LIMIT :candidate_limit") == 1
    assert statement.count("LIMIT :page_limit") == 1
    assert (
        "FROM page_promotions candidate JOIN promotions promotion ON promotion.id = candidate.id"
    ) in statement
    assert statement.endswith("ORDER BY candidate.valid_from DESC, candidate.id")
    assert set(text(queries._PROMOTION_HISTORY_QUERY).compile().params) == {
        "candidate_limit",
        "cursor_id",
        "page_limit",
        "probe_limit",
        "relation_page_limit",
        "relation_limit",
        "relation_probe_limit",
        "since",
        "store_id",
        "until",
    }


def test_promotion_expansion_uses_one_response_wide_child_budget() -> None:
    for query in (queries._ACTIVE_PROMOTIONS_QUERY, queries._PROMOTION_HISTORY_QUERY):
        statement = _normalized(query)
        budget_start = statement.index("promotion_page_state AS MATERIALIZED")
        page_start = statement.index("FROM page_promotions candidate", budget_start)

        assert statement.count(":relation_page_limit") == 1
        assert statement.count("promotion_page_state.relation_limit") >= 9
        assert statement.count("LIMIT LEAST(") == 3
        assert "promotion_page_state.has_more AS _has_more" in statement
        assert budget_start < page_start < statement.index("jsonb_agg", page_start)


def test_fuzzy_store_search_uses_distinct_indexable_first_and_cursor_shapes() -> None:
    first_page = _normalized(queries._FUZZY_STORES_FIRST_PAGE_QUERY)
    cursor_page = _normalized(queries._FUZZY_STORES_CURSOR_QUERY)

    for statement in (first_page, cursor_page):
        candidate_start = statement.index("candidate_stores AS MATERIALIZED")
        candidate_limit = statement.index("LIMIT :candidate_probe_limit", candidate_start)
        bounded_start = statement.index("bounded_candidates AS MATERIALIZED", candidate_limit)
        bounded_limit = statement.index("LIMIT :candidate_limit", bounded_start)
        match_start = statement.index("matching_stores AS MATERIALIZED", bounded_limit)
        fuzzy_predicate = statement.index("similarity(", match_start)
        page_limit = statement.index("LIMIT :page_limit", fuzzy_predicate)

        candidate_sql = statement[candidate_start:candidate_limit]
        assert "SELECT store.id" in candidate_sql
        assert "ORDER BY store.id" in candidate_sql
        assert "similarity(" not in statement[candidate_start:match_start]
        assert candidate_limit < bounded_limit < match_start < fuzzy_predicate < page_limit

    first_candidate = first_page[
        first_page.index("candidate_stores AS MATERIALIZED") : first_page.index(
            "LIMIT :candidate_probe_limit"
        )
    ]
    cursor_candidate = cursor_page[
        cursor_page.index("candidate_stores AS MATERIALIZED") : cursor_page.index(
            "LIMIT :candidate_probe_limit"
        )
    ]
    assert ":cursor_id" not in first_page
    assert "WHERE" not in first_candidate
    assert "WHERE store.id > :cursor_id" in cursor_candidate
    assert "IS NULL OR" not in cursor_candidate
    assert set(text(queries._FUZZY_STORES_FIRST_PAGE_QUERY).compile().params) == {
        "candidate_limit",
        "candidate_probe_limit",
        "city",
        "page_limit",
        "query",
        "retailer_id",
    }
    assert set(text(queries._FUZZY_STORES_CURSOR_QUERY).compile().params) == {
        "candidate_limit",
        "candidate_probe_limit",
        "city",
        "cursor_id",
        "page_limit",
        "query",
        "retailer_id",
    }


@pytest.mark.asyncio
async def test_fuzzy_store_repository_selects_query_shape_from_cursor() -> None:
    repository = queries.PostgresQueryRepository(cast(AsyncEngine, object()))
    statements: list[tuple[str, dict[str, object]]] = []

    async def rows(statement: str, parameters: dict[str, object]) -> tuple[dict[str, Any], ...]:
        statements.append((statement, parameters))
        return (
            {
                "id": None,
                "_candidate_cursor": None,
                "_candidate_has_more": False,
            },
        )

    repository._rows = rows  # type: ignore[method-assign]

    await repository.find_stores(
        query="market",
        retailer_id=None,
        city=None,
        limit=10,
        cursor=None,
    )
    await repository.find_stores(
        query="market",
        retailer_id=None,
        city=None,
        limit=10,
        cursor="10000000-0000-0000-0000-000000000001",
    )

    assert statements[0][0] is queries._FUZZY_STORES_FIRST_PAGE_QUERY
    assert "cursor_id" not in statements[0][1]
    assert statements[1][0] is queries._FUZZY_STORES_CURSOR_QUERY
    assert str(statements[1][1]["cursor_id"]) == ("10000000-0000-0000-0000-000000000001")


def test_fuzzy_store_empty_candidate_window_keeps_a_continuation_cursor() -> None:
    candidate_cursor = "10000000-0000-0000-0000-000000000100"

    page = queries._fuzzy_store_page(
        (
            {
                "id": None,
                "_candidate_cursor": candidate_cursor,
                "_candidate_has_more": True,
            },
        ),
        10,
    )

    assert page.items == ()
    assert page.next_cursor == candidate_cursor


def test_promotion_page_strips_internal_probe_state_and_keeps_keyset_cursor() -> None:
    promotion_id = "10000000-0000-0000-0000-000000000001"

    page = queries._promotion_page(({"id": promotion_id, "_has_more": True},))

    assert page.items == ({"id": promotion_id},)
    assert page.next_cursor == promotion_id


def test_freshness_limits_each_store_before_counting_and_global_contributor_lookup() -> None:
    statement = _normalized(queries._FRESHNESS_QUERY)
    candidate_start = statement.index("candidate_stores AS MATERIALIZED")
    candidate_limit = statement.index("LIMIT :limit", candidate_start)
    probe_start = statement.index("bounded_availability AS MATERIALIZED", candidate_limit)
    probe_limit = statement.index("LIMIT :item_probe_limit", probe_start)
    count_aggregate = statement.index("count(*) FILTER", probe_limit)
    contributor_start = statement.index(
        "SELECT current.last_observed_at, current.source_file_id",
        count_aggregate,
    )
    contributor_limit = statement.index("LIMIT 1", contributor_start)

    candidate_sql = statement[candidate_start:candidate_limit]
    probe_sql = statement[probe_start:probe_limit]
    assert "SELECT store.id AS store_id" in candidate_sql
    assert "available.store_id = store.id" in candidate_sql
    assert "ORDER BY store.id" in candidate_sql
    assert "JOIN LATERAL" in probe_sql
    assert "current.store_id = candidate.store_id" in probe_sql
    assert "ORDER BY current.is_available DESC, current.retailer_item_id DESC" in probe_sql
    assert "OVER (" not in probe_sql
    assert "OVER (" not in statement
    assert candidate_limit < probe_limit < count_aggregate < contributor_start
    assert (
        "ORDER BY current.last_observed_at DESC, current.source_file_id DESC, current.id DESC"
    ) in statement[contributor_start:contributor_limit]
    assert statement.count("LIMIT :limit") == 1
    assert statement.count("LIMIT :item_probe_limit") == 1
    assert statement.count("LIMIT 1") == 1
    assert "LEAST(count(*), :item_limit) AS observed_items" in statement
    assert "AS item_probe_limit, counts.items_truncated" in statement
    assert statement.endswith("ORDER BY candidate.store_id")
    assert set(text(queries._FRESHNESS_QUERY).compile().params) == {
        "cursor_id",
        "item_limit",
        "item_probe_limit",
        "limit",
    }


def test_freshness_probes_have_exact_matching_runtime_indexes() -> None:
    indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in current_availability.indexes
        if index.name is not None
    }

    assert indexes["ix_current_availability_store_available_item"] == (
        "current_availability.store_id",
        "current_availability.is_available",
        "current_availability.retailer_item_id",
    )
    assert indexes["ix_current_availability_store_latest"] == (
        "current_availability.store_id",
        "current_availability.last_observed_at DESC",
        "current_availability.source_file_id DESC",
        "current_availability.id DESC",
    )


def test_current_prices_limit_indexed_product_candidates_before_decoration() -> None:
    first_page = _normalized(queries._CURRENT_PRICES_FIRST_PAGE_QUERY)
    cursor_page = _normalized(queries._CURRENT_PRICES_CURSOR_QUERY)

    for statement in (first_page, cursor_page):
        candidate_start = statement.index("candidate_prices AS MATERIALIZED")
        candidate_limit = statement.index("LIMIT :candidate_limit", candidate_start)
        decoration_start = statement.index(
            "FROM candidate_prices candidate JOIN current_prices price",
            candidate_limit,
        )
        candidate_sql = statement[candidate_start:candidate_limit]

        assert "price.canonical_product_id = :product_id" in candidate_sql
        assert "ORDER BY price.item_price, price.id" in candidate_sql
        assert "confirmed_product_matches" not in statement
        assert candidate_limit < decoration_start
        assert statement.endswith("ORDER BY candidate.item_price, candidate.id")

    assert ":cursor_id" not in first_page
    assert "cursor_price AS MATERIALIZED" in cursor_page
    assert "price.item_price > cursor_price.item_price" in cursor_page
    for query, required_filter in (
        (queries._CURRENT_PRICES_RETAILER_FIRST_PAGE_QUERY, "price.query_retailer_id"),
        (queries._CURRENT_PRICES_STORE_FIRST_PAGE_QUERY, "price.store_id"),
        (
            queries._CURRENT_PRICES_STORE_RETAILER_FIRST_PAGE_QUERY,
            "price.query_retailer_id",
        ),
    ):
        statement = _normalized(query)
        candidate = statement[
            statement.index("candidate_prices AS MATERIALIZED") : statement.index(
                "LIMIT :candidate_limit"
            )
        ]
        assert required_filter in candidate
        assert "IS NULL OR" not in candidate


def test_price_history_limits_globally_ordered_overlap_candidates_before_decoration() -> None:
    first_page = _normalized(queries._PRICE_HISTORY_FIRST_PAGE_QUERY)
    cursor_page = _normalized(queries._PRICE_HISTORY_CURSOR_QUERY)

    for statement in (first_page, cursor_page):
        bounded_start = statement.index("bounded_history AS MATERIALIZED")
        bounded_limit = statement.index("LIMIT :probe_limit", bounded_start)
        candidate_start = statement.index("candidate_history AS MATERIALIZED")
        candidate_limit = statement.index("LIMIT :candidate_limit", candidate_start)
        decoration_start = statement.index(
            "FROM candidate_history candidate JOIN price_history history",
            candidate_limit,
        )
        bounded_sql = statement[bounded_start:bounded_limit]
        candidate_sql = statement[candidate_start:candidate_limit]

        assert "history.canonical_product_id = :product_id" in bounded_sql
        assert "history.valid_period && tstzrange(:since, :until, '[)')" in bounded_sql
        assert "FROM bounded_history history" in candidate_sql
        assert "ORDER BY history.valid_from DESC, history.id" in candidate_sql
        assert "confirmed_product_matches" not in statement
        assert bounded_limit < candidate_start < candidate_limit < decoration_start
        assert statement.endswith("ORDER BY candidate.valid_from DESC, candidate.id")

    assert ":cursor_id" not in first_page
    assert "cursor_history AS MATERIALIZED" in cursor_page
    store_page = _normalized(queries._PRICE_HISTORY_STORE_FIRST_PAGE_QUERY)
    assert (
        "history.store_id = :store_id"
        in store_page[
            store_page.index("bounded_history AS MATERIALIZED") : store_page.index(
                "LIMIT :probe_limit"
            )
        ]
    )
    assert "IS NULL OR" not in store_page
    for probe in (
        queries._PRICE_HISTORY_PROBE_QUERY,
        queries._PRICE_HISTORY_STORE_PROBE_QUERY,
    ):
        normalized_probe = _normalized(probe)
        assert "history.valid_period && tstzrange(:since, :until, '[)')" in normalized_probe
        assert normalized_probe.endswith("LIMIT :probe_limit")


def test_item_availability_limits_indexed_product_candidates_before_decoration() -> None:
    first_page = _normalized(queries._ITEM_AVAILABILITY_FIRST_PAGE_QUERY)
    cursor_page = _normalized(queries._ITEM_AVAILABILITY_CURSOR_QUERY)

    for statement in (first_page, cursor_page):
        candidate_start = statement.index("candidate_availability AS MATERIALIZED")
        candidate_limit = statement.index("LIMIT :candidate_limit", candidate_start)
        decoration_start = statement.index(
            "FROM candidate_availability candidate JOIN current_availability availability",
            candidate_limit,
        )
        candidate_sql = statement[candidate_start:candidate_limit]

        assert "availability.canonical_product_id = :product_id" in candidate_sql
        assert "ORDER BY availability.id" in candidate_sql
        assert "confirmed_product_matches" not in statement
        assert candidate_limit < decoration_start
        assert statement.endswith("ORDER BY candidate.id")

    assert ":cursor_id" not in first_page
    assert "availability.id > :cursor_id" in cursor_page
    store_page = _normalized(queries._ITEM_AVAILABILITY_STORE_FIRST_PAGE_QUERY)
    assert "availability.store_id = :store_id" in store_page
    assert "IS NULL OR" not in store_page


def test_product_projection_indexes_match_every_public_filter_and_order_shape() -> None:
    price_indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in current_prices.indexes
        if index.name is not None
    }
    history_indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in price_history.indexes
        if index.name is not None
    }
    availability_indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in current_availability.indexes
        if index.name is not None
    }

    assert price_indexes["ix_current_prices_product_price_id"] == (
        "current_prices.canonical_product_id",
        "current_prices.item_price",
        "current_prices.id",
    )
    assert price_indexes["ix_current_prices_product_retailer_price_id"] == (
        "current_prices.canonical_product_id",
        "current_prices.query_retailer_id",
        "current_prices.item_price",
        "current_prices.id",
    )
    assert price_indexes["ix_current_prices_product_store_price_id"] == (
        "current_prices.canonical_product_id",
        "current_prices.store_id",
        "current_prices.item_price",
        "current_prices.id",
    )
    assert price_indexes["ix_current_prices_product_store_retailer_price_id"] == (
        "current_prices.canonical_product_id",
        "current_prices.store_id",
        "current_prices.query_retailer_id",
        "current_prices.item_price",
        "current_prices.id",
    )
    assert history_indexes["ix_price_history_product_from_id"] == (
        "price_history.canonical_product_id",
        "price_history.valid_from DESC",
        "price_history.id",
    )
    assert history_indexes["ix_price_history_product_store_from_id"] == (
        "price_history.canonical_product_id",
        "price_history.store_id",
        "price_history.valid_from DESC",
        "price_history.id",
    )
    assert history_indexes["ix_price_history_product_period_gist"] == (
        "price_history.canonical_product_id",
        "price_history.valid_period",
    )
    assert history_indexes["ix_price_history_product_store_period_gist"] == (
        "price_history.canonical_product_id",
        "price_history.store_id",
        "price_history.valid_period",
    )
    promotion_indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in promotions.indexes
        if index.name is not None
    }
    assert promotion_indexes["ix_promotions_valid_period_gist"] == ("promotions.valid_period",)
    assert promotion_indexes["ix_promotions_active_period_gist"] == ("promotions.active_period",)
    assert availability_indexes["ix_current_availability_product_id"] == (
        "current_availability.canonical_product_id",
        "current_availability.id",
    )
    assert availability_indexes["ix_current_availability_product_store_id"] == (
        "current_availability.canonical_product_id",
        "current_availability.store_id",
        "current_availability.id",
    )


def test_city_only_store_queries_have_dedicated_keyset_shapes_and_indexes() -> None:
    city_first = _normalized(queries._CITY_STORES_FIRST_PAGE_QUERY)
    city_cursor = _normalized(queries._CITY_STORES_CURSOR_QUERY)
    retailer_city_first = _normalized(queries._RETAILER_CITY_STORES_FIRST_PAGE_QUERY)
    retailer_city_cursor = _normalized(queries._RETAILER_CITY_STORES_CURSOR_QUERY)

    assert "store.city_search = :city" in city_first
    assert "store.id > :cursor_id" in city_cursor
    assert "retailer_id" not in city_first[city_first.index("WHERE") :]
    assert "store.retailer_id = :retailer_id" in retailer_city_first
    assert "store.id > :cursor_id" in retailer_city_cursor
    for statement in (city_first, city_cursor, retailer_city_first, retailer_city_cursor):
        assert "IS NULL OR" not in statement
        assert statement.endswith("ORDER BY store.id LIMIT :limit")

    indexes = {
        str(index.name): tuple(str(expression) for expression in index.expressions)
        for index in stores.indexes
        if index.name is not None
    }
    assert indexes["ix_stores_city_search_id"] == (
        "stores.city_search",
        "stores.id",
    )
    assert indexes["ix_stores_retailer_city_search_id"] == (
        "stores.retailer_id",
        "stores.city_search",
        "stores.id",
    )


@pytest.mark.asyncio
async def test_repository_selects_filter_specific_candidate_query_shapes() -> None:
    repository = queries.PostgresQueryRepository(cast(AsyncEngine, object()))
    statements: list[tuple[str, dict[str, object]]] = []

    async def rows(statement: str, parameters: dict[str, object]) -> tuple[dict[str, Any], ...]:
        statements.append((statement, parameters))
        return ()

    async def bounded_rows(
        _probe_statement: str,
        statement: str,
        parameters: dict[str, object],
        **_kwargs: object,
    ) -> tuple[dict[str, Any], ...]:
        statements.append((statement, parameters))
        return ()

    repository._rows = rows  # type: ignore[method-assign]
    repository._bounded_probe_rows = bounded_rows  # type: ignore[assignment]
    product_id = "10000000-0000-0000-0000-000000000001"
    retailer_id = "20000000-0000-0000-0000-000000000001"
    store_id = "30000000-0000-0000-0000-000000000001"
    cursor_id = "40000000-0000-0000-0000-000000000001"
    since = datetime(2026, 1, 1, tzinfo=UTC)
    until = datetime(2026, 2, 1, tzinfo=UTC)

    await repository.current_prices(
        UUID(product_id),
        retailer_id=None,
        store_id=None,
        limit=10,
        cursor=None,
    )
    await repository.current_prices(
        UUID(product_id),
        retailer_id=UUID(retailer_id),
        store_id=UUID(store_id),
        limit=10,
        cursor=cursor_id,
    )
    await repository.price_history(
        UUID(product_id),
        store_id=None,
        since=since,
        until=until,
        limit=10,
        cursor=None,
    )
    await repository.item_availability(
        UUID(product_id),
        store_id=UUID(store_id),
        limit=10,
        cursor=cursor_id,
    )
    await repository.find_stores(
        query=None,
        retailer_id=UUID(retailer_id),
        city="Jerusalem",
        limit=10,
        cursor=cursor_id,
    )

    assert [statement for statement, _ in statements] == [
        queries._CURRENT_PRICES_FIRST_PAGE_QUERY,
        queries._CURRENT_PRICES_STORE_RETAILER_CURSOR_QUERY,
        queries._PRICE_HISTORY_FIRST_PAGE_QUERY,
        queries._ITEM_AVAILABILITY_STORE_CURSOR_QUERY,
        queries._RETAILER_CITY_STORES_CURSOR_QUERY,
    ]
    assert statements[0][1]["candidate_limit"] == 11
    assert statements[2][1]["since"] == since
    assert statements[4][1]["city"] == "jerusalem"


@pytest.mark.asyncio
async def test_repository_rejects_unbounded_history_before_sql_execution() -> None:
    repository = queries.PostgresQueryRepository(cast(AsyncEngine, object()))

    with pytest.raises(DomainValidationError, match="required"):
        await repository.price_history(
            UUID("10000000-0000-0000-0000-000000000001"),
            store_id=None,
            since=None,
            until=None,
            limit=10,
            cursor=None,
        )


def test_metadata_create_path_installs_all_projection_maintenance_triggers() -> None:
    ddl = " ".join(QUERY_PROJECTION_MAINTENANCE_DDL)

    for object_name in (
        "trg_current_prices_project_insert",
        "trg_price_history_project_insert",
        "trg_current_availability_project_insert",
        "trg_confirmed_matches_refresh_query_projection",
        "trg_confirmed_matches_rekey_query_projection",
        "trg_confirmed_matches_clear_query_projection",
        "trg_retailer_items_refresh_query_retailer",
    ):
        assert object_name in ddl
    assert "REFERENCING NEW TABLE AS makolet_inserted_confirmed_matches" in ddl
    assert "REFERENCING OLD TABLE AS makolet_old_confirmed_matches" in ddl
    assert ddl.count("DEFERRABLE INITIALLY DEFERRED") == 1
