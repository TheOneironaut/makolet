"""Bounded, deterministic PostgreSQL query repository."""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Final
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from makolet.application.models import MAXIMUM_FRESHNESS_ITEMS_PER_STORE, Page
from makolet.application.queries import (
    MAXIMUM_PRODUCT_IDENTIFIERS,
    PRODUCT_IDENTIFIER_LIMIT_MESSAGE,
)
from makolet.domain.errors import DomainValidationError, QueryLimitError
from makolet.domain.normalization import normalize_identifier, normalize_search_text

MAXIMUM_QUERY_RESULTS: Final = 200
MAXIMUM_HISTORY_QUERY_RESULTS: Final = 1_000
MAXIMUM_HISTORY_SPAN: Final = timedelta(days=366 * 10)
MAXIMUM_QUERY_LENGTH: Final = 256
MINIMUM_SEARCH_QUERY_LENGTH: Final = 3
MAXIMUM_SEARCH_CANDIDATES: Final = 10_000
MAXIMUM_HISTORY_PROBE_RESULTS: Final = 20_000
MAXIMUM_PROMOTION_PROBE_RESULTS: Final = 20_000
# One public promotion row may contain three 10,000-character publisher fields,
# item/store names, cities, and three relation collections.  Seven entries per
# collection keep even four-byte UTF-8 values below the stricter duplicated MCP
# result envelope; the explicit truncation fields preserve lossless pagination
# semantics for the promotion itself without materializing an oversized graph.
MAXIMUM_PROMOTION_RELATIONS: Final = 7
MAXIMUM_PROMOTION_CHILDREN_PER_PAGE: Final = 21
PUBLIC_INGESTION_FAILURE_MESSAGE: Final = (
    "Ingestion did not complete; use source_file_id with operator logs for details"
)

_PROMOTION_FIELDS: Final = """
    promotion.id, promotion.retailer_id,
    retailer.source_key AS retailer_key,
    retailer.display_name AS retailer_name,
    promotion.portal_id,
    portal.source_key AS portal_key,
    promotion.subchain_code,
    promotion.source_promotion_id,
    promotion.source_scope_store_code,
    promotion.description, promotion.discount_kind,
    promotion.starts_at, promotion.ends_at,
    promotion.reward_type, promotion.allows_multiple_discounts,
    promotion.minimum_quantity, promotion.maximum_quantity,
    promotion.discount_rate, promotion.minimum_purchase,
    promotion.discounted_price, promotion.discounted_unit_price,
    promotion.minimum_items_offered,
    promotion.additional_restrictions, promotion.remarks,
    promotion.is_active, promotion.valid_from, promotion.valid_to,
    promotion.last_observed_at, promotion.source_file_id,
    source.document_type AS source_document_type,
    source.source_timestamp, source.discovered_at AS source_discovered_at,
    archive.content_sha256,
    promotion_page_state.has_more AS _has_more,
    item_relations.items,
    item_relations.returned_item_count,
    item_relations.items_truncated,
    store_relations.stores,
    store_relations.returned_store_count,
    store_relations.stores_truncated,
    club_relations.clubs,
    club_relations.returned_club_count,
    club_relations.clubs_truncated
"""

_PROMOTION_JOINS: Final = """
    JOIN retailers retailer ON retailer.id = promotion.retailer_id
    JOIN portals portal ON portal.id = promotion.portal_id
    JOIN source_files source ON source.id = promotion.source_file_id
    LEFT JOIN raw_archive_objects archive
      ON archive.id = source.raw_archive_object_id
    CROSS JOIN promotion_page_state
    LEFT JOIN LATERAL (
        SELECT COALESCE(
                   jsonb_agg(
                       jsonb_build_object(
                           'retailer_item_id', bounded.retailer_item_id,
                           'source_item_code', bounded.source_item_code,
                           'name', bounded.item_name,
                           'item_type', bounded.relation_item_type,
                           'is_gift', bounded.is_gift,
                           'canonical_product_id', bounded.canonical_product_id
                       ) ORDER BY bounded.retailer_item_id
                   ) FILTER (
                       WHERE bounded.relation_number <= promotion_page_state.relation_limit
                   ),
                   '[]'::jsonb
               ) AS items,
               count(*) FILTER (
                   WHERE bounded.relation_number <= promotion_page_state.relation_limit
               )::integer AS returned_item_count,
               COALESCE(
                    bool_or(
                        bounded.relation_number > promotion_page_state.relation_limit
                    ), false
               ) AS items_truncated
          FROM (
                SELECT item.id AS retailer_item_id,
                       item.source_item_code,
                       item.name AS item_name,
                       relation.item_type AS relation_item_type,
                       relation.is_gift,
                       match.canonical_product_id,
                       row_number() OVER (
                           ORDER BY relation.retailer_item_id
                       ) AS relation_number
                  FROM (
                        SELECT child.retailer_item_id,
                               child.item_type,
                               child.is_gift
                          FROM promotion_items child
                         WHERE child.promotion_id = promotion.id
                         ORDER BY child.retailer_item_id
                         LIMIT LEAST(
                             promotion_page_state.relation_limit + 1,
                             :relation_probe_limit
                         )
                       ) relation
                  JOIN retailer_items item
                    ON item.id = relation.retailer_item_id
                  LEFT JOIN confirmed_product_matches match
                    ON match.retailer_item_id = item.id
               ) bounded
    ) item_relations ON true
    LEFT JOIN LATERAL (
        SELECT COALESCE(
                   jsonb_agg(
                       jsonb_build_object(
                           'store_id', bounded.store_id,
                           'source_store_code', bounded.source_store_code,
                           'name', bounded.store_name,
                           'city', bounded.city
                       ) ORDER BY bounded.store_id
                   ) FILTER (
                       WHERE bounded.relation_number <= promotion_page_state.relation_limit
                   ),
                   '[]'::jsonb
               ) AS stores,
               count(*) FILTER (
                   WHERE bounded.relation_number <= promotion_page_state.relation_limit
               )::integer AS returned_store_count,
               COALESCE(
                    bool_or(
                        bounded.relation_number > promotion_page_state.relation_limit
                    ), false
               ) AS stores_truncated
          FROM (
                SELECT store.id AS store_id, store.source_store_code,
                       store.name AS store_name, store.city,
                       row_number() OVER (
                           ORDER BY relation.store_id
                       ) AS relation_number
                  FROM (
                        SELECT child.store_id
                          FROM promotion_stores child
                         WHERE child.promotion_id = promotion.id
                         ORDER BY child.store_id
                         LIMIT LEAST(
                             promotion_page_state.relation_limit + 1,
                             :relation_probe_limit
                         )
                       ) relation
                  JOIN stores store ON store.id = relation.store_id
               ) bounded
    ) store_relations ON true
    LEFT JOIN LATERAL (
        SELECT COALESCE(
                   jsonb_agg(bounded.club_id ORDER BY bounded.club_id)
                       FILTER (
                           WHERE bounded.relation_number
                               <= promotion_page_state.relation_limit
                       ),
                   '[]'::jsonb
               ) AS clubs,
               count(*) FILTER (
                   WHERE bounded.relation_number <= promotion_page_state.relation_limit
               )::integer AS returned_club_count,
               COALESCE(
                    bool_or(
                        bounded.relation_number > promotion_page_state.relation_limit
                    ), false
               ) AS clubs_truncated
          FROM (
                SELECT relation.club_id,
                       row_number() OVER (
                           ORDER BY relation.club_id
                       ) AS relation_number
                  FROM (
                        SELECT child.club_id
                          FROM promotion_clubs child
                         WHERE child.promotion_id = promotion.id
                         ORDER BY child.club_id
                         LIMIT LEAST(
                             promotion_page_state.relation_limit + 1,
                             :relation_probe_limit
                         )
                       ) relation
               ) bounded
    ) club_relations ON true
"""


def _active_promotions_query(*, product_scoped: bool) -> str:
    if product_scoped:
        bounded_base = """
    product_relationships AS MATERIALIZED (
        SELECT promotion_item.promotion_id AS id
          FROM confirmed_product_matches match
          JOIN promotion_items promotion_item
            ON promotion_item.retailer_item_id = match.retailer_item_id
         WHERE match.canonical_product_id = :product_id
         LIMIT :probe_limit
    ),
    related_promotions AS MATERIALIZED (
        SELECT DISTINCT relationship.id
          FROM product_relationships relationship
    ),
        """
    else:
        bounded_base = """
    time_promotions AS MATERIALIZED (
        SELECT promotion.id
          FROM promotions promotion
         WHERE promotion.active_period @> CAST(:at AS timestamptz)
           AND COALESCE(promotion.is_active, true)
         LIMIT :probe_limit
    ),
        """
    base_name = "related_promotions" if product_scoped else "time_promotions"
    period_filter = (
        "AND promotion.active_period @> CAST(:at AS timestamptz) "
        "AND COALESCE(promotion.is_active, true)"
        if product_scoped
        else ""
    )
    return (
        "\n    WITH "  # noqa: S608 - every fragment is a module constant.
        + bounded_base
        + f"""candidate_promotions AS MATERIALIZED (
        SELECT promotion.id
          FROM {base_name} bounded
          JOIN promotions promotion ON promotion.id = bounded.id
         WHERE true
           {period_filter}
           AND (CAST(:cursor_id AS uuid) IS NULL OR promotion.id > :cursor_id)
           AND (
               CAST(:store_id AS uuid) IS NULL
               OR NOT EXISTS (
                   SELECT 1 FROM promotion_stores any_promotion_store
                    WHERE any_promotion_store.promotion_id = promotion.id
               )
               OR EXISTS (
                   SELECT 1 FROM promotion_stores promotion_store
                    WHERE promotion_store.promotion_id = promotion.id
                      AND promotion_store.store_id = :store_id
               )
           )
         ORDER BY promotion.id
         LIMIT :candidate_limit
    ),
    page_promotions AS MATERIALIZED (
        SELECT candidate.id
          FROM candidate_promotions candidate
         ORDER BY candidate.id
         LIMIT :page_limit
    ),
    promotion_page_state AS MATERIALIZED (
        SELECT count(*) > :page_limit AS has_more,
               LEAST(
                   :relation_limit,
                   :relation_page_limit / (
                       GREATEST(LEAST(count(*), :page_limit), 1) * 3
                   )
               )::integer AS relation_limit
          FROM candidate_promotions
    )
    SELECT
    """  # noqa: S608 - every fragment is a module constant.
        + _PROMOTION_FIELDS
        + """
      FROM page_promotions candidate
      JOIN promotions promotion ON promotion.id = candidate.id
    """
        + _PROMOTION_JOINS
        + """
     ORDER BY candidate.id
    """
    )


def _promotion_history_query(*, product_scoped: bool) -> str:
    if product_scoped:
        bounded_base = """
    product_relationships AS MATERIALIZED (
        SELECT promotion_item.promotion_id AS id
          FROM confirmed_product_matches match
          JOIN promotion_items promotion_item
            ON promotion_item.retailer_item_id = match.retailer_item_id
         WHERE match.canonical_product_id = :product_id
         LIMIT :probe_limit
    ),
    related_promotions AS MATERIALIZED (
        SELECT DISTINCT relationship.id
          FROM product_relationships relationship
    ),
        """
        period_filter = "AND promotion.valid_period && tstzrange(:since, :until, '[)')"
    else:
        bounded_base = """
    time_promotions AS MATERIALIZED (
        SELECT promotion.id
          FROM promotions promotion
         WHERE promotion.valid_period && tstzrange(:since, :until, '[)')
         LIMIT :probe_limit
    ),
        """
        period_filter = ""
    base_name = "related_promotions" if product_scoped else "time_promotions"
    return (
        "\n    WITH "  # noqa: S608 - every fragment is a module constant.
        + bounded_base
        + f"""filtered_promotions AS MATERIALIZED (
        SELECT promotion.id, promotion.valid_from
          FROM {base_name} bounded
          JOIN promotions promotion ON promotion.id = bounded.id
         WHERE true
           {period_filter}
           AND (
             CAST(:store_id AS uuid) IS NULL
             OR NOT EXISTS (
                 SELECT 1 FROM promotion_stores any_promotion_store
                  WHERE any_promotion_store.promotion_id = promotion.id
             )
             OR EXISTS (
                 SELECT 1 FROM promotion_stores promotion_store
                  WHERE promotion_store.promotion_id = promotion.id
                    AND promotion_store.store_id = :store_id
             )
         )
    ),
    cursor_row AS MATERIALIZED (
        SELECT valid_from, id
          FROM filtered_promotions
         WHERE id = :cursor_id
    ),
    candidate_promotions AS MATERIALIZED (
        SELECT history.id, history.valid_from
          FROM filtered_promotions history
         WHERE CAST(:cursor_id AS uuid) IS NULL
            OR history.valid_from < (SELECT valid_from FROM cursor_row)
            OR (
                history.valid_from = (SELECT valid_from FROM cursor_row)
                AND history.id > :cursor_id
            )
         ORDER BY history.valid_from DESC, history.id
         LIMIT :candidate_limit
    ),
    page_promotions AS MATERIALIZED (
        SELECT candidate.id, candidate.valid_from
          FROM candidate_promotions candidate
         ORDER BY candidate.valid_from DESC, candidate.id
         LIMIT :page_limit
    ),
    promotion_page_state AS MATERIALIZED (
        SELECT count(*) > :page_limit AS has_more,
               LEAST(
                   :relation_limit,
                   :relation_page_limit / (
                       GREATEST(LEAST(count(*), :page_limit), 1) * 3
                   )
               )::integer AS relation_limit
          FROM candidate_promotions
    )
    SELECT
    """  # noqa: S608 - every fragment is a module constant.
        + _PROMOTION_FIELDS
        + """
      FROM page_promotions candidate
      JOIN promotions promotion ON promotion.id = candidate.id
    """
        + _PROMOTION_JOINS
        + """
     ORDER BY candidate.valid_from DESC, candidate.id
    """
    )


_ACTIVE_PROMOTIONS_QUERY: Final = _active_promotions_query(product_scoped=False)
_ACTIVE_PROMOTIONS_PRODUCT_QUERY: Final = _active_promotions_query(product_scoped=True)
_PROMOTION_HISTORY_QUERY: Final = _promotion_history_query(product_scoped=False)
_PROMOTION_HISTORY_PRODUCT_QUERY: Final = _promotion_history_query(product_scoped=True)

_ACTIVE_PROMOTIONS_TIME_PROBE_QUERY: Final = """
    SELECT promotion.id
      FROM promotions promotion
     WHERE promotion.active_period @> CAST(:at AS timestamptz)
       AND COALESCE(promotion.is_active, true)
     LIMIT :probe_limit
"""

_PROMOTION_HISTORY_TIME_PROBE_QUERY: Final = """
    SELECT promotion.id
      FROM promotions promotion
     WHERE promotion.valid_period && tstzrange(:since, :until, '[)')
     LIMIT :probe_limit
"""

_PROMOTION_PRODUCT_PROBE_QUERY: Final = """
    SELECT promotion_item.promotion_id
      FROM confirmed_product_matches match
      JOIN promotion_items promotion_item
        ON promotion_item.retailer_item_id = match.retailer_item_id
     WHERE match.canonical_product_id = :product_id
     LIMIT :probe_limit
"""


_FUZZY_STORES_QUERY_TAIL: Final = """
    ,
    bounded_candidates AS MATERIALIZED (
        SELECT candidate.id
          FROM candidate_stores candidate
         ORDER BY candidate.id
         LIMIT :candidate_limit
    ),
    candidate_state AS MATERIALIZED (
        SELECT (
                   SELECT candidate.id
                     FROM bounded_candidates candidate
                    ORDER BY candidate.id DESC
                    LIMIT 1
               )
                   AS candidate_cursor,
               count(*) > :candidate_limit AS has_more
          FROM candidate_stores
    ),
    matching_stores AS MATERIALIZED (
        SELECT store.id, store.retailer_id,
               retailer.display_name AS retailer_name,
               store.portal_id, portal.source_key AS portal_key,
               store.chain_code, store.subchain_code,
               store.source_store_code, store.name, store.address,
               store.city, store.postal_code, store.is_active,
               store.first_seen_at, store.last_seen_at,
               store.last_source_file_id
          FROM bounded_candidates candidate
          JOIN stores store ON store.id = candidate.id
          JOIN retailers retailer ON retailer.id = store.retailer_id
          JOIN portals portal ON portal.id = store.portal_id
         WHERE (
               CAST(:retailer_id AS uuid) IS NULL
               OR store.retailer_id = :retailer_id
           )
           AND (
               store.name_search LIKE '%' || CAST(:query AS text) || '%'
               OR similarity(store.name_search, CAST(:query AS text)) >= 0.2
           )
           AND (
               CAST(:city AS text) IS NULL
               OR store.city_search = CAST(:city AS text)
           )
         ORDER BY store.id
         LIMIT :page_limit
    )
    SELECT matching.id, matching.retailer_id, matching.retailer_name,
           matching.portal_id, matching.portal_key,
           matching.chain_code, matching.subchain_code,
           matching.source_store_code, matching.name, matching.address,
           matching.city, matching.postal_code, matching.is_active,
           matching.first_seen_at, matching.last_seen_at,
           matching.last_source_file_id,
           state.candidate_cursor AS _candidate_cursor,
           state.has_more AS _candidate_has_more
      FROM candidate_state state
      LEFT JOIN matching_stores matching ON true
     ORDER BY matching.id NULLS LAST
"""


_FUZZY_STORES_FIRST_PAGE_QUERY: Final = (
    """
    WITH candidate_stores AS MATERIALIZED (
        SELECT store.id
          FROM stores store
         ORDER BY store.id
         LIMIT :candidate_probe_limit
    )
    """  # noqa: S608 - both fragments are module constants.
    + _FUZZY_STORES_QUERY_TAIL
)


_FUZZY_STORES_CURSOR_QUERY: Final = (
    """
    WITH candidate_stores AS MATERIALIZED (
        SELECT store.id
          FROM stores store
         WHERE store.id > :cursor_id
         ORDER BY store.id
         LIMIT :candidate_probe_limit
    )
    """  # noqa: S608 - both fragments are module constants.
    + _FUZZY_STORES_QUERY_TAIL
)


_FRESHNESS_QUERY: Final = """
    WITH candidate_stores AS MATERIALIZED (
        SELECT store.id AS store_id
          FROM stores store
         WHERE (CAST(:cursor_id AS uuid) IS NULL OR store.id > :cursor_id)
           AND EXISTS (
               SELECT 1
                 FROM current_availability available
                WHERE available.store_id = store.id
           )
         ORDER BY store.id
         LIMIT :limit
    ),
    bounded_availability AS MATERIALIZED (
        SELECT candidate.store_id,
               availability.is_available
          FROM candidate_stores candidate
          JOIN LATERAL (
                SELECT current.is_available
                  FROM current_availability current
                 WHERE current.store_id = candidate.store_id
                 ORDER BY current.is_available DESC,
                          current.retailer_item_id DESC
                 LIMIT :item_probe_limit
               ) availability ON true
    ),
    store_counts AS (
        SELECT availability.store_id,
               LEAST(
                   count(*) FILTER (WHERE availability.is_available),
                   :item_limit
               ) AS available_items,
               LEAST(count(*), :item_limit) AS observed_items,
               count(*) > :item_limit AS items_truncated
          FROM bounded_availability availability
         GROUP BY availability.store_id
    )
    SELECT retailer.id AS retailer_id,
           retailer.source_key AS retailer_key,
           retailer.display_name AS retailer_name,
           portal.id AS portal_id, portal.source_key AS portal_key,
           store.id AS store_id, store.name AS store_name,
           contributor.last_observed_at,
           counts.available_items, counts.observed_items,
           :item_limit AS item_probe_limit, counts.items_truncated,
           contributor.source_file_id,
           source.document_type AS source_document_type,
           source.source_timestamp,
           source.discovered_at AS source_discovered_at,
           archive.content_sha256
      FROM candidate_stores candidate
      JOIN store_counts counts ON counts.store_id = candidate.store_id
      JOIN stores store ON store.id = candidate.store_id
      JOIN retailers retailer ON retailer.id = store.retailer_id
      JOIN portals portal ON portal.id = store.portal_id
      JOIN LATERAL (
            SELECT current.last_observed_at, current.source_file_id
              FROM current_availability current
             WHERE current.store_id = candidate.store_id
             ORDER BY current.last_observed_at DESC,
                      current.source_file_id DESC,
                      current.id DESC
             LIMIT 1
           ) contributor ON true
      JOIN source_files source ON source.id = contributor.source_file_id
      LEFT JOIN raw_archive_objects archive
        ON archive.id = source.raw_archive_object_id
     ORDER BY candidate.store_id
    """


_STORE_FIELDS: Final = """
    store.id, store.retailer_id, retailer.display_name AS retailer_name,
    store.portal_id, portal.source_key AS portal_key,
    store.chain_code, store.subchain_code,
    store.source_store_code, store.name, store.address,
    store.city, store.postal_code, store.is_active,
    store.first_seen_at, store.last_seen_at,
    store.last_source_file_id
"""


def _city_store_query(*, retailer_scoped: bool, cursor_page: bool) -> str:
    filters = ["store.city_search = :city"]
    if retailer_scoped:
        filters.append("store.retailer_id = :retailer_id")
    if cursor_page:
        filters.append("store.id > :cursor_id")
    predicate = "\n       AND ".join(filters)
    return (
        "\n    SELECT "  # noqa: S608 - predicates are fixed module constants.
        + _STORE_FIELDS
        + f"""
      FROM stores store
      JOIN retailers retailer ON retailer.id = store.retailer_id
      JOIN portals portal ON portal.id = store.portal_id
     WHERE {predicate}
     ORDER BY store.id
     LIMIT :limit
    """
    )


_CITY_STORES_FIRST_PAGE_QUERY: Final = _city_store_query(
    retailer_scoped=False,
    cursor_page=False,
)
_CITY_STORES_CURSOR_QUERY: Final = _city_store_query(
    retailer_scoped=False,
    cursor_page=True,
)
_RETAILER_CITY_STORES_FIRST_PAGE_QUERY: Final = _city_store_query(
    retailer_scoped=True,
    cursor_page=False,
)
_RETAILER_CITY_STORES_CURSOR_QUERY: Final = _city_store_query(
    retailer_scoped=True,
    cursor_page=True,
)


_CURRENT_PRICE_FIELDS: Final = """
    price.id, price.item_price,
    price.unit_of_measure_price, price.allow_discount,
    price.source_updated_at, price.first_observed_at,
    price.last_observed_at, price.source_file_id,
    source.document_type AS source_document_type,
    source.source_timestamp,
    source.discovered_at AS source_discovered_at,
    archive.content_sha256,
    availability.is_available, availability.item_status,
    availability.source_file_id AS availability_source_file_id,
    item.id AS retailer_item_id,
    item.source_item_code, item.name AS retailer_item_name,
    item.portal_id, portal.source_key AS portal_key,
    store.id AS store_id, store.name AS store_name,
    retailer.id AS retailer_id,
    retailer.source_key AS retailer_key,
    retailer.display_name AS retailer_name
"""


def _current_prices_query(*, scope_filter: str, cursor_page: bool) -> str:
    cursor_cte = ""
    cursor_join = ""
    cursor_predicate = ""
    if cursor_page:
        cursor_cte = f"""
    cursor_price AS MATERIALIZED (
        SELECT price.item_price, price.id
          FROM current_prices price
         WHERE price.id = :cursor_id
           AND {scope_filter}
    ),
        """  # noqa: S608 - scope_filter is a fixed module constant.
        cursor_join = "CROSS JOIN cursor_price"
        cursor_predicate = """
           AND (
               price.item_price > cursor_price.item_price
               OR (
                   price.item_price = cursor_price.item_price
                   AND price.id > cursor_price.id
               )
           )
        """
    return (
        "\n    WITH "  # noqa: S608 - all fragments are fixed module constants.
        + cursor_cte
        + f"""candidate_prices AS MATERIALIZED (
        SELECT price.id, price.item_price
          FROM current_prices price
          {cursor_join}
         WHERE {scope_filter}
          {cursor_predicate}
         ORDER BY price.item_price, price.id
         LIMIT :candidate_limit
    )
    SELECT """  # noqa: S608 - all fragments are fixed module constants.
        + _CURRENT_PRICE_FIELDS
        + """
      FROM candidate_prices candidate
      JOIN current_prices price ON price.id = candidate.id
      JOIN retailer_items item ON item.id = price.retailer_item_id
      JOIN retailers retailer ON retailer.id = item.retailer_id
      JOIN portals portal ON portal.id = item.portal_id
      JOIN source_files source ON source.id = price.source_file_id
      LEFT JOIN raw_archive_objects archive
        ON archive.id = source.raw_archive_object_id
      JOIN stores store ON store.id = price.store_id
      LEFT JOIN current_availability availability
        ON availability.retailer_item_id = item.id
       AND availability.store_id = store.id
     ORDER BY candidate.item_price, candidate.id
    """
    )


_PRICE_PRODUCT_FILTER: Final = "price.canonical_product_id = :product_id"
_PRICE_RETAILER_FILTER: Final = (
    _PRICE_PRODUCT_FILTER + " AND price.query_retailer_id = :retailer_id"
)
_PRICE_STORE_FILTER: Final = _PRICE_PRODUCT_FILTER + " AND price.store_id = :store_id"
_PRICE_STORE_RETAILER_FILTER: Final = (
    _PRICE_STORE_FILTER + " AND price.query_retailer_id = :retailer_id"
)
_CURRENT_PRICES_FIRST_PAGE_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_PRODUCT_FILTER,
    cursor_page=False,
)
_CURRENT_PRICES_CURSOR_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_PRODUCT_FILTER,
    cursor_page=True,
)
_CURRENT_PRICES_RETAILER_FIRST_PAGE_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_RETAILER_FILTER,
    cursor_page=False,
)
_CURRENT_PRICES_RETAILER_CURSOR_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_RETAILER_FILTER,
    cursor_page=True,
)
_CURRENT_PRICES_STORE_FIRST_PAGE_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_STORE_FILTER,
    cursor_page=False,
)
_CURRENT_PRICES_STORE_CURSOR_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_STORE_FILTER,
    cursor_page=True,
)
_CURRENT_PRICES_STORE_RETAILER_FIRST_PAGE_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_STORE_RETAILER_FILTER,
    cursor_page=False,
)
_CURRENT_PRICES_STORE_RETAILER_CURSOR_QUERY: Final = _current_prices_query(
    scope_filter=_PRICE_STORE_RETAILER_FILTER,
    cursor_page=True,
)


_PRICE_HISTORY_FIELDS: Final = """
    history.id, history.item_price,
    history.unit_of_measure_price,
    history.allow_discount, history.source_updated_at,
    history.valid_from, history.valid_to,
    item.id AS retailer_item_id,
    item.source_item_code,
    item.name AS retailer_item_name,
    item.portal_id, portal.source_key AS portal_key,
    retailer.id AS retailer_id,
    retailer.source_key AS retailer_key,
    retailer.display_name AS retailer_name,
    store.id AS store_id, store.name AS store_name,
    history.source_file_id,
    source.document_type AS source_document_type,
    source.source_timestamp,
    source.discovered_at AS source_discovered_at,
    archive.content_sha256
"""


def _price_history_query(*, store_scoped: bool, cursor_page: bool) -> str:
    scope_filter = "history.canonical_product_id = :product_id"
    if store_scoped:
        scope_filter += " AND history.store_id = :store_id"
    overlap_filter = """
           AND history.valid_period && tstzrange(:since, :until, '[)')
    """
    cursor_cte = ""
    cursor_join = ""
    cursor_predicate = ""
    if cursor_page:
        cursor_cte = """
    cursor_history AS MATERIALIZED (
        SELECT history.valid_from, history.id
          FROM bounded_history history
         WHERE history.id = :cursor_id
    ),
        """
        cursor_join = "CROSS JOIN cursor_history"
        cursor_predicate = """
           AND (
               history.valid_from < cursor_history.valid_from
               OR (
                   history.valid_from = cursor_history.valid_from
                   AND history.id > cursor_history.id
               )
           )
        """
    return (
        "\n    WITH "  # noqa: S608 - all fragments are fixed module constants.
        + f"""bounded_history AS MATERIALIZED (
        SELECT history.id, history.valid_from
          FROM price_history history
         WHERE {scope_filter}
          {overlap_filter}
         LIMIT :probe_limit
    ),
    """  # noqa: S608 - scope_filter is a fixed module constant.
        + cursor_cte
        + f"""candidate_history AS MATERIALIZED (
        SELECT history.id, history.valid_from
          FROM bounded_history history
          {cursor_join}
         WHERE true
           {cursor_predicate}
         ORDER BY history.valid_from DESC, history.id
         LIMIT :candidate_limit
    )
    SELECT """  # noqa: S608 - all fragments are fixed module constants.
        + _PRICE_HISTORY_FIELDS
        + """
      FROM candidate_history candidate
      JOIN price_history history ON history.id = candidate.id
      JOIN retailer_items item ON item.id = history.retailer_item_id
      JOIN retailers retailer ON retailer.id = item.retailer_id
      JOIN portals portal ON portal.id = item.portal_id
      JOIN stores store ON store.id = history.store_id
      JOIN source_files source ON source.id = history.source_file_id
      LEFT JOIN raw_archive_objects archive
        ON archive.id = source.raw_archive_object_id
     ORDER BY candidate.valid_from DESC, candidate.id
    """
    )


_PRICE_HISTORY_FIRST_PAGE_QUERY: Final = _price_history_query(
    store_scoped=False,
    cursor_page=False,
)
_PRICE_HISTORY_CURSOR_QUERY: Final = _price_history_query(
    store_scoped=False,
    cursor_page=True,
)
_PRICE_HISTORY_STORE_FIRST_PAGE_QUERY: Final = _price_history_query(
    store_scoped=True,
    cursor_page=False,
)
_PRICE_HISTORY_STORE_CURSOR_QUERY: Final = _price_history_query(
    store_scoped=True,
    cursor_page=True,
)


def _price_history_probe_query(*, store_scoped: bool) -> str:
    store_filter = "AND history.store_id = :store_id" if store_scoped else ""
    return f"""
    SELECT history.id
      FROM price_history history
     WHERE history.canonical_product_id = :product_id
       {store_filter}
       AND history.valid_period && tstzrange(:since, :until, '[)')
     LIMIT :probe_limit
    """  # noqa: S608 - store_filter is a fixed module-owned fragment.


_PRICE_HISTORY_PROBE_QUERY: Final = _price_history_probe_query(store_scoped=False)
_PRICE_HISTORY_STORE_PROBE_QUERY: Final = _price_history_probe_query(store_scoped=True)


_ITEM_AVAILABILITY_FIELDS: Final = """
    availability.id, availability.is_available,
    availability.item_status, availability.last_observed_at,
    item.id AS retailer_item_id, item.source_item_code,
    item.name AS retailer_item_name,
    item.portal_id, portal.source_key AS portal_key,
    retailer.id AS retailer_id,
    retailer.source_key AS retailer_key,
    retailer.display_name AS retailer_name,
    store.id AS store_id, store.name AS store_name,
    availability.source_file_id,
    source.document_type AS source_document_type,
    source.source_timestamp,
    source.discovered_at AS source_discovered_at,
    archive.content_sha256
"""


def _item_availability_query(*, store_scoped: bool, cursor_page: bool) -> str:
    filters = ["availability.canonical_product_id = :product_id"]
    if store_scoped:
        filters.append("availability.store_id = :store_id")
    if cursor_page:
        filters.append("availability.id > :cursor_id")
    predicate = "\n           AND ".join(filters)
    return (
        f"""
    WITH candidate_availability AS MATERIALIZED (
        SELECT availability.id
          FROM current_availability availability
         WHERE {predicate}
         ORDER BY availability.id
         LIMIT :candidate_limit
    )
    SELECT """  # noqa: S608 - predicates are fixed module constants.
        + _ITEM_AVAILABILITY_FIELDS
        + """
      FROM candidate_availability candidate
      JOIN current_availability availability ON availability.id = candidate.id
      JOIN retailer_items item ON item.id = availability.retailer_item_id
      JOIN retailers retailer ON retailer.id = item.retailer_id
      JOIN portals portal ON portal.id = item.portal_id
      JOIN stores store ON store.id = availability.store_id
      JOIN source_files source ON source.id = availability.source_file_id
      LEFT JOIN raw_archive_objects archive
        ON archive.id = source.raw_archive_object_id
     ORDER BY candidate.id
    """
    )


_ITEM_AVAILABILITY_FIRST_PAGE_QUERY: Final = _item_availability_query(
    store_scoped=False,
    cursor_page=False,
)
_ITEM_AVAILABILITY_CURSOR_QUERY: Final = _item_availability_query(
    store_scoped=False,
    cursor_page=True,
)
_ITEM_AVAILABILITY_STORE_FIRST_PAGE_QUERY: Final = _item_availability_query(
    store_scoped=True,
    cursor_page=False,
)
_ITEM_AVAILABILITY_STORE_CURSOR_QUERY: Final = _item_availability_query(
    store_scoped=True,
    cursor_page=True,
)


_PRODUCT_DETAIL_QUERY: Final = """
    WITH bounded_identifiers AS MATERIALIZED (
        SELECT identifier.id, identifier.kind, identifier.value,
               identifier.normalized_value, identifier.is_validated,
               identifier.issuer_retailer_id,
               issuer_retailer.source_key AS issuer_retailer_key,
               identifier.issuer_portal_id,
               issuer_portal.source_key AS issuer_portal_key,
               identifier.validation_method,
               identifier.validation_evidence
          FROM product_identifiers identifier
          LEFT JOIN retailers issuer_retailer
            ON issuer_retailer.id = identifier.issuer_retailer_id
          LEFT JOIN portals issuer_portal
            ON issuer_portal.id = identifier.issuer_portal_id
         WHERE identifier.product_id = :product_id
         ORDER BY identifier.kind, identifier.normalized_value,
                  identifier.issuer_retailer_id, identifier.issuer_portal_id,
                  identifier.id
         LIMIT :identifier_probe_limit
    ),
    identifier_summary AS MATERIALIZED (
        SELECT COALESCE(
                   jsonb_agg(
                       jsonb_build_object(
                           'kind', identifier.kind,
                           'value', identifier.value,
                           'validated', identifier.is_validated,
                           'issuer_retailer_id', identifier.issuer_retailer_id,
                           'issuer_retailer_key', identifier.issuer_retailer_key,
                           'issuer_portal_id', identifier.issuer_portal_id,
                           'issuer_portal_key', identifier.issuer_portal_key,
                           'validation_method', identifier.validation_method,
                           'validation_evidence', identifier.validation_evidence
                       ) ORDER BY identifier.kind,
                                  identifier.normalized_value,
                                  identifier.issuer_retailer_id,
                                  identifier.issuer_portal_id,
                                  identifier.id
                   ),
                   '[]'::jsonb
               ) AS identifiers,
               count(*) > :identifier_limit AS _identifiers_overflow
          FROM bounded_identifiers identifier
    )
    SELECT product.id, product.name, product.brand,
           product.manufacturer, product.quantity,
           product.unit_of_measure, product.status,
           product.created_at, product.updated_at,
           identifier_summary.identifiers,
           identifier_summary._identifiers_overflow
      FROM canonical_products product
      CROSS JOIN identifier_summary
     WHERE product.id = :product_id
"""


_PRODUCT_SEARCH_QUERY: Final = """
    WITH prefix_candidates AS MATERIALIZED (
        SELECT product.id
          FROM canonical_products product
         WHERE product.status = 'active'
           AND product.name_search LIKE :query || '%'
         ORDER BY product.name_search, product.id
         LIMIT :candidate_limit
    ),
    trigram_candidates AS MATERIALIZED (
        SELECT product.id
          FROM canonical_products product
         WHERE product.status = 'active'
           AND product.name_search % :query
         ORDER BY product.name_search <-> :query, product.id
         LIMIT :candidate_limit
    ),
    retailer_alias_candidates AS MATERIALIZED (
        SELECT match.canonical_product_id AS id,
               max(similarity(alias.name_search, :query))
                   AS alias_similarity,
               bool_or(
                   CAST(:query_quantity AS numeric) IS NOT NULL
                   AND CAST(:query_unit AS text) IS NOT NULL
                   AND alias.normalized_quantity = :query_quantity
                   AND alias.normalized_unit = :query_unit
               ) AS structured_quantity_match
          FROM (
                SELECT item.id, item.name_search, item.quantity,
                       item.unit_of_measure,
                       CASE
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'kg', 'ק"ג', 'ק״ג', 'קג',
                               'קילוגרם', 'קילוגרמים'
                           ) THEN item.quantity * 1000
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'g', 'גרם', 'גרמים'
                           ) THEN item.quantity
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'l', 'liter', 'litre', 'ליטר', 'ליטרים'
                           ) THEN item.quantity * 1000
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'ml', 'מ"ל', 'מ״ל', 'מל',
                               'מיליליטר', 'מיליליטרים'
                           ) THEN item.quantity
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'ea', 'each', 'יחידה', 'יחידות', 'יח'''
                           ) THEN item.quantity
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'm', 'מטר', 'מטרים'
                           ) THEN item.quantity
                           ELSE NULL
                       END AS normalized_quantity,
                       CASE
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'kg', 'ק"ג', 'ק״ג', 'קג',
                               'קילוגרם', 'קילוגרמים',
                               'g', 'גרם', 'גרמים'
                           ) THEN 'g'
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'l', 'liter', 'litre', 'ליטר', 'ליטרים',
                               'ml', 'מ"ל', 'מ״ל', 'מל',
                               'מיליליטר', 'מיליליטרים'
                           ) THEN 'ml'
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'ea', 'each', 'יחידה', 'יחידות', 'יח'''
                           ) THEN 'each'
                           WHEN lower(trim(item.unit_of_measure)) IN (
                               'm', 'מטר', 'מטרים'
                           ) THEN 'm'
                           ELSE NULL
                       END AS normalized_unit
                  FROM retailer_items item
                 WHERE item.name_search % :query
                 ORDER BY item.name_search <-> :query, item.id
                 LIMIT :candidate_limit
               ) alias
          JOIN confirmed_product_matches match
            ON match.retailer_item_id = alias.id
         GROUP BY match.canonical_product_id
    ),
    identifier_candidates AS MATERIALIZED (
        SELECT identifier.product_id AS id,
               bool_or(
                   identifier.issuer_retailer_id IS NULL
                   AND identifier.is_validated
               ) AS globally_validated
          FROM product_identifiers identifier
         WHERE identifier.kind = 'gtin'
           AND identifier.normalized_value = :query
           AND (
               identifier.issuer_retailer_id IS NOT NULL
               OR identifier.is_validated
           )
         GROUP BY identifier.product_id
    ),
    candidates AS MATERIALIZED (
        SELECT id FROM prefix_candidates
        UNION
        SELECT id FROM trigram_candidates
        UNION
        SELECT id FROM retailer_alias_candidates
        UNION
        SELECT id FROM identifier_candidates
    ),
    ranked AS (
        SELECT product.id, product.name, product.brand,
               product.manufacturer, product.quantity,
               product.unit_of_measure,
               CASE
                   WHEN identifier.globally_validated THEN 100.0
                   WHEN identifier.id IS NOT NULL THEN 90.0
                   WHEN product.name_search = :query THEN 50.0
                   WHEN product.name_search LIKE :query || '%' THEN
                        20.0 + similarity(product.name_search, :query)
                   ELSE GREATEST(
                       similarity(product.name_search, :query),
                       10.0 + COALESCE(alias.alias_similarity, 0.0)
                   )
               END
               + CASE WHEN alias.structured_quantity_match
                      THEN 2.0 ELSE 0.0 END AS rank
          FROM candidates candidate
          JOIN canonical_products product ON product.id = candidate.id
          LEFT JOIN identifier_candidates identifier
            ON identifier.id = product.id
          LEFT JOIN retailer_alias_candidates alias
            ON alias.id = product.id
    ),
    cursor_row AS (
        SELECT rank, id FROM ranked WHERE id = :cursor_id
    )
    SELECT ranked.id, ranked.name, ranked.brand, ranked.manufacturer,
           ranked.quantity, ranked.unit_of_measure, ranked.rank
      FROM ranked
     WHERE CAST(:cursor_id AS uuid) IS NULL
        OR ranked.rank < (SELECT rank FROM cursor_row)
        OR (
            ranked.rank = (SELECT rank FROM cursor_row)
            AND ranked.id > :cursor_id
        )
     ORDER BY ranked.rank DESC, ranked.id
     LIMIT :limit
    """


class PostgresQueryRepository:
    """Serve public reads without exposing unbounded scans or unstable ordering."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def list_retailers(
        self,
        *,
        limit: int,
        cursor: str | None,
    ) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            """
            SELECT id, source_key, legal_name, display_name, edi, is_active,
                   created_at, updated_at
              FROM retailers
             WHERE (CAST(:cursor_id AS uuid) IS NULL OR id > :cursor_id)
             ORDER BY id
             LIMIT :limit
            """,
            {"cursor_id": cursor_id, "limit": bounded_limit + 1},
        )
        return _page(rows, bounded_limit, cursor_key="id")

    async def find_stores(
        self,
        *,
        query: str | None,
        retailer_id: UUID | None,
        city: str | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        normalized_query = _optional_search_query(query)
        normalized_city = _optional_search_query(city)
        if normalized_query is not None:
            fuzzy_query = (
                _FUZZY_STORES_FIRST_PAGE_QUERY if cursor_id is None else _FUZZY_STORES_CURSOR_QUERY
            )
            fuzzy_parameters: dict[str, object] = {
                "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
                "candidate_probe_limit": MAXIMUM_SEARCH_CANDIDATES + 1,
                "city": normalized_city,
                "page_limit": bounded_limit + 1,
                "query": normalized_query,
                "retailer_id": retailer_id,
            }
            if cursor_id is not None:
                fuzzy_parameters["cursor_id"] = cursor_id
            rows = await self._rows(
                fuzzy_query,
                fuzzy_parameters,
            )
            return _fuzzy_store_page(rows, bounded_limit)
        if normalized_city is not None:
            if retailer_id is None:
                city_query = (
                    _CITY_STORES_FIRST_PAGE_QUERY
                    if cursor_id is None
                    else _CITY_STORES_CURSOR_QUERY
                )
            else:
                city_query = (
                    _RETAILER_CITY_STORES_FIRST_PAGE_QUERY
                    if cursor_id is None
                    else _RETAILER_CITY_STORES_CURSOR_QUERY
                )
            city_parameters: dict[str, object] = {
                "city": normalized_city,
                "limit": bounded_limit + 1,
            }
            if retailer_id is not None:
                city_parameters["retailer_id"] = retailer_id
            if cursor_id is not None:
                city_parameters["cursor_id"] = cursor_id
            rows = await self._rows(city_query, city_parameters)
            return _page(rows, bounded_limit, cursor_key="id")
        rows = await self._rows(
            """
            SELECT store.id, store.retailer_id, retailer.display_name AS retailer_name,
                   store.portal_id, portal.source_key AS portal_key,
                   store.chain_code, store.subchain_code,
                   store.source_store_code, store.name, store.address,
                   store.city, store.postal_code, store.is_active,
                   store.first_seen_at, store.last_seen_at,
                   store.last_source_file_id
              FROM stores store
              JOIN retailers retailer ON retailer.id = store.retailer_id
              JOIN portals portal ON portal.id = store.portal_id
             WHERE (CAST(:cursor_id AS uuid) IS NULL OR store.id > :cursor_id)
               AND (
                   CAST(:retailer_id AS uuid) IS NULL
                   OR store.retailer_id = :retailer_id
               )
             ORDER BY store.id
             LIMIT :limit
            """,
            {
                "cursor_id": cursor_id,
                "retailer_id": retailer_id,
                "limit": bounded_limit + 1,
            },
        )
        return _page(rows, bounded_limit, cursor_key="id")

    async def search_products(
        self,
        query: str,
        *,
        quantity: Decimal | None,
        unit: str | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        bounded_limit = _bounded_limit(limit)
        normalized_query = _required_search_query(query)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            _PRODUCT_SEARCH_QUERY,
            {
                "query": normalized_query,
                "query_quantity": quantity,
                "query_unit": unit,
                "cursor_id": cursor_id,
                "limit": bounded_limit + 1,
                "candidate_limit": MAXIMUM_SEARCH_CANDIDATES,
            },
        )
        return _page(rows, bounded_limit, cursor_key="id")

    async def get_product(self, product_id: UUID) -> dict[str, object] | None:
        rows = await self._rows(
            _PRODUCT_DETAIL_QUERY,
            {
                "identifier_limit": MAXIMUM_PRODUCT_IDENTIFIERS,
                "identifier_probe_limit": MAXIMUM_PRODUCT_IDENTIFIERS + 1,
                "product_id": product_id,
            },
        )
        if not rows:
            return None
        product = rows[0]
        identifiers_overflow = bool(product.pop("_identifiers_overflow"))
        if identifiers_overflow:
            raise QueryLimitError(PRODUCT_IDENTIFIER_LIMIT_MESSAGE)
        return product

    async def find_product_by_barcode(self, barcode: str) -> dict[str, object] | None:
        normalized = normalize_identifier(barcode)
        rows = await self._rows(
            """
            WITH matches AS (
                SELECT product.id, product.name, product.brand,
                       product.manufacturer, product.quantity,
                       product.unit_of_measure,
                       bool_or(
                           identifier.issuer_retailer_id IS NULL
                           AND identifier.is_validated
                       ) AS globally_validated,
                       bool_or(identifier.is_validated) AS barcode_validated,
                       jsonb_agg(
                           jsonb_build_object(
                               'identifier_id', identifier.id,
                               'value', identifier.value,
                               'validated', identifier.is_validated,
                               'issuer_retailer_id', identifier.issuer_retailer_id,
                               'issuer_retailer_key', issuer_retailer.source_key,
                               'issuer_portal_id', identifier.issuer_portal_id,
                               'issuer_portal_key', issuer_portal.source_key,
                               'validation_method', identifier.validation_method,
                               'validation_evidence', identifier.validation_evidence
                           ) ORDER BY identifier.issuer_retailer_id,
                                      identifier.issuer_portal_id,
                                      identifier.id
                       ) AS identifier_provenance
                  FROM product_identifiers identifier
                  JOIN canonical_products product
                    ON product.id = identifier.product_id
                  LEFT JOIN retailers issuer_retailer
                    ON issuer_retailer.id = identifier.issuer_retailer_id
                  LEFT JOIN portals issuer_portal
                    ON issuer_portal.id = identifier.issuer_portal_id
                 WHERE identifier.kind = 'gtin'
                   AND identifier.normalized_value = :barcode
                   AND (
                       identifier.issuer_retailer_id IS NOT NULL
                       OR identifier.is_validated
                   )
                   AND product.status = 'active'
                 GROUP BY product.id
            )
            SELECT matches.*,
                   :barcode AS barcode,
                   CASE WHEN matches.globally_validated
                        THEN 'global'
                        ELSE 'portal_asserted'
                   END AS identifier_scope
              FROM matches
             ORDER BY matches.globally_validated DESC, matches.id
             LIMIT 2
            """,
            {"barcode": normalized},
        )
        if not rows:
            return None
        if len(rows) > 1 and not bool(rows[0]["globally_validated"]):
            raise DomainValidationError(
                "Barcode resolves to multiple provisional products; use product search "
                "and issuer portal provenance to disambiguate"
            )
        return rows[0]

    async def find_product_by_retailer_item_code(
        self,
        retailer_id: UUID,
        item_code: str,
        *,
        portal_id: UUID | None = None,
    ) -> dict[str, object] | None:
        normalized = normalize_identifier(item_code)
        rows = await self._rows(
            """
            SELECT product.id, product.name, product.brand,
                   product.manufacturer, product.quantity,
                   product.unit_of_measure,
                   item.id AS retailer_item_id,
                   item.source_item_code AS retailer_item_code,
                   item.name AS retailer_item_name,
                   item.last_source_file_id AS retailer_item_source_file_id,
                   retailer.id AS retailer_id,
                   retailer.source_key AS retailer_key,
                   retailer.display_name AS retailer_name,
                   portal.id AS portal_id,
                   portal.source_key AS portal_key,
                   match.method AS match_method,
                   match.evidence AS match_evidence
              FROM retailer_items item
              JOIN retailers retailer ON retailer.id = item.retailer_id
              JOIN portals portal ON portal.id = item.portal_id
              JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
              JOIN canonical_products product
                ON product.id = match.canonical_product_id
             WHERE item.retailer_id = :retailer_id
               AND (
                   CAST(:portal_id AS uuid) IS NULL
                   OR item.portal_id = :portal_id
               )
               AND item.source_item_code = :item_code
               AND product.status = 'active'
             ORDER BY portal.id, product.id
             LIMIT 2
            """,
            {
                "retailer_id": retailer_id,
                "portal_id": portal_id,
                "item_code": normalized,
            },
        )
        if len(rows) > 1:
            raise DomainValidationError(
                "Retailer item code is ambiguous across source portals; provide portal_id"
            )
        return rows[0] if rows else None

    async def current_prices(
        self,
        product_id: UUID,
        *,
        retailer_id: UUID | None,
        store_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        if store_id is not None and retailer_id is not None:
            statement = (
                _CURRENT_PRICES_STORE_RETAILER_FIRST_PAGE_QUERY
                if cursor_id is None
                else _CURRENT_PRICES_STORE_RETAILER_CURSOR_QUERY
            )
        elif store_id is not None:
            statement = (
                _CURRENT_PRICES_STORE_FIRST_PAGE_QUERY
                if cursor_id is None
                else _CURRENT_PRICES_STORE_CURSOR_QUERY
            )
        elif retailer_id is not None:
            statement = (
                _CURRENT_PRICES_RETAILER_FIRST_PAGE_QUERY
                if cursor_id is None
                else _CURRENT_PRICES_RETAILER_CURSOR_QUERY
            )
        else:
            statement = (
                _CURRENT_PRICES_FIRST_PAGE_QUERY
                if cursor_id is None
                else _CURRENT_PRICES_CURSOR_QUERY
            )
        parameters: dict[str, object] = {
            "product_id": product_id,
            "candidate_limit": bounded_limit + 1,
        }
        if retailer_id is not None:
            parameters["retailer_id"] = retailer_id
        if store_id is not None:
            parameters["store_id"] = store_id
        if cursor_id is not None:
            parameters["cursor_id"] = cursor_id
        rows = await self._rows(statement, parameters)
        return _page(rows, bounded_limit, cursor_key="id")

    async def price_history(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        _validate_time_range(since, until)
        bounded_limit = _bounded_limit(limit, maximum=MAXIMUM_HISTORY_QUERY_RESULTS)
        cursor_id = _uuid_cursor(cursor)
        if store_id is None:
            statement = (
                _PRICE_HISTORY_FIRST_PAGE_QUERY
                if cursor_id is None
                else _PRICE_HISTORY_CURSOR_QUERY
            )
        else:
            statement = (
                _PRICE_HISTORY_STORE_FIRST_PAGE_QUERY
                if cursor_id is None
                else _PRICE_HISTORY_STORE_CURSOR_QUERY
            )
        parameters: dict[str, object] = {
            "product_id": product_id,
            "since": since,
            "until": until,
            "candidate_limit": bounded_limit + 1,
        }
        if store_id is not None:
            parameters["store_id"] = store_id
        if cursor_id is not None:
            parameters["cursor_id"] = cursor_id
        rows = await self._bounded_probe_rows(
            (_PRICE_HISTORY_PROBE_QUERY if store_id is None else _PRICE_HISTORY_STORE_PROBE_QUERY),
            statement,
            parameters,
            maximum_probe_results=MAXIMUM_HISTORY_PROBE_RESULTS,
            resource_name="price history",
        )
        return _page(rows, bounded_limit, cursor_key="id")

    async def active_promotions(
        self,
        *,
        product_id: UUID | None,
        store_id: UUID | None,
        at: datetime,
        limit: int,
        cursor: str | None,
    ) -> Page:
        if at.tzinfo is None or at.utcoffset() is None:
            raise DomainValidationError("at must include a timezone")
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        statement = (
            _ACTIVE_PROMOTIONS_QUERY if product_id is None else _ACTIVE_PROMOTIONS_PRODUCT_QUERY
        )
        probe_statement = (
            _ACTIVE_PROMOTIONS_TIME_PROBE_QUERY
            if product_id is None
            else _PROMOTION_PRODUCT_PROBE_QUERY
        )
        rows = await self._bounded_probe_rows(
            probe_statement,
            statement,
            {
                "product_id": product_id,
                "store_id": store_id,
                "at": at,
                "cursor_id": cursor_id,
                "candidate_limit": bounded_limit + 1,
                "page_limit": bounded_limit,
                "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
                "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
                "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
            },
            maximum_probe_results=MAXIMUM_PROMOTION_PROBE_RESULTS,
            resource_name="active promotion",
        )
        return _promotion_page(rows)

    async def promotion_history(
        self,
        *,
        product_id: UUID | None,
        store_id: UUID | None,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        _validate_time_range(since, until)
        bounded_limit = _bounded_limit(limit, maximum=MAXIMUM_HISTORY_QUERY_RESULTS)
        cursor_id = _uuid_cursor(cursor)
        statement = (
            _PROMOTION_HISTORY_QUERY if product_id is None else _PROMOTION_HISTORY_PRODUCT_QUERY
        )
        probe_statement = (
            _PROMOTION_HISTORY_TIME_PROBE_QUERY
            if product_id is None
            else _PROMOTION_PRODUCT_PROBE_QUERY
        )
        rows = await self._bounded_probe_rows(
            probe_statement,
            statement,
            {
                "product_id": product_id,
                "store_id": store_id,
                "since": since,
                "until": until,
                "cursor_id": cursor_id,
                "candidate_limit": bounded_limit + 1,
                "page_limit": bounded_limit,
                "relation_page_limit": MAXIMUM_PROMOTION_CHILDREN_PER_PAGE,
                "relation_limit": MAXIMUM_PROMOTION_RELATIONS,
                "relation_probe_limit": MAXIMUM_PROMOTION_RELATIONS + 1,
            },
            maximum_probe_results=MAXIMUM_PROMOTION_PROBE_RESULTS,
            resource_name="promotion history",
        )
        return _promotion_page(rows)

    async def item_availability(
        self,
        product_id: UUID,
        *,
        store_id: UUID | None,
        limit: int,
        cursor: str | None,
    ) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        if store_id is None:
            statement = (
                _ITEM_AVAILABILITY_FIRST_PAGE_QUERY
                if cursor_id is None
                else _ITEM_AVAILABILITY_CURSOR_QUERY
            )
        else:
            statement = (
                _ITEM_AVAILABILITY_STORE_FIRST_PAGE_QUERY
                if cursor_id is None
                else _ITEM_AVAILABILITY_STORE_CURSOR_QUERY
            )
        parameters: dict[str, object] = {
            "product_id": product_id,
            "candidate_limit": bounded_limit + 1,
        }
        if store_id is not None:
            parameters["store_id"] = store_id
        if cursor_id is not None:
            parameters["cursor_id"] = cursor_id
        rows = await self._rows(statement, parameters)
        return _page(rows, bounded_limit, cursor_key="id")

    async def freshness(self, *, limit: int, cursor: str | None = None) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            _FRESHNESS_QUERY,
            {
                "cursor_id": cursor_id,
                "limit": bounded_limit + 1,
                "item_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE,
                "item_probe_limit": MAXIMUM_FRESHNESS_ITEMS_PER_STORE + 1,
            },
        )
        return _page(rows, bounded_limit, cursor_key="store_id")

    async def source_status(self, *, limit: int, cursor: str | None = None) -> Page:
        bounded_limit = _bounded_limit(limit)
        cursor_id = _uuid_cursor(cursor)
        rows = await self._rows(
            """
            SELECT portal.id AS portal_id, portal.source_key AS portal_key,
                   portal.family, portal.protocol,
                   retailer.id AS retailer_id,
                   retailer.display_name AS retailer_name,
                   source.id AS source_file_id, source.status,
                   source.document_type, source.source_timestamp,
                   source.discovered_at, source.updated_at,
                    source.error_code, source.warning_count,
                    source.record_rejection_count,
                    source.file_quarantine_count,
                   source.source_failure_count,
                   source.system_failure_count,
                   CASE WHEN source.error_code IS NULL THEN NULL
                         ELSE :public_error_message
                    END AS error_message,
                   last_good.id AS last_good_source_file_id,
                   last_good.document_type AS last_good_document_type,
                   last_good.source_timestamp AS last_good_source_timestamp,
                   last_good.discovered_at AS last_good_discovered_at,
                   last_good.updated_at AS last_good_updated_at,
                   collection.attempt_id AS collection_attempt_id,
                   collection.attempt_status AS collection_attempt_status,
                   collection.operation AS collection_operation,
                   collection.generation AS collection_generation,
                   collection.range_since AS collection_range_since,
                   collection.range_until AS collection_range_until,
                   collection.archive_only AS collection_archive_only,
                   collection.started_at AS collection_started_at,
                   collection.finished_at AS collection_finished_at,
                   collection.discovered_count AS collection_discovered_count,
                   collection.processed_count AS collection_processed_count,
                   collection.skipped_unknown_count AS collection_skipped_unknown_count,
                   collection.warning_count AS collection_warning_count,
                   collection.charged_bytes AS collection_charged_bytes,
                   collection.truncated AS collection_truncated,
                   collection.truncation_reason AS collection_truncation_reason,
                   collection.error_code AS collection_error_code,
                   CASE WHEN collection.error_code IS NULL THEN NULL
                        ELSE :public_collection_error_message
                    END AS collection_error_message
              FROM portals portal
              JOIN retailers retailer ON retailer.id = portal.retailer_id
              LEFT JOIN LATERAL (
                  SELECT candidate.id, candidate.status,
                         candidate.document_type, candidate.source_timestamp,
                         candidate.discovered_at, candidate.updated_at,
                         candidate.error_code,
                         COALESCE(quality.warning_count, 0) AS warning_count,
                         COALESCE(quality.record_rejection_count, 0)
                             AS record_rejection_count,
                         GREATEST(
                             COALESCE(quality.file_quarantine_count, 0),
                             CASE WHEN candidate.status = 'quarantined' THEN 1 ELSE 0 END
                         ) AS file_quarantine_count,
                         CASE
                             WHEN candidate.status IN ('failed_retryable', 'failed_terminal')
                              AND candidate.error_code IN (
                                  'source_access_error', 'source_response_error',
                                  'source_blocked', 'unsafe_remote',
                                  'download_limit_exceeded'
                              ) THEN 1 ELSE 0
                         END AS source_failure_count,
                         CASE
                             WHEN candidate.status IN ('failed_retryable', 'failed_terminal')
                              AND candidate.error_code NOT IN (
                                  'source_access_error', 'source_response_error',
                                  'source_blocked', 'unsafe_remote',
                                  'download_limit_exceeded'
                              ) THEN 1 ELSE 0
                         END AS system_failure_count
                    FROM source_files candidate
                    LEFT JOIN LATERAL (
                        SELECT run.warnings AS warning_count,
                               run.rejected_records AS record_rejection_count,
                               run.file_quarantine_issues AS file_quarantine_count
                          FROM ingestion_runs run
                         WHERE run.source_file_id = candidate.id
                         ORDER BY run.attempt DESC
                         LIMIT 1
                    ) quality ON true
                   WHERE candidate.portal_id = portal.id
                   ORDER BY candidate.discovered_at DESC, candidate.id DESC
                   LIMIT 1
              ) source ON true
              LEFT JOIN LATERAL (
                  SELECT candidate.id, candidate.document_type,
                         candidate.source_timestamp, candidate.discovered_at,
                         candidate.updated_at
                    FROM source_files candidate
                   WHERE candidate.portal_id = portal.id
                     AND candidate.status = 'completed'
                   ORDER BY candidate.discovered_at DESC, candidate.id DESC
                   LIMIT 1
              ) last_good ON true
              LEFT JOIN LATERAL (
                  SELECT attempt.id AS attempt_id,
                         attempt.status AS attempt_status,
                         checkpoint.operation, attempt.generation,
                         checkpoint.range_since, checkpoint.range_until,
                         checkpoint.archive_only,
                         attempt.started_at, attempt.finished_at,
                         attempt.discovered_count, attempt.processed_count,
                         attempt.skipped_unknown_count, attempt.warning_count,
                         attempt.charged_bytes, attempt.truncated,
                         attempt.truncation_reason, attempt.error_code
                    FROM collection_checkpoints checkpoint
                    JOIN collection_attempts attempt
                      ON attempt.checkpoint_id = checkpoint.id
                   WHERE checkpoint.retailer_id = retailer.id
                     AND checkpoint.portal_ids @> jsonb_build_array(portal.source_key)
                   ORDER BY attempt.started_at DESC, attempt.id DESC
                   LIMIT 1
              ) collection ON true
             WHERE (CAST(:cursor_id AS uuid) IS NULL OR portal.id > :cursor_id)
              ORDER BY portal.id
              LIMIT :limit
            """,
            {
                "cursor_id": cursor_id,
                "limit": bounded_limit + 1,
                "public_error_message": PUBLIC_INGESTION_FAILURE_MESSAGE,
                "public_collection_error_message": (
                    "Collection did not complete; use collection_attempt_id with operator logs"
                ),
            },
        )
        return _page(rows, bounded_limit, cursor_key="portal_id")

    async def maintenance_status(self) -> dict[str, object]:
        rows = await self._rows(
            """
            SELECT control.active_rebuild_run_id,
                   rebuild.status,
                   rebuild.archive_cutoff_at,
                   rebuild.source_files_total,
                   rebuild.source_files_completed,
                   rebuild.last_source_file_id,
                   rebuild.started_at,
                   rebuild.updated_at,
                   rebuild.finished_at
              FROM normalized_rebuild_control control
              LEFT JOIN normalized_rebuild_runs rebuild
                ON rebuild.id = control.active_rebuild_run_id
             WHERE control.singleton_id = 1
             LIMIT 1
            """,
            {},
        )
        if not rows or rows[0]["active_rebuild_run_id"] is None:
            return {"active": False, "mode": "normal"}
        row = rows[0]
        return {
            "active": True,
            "mode": "normalized_rebuild",
            "warning": "normalized query state is partial until the rebuild completes",
            "rebuild_run_id": row["active_rebuild_run_id"],
            "status": row["status"],
            "archive_cutoff_at": row["archive_cutoff_at"],
            "source_files_total": row["source_files_total"],
            "source_files_completed": row["source_files_completed"],
            "last_source_file_id": row["last_source_file_id"],
            "started_at": row["started_at"],
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
        }

    async def _rows(
        self,
        statement: str,
        parameters: dict[str, object],
    ) -> tuple[dict[str, object], ...]:
        async with self._engine.connect() as connection:
            rows = (await connection.execute(text(statement), parameters)).mappings().all()
        return tuple(dict(row) for row in rows)

    async def _bounded_probe_rows(
        self,
        probe_statement: str,
        statement: str,
        parameters: dict[str, object],
        *,
        maximum_probe_results: int,
        resource_name: str,
    ) -> tuple[dict[str, object], ...]:
        """Probe and decorate one bounded candidate set in a stable database snapshot."""

        probe_parameters = {
            **parameters,
            "probe_limit": maximum_probe_results + 1,
        }
        query_parameters = {
            **parameters,
            "probe_limit": maximum_probe_results,
        }
        async with self._engine.connect() as raw_connection:
            connection = await raw_connection.execution_options(isolation_level="REPEATABLE READ")
            async with connection.begin():
                probe_rows = (
                    (await connection.execute(text(probe_statement), probe_parameters))
                    .scalars()
                    .all()
                )
                if len(probe_rows) > maximum_probe_results:
                    raise QueryLimitError(
                        f"Requested {resource_name} scope exceeds its physical probe limit"
                    )
                rows = (
                    (await connection.execute(text(statement), query_parameters)).mappings().all()
                )
        return tuple(dict(row) for row in rows)


def _bounded_limit(limit: int, *, maximum: int = MAXIMUM_QUERY_RESULTS) -> int:
    if not 1 <= limit <= maximum:
        raise QueryLimitError(f"limit must be between 1 and {maximum}")
    return limit


def _page(
    rows: tuple[dict[str, object], ...],
    limit: int,
    *,
    cursor_key: str,
) -> Page:
    items = rows[:limit]
    next_cursor = None
    if len(rows) > limit and items:
        next_cursor = str(items[-1][cursor_key])
    return Page(items=items, next_cursor=next_cursor)


def _promotion_page(rows: tuple[dict[str, object], ...]) -> Page:
    items: list[dict[str, object]] = []
    has_more: bool | None = None
    for raw_row in rows:
        row = dict(raw_row)
        row_has_more = row.pop("_has_more", None)
        if not isinstance(row_has_more, bool) or (
            has_more is not None and row_has_more is not has_more
        ):
            raise DomainValidationError("Repository returned invalid promotion page state")
        has_more = row_has_more
        items.append(row)
    next_cursor = str(items[-1]["id"]) if has_more and items else None
    return Page(items=tuple(items), next_cursor=next_cursor)


def _fuzzy_store_page(
    rows: tuple[dict[str, object], ...],
    limit: int,
) -> Page:
    if not rows:
        raise DomainValidationError("Repository returned no fuzzy-store page state")
    candidate_cursor = rows[0].get("_candidate_cursor")
    candidate_has_more = rows[0].get("_candidate_has_more")
    if not isinstance(candidate_has_more, bool):
        raise DomainValidationError("Repository returned invalid fuzzy-store page state")

    matches: list[dict[str, object]] = []
    for raw_row in rows:
        if (
            raw_row.get("_candidate_cursor") != candidate_cursor
            or raw_row.get("_candidate_has_more") is not candidate_has_more
        ):
            raise DomainValidationError("Repository returned inconsistent fuzzy-store page state")
        row = dict(raw_row)
        row.pop("_candidate_cursor", None)
        row.pop("_candidate_has_more", None)
        if row.get("id") is not None:
            matches.append(row)

    items = tuple(matches[:limit])
    if len(matches) > limit and items:
        next_cursor = str(items[-1]["id"])
    elif candidate_has_more:
        if candidate_cursor is None:
            raise DomainValidationError("Repository omitted the fuzzy-store candidate cursor")
        next_cursor = str(candidate_cursor)
    else:
        next_cursor = None
    return Page(items=items, next_cursor=next_cursor)


def _uuid_cursor(cursor: str | None) -> UUID | None:
    if cursor is None:
        return None
    if len(cursor) > 64:
        raise DomainValidationError("cursor is too long")
    try:
        return UUID(cursor)
    except ValueError as error:
        raise DomainValidationError("cursor is not a valid UUID") from error


def _required_search_query(query: str) -> str:
    normalized = normalize_search_text(query)
    if not normalized:
        raise DomainValidationError("query must contain searchable letters or digits")
    if len(normalized) < MINIMUM_SEARCH_QUERY_LENGTH:
        raise QueryLimitError(
            f"query must contain at least {MINIMUM_SEARCH_QUERY_LENGTH} searchable characters"
        )
    if len(normalized) > MAXIMUM_QUERY_LENGTH:
        raise QueryLimitError(f"query exceeds {MAXIMUM_QUERY_LENGTH} characters")
    return normalized


def _optional_search_query(query: str | None) -> str | None:
    if query is None:
        return None
    return _required_search_query(query)


def _validate_time_range(since: datetime | None, until: datetime | None) -> None:
    if since is None or until is None:
        raise DomainValidationError("since and until are required for history queries")
    for name, value in (("since", since), ("until", until)):
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise DomainValidationError(f"{name} must include a timezone")
    if since is not None and until is not None and since >= until:
        raise DomainValidationError("since must be earlier than until")
    if until - since > MAXIMUM_HISTORY_SPAN:
        raise QueryLimitError("Requested history range is too wide")
