"""Scope normalized source identities to the originating portal and subchain.

Revision ID: 0006_portal_scoped_identity
Revises: 0005_catalog_matching
Created: 2026-08-12
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0006_portal_scoped_identity"
down_revision: str | None = "0005_catalog_matching"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _execute(sql: str) -> None:
    """Keep each statement compatible with asyncpg's prepared execution."""

    for statement in sql.split(";"):
        if selected := statement.strip():
            op.execute(selected)


def _assert_upgrade_is_safe() -> None:
    """Fail before DDL when legacy rows cannot be assigned to one portal."""

    connection = op.get_bind()
    active_rebuild = connection.execute(
        text(
            """
            SELECT active_rebuild_run_id
              FROM normalized_rebuild_control
             WHERE singleton_id = :singleton_id
            """
        ),
        {"singleton_id": 1},
    ).scalar_one_or_none()
    if active_rebuild is not None:
        raise RuntimeError(
            "Complete or recover the active normalized rebuild before applying "
            "0006_portal_scoped_identity"
        )

    conflict = connection.execute(
        text(
            """
            WITH store_evidence AS (
                SELECT store.id AS entity_id, source.portal_id
                  FROM stores store
                  JOIN source_files source
                    ON source.id = store.last_source_file_id
                UNION SELECT current.store_id, source.portal_id
                  FROM current_prices current
                  JOIN source_files source ON source.id = current.source_file_id
                UNION SELECT history.store_id, source.portal_id
                  FROM price_history history
                  JOIN source_files source ON source.id = history.source_file_id
                UNION SELECT current.store_id, source.portal_id
                  FROM current_availability current
                  JOIN source_files source ON source.id = current.source_file_id
                UNION SELECT history.store_id, source.portal_id
                  FROM availability_history history
                  JOIN source_files source ON source.id = history.source_file_id
                UNION SELECT relation.store_id, source.portal_id
                  FROM promotion_stores relation
                  JOIN promotions promotion ON promotion.id = relation.promotion_id
                  JOIN source_files source ON source.id = promotion.source_file_id
            ),
            item_evidence AS (
                SELECT item.id AS entity_id, source.portal_id
                  FROM retailer_items item
                  JOIN source_files source
                    ON source.id = item.last_source_file_id
                UNION SELECT current.retailer_item_id, source.portal_id
                  FROM current_prices current
                  JOIN source_files source ON source.id = current.source_file_id
                UNION SELECT history.retailer_item_id, source.portal_id
                  FROM price_history history
                  JOIN source_files source ON source.id = history.source_file_id
                UNION SELECT current.retailer_item_id, source.portal_id
                  FROM current_availability current
                  JOIN source_files source ON source.id = current.source_file_id
                UNION SELECT history.retailer_item_id, source.portal_id
                  FROM availability_history history
                  JOIN source_files source ON source.id = history.source_file_id
                UNION SELECT assertion.retailer_item_id, source.portal_id
                  FROM retailer_identifier_assertions assertion
                  JOIN source_files source ON source.id = assertion.source_file_id
                UNION SELECT relation.retailer_item_id, source.portal_id
                  FROM promotion_items relation
                  JOIN promotions promotion ON promotion.id = relation.promotion_id
                  JOIN source_files source ON source.id = promotion.source_file_id
            ),
            conflicts AS (
                SELECT 'store' AS entity_kind, store.id AS entity_id
                  FROM stores store
                  LEFT JOIN store_evidence evidence ON evidence.entity_id = store.id
                 GROUP BY store.id
                HAVING count(DISTINCT evidence.portal_id) <> 1
                UNION ALL
                SELECT 'retailer_item', item.id
                  FROM retailer_items item
                  LEFT JOIN item_evidence evidence ON evidence.entity_id = item.id
                 GROUP BY item.id
                HAVING count(DISTINCT evidence.portal_id) <> 1
            )
            SELECT entity_kind FROM conflicts ORDER BY entity_kind LIMIT 1
            """
        )
    ).scalar_one_or_none()
    if conflict is not None:
        raise RuntimeError(
            "Legacy normalized portal evidence is missing or spans multiple portals for "
            f"at least one {conflict}; rebuild or repair the ambiguous identity before "
            "applying 0006_portal_scoped_identity"
        )


def _assert_downgrade_is_safe() -> None:
    """Refuse a lossy collapse from portal-scoped back to retailer-scoped keys."""

    conflict = (
        op.get_bind()
        .execute(
            text(
                """
            WITH conflicts AS (
                SELECT 'store' AS entity_kind
                  FROM stores
                 GROUP BY retailer_id, subchain_code, source_store_code
                HAVING count(DISTINCT portal_id) > 1
                UNION ALL
                SELECT 'store_alias'
                  FROM store_aliases
                 GROUP BY retailer_id, alias_kind, alias_value
                HAVING count(DISTINCT portal_id) > 1
                UNION ALL
                SELECT 'retailer_item'
                  FROM retailer_items
                 GROUP BY retailer_id, source_item_code
                HAVING count(DISTINCT portal_id) > 1
                UNION ALL
                SELECT 'watermark'
                  FROM source_scope_watermarks
                 GROUP BY retailer_id, document_family,
                          subchain_code, source_scope_code
                HAVING count(DISTINCT portal_id) > 1
                UNION ALL
                SELECT 'applied_content'
                  FROM applied_source_contents
                 GROUP BY retailer_id, document_type, content_sha256
                HAVING count(*) > 1
                UNION ALL
                SELECT 'product_identifier'
                  FROM product_identifiers
                 GROUP BY kind, normalized_value, issuer_retailer_id
                HAVING count(DISTINCT issuer_portal_id) > 1
                UNION ALL
                SELECT 'promotion'
                  FROM promotions
                 WHERE valid_to IS NULL
                 GROUP BY retailer_id, source_promotion_id,
                          source_scope_store_code
                HAVING count(*) > 1
            )
            SELECT entity_kind
              FROM conflicts
             ORDER BY entity_kind
             LIMIT 1
            """
            )
        )
        .scalar_one_or_none()
    )
    if conflict is not None:
        raise RuntimeError(
            "Cannot downgrade 0006_portal_scoped_identity without losing or merging "
            f"valid cross-portal {conflict} identities; remove the collision only after "
            "an explicit data-retention decision"
        )


def upgrade() -> None:
    _assert_upgrade_is_safe()
    _execute(
        """
        ALTER TABLE stores ADD COLUMN portal_id UUID;
        ALTER TABLE store_aliases ADD COLUMN portal_id UUID;
        ALTER TABLE retailer_items ADD COLUMN portal_id UUID;
        ALTER TABLE promotions
            ADD COLUMN portal_id UUID,
            ADD COLUMN subchain_code VARCHAR(128) DEFAULT '' NOT NULL;
        ALTER TABLE source_scope_watermarks ADD COLUMN portal_id UUID;
        ALTER TABLE applied_source_contents ADD COLUMN portal_id UUID;
        ALTER TABLE product_identifiers ADD COLUMN issuer_portal_id UUID
        """
    )
    _execute(
        """
        UPDATE stores entity
           SET portal_id = source.portal_id
          FROM source_files source
         WHERE source.id = entity.last_source_file_id;

        UPDATE store_aliases alias
           SET portal_id = store.portal_id
          FROM stores store
         WHERE store.id = alias.store_id;

        UPDATE retailer_items entity
           SET portal_id = source.portal_id
          FROM source_files source
         WHERE source.id = entity.last_source_file_id;

        UPDATE promotions entity
           SET portal_id = source.portal_id
          FROM source_files source
         WHERE source.id = entity.source_file_id;

        UPDATE promotions promotion
           SET subchain_code = staged.subchain_id
          FROM staged_promotions staged
         WHERE staged.source_file_id = promotion.source_file_id
           AND staged.source_promotion_id = promotion.source_promotion_id
           AND staged.source_scope_store_code =
               promotion.source_scope_store_code;

        UPDATE source_scope_watermarks watermark
           SET portal_id = source.portal_id
          FROM source_files source
         WHERE source.id = watermark.source_file_id;

        UPDATE applied_source_contents applied
           SET portal_id = source.portal_id
          FROM source_files source
         WHERE source.id = applied.source_file_id;

        UPDATE product_identifiers identifier
           SET issuer_portal_id = item.portal_id
          FROM confirmed_product_matches confirmed
          JOIN retailer_items item
            ON item.id = confirmed.retailer_item_id
         WHERE identifier.kind = 'retailer_item'
           AND identifier.product_id = confirmed.canonical_product_id
           AND identifier.issuer_retailer_id = item.retailer_id
           AND identifier.normalized_value = item.source_item_code
        """
    )
    _execute(
        """
        ALTER TABLE stores ALTER COLUMN portal_id SET NOT NULL;
        ALTER TABLE store_aliases ALTER COLUMN portal_id SET NOT NULL;
        ALTER TABLE retailer_items ALTER COLUMN portal_id SET NOT NULL;
        ALTER TABLE promotions ALTER COLUMN portal_id SET NOT NULL;
        ALTER TABLE source_scope_watermarks ALTER COLUMN portal_id SET NOT NULL;
        ALTER TABLE applied_source_contents ALTER COLUMN portal_id SET NOT NULL;

        ALTER TABLE stores
            ADD CONSTRAINT fk_stores_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE RESTRICT;
        ALTER TABLE store_aliases
            ADD CONSTRAINT fk_store_aliases_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE RESTRICT;
        ALTER TABLE retailer_items
            ADD CONSTRAINT fk_retailer_items_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE RESTRICT;
        ALTER TABLE promotions
            ADD CONSTRAINT fk_promotions_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE RESTRICT;
        ALTER TABLE source_scope_watermarks
            ADD CONSTRAINT fk_source_scope_watermarks_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE CASCADE;
        ALTER TABLE applied_source_contents
            ADD CONSTRAINT fk_applied_source_contents_portal_id_portals
            FOREIGN KEY (portal_id) REFERENCES portals (id) ON DELETE RESTRICT;
        ALTER TABLE product_identifiers
            ADD CONSTRAINT fk_product_identifiers_issuer_portal_id_portals
            FOREIGN KEY (issuer_portal_id) REFERENCES portals (id) ON DELETE CASCADE,
            ADD CONSTRAINT ck_product_identifiers_portal_scope
            CHECK (issuer_portal_id IS NULL OR issuer_retailer_id IS NOT NULL),
            ADD CONSTRAINT ck_product_identifiers_item_portal_scope
            CHECK (kind <> 'retailer_item' OR issuer_portal_id IS NOT NULL)
        """
    )
    _execute(
        """
        ALTER TABLE stores
            DROP CONSTRAINT uq_stores_retailer_id_subchain_code_source_store_code,
            ADD CONSTRAINT uq_stores_retailer_portal_subchain_code
            UNIQUE (retailer_id, portal_id, subchain_code, source_store_code);
        ALTER TABLE store_aliases
            DROP CONSTRAINT uq_store_aliases_retailer_id_alias_kind_alias_value,
            ADD CONSTRAINT uq_store_aliases_retailer_portal_kind_value
            UNIQUE (retailer_id, portal_id, alias_kind, alias_value);
        ALTER TABLE retailer_items
            DROP CONSTRAINT uq_retailer_items_retailer_id_source_item_code,
            ADD CONSTRAINT uq_retailer_items_retailer_portal_code
            UNIQUE (retailer_id, portal_id, source_item_code);
        ALTER TABLE applied_source_contents
            DROP CONSTRAINT uq_applied_source_contents_retailer_id_document_type_co_7fc6;
        ALTER TABLE source_scope_watermarks
            DROP CONSTRAINT uq_source_scope_watermarks_retailer_family_scope,
            ADD CONSTRAINT uq_source_scope_watermarks_retailer_family_scope
            UNIQUE (
                retailer_id, portal_id, document_family,
                subchain_code, source_scope_code
            )
        """
    )
    _execute("DROP INDEX uq_product_identifiers_identity")
    _execute(
        """
        CREATE UNIQUE INDEX uq_product_identifiers_identity
            ON product_identifiers (
                kind, normalized_value, issuer_retailer_id, issuer_portal_id
            ) NULLS NOT DISTINCT
        """
    )
    _execute("DROP INDEX uq_promotions_open")
    _execute(
        """
        CREATE UNIQUE INDEX uq_promotions_open
            ON promotions (
                retailer_id, portal_id, subchain_code,
                source_promotion_id, source_scope_store_code
            )
            WHERE valid_to IS NULL
        """
    )


def downgrade() -> None:
    _assert_downgrade_is_safe()
    _execute("DROP INDEX uq_promotions_open")
    _execute(
        """
        CREATE UNIQUE INDEX uq_promotions_open
            ON promotions (
                retailer_id, source_promotion_id, source_scope_store_code
            )
            WHERE valid_to IS NULL
        """
    )
    _execute("DROP INDEX uq_product_identifiers_identity")
    _execute(
        """
        CREATE UNIQUE INDEX uq_product_identifiers_identity
            ON product_identifiers (
                kind, normalized_value, issuer_retailer_id
            ) NULLS NOT DISTINCT
        """
    )
    _execute(
        """
        ALTER TABLE source_scope_watermarks
            DROP CONSTRAINT uq_source_scope_watermarks_retailer_family_scope,
            ADD CONSTRAINT uq_source_scope_watermarks_retailer_family_scope
            UNIQUE (
                retailer_id, document_family,
                subchain_code, source_scope_code
            );
        ALTER TABLE applied_source_contents
            ADD CONSTRAINT uq_applied_source_contents_retailer_id_document_type_co_7fc6
            UNIQUE (retailer_id, document_type, content_sha256);
        ALTER TABLE retailer_items
            DROP CONSTRAINT uq_retailer_items_retailer_portal_code,
            ADD CONSTRAINT uq_retailer_items_retailer_id_source_item_code
            UNIQUE (retailer_id, source_item_code);
        ALTER TABLE store_aliases
            DROP CONSTRAINT uq_store_aliases_retailer_portal_kind_value,
            ADD CONSTRAINT uq_store_aliases_retailer_id_alias_kind_alias_value
            UNIQUE (retailer_id, alias_kind, alias_value);
        ALTER TABLE stores
            DROP CONSTRAINT uq_stores_retailer_portal_subchain_code,
            ADD CONSTRAINT uq_stores_retailer_id_subchain_code_source_store_code
            UNIQUE (retailer_id, subchain_code, source_store_code)
        """
    )
    _execute(
        """
        ALTER TABLE product_identifiers
            DROP CONSTRAINT ck_product_identifiers_item_portal_scope,
            DROP CONSTRAINT ck_product_identifiers_portal_scope,
            DROP CONSTRAINT fk_product_identifiers_issuer_portal_id_portals,
            DROP COLUMN issuer_portal_id;
        ALTER TABLE applied_source_contents
            DROP CONSTRAINT fk_applied_source_contents_portal_id_portals,
            DROP COLUMN portal_id;
        ALTER TABLE source_scope_watermarks
            DROP CONSTRAINT fk_source_scope_watermarks_portal_id_portals,
            DROP COLUMN portal_id;
        ALTER TABLE promotions
            DROP CONSTRAINT fk_promotions_portal_id_portals,
            DROP COLUMN subchain_code,
            DROP COLUMN portal_id;
        ALTER TABLE retailer_items
            DROP CONSTRAINT fk_retailer_items_portal_id_portals,
            DROP COLUMN portal_id;
        ALTER TABLE store_aliases
            DROP CONSTRAINT fk_store_aliases_portal_id_portals,
            DROP COLUMN portal_id;
        ALTER TABLE stores
            DROP CONSTRAINT fk_stores_portal_id_portals,
            DROP COLUMN portal_id
        """
    )
