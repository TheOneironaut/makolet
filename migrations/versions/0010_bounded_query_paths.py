"""Add exact bounded public-query projection and city keyset paths.

Revision ID: 0010_bounded_query_paths
Revises: 0009_collection_charge_budgets
Created: 2026-08-17

The three projection backfills advance in 10,000-row primary-key batches. The
migration is transactional: an interruption rolls the revision back completely,
after which the ordinary Alembic upgrade can be rerun. Index creation is intentionally
inside that transaction and can hold write-blocking locks; operators should use the
documented maintenance window and recover by cancelling the migration, allowing the
transaction to roll back, and rerunning it after capacity has been restored.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0010_bounded_query_paths"
down_revision: str | None = "0009_collection_charge_budgets"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE stores
            ADD COLUMN city_search TEXT GENERATED ALWAYS AS (
                btrim(lower(regexp_replace(
                    normalize(city, NFKC),
                    '[[:punct:][:space:]]+', ' ', 'g'
                )))
            ) STORED
        """
    )
    op.execute(
        """
        ALTER TABLE current_prices
            ADD COLUMN canonical_product_id UUID,
            ADD COLUMN query_retailer_id UUID,
            ADD CONSTRAINT fk_current_prices_canonical_product_id_canonical_products
                FOREIGN KEY (canonical_product_id)
                REFERENCES canonical_products (id) ON DELETE SET NULL,
            ADD CONSTRAINT fk_current_prices_query_retailer_id_retailers
                FOREIGN KEY (query_retailer_id)
                REFERENCES retailers (id) ON DELETE CASCADE
        """
    )
    op.execute(
        """
        ALTER TABLE price_history
            ADD COLUMN canonical_product_id UUID,
            ADD CONSTRAINT fk_price_history_canonical_product_id_canonical_products
                FOREIGN KEY (canonical_product_id)
                REFERENCES canonical_products (id) ON DELETE SET NULL
        """
    )
    op.execute(
        """
        ALTER TABLE current_availability
            ADD COLUMN canonical_product_id UUID,
            ADD CONSTRAINT fk_current_availability_canonical_product_id_canonical_products
                FOREIGN KEY (canonical_product_id)
                REFERENCES canonical_products (id) ON DELETE SET NULL
        """
    )

    op.execute(
        """
        DO $$
        DECLARE
            last_id UUID := NULL;
            batch_last_id UUID;
        BEGIN
            LOOP
                SELECT bounded.id
                  INTO batch_last_id
                  FROM (
                        SELECT price.id
                          FROM current_prices price
                         WHERE last_id IS NULL OR price.id > last_id
                         ORDER BY price.id
                         LIMIT 10000
                       ) bounded
                 ORDER BY bounded.id DESC
                 LIMIT 1;
                EXIT WHEN NOT FOUND;

                UPDATE current_prices target
                   SET canonical_product_id = match.canonical_product_id,
                       query_retailer_id = item.retailer_id
                  FROM retailer_items item
                  LEFT JOIN confirmed_product_matches match
                    ON match.retailer_item_id = item.id
                 WHERE target.retailer_item_id = item.id
                   AND (last_id IS NULL OR target.id > last_id)
                   AND target.id <= batch_last_id;
                last_id := batch_last_id;
            END LOOP;
        END
        $$
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            last_id UUID := NULL;
            batch_last_id UUID;
        BEGIN
            LOOP
                SELECT bounded.id
                  INTO batch_last_id
                  FROM (
                        SELECT history.id
                          FROM price_history history
                         WHERE last_id IS NULL OR history.id > last_id
                         ORDER BY history.id
                         LIMIT 10000
                       ) bounded
                 ORDER BY bounded.id DESC
                 LIMIT 1;
                EXIT WHEN NOT FOUND;

                UPDATE price_history target
                   SET canonical_product_id = match.canonical_product_id
                  FROM confirmed_product_matches match
                 WHERE target.retailer_item_id = match.retailer_item_id
                   AND (last_id IS NULL OR target.id > last_id)
                   AND target.id <= batch_last_id;
                last_id := batch_last_id;
            END LOOP;
        END
        $$
        """
    )
    op.execute(
        """
        DO $$
        DECLARE
            last_id UUID := NULL;
            batch_last_id UUID;
        BEGIN
            LOOP
                SELECT bounded.id
                  INTO batch_last_id
                  FROM (
                        SELECT availability.id
                          FROM current_availability availability
                         WHERE last_id IS NULL OR availability.id > last_id
                         ORDER BY availability.id
                         LIMIT 10000
                       ) bounded
                 ORDER BY bounded.id DESC
                 LIMIT 1;
                EXIT WHEN NOT FOUND;

                UPDATE current_availability target
                   SET canonical_product_id = match.canonical_product_id
                  FROM confirmed_product_matches match
                 WHERE target.retailer_item_id = match.retailer_item_id
                   AND (last_id IS NULL OR target.id > last_id)
                   AND target.id <= batch_last_id;
                last_id := batch_last_id;
            END LOOP;
        END
        $$
        """
    )

    op.execute(
        """
        CREATE FUNCTION makolet_project_inserted_current_prices()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE current_prices target
               SET canonical_product_id = match.canonical_product_id,
                   query_retailer_id = item.retailer_id
              FROM makolet_inserted_current_prices inserted
              JOIN retailer_items item ON item.id = inserted.retailer_item_id
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
             WHERE target.id = inserted.id
               AND ROW(target.canonical_product_id, target.query_retailer_id)
                   IS DISTINCT FROM
                   ROW(match.canonical_product_id, item.retailer_id);
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_current_prices_project_insert
        AFTER INSERT ON current_prices
        REFERENCING NEW TABLE AS makolet_inserted_current_prices
        FOR EACH STATEMENT
        EXECUTE FUNCTION makolet_project_inserted_current_prices()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_project_inserted_price_history()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE price_history target
               SET canonical_product_id = match.canonical_product_id
              FROM makolet_inserted_price_history inserted
              JOIN retailer_items item ON item.id = inserted.retailer_item_id
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
             WHERE target.id = inserted.id
               AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_price_history_project_insert
        AFTER INSERT ON price_history
        REFERENCING NEW TABLE AS makolet_inserted_price_history
        FOR EACH STATEMENT
        EXECUTE FUNCTION makolet_project_inserted_price_history()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_project_inserted_current_availability()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE current_availability target
               SET canonical_product_id = match.canonical_product_id
              FROM makolet_inserted_current_availability inserted
              JOIN retailer_items item ON item.id = inserted.retailer_item_id
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
             WHERE target.id = inserted.id
               AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_current_availability_project_insert
        AFTER INSERT ON current_availability
        REFERENCING NEW TABLE AS makolet_inserted_current_availability
        FOR EACH STATEMENT
        EXECUTE FUNCTION makolet_project_inserted_current_availability()
        """
    )

    op.execute(
        """
        CREATE FUNCTION makolet_project_current_price_rekey()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            SELECT match.canonical_product_id, item.retailer_id
              INTO NEW.canonical_product_id, NEW.query_retailer_id
              FROM retailer_items item
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
             WHERE item.id = NEW.retailer_item_id;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_current_prices_project_rekey
        BEFORE UPDATE OF retailer_item_id ON current_prices
        FOR EACH ROW
        EXECUTE FUNCTION makolet_project_current_price_rekey()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_project_product_row_rekey()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            SELECT match.canonical_product_id
              INTO NEW.canonical_product_id
              FROM confirmed_product_matches match
             WHERE match.retailer_item_id = NEW.retailer_item_id;
            RETURN NEW;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_price_history_project_rekey
        BEFORE UPDATE OF retailer_item_id ON price_history
        FOR EACH ROW
        EXECUTE FUNCTION makolet_project_product_row_rekey()
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_current_availability_project_rekey
        BEFORE UPDATE OF retailer_item_id ON current_availability
        FOR EACH ROW
        EXECUTE FUNCTION makolet_project_product_row_rekey()
        """
    )

    op.execute(
        """
        CREATE FUNCTION makolet_project_inserted_confirmed_matches()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE current_prices target
               SET canonical_product_id = inserted.canonical_product_id,
                   query_retailer_id = item.retailer_id
              FROM makolet_inserted_confirmed_matches inserted
              JOIN retailer_items item ON item.id = inserted.retailer_item_id
             WHERE target.retailer_item_id = inserted.retailer_item_id
               AND ROW(target.canonical_product_id, target.query_retailer_id)
                   IS DISTINCT FROM
                   ROW(inserted.canonical_product_id, item.retailer_id);
            UPDATE price_history target
               SET canonical_product_id = inserted.canonical_product_id
              FROM makolet_inserted_confirmed_matches inserted
             WHERE target.retailer_item_id = inserted.retailer_item_id
               AND target.canonical_product_id
                   IS DISTINCT FROM inserted.canonical_product_id;
            UPDATE current_availability target
               SET canonical_product_id = inserted.canonical_product_id
              FROM makolet_inserted_confirmed_matches inserted
             WHERE target.retailer_item_id = inserted.retailer_item_id
               AND target.canonical_product_id
                   IS DISTINCT FROM inserted.canonical_product_id;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_confirmed_matches_refresh_query_projection
        AFTER INSERT ON confirmed_product_matches
        REFERENCING NEW TABLE AS makolet_inserted_confirmed_matches
        FOR EACH STATEMENT
        EXECUTE FUNCTION makolet_project_inserted_confirmed_matches()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_project_updated_confirmed_matches()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            WITH affected AS MATERIALIZED (
                SELECT old_match.retailer_item_id
                  FROM makolet_old_confirmed_matches old_match
                  LEFT JOIN makolet_new_confirmed_matches new_match
                    ON new_match.id = old_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
                UNION
                SELECT new_match.retailer_item_id
                  FROM makolet_new_confirmed_matches new_match
                  LEFT JOIN makolet_old_confirmed_matches old_match
                    ON old_match.id = new_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
            )
            UPDATE current_prices target
               SET canonical_product_id = match.canonical_product_id,
                   query_retailer_id = item.retailer_id
              FROM affected
              JOIN retailer_items item ON item.id = affected.retailer_item_id
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = item.id
             WHERE target.retailer_item_id = affected.retailer_item_id
               AND ROW(target.canonical_product_id, target.query_retailer_id)
                   IS DISTINCT FROM
                   ROW(match.canonical_product_id, item.retailer_id);

            WITH affected AS MATERIALIZED (
                SELECT old_match.retailer_item_id
                  FROM makolet_old_confirmed_matches old_match
                  LEFT JOIN makolet_new_confirmed_matches new_match
                    ON new_match.id = old_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
                UNION
                SELECT new_match.retailer_item_id
                  FROM makolet_new_confirmed_matches new_match
                  LEFT JOIN makolet_old_confirmed_matches old_match
                    ON old_match.id = new_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
            )
            UPDATE price_history target
               SET canonical_product_id = match.canonical_product_id
              FROM affected
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = affected.retailer_item_id
             WHERE target.retailer_item_id = affected.retailer_item_id
               AND target.canonical_product_id
                   IS DISTINCT FROM match.canonical_product_id;

            WITH affected AS MATERIALIZED (
                SELECT old_match.retailer_item_id
                  FROM makolet_old_confirmed_matches old_match
                  LEFT JOIN makolet_new_confirmed_matches new_match
                    ON new_match.id = old_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
                UNION
                SELECT new_match.retailer_item_id
                  FROM makolet_new_confirmed_matches new_match
                  LEFT JOIN makolet_old_confirmed_matches old_match
                    ON old_match.id = new_match.id
                 WHERE ROW(old_match.retailer_item_id,
                           old_match.canonical_product_id)
                       IS DISTINCT FROM
                       ROW(new_match.retailer_item_id,
                           new_match.canonical_product_id)
            )
            UPDATE current_availability target
               SET canonical_product_id = match.canonical_product_id
              FROM affected
              LEFT JOIN confirmed_product_matches match
                ON match.retailer_item_id = affected.retailer_item_id
             WHERE target.retailer_item_id = affected.retailer_item_id
               AND target.canonical_product_id
                   IS DISTINCT FROM match.canonical_product_id;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_confirmed_matches_rekey_query_projection
        AFTER UPDATE ON confirmed_product_matches
        REFERENCING OLD TABLE AS makolet_old_confirmed_matches
                    NEW TABLE AS makolet_new_confirmed_matches
        FOR EACH STATEMENT
        EXECUTE FUNCTION makolet_project_updated_confirmed_matches()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_clear_deleted_confirmed_match_projection()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM confirmed_product_matches match
                 WHERE match.retailer_item_id = OLD.retailer_item_id
            ) THEN
                RETURN NULL;
            END IF;
            UPDATE current_prices
               SET canonical_product_id = NULL
             WHERE retailer_item_id = OLD.retailer_item_id
               AND canonical_product_id IS NOT NULL;
            UPDATE price_history
               SET canonical_product_id = NULL
             WHERE retailer_item_id = OLD.retailer_item_id
               AND canonical_product_id IS NOT NULL;
            UPDATE current_availability
               SET canonical_product_id = NULL
             WHERE retailer_item_id = OLD.retailer_item_id
               AND canonical_product_id IS NOT NULL;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER trg_confirmed_matches_clear_query_projection
        AFTER DELETE ON confirmed_product_matches
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION makolet_clear_deleted_confirmed_match_projection()
        """
    )
    op.execute(
        """
        CREATE FUNCTION makolet_refresh_query_retailer_for_item()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            UPDATE current_prices
               SET query_retailer_id = NEW.retailer_id
             WHERE retailer_item_id = NEW.id
               AND query_retailer_id IS DISTINCT FROM NEW.retailer_id;
            RETURN NULL;
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_retailer_items_refresh_query_retailer
        AFTER UPDATE OF retailer_id ON retailer_items
        FOR EACH ROW
        EXECUTE FUNCTION makolet_refresh_query_retailer_for_item()
        """
    )

    op.execute(
        """
        CREATE INDEX ix_stores_city_search_id
            ON stores (city_search, id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_stores_retailer_city_search_id
            ON stores (retailer_id, city_search, id)
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_prices_product_price_id
            ON current_prices (canonical_product_id, item_price, id)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_prices_product_retailer_price_id
            ON current_prices (
                canonical_product_id, query_retailer_id, item_price, id
            )
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_prices_product_store_price_id
            ON current_prices (canonical_product_id, store_id, item_price, id)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_prices_product_store_retailer_price_id
            ON current_prices (
                canonical_product_id, store_id, query_retailer_id, item_price, id
            )
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_price_history_product_from_id
            ON price_history (canonical_product_id, valid_from DESC, id)
            INCLUDE (valid_to, store_id)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_price_history_product_store_from_id
            ON price_history (canonical_product_id, store_id, valid_from DESC, id)
            INCLUDE (valid_to)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_availability_product_id
            ON current_availability (canonical_product_id, id)
            WHERE canonical_product_id IS NOT NULL
        """
    )
    op.execute(
        """
        CREATE INDEX ix_current_availability_product_store_id
            ON current_availability (canonical_product_id, store_id, id)
            WHERE canonical_product_id IS NOT NULL
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_retailer_items_refresh_query_retailer ON retailer_items")
    op.execute("DROP FUNCTION makolet_refresh_query_retailer_for_item()")
    op.execute(
        "DROP TRIGGER trg_confirmed_matches_clear_query_projection ON confirmed_product_matches"
    )
    op.execute(
        "DROP TRIGGER trg_confirmed_matches_rekey_query_projection ON confirmed_product_matches"
    )
    op.execute(
        "DROP TRIGGER trg_confirmed_matches_refresh_query_projection ON confirmed_product_matches"
    )
    op.execute("DROP FUNCTION makolet_clear_deleted_confirmed_match_projection()")
    op.execute("DROP FUNCTION makolet_project_updated_confirmed_matches()")
    op.execute("DROP FUNCTION makolet_project_inserted_confirmed_matches()")
    op.execute("DROP TRIGGER trg_current_availability_project_rekey ON current_availability")
    op.execute("DROP TRIGGER trg_price_history_project_rekey ON price_history")
    op.execute("DROP FUNCTION makolet_project_product_row_rekey()")
    op.execute("DROP TRIGGER trg_current_prices_project_rekey ON current_prices")
    op.execute("DROP FUNCTION makolet_project_current_price_rekey()")
    op.execute("DROP TRIGGER trg_current_availability_project_insert ON current_availability")
    op.execute("DROP FUNCTION makolet_project_inserted_current_availability()")
    op.execute("DROP TRIGGER trg_price_history_project_insert ON price_history")
    op.execute("DROP FUNCTION makolet_project_inserted_price_history()")
    op.execute("DROP TRIGGER trg_current_prices_project_insert ON current_prices")
    op.execute("DROP FUNCTION makolet_project_inserted_current_prices()")

    op.drop_index(
        "ix_current_availability_product_store_id",
        table_name="current_availability",
    )
    op.drop_index("ix_current_availability_product_id", table_name="current_availability")
    op.drop_index("ix_price_history_product_store_from_id", table_name="price_history")
    op.drop_index("ix_price_history_product_from_id", table_name="price_history")
    op.drop_index(
        "ix_current_prices_product_store_retailer_price_id",
        table_name="current_prices",
    )
    op.drop_index("ix_current_prices_product_store_price_id", table_name="current_prices")
    op.drop_index(
        "ix_current_prices_product_retailer_price_id",
        table_name="current_prices",
    )
    op.drop_index("ix_current_prices_product_price_id", table_name="current_prices")
    op.drop_index("ix_stores_retailer_city_search_id", table_name="stores")
    op.drop_index("ix_stores_city_search_id", table_name="stores")

    op.execute(
        """
        ALTER TABLE current_availability
            DROP COLUMN canonical_product_id
        """
    )
    op.execute(
        """
        ALTER TABLE price_history
            DROP COLUMN canonical_product_id
        """
    )
    op.execute(
        """
        ALTER TABLE current_prices
            DROP COLUMN query_retailer_id,
            DROP COLUMN canonical_product_id
        """
    )
    op.execute("ALTER TABLE stores DROP COLUMN city_search")
