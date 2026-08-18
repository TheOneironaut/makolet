"""SQLAlchemy Core schema for PostgreSQL 18.

The tables deliberately use textual check-constrained states instead of native
PostgreSQL enums. Adding a source lifecycle state therefore remains an ordinary,
forward-only constraint migration rather than a database enum rewrite.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Computed,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TSTZRANGE, UUID

from makolet.domain.enums import (
    CompressionFormat,
    DiscountKind,
    DocumentType,
    IdentifierKind,
    IngestionStatus,
    IssueSeverity,
    SourceProtocol,
)

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(naming_convention=NAMING_CONVENTION)

UUID_PK = UUID(as_uuid=True)
UTC_TIMESTAMP = DateTime(timezone=True)
MONEY = Numeric(14, 4)
QUANTITY = Numeric(18, 6)
SCORE = Numeric(8, 7)


def _uuid_primary_key() -> Column[uuid.UUID]:
    return Column(
        "id",
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("uuidv7()"),
    )


def _values(column: str, values: Sequence[str]) -> CheckConstraint:
    quoted = ", ".join(f"'{value}'" for value in values)
    return CheckConstraint(f"{column} IN ({quoted})", name=f"{column}_value")


def _nonempty(column: str, *, maximum: int) -> CheckConstraint:
    return CheckConstraint(
        f"length({column}) BETWEEN 1 AND {maximum}",
        name=f"{column}_length",
    )


_SEARCH_EXPRESSION = (
    "lower(regexp_replace(normalize(name, NFKC), '[[:punct:][:space:]]+', ' ', 'g'))"
)
_CITY_SEARCH_EXPRESSION = (
    "btrim(lower(regexp_replace(normalize(city, NFKC), '[[:punct:][:space:]]+', ' ', 'g')))"
)

retailers = Table(
    "retailers",
    metadata,
    _uuid_primary_key(),
    Column("source_key", String(128), nullable=False),
    Column("legal_name", Text),
    Column("display_name", Text, nullable=False),
    Column("edi", String(64)),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint("source_key"),
    UniqueConstraint("edi"),
    _nonempty("source_key", maximum=128),
    _nonempty("display_name", maximum=512),
)

portals = Table(
    "portals",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("source_key", String(128), nullable=False),
    Column("family", String(64), nullable=False, server_default=text("'custom'")),
    Column("protocol", String(16), nullable=False),
    Column("base_url", Text),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint("retailer_id", "source_key"),
    _values("protocol", [value.value for value in SourceProtocol]),
    _nonempty("source_key", maximum=128),
)
Index("ix_portals_source_key", portals.c.source_key)

collection_checkpoints = Table(
    "collection_checkpoints",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("portal_ids", JSONB, nullable=False),
    Column("portal_generation", String(64), nullable=False),
    Column("operation", String(16), nullable=False),
    Column("range_since", UTC_TIMESTAMP),
    Column("range_until", UTC_TIMESTAMP),
    Column("archive_only", Boolean, nullable=False, server_default=text("false")),
    Column("generation", BigInteger, nullable=False, server_default=text("1")),
    Column("publisher_cursor", Text),
    Column("page_offset", Integer, nullable=False, server_default=text("0")),
    Column("generation_recognized_count", BigInteger, nullable=False, server_default=text("0")),
    Column("generation_unknown_count", BigInteger, nullable=False, server_default=text("0")),
    Column("traversal_complete", Boolean, nullable=False, server_default=text("false")),
    Column("last_completed_at", UTC_TIMESTAMP),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("operation", ["ordinary", "backfill"]),
    CheckConstraint(
        "jsonb_typeof(portal_ids) = 'array' AND jsonb_array_length(portal_ids) BETWEEN 1 AND 64",
        name="portal_ids_array",
    ),
    CheckConstraint(
        "portal_generation ~ '^[0-9a-f]{64}$'",
        name="portal_generation_format",
    ),
    CheckConstraint(
        "range_since IS NULL OR range_until IS NULL OR range_until >= range_since",
        name="range_order",
    ),
    CheckConstraint(
        "(operation = 'ordinary' AND range_since IS NULL AND range_until IS NULL "
        "AND NOT archive_only) OR (operation = 'backfill' AND range_since IS NOT NULL "
        "AND range_until IS NOT NULL)",
        name="operation_scope",
    ),
    CheckConstraint("generation > 0", name="generation_positive"),
    CheckConstraint("page_offset >= 0", name="page_offset_nonnegative"),
    CheckConstraint(
        "publisher_cursor IS NULL OR octet_length(publisher_cursor) BETWEEN 1 AND 8192",
        name="publisher_cursor_length",
    ),
    CheckConstraint(
        "generation_recognized_count >= 0 AND generation_unknown_count >= 0",
        name="generation_counts_nonnegative",
    ),
)
Index(
    "uq_collection_checkpoints_scope",
    collection_checkpoints.c.retailer_id,
    collection_checkpoints.c.portal_generation,
    collection_checkpoints.c.operation,
    collection_checkpoints.c.range_since,
    collection_checkpoints.c.range_until,
    collection_checkpoints.c.archive_only,
    unique=True,
    postgresql_nulls_not_distinct=True,
)
Index(
    "ix_collection_checkpoints_portal_ids",
    collection_checkpoints.c.portal_ids,
    postgresql_using="gin",
)

collection_attempts = Table(
    "collection_attempts",
    metadata,
    _uuid_primary_key(),
    Column(
        "checkpoint_id",
        UUID_PK,
        ForeignKey("collection_checkpoints.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("generation", BigInteger, nullable=False),
    Column("status", String(16), nullable=False, server_default=text("'running'")),
    Column("started_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("finished_at", UTC_TIMESTAMP),
    Column("start_cursor", Text),
    Column("start_page_offset", Integer, nullable=False),
    Column("checkpoint_cursor", Text),
    Column("checkpoint_page_offset", Integer, nullable=False),
    Column("discovered_count", BigInteger, nullable=False, server_default=text("0")),
    Column("processed_count", BigInteger, nullable=False, server_default=text("0")),
    Column("duplicate_count", BigInteger, nullable=False, server_default=text("0")),
    Column("skipped_unknown_count", BigInteger, nullable=False, server_default=text("0")),
    Column("warning_count", BigInteger, nullable=False, server_default=text("0")),
    Column("charged_bytes", BigInteger, server_default=text("0")),
    Column("truncated", Boolean, nullable=False, server_default=text("false")),
    Column("truncation_reason", String(64)),
    Column("error_code", String(128)),
    Column("error_message", Text),
    _values("status", ["running", "completed", "bounded", "failed"]),
    CheckConstraint("generation > 0", name="generation_positive"),
    CheckConstraint(
        "start_page_offset >= 0 AND checkpoint_page_offset >= 0",
        name="page_offsets_nonnegative",
    ),
    CheckConstraint(
        "start_cursor IS NULL OR octet_length(start_cursor) BETWEEN 1 AND 8192",
        name="start_cursor_length",
    ),
    CheckConstraint(
        "checkpoint_cursor IS NULL OR octet_length(checkpoint_cursor) BETWEEN 1 AND 8192",
        name="checkpoint_cursor_length",
    ),
    CheckConstraint(
        "discovered_count >= 0 AND processed_count >= 0 AND duplicate_count >= 0 "
        "AND skipped_unknown_count >= 0 AND warning_count >= 0 "
        "AND (charged_bytes IS NULL OR charged_bytes >= 0)",
        name="counts_nonnegative",
    ),
    CheckConstraint(
        "truncation_reason IS NULL OR truncation_reason IN "
        "('file_limit', 'discovery_limit', 'charged_byte_run_limit', "
        "'charged_byte_day_limit', 'identity_day_limit', 'attempt_day_limit', "
        "'success_day_limit', 'legacy_limit')",
        name="truncation_reason_value",
    ),
    CheckConstraint(
        "(truncated AND truncation_reason IS NOT NULL) OR "
        "(NOT truncated AND truncation_reason IS NULL)",
        name="truncation_reason_state",
    ),
    CheckConstraint(
        "(status = 'running' AND finished_at IS NULL) OR "
        "(status <> 'running' AND finished_at IS NOT NULL)",
        name="finish_state",
    ),
    CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="attempt_time_order",
    ),
)
Index(
    "uq_collection_attempts_running_checkpoint",
    collection_attempts.c.checkpoint_id,
    unique=True,
    postgresql_where=collection_attempts.c.status == "running",
)
Index(
    "ix_collection_attempts_checkpoint_started",
    collection_attempts.c.checkpoint_id,
    collection_attempts.c.started_at.desc(),
    collection_attempts.c.id.desc(),
)

collection_charge_budgets = Table(
    "collection_charge_budgets",
    metadata,
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("window_started_at", UTC_TIMESTAMP, nullable=False),
    Column("charged_bytes", BigInteger, nullable=False, server_default=text("0")),
    Column("identity_count", BigInteger, nullable=False, server_default=text("0")),
    Column("attempt_count", BigInteger, nullable=False, server_default=text("0")),
    Column("success_count", BigInteger, nullable=False, server_default=text("0")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    CheckConstraint(
        "charged_bytes >= 0 AND identity_count >= 0 AND attempt_count >= 0 AND success_count >= 0",
        name="counts_nonnegative",
    ),
)

collection_budget_buckets = Table(
    "collection_budget_buckets",
    metadata,
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("bucket_started_at", UTC_TIMESTAMP, primary_key=True),
    Column("charged_bytes", BigInteger, nullable=False, server_default=text("0")),
    Column("identity_count", BigInteger, nullable=False, server_default=text("0")),
    Column("attempt_count", BigInteger, nullable=False, server_default=text("0")),
    Column("success_count", BigInteger, nullable=False, server_default=text("0")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    CheckConstraint(
        "charged_bytes >= 0 AND identity_count >= 0 AND attempt_count >= 0 AND success_count >= 0",
        name="counts_nonnegative",
    ),
)

collection_identity_observations = Table(
    "collection_identity_observations",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("observed_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
)
Index(
    "ix_collection_identity_observations_retailer_observed",
    collection_identity_observations.c.retailer_id,
    collection_identity_observations.c.observed_at,
    collection_identity_observations.c.source_file_id,
)

collection_archive_charges = Table(
    "collection_archive_charges",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "attempt_id",
        UUID_PK,
        ForeignKey("collection_attempts.id", ondelete="SET NULL"),
    ),
    Column("content_length", BigInteger, nullable=False),
    Column("charged_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    CheckConstraint("content_length >= 0", name="content_length_nonnegative"),
)
Index(
    "ix_collection_archive_charges_retailer_charged",
    collection_archive_charges.c.retailer_id,
    collection_archive_charges.c.charged_at,
    collection_archive_charges.c.source_file_id,
)

collection_transfer_charges = Table(
    "collection_transfer_charges",
    metadata,
    Column(
        "attempt_id",
        UUID_PK,
        ForeignKey("collection_attempts.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column(
        "retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("content_length", BigInteger, nullable=False),
    Column("settled", Boolean, nullable=False, server_default=text("false")),
    Column("archive_attached", Boolean, nullable=False, server_default=text("false")),
    Column("charged_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    CheckConstraint("content_length >= 0", name="content_length_nonnegative"),
)
Index(
    "ix_collection_transfer_charges_retailer_charged",
    collection_transfer_charges.c.retailer_id,
    collection_transfer_charges.c.charged_at,
    collection_transfer_charges.c.source_file_id,
)
Index(
    "ix_collection_transfer_charges_source_charged",
    collection_transfer_charges.c.source_file_id,
    collection_transfer_charges.c.charged_at,
    collection_transfer_charges.c.attempt_id,
)

stores = Table(
    "stores",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("chain_code", String(128), nullable=False),
    Column("subchain_code", String(128), nullable=False, server_default=text("''")),
    Column("source_store_code", String(128), nullable=False),
    Column("audit_number", String(128)),
    Column("store_type", String(128)),
    Column("chain_name", Text),
    Column("subchain_name", Text),
    Column("name", Text, nullable=False),
    Column("name_search", Text, Computed(_SEARCH_EXPRESSION, persisted=True)),
    Column("address", Text),
    Column("city", Text),
    Column("city_search", Text, Computed(_CITY_SEARCH_EXPRESSION, persisted=True)),
    Column("postal_code", String(32)),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column(
        "first_seen_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")
    ),
    Column("last_seen_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column(
        "last_source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
    ),
    UniqueConstraint(
        "retailer_id",
        "portal_id",
        "subchain_code",
        "source_store_code",
        name="uq_stores_retailer_portal_subchain_code",
    ),
    _nonempty("chain_code", maximum=128),
    _nonempty("source_store_code", maximum=128),
    _nonempty("name", maximum=1024),
    CheckConstraint("last_seen_at >= first_seen_at", name="seen_time_order"),
)
Index("ix_stores_retailer_city_id", stores.c.retailer_id, stores.c.city, stores.c.id)
Index("ix_stores_city_search_id", stores.c.city_search, stores.c.id)
Index(
    "ix_stores_retailer_city_search_id",
    stores.c.retailer_id,
    stores.c.city_search,
    stores.c.id,
)
Index(
    "ix_stores_name_search_trgm",
    stores.c.name_search,
    postgresql_using="gin",
    postgresql_ops={"name_search": "gin_trgm_ops"},
)

store_aliases = Table(
    "store_aliases",
    metadata,
    _uuid_primary_key(),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("alias_kind", String(32), nullable=False),
    Column("alias_value", String(256), nullable=False),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint(
        "retailer_id",
        "portal_id",
        "alias_kind",
        "alias_value",
        name="uq_store_aliases_retailer_portal_kind_value",
    ),
    _nonempty("alias_value", maximum=256),
)

raw_archive_objects = Table(
    "raw_archive_objects",
    metadata,
    _uuid_primary_key(),
    Column("content_sha256", String(64), nullable=False),
    Column("object_key", Text, nullable=False),
    Column("content_length", BigInteger, nullable=False),
    Column("archived_at", UTC_TIMESTAMP, nullable=False),
    Column("verified_at", UTC_TIMESTAMP),
    UniqueConstraint("content_sha256"),
    UniqueConstraint("object_key"),
    CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'", name="content_sha256_format"),
    CheckConstraint("content_length >= 0", name="content_length_nonnegative"),
)
Index(
    "ix_raw_archive_objects_archived_id",
    raw_archive_objects.c.archived_at,
    raw_archive_objects.c.id,
)
source_files = Table(
    "source_files",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="RESTRICT"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("remote_id", Text, nullable=False),
    Column("download_url", Text, nullable=False),
    Column("original_filename", Text, nullable=False),
    Column("document_type", String(32), nullable=False),
    Column("compression", String(16), nullable=False),
    Column("protocol", String(16), nullable=False),
    Column("status", String(32), nullable=False),
    Column("discovered_at", UTC_TIMESTAMP, nullable=False),
    Column("source_timestamp", UTC_TIMESTAMP),
    Column("declared_content_length", BigInteger),
    Column("media_type", Text),
    Column("etag", Text),
    Column("last_modified", UTC_TIMESTAMP),
    Column("response_metadata", JSONB, nullable=False, server_default=text("'[]'::jsonb")),
    Column(
        "raw_archive_object_id", UUID_PK, ForeignKey("raw_archive_objects.id", ondelete="RESTRICT")
    ),
    Column("parser_version", String(128)),
    Column("download_started_at", UTC_TIMESTAMP),
    Column("download_finished_at", UTC_TIMESTAMP),
    Column("download_status_code", Integer),
    Column("download_content_length", BigInteger),
    Column("download_response_metadata", JSONB),
    Column("error_code", String(128)),
    Column("error_message", Text),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint("portal_id", "remote_id"),
    _values("document_type", [value.value for value in DocumentType]),
    _values("compression", [value.value for value in CompressionFormat]),
    _values("protocol", [value.value for value in SourceProtocol]),
    _values("status", [value.value for value in IngestionStatus]),
    _nonempty("remote_id", maximum=4096),
    _nonempty("original_filename", maximum=2048),
    CheckConstraint(
        "declared_content_length IS NULL OR declared_content_length >= 0",
        name="declared_content_length_nonnegative",
    ),
    CheckConstraint(
        "download_content_length IS NULL OR download_content_length >= 0",
        name="download_content_length_nonnegative",
    ),
    CheckConstraint(
        "download_status_code IS NULL OR download_status_code BETWEEN 100 AND 599",
        name="download_status_code_range",
    ),
    CheckConstraint(
        "download_finished_at IS NULL OR download_started_at IS NULL "
        "OR download_finished_at >= download_started_at",
        name="download_time_order",
    ),
)
Index(
    "ix_source_files_archived_id",
    source_files.c.download_finished_at,
    source_files.c.id,
    postgresql_where=source_files.c.raw_archive_object_id.is_not(None),
)
Index(
    "ix_source_files_retailer_document_source_time",
    source_files.c.retailer_id,
    source_files.c.document_type,
    source_files.c.source_timestamp.desc(),
)
Index(
    "ix_source_files_status_discovered_id",
    source_files.c.status,
    source_files.c.discovered_at,
    source_files.c.id,
)
Index("ix_source_files_archive_object", source_files.c.raw_archive_object_id)
Index(
    "ix_source_files_portal_latest",
    source_files.c.portal_id,
    source_files.c.discovered_at.desc(),
    source_files.c.id.desc(),
)

source_scope_watermarks = Table(
    "source_scope_watermarks",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="CASCADE"), nullable=False),
    Column("document_family", String(16), nullable=False),
    Column("subchain_code", String(128), nullable=False, server_default=text("''")),
    Column("source_scope_code", String(128), nullable=False, server_default=text("''")),
    Column("effective_source_timestamp", UTC_TIMESTAMP, nullable=False),
    Column("source_content_sha256", String(64), nullable=False),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint(
        "retailer_id",
        "portal_id",
        "document_family",
        "subchain_code",
        "source_scope_code",
        name="uq_source_scope_watermarks_retailer_family_scope",
    ),
    _values("document_family", ["stores", "prices", "promotions"]),
    CheckConstraint(
        "source_content_sha256 ~ '^[0-9a-f]{64}$'",
        name="content_sha256_format",
    ),
    CheckConstraint("length(subchain_code) <= 128", name="subchain_code_length"),
    CheckConstraint("length(source_scope_code) <= 128", name="source_scope_code_length"),
)
Index("ix_source_scope_watermarks_source_file", source_scope_watermarks.c.source_file_id)

ingestion_runs = Table(
    "ingestion_runs",
    metadata,
    _uuid_primary_key(),
    Column(
        "source_file_id", UUID_PK, ForeignKey("source_files.id", ondelete="CASCADE"), nullable=False
    ),
    Column("attempt", Integer, nullable=False),
    Column("status", String(32), nullable=False),
    Column("started_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("finished_at", UTC_TIMESTAMP),
    Column("metadata_records", BigInteger, nullable=False, server_default=text("0")),
    Column("store_records", BigInteger, nullable=False, server_default=text("0")),
    Column("price_records", BigInteger, nullable=False, server_default=text("0")),
    Column("promotion_records", BigInteger, nullable=False, server_default=text("0")),
    Column("warnings", BigInteger, nullable=False, server_default=text("0")),
    Column("rejected_records", BigInteger, nullable=False, server_default=text("0")),
    Column("file_quarantine_issues", BigInteger, nullable=False, server_default=text("0")),
    Column("validation_issue_bytes", BigInteger, nullable=False, server_default=text("0")),
    Column("validation_issue_samples", BigInteger, nullable=False, server_default=text("0")),
    Column("inserted_records", BigInteger, nullable=False, server_default=text("0")),
    Column("updated_records", BigInteger, nullable=False, server_default=text("0")),
    Column("unchanged_records", BigInteger, nullable=False, server_default=text("0")),
    Column("unavailable_records", BigInteger, nullable=False, server_default=text("0")),
    Column("history_events", BigInteger, nullable=False, server_default=text("0")),
    Column("error_code", String(128)),
    Column("error_message", Text),
    UniqueConstraint("source_file_id", "attempt"),
    _values("status", [value.value for value in IngestionStatus]),
    CheckConstraint("attempt > 0", name="attempt_positive"),
    CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="run_time_order",
    ),
    CheckConstraint(
        "metadata_records >= 0 AND store_records >= 0 AND price_records >= 0 "
        "AND promotion_records >= 0 AND warnings >= 0 AND rejected_records >= 0 "
        "AND file_quarantine_issues >= 0 AND validation_issue_bytes >= 0 "
        "AND validation_issue_samples >= 0 "
        "AND inserted_records >= 0 AND updated_records >= 0 "
        "AND unchanged_records >= 0 AND unavailable_records >= 0 "
        "AND history_events >= 0",
        name="counts_nonnegative",
    ),
)
Index(
    "ix_ingestion_runs_source_file_started",
    ingestion_runs.c.source_file_id,
    ingestion_runs.c.started_at.desc(),
)

source_file_events = Table(
    "source_file_events",
    metadata,
    _uuid_primary_key(),
    Column(
        "source_file_id", UUID_PK, ForeignKey("source_files.id", ondelete="CASCADE"), nullable=False
    ),
    Column(
        "ingestion_run_id",
        UUID_PK,
        ForeignKey("ingestion_runs.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("from_status", String(32)),
    Column("to_status", String(32), nullable=False),
    Column("error_code", String(128)),
    Column("error_message", Text),
    Column("occurred_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("from_status", [value.value for value in IngestionStatus]),
    _values("to_status", [value.value for value in IngestionStatus]),
)
Index(
    "ix_source_file_events_file_time",
    source_file_events.c.source_file_id,
    source_file_events.c.occurred_at,
    source_file_events.c.id,
)

normalized_rebuild_runs = Table(
    "normalized_rebuild_runs",
    metadata,
    _uuid_primary_key(),
    Column("status", String(32), nullable=False),
    Column("requested_by", Text, nullable=False),
    Column("requested_parser_version", String(128), nullable=False),
    Column("archive_cutoff_at", UTC_TIMESTAMP, nullable=False),
    Column("source_files_total", BigInteger, nullable=False, server_default=text("0")),
    Column("source_files_completed", BigInteger, nullable=False, server_default=text("0")),
    Column("last_sequence", BigInteger),
    Column(
        "last_source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
    ),
    Column("last_archived_at", UTC_TIMESTAMP),
    Column("started_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("finished_at", UTC_TIMESTAMP),
    Column("error_code", String(128)),
    Column("error_message", Text),
    _values("status", ["running", "failed", "completed"]),
    _nonempty("requested_by", maximum=128),
    _nonempty("requested_parser_version", maximum=128),
    CheckConstraint(
        "source_files_total >= 0 AND source_files_completed >= 0 "
        "AND source_files_completed <= source_files_total",
        name="source_file_counts_valid",
    ),
    CheckConstraint("last_sequence IS NULL OR last_sequence > 0", name="last_sequence_positive"),
    CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="run_time_order",
    ),
)

normalized_rebuild_files = Table(
    "normalized_rebuild_files",
    metadata,
    Column(
        "rebuild_run_id",
        UUID_PK,
        ForeignKey(
            "normalized_rebuild_runs.id",
            ondelete="CASCADE",
            name="fk_normalized_rebuild_files_run",
        ),
        primary_key=True,
    ),
    Column("sequence", BigInteger, primary_key=True),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("archived_at", UTC_TIMESTAMP, nullable=False),
    Column("original_applied_at", UTC_TIMESTAMP),
    Column("original_parser_version", String(128)),
    Column("effective_source_timestamp", UTC_TIMESTAMP, nullable=False),
    Column("status", String(32), nullable=False, server_default=text("'pending'")),
    Column("completed_at", UTC_TIMESTAMP),
    UniqueConstraint("rebuild_run_id", "source_file_id"),
    _values("status", ["pending", "completed"]),
    CheckConstraint("sequence > 0", name="sequence_positive"),
    CheckConstraint(
        "original_parser_version IS NULL OR length(original_parser_version) BETWEEN 1 AND 128",
        name="original_parser_version_length",
    ),
    CheckConstraint(
        "(status = 'pending' AND completed_at IS NULL) "
        "OR (status = 'completed' AND completed_at IS NOT NULL)",
        name="completion_state_consistent",
    ),
)
Index(
    "ix_normalized_rebuild_files_pending",
    normalized_rebuild_files.c.rebuild_run_id,
    normalized_rebuild_files.c.sequence,
    postgresql_where=normalized_rebuild_files.c.status == "pending",
)

normalized_rebuild_snapshots = Table(
    "normalized_rebuild_snapshots",
    metadata,
    Column(
        "rebuild_run_id",
        UUID_PK,
        ForeignKey(
            "normalized_rebuild_runs.id",
            ondelete="CASCADE",
            name="fk_normalized_rebuild_snapshots_run",
        ),
        primary_key=True,
    ),
    Column("phase", String(16), primary_key=True),
    Column("entity", String(64), primary_key=True),
    Column("row_key", String(512), primary_key=True),
    Column("payload", JSONB, nullable=False),
    Column("outcome", String(16)),
    Column("captured_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("phase", ["original", "rebuilt"]),
    _values(
        "entity",
        [
            "stores",
            "store_aliases",
            "retailer_items",
            "canonical_products",
            "product_identifiers",
            "identifier_match_groups",
            "retailer_identifier_assertions",
            "product_match_candidates",
            "confirmed_product_matches",
            "current_prices",
            "price_history",
            "current_availability",
            "availability_history",
            "promotions",
            "promotion_items",
            "promotion_stores",
            "promotion_clubs",
            "applied_source_contents",
            "source_scope_watermarks",
        ],
    ),
    _values("outcome", ["preserved", "superseded"]),
    CheckConstraint("octet_length(row_key) BETWEEN 1 AND 512", name="row_key_length"),
    CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
    CheckConstraint(
        "octet_length(payload::text) <= 1048576",
        name="payload_size",
    ),
)
Index(
    "ix_normalized_rebuild_snapshots_run_entity",
    normalized_rebuild_snapshots.c.rebuild_run_id,
    normalized_rebuild_snapshots.c.entity,
)

normalized_rebuild_control = Table(
    "normalized_rebuild_control",
    metadata,
    Column("singleton_id", Integer, primary_key=True),
    Column(
        "active_rebuild_run_id",
        UUID_PK,
        ForeignKey(
            "normalized_rebuild_runs.id",
            ondelete="RESTRICT",
            name="fk_normalized_rebuild_control_active",
        ),
    ),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    CheckConstraint("singleton_id = 1", name="singleton_id_value"),
)

validation_issues = Table(
    "validation_issues",
    metadata,
    _uuid_primary_key(),
    Column(
        "source_file_id", UUID_PK, ForeignKey("source_files.id", ondelete="CASCADE"), nullable=False
    ),
    Column(
        "ingestion_run_id",
        UUID_PK,
        ForeignKey("ingestion_runs.id", ondelete="CASCADE"),
    ),
    Column("replay_run_id", UUID_PK, ForeignKey("replay_runs.id", ondelete="CASCADE")),
    Column("severity", String(32), nullable=False),
    Column("code", String(128), nullable=False),
    Column("message", Text, nullable=False),
    Column("record_index", BigInteger),
    Column("field_name", String(256)),
    Column("rejected_value", Text),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("severity", [value.value for value in IssueSeverity]),
    _nonempty("code", maximum=128),
    CheckConstraint(
        "num_nonnulls(ingestion_run_id, replay_run_id) = 1",
        name="exactly_one_run",
    ),
)
Index(
    "ix_validation_issues_file_severity_record",
    validation_issues.c.source_file_id,
    validation_issues.c.severity,
    validation_issues.c.record_index,
)
Index(
    "ix_validation_issues_ingestion_evidence",
    validation_issues.c.ingestion_run_id,
    validation_issues.c.created_at,
    validation_issues.c.id,
    postgresql_where=validation_issues.c.ingestion_run_id.is_not(None),
)
Index(
    "ix_validation_issues_replay_evidence",
    validation_issues.c.replay_run_id,
    validation_issues.c.created_at,
    validation_issues.c.id,
    postgresql_where=validation_issues.c.replay_run_id.is_not(None),
)

replay_runs = Table(
    "replay_runs",
    metadata,
    _uuid_primary_key(),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "rebuild_run_id",
        UUID_PK,
        ForeignKey("normalized_rebuild_runs.id", ondelete="RESTRICT"),
    ),
    Column("requested_parser_version", String(128), nullable=False),
    Column("previous_parser_version", String(128)),
    Column("status", String(32), nullable=False),
    Column("started_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("finished_at", UTC_TIMESTAMP),
    Column("result_summary", JSONB),
    Column("error_message", Text),
    _values("status", [value.value for value in IngestionStatus]),
    CheckConstraint(
        "finished_at IS NULL OR finished_at >= started_at",
        name="run_time_order",
    ),
)
Index(
    "ix_replay_runs_source_file_started",
    replay_runs.c.source_file_id,
    replay_runs.c.started_at.desc(),
)
Index("ix_replay_runs_rebuild_started", replay_runs.c.rebuild_run_id, replay_runs.c.started_at)
Index(
    "uq_replay_runs_open_source_file",
    replay_runs.c.source_file_id,
    unique=True,
    postgresql_where=replay_runs.c.finished_at.is_(None),
)

applied_source_contents = Table(
    "applied_source_contents",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="RESTRICT"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("document_type", String(32), nullable=False),
    Column("content_sha256", String(64), nullable=False),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("applied_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint("source_file_id"),
    _values("document_type", [value.value for value in DocumentType]),
    CheckConstraint("content_sha256 ~ '^[0-9a-f]{64}$'", name="content_sha256_format"),
)

retailer_items = Table(
    "retailer_items",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("source_item_code", String(128), nullable=False),
    Column("gtin", String(14)),
    Column("item_type", Integer),
    Column("name", Text, nullable=False),
    Column("name_search", Text, Computed(_SEARCH_EXPRESSION, persisted=True)),
    Column("manufacturer_name", Text),
    Column("manufacturer_country", Text),
    Column("manufacturer_description", Text),
    Column("unit_quantity", Text),
    Column("quantity", QUANTITY),
    Column("unit_of_measure", String(64)),
    Column("is_weighted", Boolean),
    Column("quantity_in_package", QUANTITY),
    Column("first_seen_at", UTC_TIMESTAMP, nullable=False),
    Column("last_seen_at", UTC_TIMESTAMP, nullable=False),
    Column(
        "last_source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    UniqueConstraint(
        "retailer_id",
        "portal_id",
        "source_item_code",
        name="uq_retailer_items_retailer_portal_code",
    ),
    _nonempty("source_item_code", maximum=128),
    _nonempty("name", maximum=2048),
    CheckConstraint("gtin IS NULL OR gtin ~ '^[0-9]{8,14}$'", name="gtin_digits"),
    CheckConstraint("quantity IS NULL OR quantity >= 0", name="quantity_nonnegative"),
    CheckConstraint(
        "quantity_in_package IS NULL OR quantity_in_package >= 0",
        name="package_quantity_nonnegative",
    ),
    CheckConstraint("last_seen_at >= first_seen_at", name="seen_time_order"),
)
Index("ix_retailer_items_gtin", retailer_items.c.gtin)
Index(
    "ix_retailer_items_last_source_file_id_id",
    retailer_items.c.last_source_file_id,
    retailer_items.c.id,
)
Index(
    "ix_retailer_items_name_search_trgm",
    retailer_items.c.name_search,
    postgresql_using="gin",
    postgresql_ops={"name_search": "gin_trgm_ops"},
)

canonical_products = Table(
    "canonical_products",
    metadata,
    _uuid_primary_key(),
    Column("name", Text, nullable=False),
    Column("name_search", Text, Computed(_SEARCH_EXPRESSION, persisted=True)),
    Column("brand", Text),
    Column("manufacturer", Text),
    Column("quantity", QUANTITY),
    Column("unit_of_measure", String(64)),
    Column("status", String(32), nullable=False, server_default=text("'active'")),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("status", ["active", "merged", "retired"]),
    _nonempty("name", maximum=2048),
    CheckConstraint("quantity IS NULL OR quantity >= 0", name="quantity_nonnegative"),
)
Index(
    "ix_canonical_products_name_search_trgm",
    canonical_products.c.name_search,
    postgresql_using="gin",
    postgresql_ops={"name_search": "gin_trgm_ops"},
)
Index(
    "ix_canonical_products_active_name_prefix",
    canonical_products.c.name_search,
    canonical_products.c.id,
    postgresql_ops={"name_search": "text_pattern_ops"},
    postgresql_where=canonical_products.c.status == "active",
)
Index(
    "ix_canonical_products_active_name_trgm_gist",
    canonical_products.c.name_search,
    postgresql_using="gist",
    postgresql_ops={"name_search": "gist_trgm_ops"},
    postgresql_where=canonical_products.c.status == "active",
)

product_identifiers = Table(
    "product_identifiers",
    metadata,
    _uuid_primary_key(),
    Column(
        "product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("kind", String(32), nullable=False),
    Column("value", String(128), nullable=False),
    Column("normalized_value", String(128), nullable=False),
    Column("issuer_retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE")),
    Column("issuer_portal_id", UUID_PK, ForeignKey("portals.id", ondelete="CASCADE")),
    Column("is_validated", Boolean, nullable=False, server_default=text("false")),
    Column("validation_method", String(64)),
    Column(
        "validation_evidence",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
    ),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    _values("kind", [value.value for value in IdentifierKind]),
    _nonempty("normalized_value", maximum=128),
    CheckConstraint(
        "issuer_portal_id IS NULL OR issuer_retailer_id IS NOT NULL",
        name="portal_scope",
    ),
    CheckConstraint(
        "kind <> 'retailer_item' OR issuer_portal_id IS NOT NULL",
        name="item_portal_scope",
    ),
)
Index(
    "uq_product_identifiers_identity",
    product_identifiers.c.kind,
    product_identifiers.c.normalized_value,
    product_identifiers.c.issuer_retailer_id,
    product_identifiers.c.issuer_portal_id,
    unique=True,
    postgresql_nulls_not_distinct=True,
)
Index("ix_product_identifiers_product", product_identifiers.c.product_id)

identifier_match_groups = Table(
    "identifier_match_groups",
    metadata,
    _uuid_primary_key(),
    Column("kind", String(32), nullable=False),
    Column("normalized_value", String(128), nullable=False),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("updated_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    UniqueConstraint("kind", "normalized_value"),
    _values("kind", [value.value for value in IdentifierKind]),
    _nonempty("normalized_value", maximum=128),
)
Index("ix_identifier_match_groups_product", identifier_match_groups.c.canonical_product_id)

retailer_identifier_assertions = Table(
    "retailer_identifier_assertions",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("kind", String(32), nullable=False),
    Column("value", String(128), nullable=False),
    Column("normalized_value", String(128), nullable=False),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("validation_method", String(64), nullable=False),
    Column("asserted_at", UTC_TIMESTAMP, nullable=False),
    Column("superseded_at", UTC_TIMESTAMP),
    UniqueConstraint(
        "retailer_item_id",
        "kind",
        "source_file_id",
        name="uq_retailer_identifier_assertion_item_kind_source",
    ),
    _values("kind", [value.value for value in IdentifierKind]),
    _nonempty("value", maximum=128),
    _nonempty("normalized_value", maximum=128),
    CheckConstraint(
        "superseded_at IS NULL OR superseded_at >= asserted_at",
        name="superseded_after_assertion",
    ),
)
Index(
    "uq_retailer_identifier_assertions_current",
    retailer_identifier_assertions.c.retailer_item_id,
    retailer_identifier_assertions.c.kind,
    unique=True,
    postgresql_where=retailer_identifier_assertions.c.superseded_at.is_(None),
)
Index(
    "ix_retailer_identifier_assertions_active_value",
    retailer_identifier_assertions.c.kind,
    retailer_identifier_assertions.c.normalized_value,
    retailer_identifier_assertions.c.retailer_item_id,
    postgresql_where=retailer_identifier_assertions.c.superseded_at.is_(None),
)
Index(
    "ix_retailer_identifier_assertions_source",
    retailer_identifier_assertions.c.source_file_id,
)

product_match_candidates = Table(
    "product_match_candidates",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("method", String(64), nullable=False),
    Column("score", SCORE, nullable=False),
    Column("status", String(32), nullable=False, server_default=text("'pending'")),
    Column("evidence", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("created_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("reviewed_at", UTC_TIMESTAMP),
    Column("reviewed_by", Text),
    UniqueConstraint("retailer_item_id", "canonical_product_id", "method"),
    _values("status", ["pending", "accepted", "rejected", "superseded"]),
    CheckConstraint("score BETWEEN 0 AND 1", name="score_range"),
)
Index(
    "ix_match_candidates_status_score",
    product_match_candidates.c.status,
    product_match_candidates.c.score.desc(),
    product_match_candidates.c.id,
)

confirmed_product_matches = Table(
    "confirmed_product_matches",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("method", String(64), nullable=False),
    Column("evidence", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("confirmed_at", UTC_TIMESTAMP, nullable=False, server_default=text("clock_timestamp()")),
    Column("confirmed_by", Text, nullable=False),
    UniqueConstraint("retailer_item_id"),
)
Index(
    "ix_confirmed_product_matches_product_item",
    confirmed_product_matches.c.canonical_product_id,
    confirmed_product_matches.c.retailer_item_id,
)

current_prices = Table(
    "current_prices",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="SET NULL"),
    ),
    Column(
        "query_retailer_id",
        UUID_PK,
        ForeignKey("retailers.id", ondelete="CASCADE"),
    ),
    Column("item_price", MONEY, nullable=False),
    Column("unit_of_measure_price", MONEY),
    Column("allow_discount", Boolean),
    Column("source_updated_at", UTC_TIMESTAMP),
    Column("last_sale_at", UTC_TIMESTAMP),
    Column("audit_number", String(128)),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("first_observed_at", UTC_TIMESTAMP, nullable=False),
    Column("last_observed_at", UTC_TIMESTAMP, nullable=False),
    UniqueConstraint("retailer_item_id", "store_id"),
    CheckConstraint("item_price >= 0", name="item_price_nonnegative"),
    CheckConstraint(
        "unit_of_measure_price IS NULL OR unit_of_measure_price >= 0", name="unit_price_nonnegative"
    ),
    CheckConstraint("last_observed_at >= first_observed_at", name="observation_time_order"),
)
Index(
    "ix_current_prices_store_price_item",
    current_prices.c.store_id,
    current_prices.c.item_price,
    current_prices.c.retailer_item_id,
)
Index(
    "ix_current_prices_product_price_id",
    current_prices.c.canonical_product_id,
    current_prices.c.item_price,
    current_prices.c.id,
    postgresql_where=current_prices.c.canonical_product_id.is_not(None),
)
Index(
    "ix_current_prices_product_retailer_price_id",
    current_prices.c.canonical_product_id,
    current_prices.c.query_retailer_id,
    current_prices.c.item_price,
    current_prices.c.id,
    postgresql_where=current_prices.c.canonical_product_id.is_not(None),
)
Index(
    "ix_current_prices_product_store_price_id",
    current_prices.c.canonical_product_id,
    current_prices.c.store_id,
    current_prices.c.item_price,
    current_prices.c.id,
    postgresql_where=current_prices.c.canonical_product_id.is_not(None),
)
Index(
    "ix_current_prices_product_store_retailer_price_id",
    current_prices.c.canonical_product_id,
    current_prices.c.store_id,
    current_prices.c.query_retailer_id,
    current_prices.c.item_price,
    current_prices.c.id,
    postgresql_where=current_prices.c.canonical_product_id.is_not(None),
)

price_history = Table(
    "price_history",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="SET NULL"),
    ),
    Column("item_price", MONEY, nullable=False),
    Column("unit_of_measure_price", MONEY),
    Column("allow_discount", Boolean),
    Column("source_updated_at", UTC_TIMESTAMP),
    Column("last_sale_at", UTC_TIMESTAMP),
    Column("audit_number", String(128)),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("valid_from", UTC_TIMESTAMP, nullable=False),
    Column("valid_to", UTC_TIMESTAMP),
    Column(
        "valid_period",
        TSTZRANGE,
        Computed("tstzrange(valid_from, valid_to, '[)')", persisted=True),
        nullable=False,
    ),
    CheckConstraint("valid_to IS NULL OR valid_to > valid_from", name="validity_order"),
    CheckConstraint("item_price >= 0", name="item_price_nonnegative"),
    CheckConstraint(
        "unit_of_measure_price IS NULL OR unit_of_measure_price >= 0",
        name="unit_price_nonnegative",
    ),
)
Index(
    "uq_price_history_open",
    price_history.c.retailer_item_id,
    price_history.c.store_id,
    unique=True,
    postgresql_where=price_history.c.valid_to.is_(None),
)
Index(
    "ix_price_history_item_store_from",
    price_history.c.retailer_item_id,
    price_history.c.store_id,
    price_history.c.valid_from.desc(),
    price_history.c.id,
)
Index(
    "ix_price_history_product_from_id",
    price_history.c.canonical_product_id,
    price_history.c.valid_from.desc(),
    price_history.c.id,
    postgresql_include=["valid_to", "store_id"],
    postgresql_where=price_history.c.canonical_product_id.is_not(None),
)
Index(
    "ix_price_history_product_store_from_id",
    price_history.c.canonical_product_id,
    price_history.c.store_id,
    price_history.c.valid_from.desc(),
    price_history.c.id,
    postgresql_include=["valid_to"],
    postgresql_where=price_history.c.canonical_product_id.is_not(None),
)
Index(
    "ix_price_history_product_period_gist",
    price_history.c.canonical_product_id,
    price_history.c.valid_period,
    postgresql_using="gist",
    postgresql_where=price_history.c.canonical_product_id.is_not(None),
)
Index(
    "ix_price_history_product_store_period_gist",
    price_history.c.canonical_product_id,
    price_history.c.store_id,
    price_history.c.valid_period,
    postgresql_using="gist",
    postgresql_where=price_history.c.canonical_product_id.is_not(None),
)

current_availability = Table(
    "current_availability",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False),
    Column(
        "canonical_product_id",
        UUID_PK,
        ForeignKey("canonical_products.id", ondelete="SET NULL"),
    ),
    Column("is_available", Boolean, nullable=False),
    Column("item_status", Integer),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("first_observed_at", UTC_TIMESTAMP, nullable=False),
    Column("last_observed_at", UTC_TIMESTAMP, nullable=False),
    UniqueConstraint("retailer_item_id", "store_id"),
    CheckConstraint("last_observed_at >= first_observed_at", name="observation_time_order"),
)
Index(
    "ix_current_availability_store_available_item",
    current_availability.c.store_id,
    current_availability.c.is_available,
    current_availability.c.retailer_item_id,
)
Index(
    "ix_current_availability_store_latest",
    current_availability.c.store_id,
    current_availability.c.last_observed_at.desc(),
    current_availability.c.source_file_id.desc(),
    current_availability.c.id.desc(),
)
Index(
    "ix_current_availability_product_id",
    current_availability.c.canonical_product_id,
    current_availability.c.id,
    postgresql_where=current_availability.c.canonical_product_id.is_not(None),
)
Index(
    "ix_current_availability_product_store_id",
    current_availability.c.canonical_product_id,
    current_availability.c.store_id,
    current_availability.c.id,
    postgresql_where=current_availability.c.canonical_product_id.is_not(None),
)

availability_history = Table(
    "availability_history",
    metadata,
    _uuid_primary_key(),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), nullable=False),
    Column("is_available", Boolean, nullable=False),
    Column("item_status", Integer),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("valid_from", UTC_TIMESTAMP, nullable=False),
    Column("valid_to", UTC_TIMESTAMP),
    CheckConstraint("valid_to IS NULL OR valid_to > valid_from", name="validity_order"),
)
Index(
    "uq_availability_history_open",
    availability_history.c.retailer_item_id,
    availability_history.c.store_id,
    unique=True,
    postgresql_where=availability_history.c.valid_to.is_(None),
)
Index(
    "ix_availability_history_item_store_from",
    availability_history.c.retailer_item_id,
    availability_history.c.store_id,
    availability_history.c.valid_from.desc(),
    availability_history.c.id,
)

promotions = Table(
    "promotions",
    metadata,
    _uuid_primary_key(),
    Column("retailer_id", UUID_PK, ForeignKey("retailers.id", ondelete="CASCADE"), nullable=False),
    Column("portal_id", UUID_PK, ForeignKey("portals.id", ondelete="RESTRICT"), nullable=False),
    Column("subchain_code", String(128), nullable=False, server_default=text("''")),
    Column("source_promotion_id", String(256), nullable=False),
    Column("source_scope_store_code", String(128), nullable=False, server_default=text("''")),
    Column("description", Text),
    Column("discount_kind", String(32), nullable=False),
    Column("starts_at", UTC_TIMESTAMP),
    Column("ends_at", UTC_TIMESTAMP),
    Column("reward_type", Integer),
    Column("allows_multiple_discounts", Boolean),
    Column("minimum_quantity", QUANTITY),
    Column("maximum_quantity", QUANTITY),
    Column("discount_rate", Numeric(10, 6)),
    Column("minimum_purchase", MONEY),
    Column("discounted_price", MONEY),
    Column("discounted_unit_price", MONEY),
    Column("minimum_items_offered", Integer),
    Column("additional_restrictions", Text),
    Column("remarks", Text),
    Column("is_active", Boolean),
    Column("fingerprint_sha256", String(64), nullable=False),
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("valid_from", UTC_TIMESTAMP, nullable=False),
    Column("valid_to", UTC_TIMESTAMP),
    Column(
        "valid_period",
        TSTZRANGE,
        Computed("tstzrange(valid_from, valid_to, '[)')", persisted=True),
        nullable=False,
    ),
    Column(
        "active_period",
        TSTZRANGE,
        Computed(
            "tstzrange(valid_from, valid_to, '[)') * tstzrange(starts_at, ends_at, '[]')",
            persisted=True,
        ),
        nullable=False,
    ),
    Column("last_observed_at", UTC_TIMESTAMP, nullable=False),
    _values("discount_kind", [value.value for value in DiscountKind]),
    CheckConstraint("fingerprint_sha256 ~ '^[0-9a-f]{64}$'", name="fingerprint_format"),
    CheckConstraint("valid_to IS NULL OR valid_to > valid_from", name="validity_order"),
    CheckConstraint(
        "ends_at IS NULL OR starts_at IS NULL OR ends_at >= starts_at", name="source_time_order"
    ),
    CheckConstraint(
        "minimum_quantity IS NULL OR minimum_quantity >= 0",
        name="minimum_quantity_nonnegative",
    ),
    CheckConstraint(
        "maximum_quantity IS NULL OR maximum_quantity >= 0",
        name="maximum_quantity_nonnegative",
    ),
    CheckConstraint(
        "maximum_quantity IS NULL OR minimum_quantity IS NULL "
        "OR maximum_quantity >= minimum_quantity",
        name="quantity_range",
    ),
    CheckConstraint(
        "discount_rate IS NULL OR discount_rate >= 0",
        name="discount_rate_nonnegative",
    ),
    CheckConstraint(
        "minimum_purchase IS NULL OR minimum_purchase >= 0",
        name="minimum_purchase_nonnegative",
    ),
    CheckConstraint(
        "discounted_price IS NULL OR discounted_price >= 0",
        name="discounted_price_nonnegative",
    ),
    CheckConstraint(
        "discounted_unit_price IS NULL OR discounted_unit_price >= 0",
        name="discounted_unit_price_nonnegative",
    ),
    CheckConstraint(
        "minimum_items_offered IS NULL OR minimum_items_offered >= 0",
        name="minimum_items_nonnegative",
    ),
)
Index(
    "uq_promotions_open",
    promotions.c.retailer_id,
    promotions.c.portal_id,
    promotions.c.subchain_code,
    promotions.c.source_promotion_id,
    promotions.c.source_scope_store_code,
    unique=True,
    postgresql_where=promotions.c.valid_to.is_(None),
)
Index(
    "ix_promotions_active_window",
    promotions.c.starts_at,
    promotions.c.ends_at,
    promotions.c.id,
    postgresql_where=promotions.c.valid_to.is_(None),
)
Index(
    "ix_promotions_history_from_id",
    promotions.c.valid_from.desc(),
    promotions.c.id,
)
Index(
    "ix_promotions_valid_period_gist",
    promotions.c.valid_period,
    postgresql_using="gist",
)
Index(
    "ix_promotions_active_period_gist",
    promotions.c.active_period,
    postgresql_using="gist",
    postgresql_where=text("COALESCE(is_active, true)"),
)

promotion_items = Table(
    "promotion_items",
    metadata,
    Column(
        "promotion_id", UUID_PK, ForeignKey("promotions.id", ondelete="CASCADE"), primary_key=True
    ),
    Column(
        "retailer_item_id",
        UUID_PK,
        ForeignKey("retailer_items.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("item_type", Integer),
    Column("is_gift", Boolean, nullable=False, server_default=text("false")),
)
Index(
    "ix_promotion_items_item_promotion",
    promotion_items.c.retailer_item_id,
    promotion_items.c.promotion_id,
)

promotion_stores = Table(
    "promotion_stores",
    metadata,
    Column(
        "promotion_id", UUID_PK, ForeignKey("promotions.id", ondelete="CASCADE"), primary_key=True
    ),
    Column("store_id", UUID_PK, ForeignKey("stores.id", ondelete="CASCADE"), primary_key=True),
)
Index(
    "ix_promotion_stores_store_promotion",
    promotion_stores.c.store_id,
    promotion_stores.c.promotion_id,
)

promotion_clubs = Table(
    "promotion_clubs",
    metadata,
    Column(
        "promotion_id", UUID_PK, ForeignKey("promotions.id", ondelete="CASCADE"), primary_key=True
    ),
    Column("club_id", String(128), primary_key=True),
)

staged_documents = Table(
    "staged_documents",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("metadata_index", Integer, primary_key=True),
    Column("document_type", String(32), nullable=False),
    Column("chain_id", String(128)),
    Column("subchain_id", String(128)),
    Column("store_id", String(128)),
    Column("audit_number", String(128)),
    Column("source_updated_at", UTC_TIMESTAMP),
    _values("document_type", [value.value for value in DocumentType]),
)

staged_stores = Table(
    "staged_stores",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("record_index", BigInteger, primary_key=True),
    Column("chain_id", String(128), nullable=False),
    Column("subchain_id", String(128), nullable=False),
    Column("source_store_id", String(128), nullable=False),
    Column("audit_number", String(128)),
    Column("store_type", String(128)),
    Column("chain_name", Text),
    Column("subchain_name", Text),
    Column("store_name", Text, nullable=False),
    Column("address", Text),
    Column("city", Text),
    Column("postal_code", String(32)),
)
Index(
    "ix_staged_stores_file_identity",
    staged_stores.c.source_file_id,
    staged_stores.c.subchain_id,
    staged_stores.c.source_store_id,
)

staged_prices = Table(
    "staged_prices",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("record_index", BigInteger, primary_key=True),
    Column("chain_id", String(128), nullable=False),
    Column("subchain_id", String(128), nullable=False),
    Column("source_store_id", String(128), nullable=False),
    Column("source_item_code", String(128), nullable=False),
    Column("gtin", String(14)),
    Column("item_type", Integer),
    Column("item_name", Text, nullable=False),
    Column("manufacturer_name", Text),
    Column("manufacturer_country", Text),
    Column("manufacturer_description", Text),
    Column("unit_quantity", Text),
    Column("quantity", QUANTITY),
    Column("unit_of_measure", String(64)),
    Column("is_weighted", Boolean),
    Column("quantity_in_package", QUANTITY),
    Column("item_price", MONEY, nullable=False),
    Column("unit_of_measure_price", MONEY),
    Column("allow_discount", Boolean),
    Column("item_status", Integer),
    Column("price_updated_at", UTC_TIMESTAMP),
    Column("last_sale_at", UTC_TIMESTAMP),
    Column("audit_number", String(128)),
    CheckConstraint("item_price >= 0", name="item_price_nonnegative"),
    CheckConstraint(
        "unit_of_measure_price IS NULL OR unit_of_measure_price >= 0",
        name="unit_price_nonnegative",
    ),
    CheckConstraint("quantity IS NULL OR quantity >= 0", name="quantity_nonnegative"),
    CheckConstraint(
        "quantity_in_package IS NULL OR quantity_in_package >= 0",
        name="package_quantity_nonnegative",
    ),
)
Index(
    "ix_staged_prices_file_store_item_record",
    staged_prices.c.source_file_id,
    staged_prices.c.subchain_id,
    staged_prices.c.source_store_id,
    staged_prices.c.source_item_code,
    staged_prices.c.record_index.desc(),
)

staged_promotions = Table(
    "staged_promotions",
    metadata,
    Column(
        "source_file_id",
        UUID_PK,
        ForeignKey("source_files.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("record_index", BigInteger, primary_key=True),
    Column("chain_id", String(128), nullable=False),
    Column("subchain_id", String(128), nullable=False),
    Column("source_scope_store_code", String(128), nullable=False),
    Column("source_promotion_id", String(256), nullable=False),
    Column("description", Text),
    Column("discount_kind", String(32), nullable=False),
    Column("starts_at", UTC_TIMESTAMP),
    Column("ends_at", UTC_TIMESTAMP),
    Column("reward_type", Integer),
    Column("allows_multiple_discounts", Boolean),
    Column("minimum_quantity", QUANTITY),
    Column("maximum_quantity", QUANTITY),
    Column("discount_rate", Numeric(10, 6)),
    Column("minimum_purchase", MONEY),
    Column("discounted_price", MONEY),
    Column("discounted_unit_price", MONEY),
    Column("minimum_items_offered", Integer),
    Column("additional_restrictions", Text),
    Column("remarks", Text),
    Column("is_active", Boolean),
    Column("fingerprint_sha256", String(64), nullable=False),
    _values("discount_kind", [value.value for value in DiscountKind]),
)
Index(
    "ix_staged_promotions_file_identity_record",
    staged_promotions.c.source_file_id,
    staged_promotions.c.source_promotion_id,
    staged_promotions.c.source_scope_store_code,
    staged_promotions.c.record_index.desc(),
)

staged_promotion_items = Table(
    "staged_promotion_items",
    metadata,
    Column("source_file_id", UUID_PK, primary_key=True),
    Column("record_index", BigInteger, primary_key=True),
    Column("item_index", Integer, primary_key=True),
    Column("source_item_code", String(128), nullable=False),
    Column("item_type", Integer),
    Column("is_gift", Boolean, nullable=False),
    ForeignKeyConstraint(
        ["source_file_id", "record_index"],
        ["staged_promotions.source_file_id", "staged_promotions.record_index"],
        ondelete="CASCADE",
        deferrable=True,
        initially="DEFERRED",
    ),
)

staged_promotion_stores = Table(
    "staged_promotion_stores",
    metadata,
    Column("source_file_id", UUID_PK, primary_key=True),
    Column("record_index", BigInteger, primary_key=True),
    Column("store_index", Integer, primary_key=True),
    Column("source_store_code", String(128), nullable=False),
    ForeignKeyConstraint(
        ["source_file_id", "record_index"],
        ["staged_promotions.source_file_id", "staged_promotions.record_index"],
        ondelete="CASCADE",
        deferrable=True,
        initially="DEFERRED",
    ),
)

staged_promotion_clubs = Table(
    "staged_promotion_clubs",
    metadata,
    Column("source_file_id", UUID_PK, primary_key=True),
    Column("record_index", BigInteger, primary_key=True),
    Column("club_index", Integer, primary_key=True),
    Column("club_id", String(128), nullable=False),
    ForeignKeyConstraint(
        ["source_file_id", "record_index"],
        ["staged_promotions.source_file_id", "staged_promotions.record_index"],
        ondelete="CASCADE",
        deferrable=True,
        initially="DEFERRED",
    ),
)

leases = Table(
    "leases",
    metadata,
    Column("resource", String(512), primary_key=True),
    Column("owner", String(256), nullable=False),
    Column("lease_token", UUID_PK, nullable=False),
    Column("acquired_at", UTC_TIMESTAMP, nullable=False),
    Column("expires_at", UTC_TIMESTAMP, nullable=False),
    CheckConstraint("expires_at > acquired_at", name="expiry_after_acquisition"),
    _nonempty("resource", maximum=512),
    _nonempty("owner", maximum=256),
)
Index("ix_leases_expiry", leases.c.expires_at)


# `metadata.create_all()` is used only by the isolated scale benchmark. Alembic is
# authoritative for deployed databases, but the benchmark must install the same
# projection-maintenance boundary so its ingestion and query measurements remain
# representative of the migrated runtime.
QUERY_PROJECTION_MAINTENANCE_DDL: tuple[str, ...] = (
    """
    CREATE FUNCTION makolet_project_inserted_current_prices()
    RETURNS trigger LANGUAGE plpgsql AS $$
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
               IS DISTINCT FROM ROW(match.canonical_product_id, item.retailer_id);
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_current_prices_project_insert
    AFTER INSERT ON current_prices
    REFERENCING NEW TABLE AS makolet_inserted_current_prices
    FOR EACH STATEMENT EXECUTE FUNCTION makolet_project_inserted_current_prices()
    """,
    """
    CREATE FUNCTION makolet_project_inserted_price_history()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE price_history target
           SET canonical_product_id = match.canonical_product_id
          FROM makolet_inserted_price_history inserted
          JOIN retailer_items item ON item.id = inserted.retailer_item_id
          LEFT JOIN confirmed_product_matches match ON match.retailer_item_id = item.id
         WHERE target.id = inserted.id
           AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_price_history_project_insert
    AFTER INSERT ON price_history
    REFERENCING NEW TABLE AS makolet_inserted_price_history
    FOR EACH STATEMENT EXECUTE FUNCTION makolet_project_inserted_price_history()
    """,
    """
    CREATE FUNCTION makolet_project_inserted_current_availability()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE current_availability target
           SET canonical_product_id = match.canonical_product_id
          FROM makolet_inserted_current_availability inserted
          JOIN retailer_items item ON item.id = inserted.retailer_item_id
          LEFT JOIN confirmed_product_matches match ON match.retailer_item_id = item.id
         WHERE target.id = inserted.id
           AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_current_availability_project_insert
    AFTER INSERT ON current_availability
    REFERENCING NEW TABLE AS makolet_inserted_current_availability
    FOR EACH STATEMENT EXECUTE FUNCTION makolet_project_inserted_current_availability()
    """,
    """
    CREATE FUNCTION makolet_project_current_price_rekey()
    RETURNS trigger LANGUAGE plpgsql AS $$
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
    """,
    """
    CREATE TRIGGER trg_current_prices_project_rekey
    BEFORE UPDATE OF retailer_item_id ON current_prices
    FOR EACH ROW EXECUTE FUNCTION makolet_project_current_price_rekey()
    """,
    """
    CREATE FUNCTION makolet_project_product_row_rekey()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        SELECT match.canonical_product_id
          INTO NEW.canonical_product_id
          FROM confirmed_product_matches match
         WHERE match.retailer_item_id = NEW.retailer_item_id;
        RETURN NEW;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_price_history_project_rekey
    BEFORE UPDATE OF retailer_item_id ON price_history
    FOR EACH ROW EXECUTE FUNCTION makolet_project_product_row_rekey()
    """,
    """
    CREATE TRIGGER trg_current_availability_project_rekey
    BEFORE UPDATE OF retailer_item_id ON current_availability
    FOR EACH ROW EXECUTE FUNCTION makolet_project_product_row_rekey()
    """,
    """
    CREATE FUNCTION makolet_project_inserted_confirmed_matches()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE current_prices target
           SET canonical_product_id = inserted.canonical_product_id,
               query_retailer_id = item.retailer_id
          FROM makolet_inserted_confirmed_matches inserted
          JOIN retailer_items item ON item.id = inserted.retailer_item_id
         WHERE target.retailer_item_id = inserted.retailer_item_id
           AND ROW(target.canonical_product_id, target.query_retailer_id)
               IS DISTINCT FROM ROW(inserted.canonical_product_id, item.retailer_id);
        UPDATE price_history target
           SET canonical_product_id = inserted.canonical_product_id
          FROM makolet_inserted_confirmed_matches inserted
         WHERE target.retailer_item_id = inserted.retailer_item_id
           AND target.canonical_product_id IS DISTINCT FROM inserted.canonical_product_id;
        UPDATE current_availability target
           SET canonical_product_id = inserted.canonical_product_id
          FROM makolet_inserted_confirmed_matches inserted
         WHERE target.retailer_item_id = inserted.retailer_item_id
           AND target.canonical_product_id IS DISTINCT FROM inserted.canonical_product_id;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_confirmed_matches_refresh_query_projection
    AFTER INSERT ON confirmed_product_matches
    REFERENCING NEW TABLE AS makolet_inserted_confirmed_matches
    FOR EACH STATEMENT EXECUTE FUNCTION makolet_project_inserted_confirmed_matches()
    """,
    """
    CREATE FUNCTION makolet_project_updated_confirmed_matches()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        WITH affected AS MATERIALIZED (
            SELECT old_match.retailer_item_id
              FROM makolet_old_confirmed_matches old_match
              LEFT JOIN makolet_new_confirmed_matches new_match ON new_match.id = old_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
            UNION
            SELECT new_match.retailer_item_id
              FROM makolet_new_confirmed_matches new_match
              LEFT JOIN makolet_old_confirmed_matches old_match ON old_match.id = new_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
        )
        UPDATE current_prices target
           SET canonical_product_id = match.canonical_product_id,
               query_retailer_id = item.retailer_id
          FROM affected
          JOIN retailer_items item ON item.id = affected.retailer_item_id
          LEFT JOIN confirmed_product_matches match ON match.retailer_item_id = item.id
         WHERE target.retailer_item_id = affected.retailer_item_id
           AND ROW(target.canonical_product_id, target.query_retailer_id)
               IS DISTINCT FROM ROW(match.canonical_product_id, item.retailer_id);

        WITH affected AS MATERIALIZED (
            SELECT old_match.retailer_item_id
              FROM makolet_old_confirmed_matches old_match
              LEFT JOIN makolet_new_confirmed_matches new_match ON new_match.id = old_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
            UNION
            SELECT new_match.retailer_item_id
              FROM makolet_new_confirmed_matches new_match
              LEFT JOIN makolet_old_confirmed_matches old_match ON old_match.id = new_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
        )
        UPDATE price_history target SET canonical_product_id = match.canonical_product_id
          FROM affected
          LEFT JOIN confirmed_product_matches match
            ON match.retailer_item_id = affected.retailer_item_id
         WHERE target.retailer_item_id = affected.retailer_item_id
           AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;

        WITH affected AS MATERIALIZED (
            SELECT old_match.retailer_item_id
              FROM makolet_old_confirmed_matches old_match
              LEFT JOIN makolet_new_confirmed_matches new_match ON new_match.id = old_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
            UNION
            SELECT new_match.retailer_item_id
              FROM makolet_new_confirmed_matches new_match
              LEFT JOIN makolet_old_confirmed_matches old_match ON old_match.id = new_match.id
             WHERE ROW(old_match.retailer_item_id, old_match.canonical_product_id)
                   IS DISTINCT FROM
                   ROW(new_match.retailer_item_id, new_match.canonical_product_id)
        )
        UPDATE current_availability target
           SET canonical_product_id = match.canonical_product_id
          FROM affected
          LEFT JOIN confirmed_product_matches match
            ON match.retailer_item_id = affected.retailer_item_id
         WHERE target.retailer_item_id = affected.retailer_item_id
           AND target.canonical_product_id IS DISTINCT FROM match.canonical_product_id;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_confirmed_matches_rekey_query_projection
    AFTER UPDATE ON confirmed_product_matches
    REFERENCING OLD TABLE AS makolet_old_confirmed_matches
                NEW TABLE AS makolet_new_confirmed_matches
    FOR EACH STATEMENT EXECUTE FUNCTION makolet_project_updated_confirmed_matches()
    """,
    """
    CREATE FUNCTION makolet_clear_deleted_confirmed_match_projection()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM confirmed_product_matches match
             WHERE match.retailer_item_id = OLD.retailer_item_id
        ) THEN
            RETURN NULL;
        END IF;
        UPDATE current_prices SET canonical_product_id = NULL
         WHERE retailer_item_id = OLD.retailer_item_id AND canonical_product_id IS NOT NULL;
        UPDATE price_history SET canonical_product_id = NULL
         WHERE retailer_item_id = OLD.retailer_item_id AND canonical_product_id IS NOT NULL;
        UPDATE current_availability SET canonical_product_id = NULL
         WHERE retailer_item_id = OLD.retailer_item_id AND canonical_product_id IS NOT NULL;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE CONSTRAINT TRIGGER trg_confirmed_matches_clear_query_projection
    AFTER DELETE ON confirmed_product_matches DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION makolet_clear_deleted_confirmed_match_projection()
    """,
    """
    CREATE FUNCTION makolet_refresh_query_retailer_for_item()
    RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE current_prices SET query_retailer_id = NEW.retailer_id
         WHERE retailer_item_id = NEW.id
           AND query_retailer_id IS DISTINCT FROM NEW.retailer_id;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER trg_retailer_items_refresh_query_retailer
    AFTER UPDATE OF retailer_id ON retailer_items
    FOR EACH ROW EXECUTE FUNCTION makolet_refresh_query_retailer_for_item()
    """,
)
