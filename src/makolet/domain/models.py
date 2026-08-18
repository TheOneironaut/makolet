"""Immutable domain records shared by ingestion and query interfaces."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import UUID

from makolet.domain.enums import (
    CompressionFormat,
    DiscountKind,
    DocumentType,
    IssueSeverity,
    SourceProtocol,
)
from makolet.domain.errors import DomainValidationError
from makolet.domain.filenames import safe_basename

MAXIMUM_PLAUSIBLE_QUANTITY = Decimal("1000000")
MAXIMUM_STORED_MONEY = Decimal("9999999999.9999")
MAXIMUM_REMOTE_ID_CHARACTERS = 4_096
MAXIMUM_DOWNLOAD_URL_CHARACTERS = 8_192


def _ensure_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DomainValidationError(f"{field_name} must include a timezone")


def _ensure_text_length(
    value: str | None,
    field_name: str,
    maximum: int,
    *,
    required: bool = False,
) -> None:
    if required and not value:
        raise DomainValidationError(f"{field_name} is required")
    if value is not None and len(value) > maximum:
        raise DomainValidationError(f"{field_name} exceeds {maximum} characters")


def _ensure_decimal_range(
    value: Decimal | None,
    field_name: str,
    *,
    maximum: Decimal,
) -> None:
    if value is None:
        return
    if not value.is_finite() or value < 0 or value > maximum:
        raise DomainValidationError(f"{field_name} is outside the supported range")


def _validate_download_url(value: str, protocol: SourceProtocol) -> None:
    if not value:
        raise DomainValidationError("download_url is required")
    if len(value) > MAXIMUM_DOWNLOAD_URL_CHARACTERS:
        raise DomainValidationError(
            f"download_url exceeds {MAXIMUM_DOWNLOAD_URL_CHARACTERS} characters"
        )
    if any(
        unicodedata.category(character) in {"Cc", "Cs", "Cf", "Zl", "Zp"} for character in value
    ):
        raise DomainValidationError("download_url contains unsafe Unicode controls")
    try:
        parsed = urlsplit(value)
        has_user_information = parsed.username is not None or parsed.password is not None
        hostname = parsed.hostname
        _ = parsed.port
    except ValueError as error:
        raise DomainValidationError("download_url is malformed") from error
    if has_user_information:
        raise DomainValidationError("download_url must not contain credentials")
    if protocol is SourceProtocol.FIXTURE:
        if not parsed.scheme or (not parsed.netloc and not parsed.path):
            raise DomainValidationError("fixture download_url has no resource")
        return
    if parsed.scheme.casefold() != protocol.value:
        raise DomainValidationError("download_url scheme does not match protocol")
    if not hostname or any(character.isspace() for character in hostname):
        raise DomainValidationError("download_url has no valid host")


@dataclass(frozen=True, slots=True)
class RemoteFile:
    """Stable metadata returned by source discovery."""

    retailer_id: str
    portal_id: str
    protocol: SourceProtocol
    remote_id: str
    download_url: str
    original_filename: str
    document_type: DocumentType
    compression: CompressionFormat
    discovered_at: datetime
    source_timestamp: datetime | None = None
    content_length: int | None = None
    media_type: str | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    response_metadata: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.retailer_id or not self.portal_id or not self.remote_id:
            raise DomainValidationError("retailer_id, portal_id, and remote_id are required")
        if len(self.remote_id) > MAXIMUM_REMOTE_ID_CHARACTERS:
            raise DomainValidationError(
                f"remote_id exceeds {MAXIMUM_REMOTE_ID_CHARACTERS} characters"
            )
        if safe_basename(self.original_filename) != self.original_filename:
            raise DomainValidationError("original_filename must be a decoded basename")
        _validate_download_url(self.download_url, self.protocol)
        _ensure_aware(self.discovered_at, "discovered_at")
        if self.source_timestamp is not None:
            _ensure_aware(self.source_timestamp, "source_timestamp")
        if self.last_modified is not None:
            _ensure_aware(self.last_modified, "last_modified")
        if self.content_length is not None and self.content_length < 0:
            raise DomainValidationError("content_length cannot be negative")


@dataclass(frozen=True, slots=True)
class ArchiveReceipt:
    """Immutable result of committing exact source bytes."""

    content_sha256: str
    object_key: str
    content_length: int
    archived_at: datetime
    created: bool

    def __post_init__(self) -> None:
        if len(self.content_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_sha256
        ):
            raise DomainValidationError("content_sha256 must be lowercase SHA-256 hex")
        if self.content_length < 0:
            raise DomainValidationError("content_length cannot be negative")
        _ensure_aware(self.archived_at, "archived_at")


@dataclass(frozen=True, slots=True)
class DocumentMetadata:
    """Header metadata observed before or alongside document records."""

    source_file_id: UUID
    document_type: DocumentType
    chain_id: str | None = None
    subchain_id: str | None = None
    store_id: str | None = None
    audit_number: str | None = None
    source_updated_at: datetime | None = None

    def __post_init__(self) -> None:
        _ensure_text_length(self.chain_id, "chain_id", 128)
        _ensure_text_length(self.subchain_id, "subchain_id", 128)
        _ensure_text_length(self.store_id, "store_id", 128)
        _ensure_text_length(self.audit_number, "audit_number", 128)
        if self.source_updated_at is not None:
            _ensure_aware(self.source_updated_at, "source_updated_at")


@dataclass(frozen=True, slots=True)
class StoreRecord:
    source_file_id: UUID
    record_index: int
    chain_id: str
    subchain_id: str
    store_id: str
    audit_number: str | None
    store_type: str | None
    chain_name: str | None
    subchain_name: str | None
    store_name: str
    address: str | None
    city: str | None
    postal_code: str | None

    def __post_init__(self) -> None:
        _ensure_text_length(self.chain_id, "chain_id", 128, required=True)
        _ensure_text_length(self.subchain_id, "subchain_id", 128)
        _ensure_text_length(self.store_id, "store_id", 128, required=True)
        _ensure_text_length(self.audit_number, "audit_number", 128)
        _ensure_text_length(self.store_type, "store_type", 128)
        _ensure_text_length(self.store_name, "store_name", 1024, required=True)
        _ensure_text_length(self.postal_code, "postal_code", 32)


@dataclass(frozen=True, slots=True)
class PriceRecord:
    source_file_id: UUID
    record_index: int
    chain_id: str
    subchain_id: str
    store_id: str
    item_code: str
    item_type: int | None
    item_name: str
    manufacturer_name: str | None
    manufacturer_country: str | None
    manufacturer_description: str | None
    unit_quantity: str | None
    quantity: Decimal | None
    unit_of_measure: str | None
    is_weighted: bool | None
    quantity_in_package: Decimal | None
    item_price: Decimal
    unit_of_measure_price: Decimal | None
    allow_discount: bool | None
    item_status: int | None
    price_updated_at: datetime | None
    last_sale_at: datetime | None
    audit_number: str | None = None

    def __post_init__(self) -> None:
        _ensure_text_length(self.chain_id, "chain_id", 128, required=True)
        _ensure_text_length(self.subchain_id, "subchain_id", 128)
        _ensure_text_length(self.store_id, "store_id", 128, required=True)
        _ensure_text_length(self.item_code, "item_code", 128, required=True)
        _ensure_text_length(self.item_name, "item_name", 2048, required=True)
        _ensure_text_length(self.unit_of_measure, "unit_of_measure", 64)
        _ensure_text_length(self.audit_number, "audit_number", 128)
        _ensure_decimal_range(
            self.quantity,
            "quantity",
            maximum=MAXIMUM_PLAUSIBLE_QUANTITY,
        )
        _ensure_decimal_range(
            self.quantity_in_package,
            "quantity_in_package",
            maximum=MAXIMUM_PLAUSIBLE_QUANTITY,
        )
        _ensure_decimal_range(self.item_price, "item_price", maximum=MAXIMUM_STORED_MONEY)
        _ensure_decimal_range(
            self.unit_of_measure_price,
            "unit_of_measure_price",
            maximum=MAXIMUM_STORED_MONEY,
        )
        if self.price_updated_at is not None:
            _ensure_aware(self.price_updated_at, "price_updated_at")
        if self.last_sale_at is not None:
            _ensure_aware(self.last_sale_at, "last_sale_at")


@dataclass(frozen=True, slots=True)
class PromotionItem:
    item_code: str
    item_type: int | None = None
    is_gift: bool = False

    def __post_init__(self) -> None:
        _ensure_text_length(self.item_code, "item_code", 128, required=True)


@dataclass(frozen=True, slots=True)
class PromotionRecord:
    source_file_id: UUID
    record_index: int
    chain_id: str
    subchain_id: str
    source_store_id: str | None
    promotion_id: str
    description: str | None
    discount_kind: DiscountKind
    starts_at: datetime | None
    ends_at: datetime | None
    items: tuple[PromotionItem, ...]
    store_ids: tuple[str, ...]
    club_ids: tuple[str, ...]
    reward_type: int | None = None
    allows_multiple_discounts: bool | None = None
    minimum_quantity: Decimal | None = None
    maximum_quantity: Decimal | None = None
    discount_rate: Decimal | None = None
    minimum_purchase: Decimal | None = None
    discounted_price: Decimal | None = None
    discounted_unit_price: Decimal | None = None
    minimum_items_offered: int | None = None
    additional_restrictions: str | None = None
    remarks: str | None = None
    is_active: bool | None = None

    def __post_init__(self) -> None:
        _ensure_text_length(self.chain_id, "chain_id", 128, required=True)
        _ensure_text_length(self.subchain_id, "subchain_id", 128)
        _ensure_text_length(self.source_store_id, "source_store_id", 128)
        _ensure_text_length(self.promotion_id, "promotion_id", 256, required=True)
        for store_id in self.store_ids:
            _ensure_text_length(store_id, "store_id", 128, required=True)
        for club_id in self.club_ids:
            _ensure_text_length(club_id, "club_id", 128, required=True)
        if self.starts_at is not None:
            _ensure_aware(self.starts_at, "starts_at")
        if self.ends_at is not None:
            _ensure_aware(self.ends_at, "ends_at")
        if (
            self.starts_at is not None
            and self.ends_at is not None
            and self.ends_at < self.starts_at
        ):
            raise DomainValidationError("ends_at cannot precede starts_at")
        _ensure_decimal_range(
            self.minimum_quantity,
            "minimum_quantity",
            maximum=MAXIMUM_PLAUSIBLE_QUANTITY,
        )
        _ensure_decimal_range(
            self.maximum_quantity,
            "maximum_quantity",
            maximum=MAXIMUM_PLAUSIBLE_QUANTITY,
        )
        if (
            self.minimum_quantity is not None
            and self.maximum_quantity is not None
            and self.maximum_quantity < self.minimum_quantity
        ):
            raise DomainValidationError("maximum_quantity cannot be less than minimum_quantity")
        _ensure_decimal_range(self.discount_rate, "discount_rate", maximum=Decimal("100"))
        for field_name, value in (
            ("minimum_purchase", self.minimum_purchase),
            ("discounted_price", self.discounted_price),
            ("discounted_unit_price", self.discounted_unit_price),
        ):
            _ensure_decimal_range(value, field_name, maximum=MAXIMUM_STORED_MONEY)
        if self.minimum_items_offered is not None and not 0 <= self.minimum_items_offered <= int(
            MAXIMUM_PLAUSIBLE_QUANTITY
        ):
            raise DomainValidationError("minimum_items_offered is outside the supported range")


def effective_promotion_store_ids(event: PromotionRecord) -> tuple[str, ...]:
    """Return the deduplicated store relationships persisted for a promotion."""

    scoped_store_ids = (event.source_store_id,) if event.source_store_id else ()
    return tuple(dict.fromkeys((*event.store_ids, *scoped_store_ids)))


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    source_file_id: UUID
    severity: IssueSeverity
    code: str
    message: str
    record_index: int | None = None
    field_name: str | None = None
    rejected_value: str | None = None


type ParsedEvent = DocumentMetadata | StoreRecord | PriceRecord | PromotionRecord | ValidationIssue
