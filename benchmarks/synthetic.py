"""Bounded-memory deterministic supermarket price data generators.

The generated values are independently authored and intentionally synthetic.  They
contain no retailer data and are stable across Python processes and platforms.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from makolet.domain.models import PriceRecord

SYNTHETIC_TIMESTAMP = datetime(2026, 1, 15, 10, 30, tzinfo=UTC)
_HEBREW_PRODUCT_KINDS = (
    "קפה",
    "חלב",
    "אורז",
    "תה",
    "פסטה",
    "סבון",
    "שוקולד",
    "מיץ",
)


@dataclass(slots=True)
class XmlGenerationStats:
    """Mutable counters populated while an asynchronous XML stream is consumed."""

    records: int = 0
    bytes_emitted: int = 0
    chunks_emitted: int = 0


def gtin14(index: int) -> str:
    """Return a valid, deterministic GTIN-14 for an index below ten billion."""

    if not 0 <= index < 10_000_000_000:
        raise ValueError("GTIN synthetic index must be between 0 and 9,999,999,999")
    payload = f"729{index:010d}"
    weighted_sum = sum(
        int(digit) * (3 if offset % 2 == 0 else 1) for offset, digit in enumerate(reversed(payload))
    )
    check_digit = (-weighted_sum) % 10
    return f"{payload}{check_digit}"


def product_name(index: int) -> str:
    """Return mixed Hebrew/Latin text with a stable searchable suffix."""

    kind = _HEBREW_PRODUCT_KINDS[index % len(_HEBREW_PRODUCT_KINDS)]
    mixed = index & 0xFFFFFFFFFFFFFFFF
    mixed = ((mixed ^ (mixed >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    mixed = ((mixed ^ (mixed >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    mixed ^= mixed >> 31
    return f"{kind} MK{mixed:016x}"


def price_text(index: int) -> str:
    """Return a positive decimal price without using binary floating point."""

    agorot = 100 + (index * 37) % 49_900
    return f"{agorot // 100}.{agorot % 100:02d}"


def price_record(
    source_file_id: UUID,
    index: int,
    *,
    store_id: str = "store-0001",
    record_index: int | None = None,
) -> PriceRecord:
    """Build one normalized price event with deterministic provenance fields."""

    price = Decimal(price_text(index))
    return PriceRecord(
        source_file_id=source_file_id,
        record_index=index + 1 if record_index is None else record_index,
        chain_id="synthetic-chain",
        subchain_id="synthetic-subchain",
        store_id=store_id,
        item_code=gtin14(index),
        item_type=1,
        item_name=product_name(index),
        manufacturer_name=f"Synthetic Brand {index % 97:02d}",
        manufacturer_country="IL",
        manufacturer_description=None,
        unit_quantity="1 each",
        quantity=Decimal(1),
        unit_of_measure="each",
        is_weighted=False,
        quantity_in_package=Decimal(1),
        item_price=price,
        unit_of_measure_price=price,
        allow_discount=True,
        item_status=1,
        price_updated_at=SYNTHETIC_TIMESTAMP,
        last_sale_at=None,
        audit_number="synthetic-audit",
    )


def price_record_batches(
    source_file_id: UUID,
    record_count: int,
    *,
    batch_size: int = 5_000,
    start_index: int = 0,
    store_id: str = "store-0001",
) -> Iterator[list[PriceRecord]]:
    """Yield bounded normalized-record batches; no full dataset is retained."""

    if record_count < 0:
        raise ValueError("record_count cannot be negative")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    if start_index < 0:
        raise ValueError("start_index cannot be negative")
    stop_index = start_index + record_count
    for batch_start in range(start_index, stop_index, batch_size):
        batch_stop = min(batch_start + batch_size, stop_index)
        yield [
            price_record(source_file_id, index, store_id=store_id)
            for index in range(batch_start, batch_stop)
        ]


def price_grid_record_batches(
    source_file_id: UUID,
    record_count: int,
    *,
    unique_products: int,
    batch_size: int = 5_000,
) -> Iterator[list[PriceRecord]]:
    """Yield a product/store grid with globally unique record indexes."""

    if record_count < 0:
        raise ValueError("record_count cannot be negative")
    if unique_products <= 0:
        raise ValueError("unique_products must be positive")
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    for batch_start in range(0, record_count, batch_size):
        batch_stop = min(batch_start + batch_size, record_count)
        batch: list[PriceRecord] = []
        for global_index in range(batch_start, batch_stop):
            product_index = global_index % unique_products
            store_number = global_index // unique_products + 1
            batch.append(
                price_record(
                    source_file_id,
                    product_index,
                    store_id=f"store-{store_number:04d}",
                    record_index=global_index + 1,
                )
            )
        yield batch


async def price_full_xml_chunks(
    record_count: int,
    *,
    chunk_size: int = 64 * 1024,
    stats: XmlGenerationStats | None = None,
) -> AsyncIterator[bytes]:
    """Stream a valid PriceFull XML document using at most roughly one chunk."""

    if record_count < 0:
        raise ValueError("record_count cannot be negative")
    if chunk_size < 1024:
        raise ValueError("chunk_size must be at least 1024 bytes")
    counters = stats if stats is not None else XmlGenerationStats()
    pending = bytearray()

    def append(fragment: str) -> None:
        pending.extend(fragment.encode("utf-8"))

    append(
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Root><ChainId>synthetic-chain</ChainId>"
        "<SubChainId>synthetic-subchain</SubChainId>"
        "<StoreId>store-0001</StoreId>"
        "<LastUpdateDate>2026-01-15</LastUpdateDate>"
        "<LastUpdateTime>10:30:00</LastUpdateTime><Items>"
    )
    for index in range(record_count):
        value = price_text(index)
        append(
            "<Item>"
            f"<ItemCode>{gtin14(index)}</ItemCode>"
            f"<ItemName>{product_name(index)}</ItemName>"
            f"<ManufacturerName>Synthetic Brand {index % 97:02d}</ManufacturerName>"
            "<ManufacturerCountry>IL</ManufacturerCountry>"
            "<ItemType>1</ItemType><UnitQty>1 each</UnitQty>"
            "<Quantity>1</Quantity><UnitOfMeasure>each</UnitOfMeasure>"
            "<IsWeighted>0</IsWeighted><QtyInPackage>1</QtyInPackage>"
            f"<ItemPrice>{value}</ItemPrice>"
            f"<UnitOfMeasurePrice>{value}</UnitOfMeasurePrice>"
            "<AllowDiscount>1</AllowDiscount><ItemStatus>1</ItemStatus>"
            "<PriceUpdateDate>2026-01-15</PriceUpdateDate>"
            "<PriceUpdateTime>10:30:00</PriceUpdateTime>"
            "</Item>"
        )
        counters.records += 1
        while len(pending) >= chunk_size:
            chunk = bytes(pending[:chunk_size])
            del pending[:chunk_size]
            counters.bytes_emitted += len(chunk)
            counters.chunks_emitted += 1
            yield chunk
    append("</Items></Root>")
    if pending:
        chunk = bytes(pending)
        counters.bytes_emitted += len(chunk)
        counters.chunks_emitted += 1
        yield chunk
