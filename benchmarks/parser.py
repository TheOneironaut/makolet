"""Measured streaming PriceFull parser scenarios."""

from __future__ import annotations

import time
from typing import Final
from uuid import UUID

from benchmarks.measure import bytes_to_mib, measured_memory, rows_per_second
from benchmarks.synthetic import XmlGenerationStats, price_full_xml_chunks
from makolet.adapters.parsers.streams import CompressionLimits
from makolet.adapters.parsers.xml import RetailXmlParser, XmlParserLimits
from makolet.domain.enums import CompressionFormat, DocumentType
from makolet.domain.models import DocumentMetadata, PriceRecord, ValidationIssue

_SOURCE_FILE_ID: Final = UUID("018f0000-0000-7000-8000-000000000001")


async def run_parser_benchmark(record_count: int) -> dict[str, object]:
    """Parse generated XML and report logical bytes, rate, errors, and process RSS."""

    if record_count <= 0:
        raise ValueError("record_count must be positive")
    # Limits are explicit benchmark inputs rather than weaker production defaults.
    estimated_bytes = record_count * 640 + 1024
    parser = RetailXmlParser(
        XmlParserLimits(
            compression=CompressionLimits(
                maximum_compressed_bytes=estimated_bytes,
                maximum_decompressed_bytes=estimated_bytes,
                maximum_expansion_ratio=250,
                maximum_zip_entries=1,
                maximum_chunk_bytes=64 * 1024,
                spool_memory_bytes=8 * 1024 * 1024,
            ),
            maximum_depth=32,
            maximum_elements=record_count * 20 + 32,
            maximum_records=record_count,
            maximum_record_bytes=16 * 1024,
            maximum_field_characters=2_048,
        )
    )
    generated = XmlGenerationStats()
    parsed_prices = 0
    metadata_records = 0
    rejected_records = 0
    started = time.perf_counter()
    with measured_memory() as memory:
        async for event in parser.parse(
            price_full_xml_chunks(record_count, stats=generated),
            source_file_id=_SOURCE_FILE_ID,
            document_type=DocumentType.PRICE_FULL,
            compression=CompressionFormat.NONE,
            filename="PriceFull-synthetic.xml",
        ):
            if isinstance(event, PriceRecord):
                parsed_prices += 1
            elif isinstance(event, DocumentMetadata):
                metadata_records += 1
            elif isinstance(event, ValidationIssue):
                rejected_records += 1
    duration = time.perf_counter() - started
    if parsed_prices != record_count or metadata_records != 1 or rejected_records:
        raise RuntimeError("Synthetic parser benchmark did not preserve the expected record counts")
    reading = memory.reading
    return {
        "scenario": "streaming_price_full_parser",
        "records": parsed_prices,
        "metadata_records": metadata_records,
        "rejected_records": rejected_records,
        "xml_bytes": generated.bytes_emitted,
        "xml_mib": bytes_to_mib(generated.bytes_emitted),
        "chunks": generated.chunks_emitted,
        "chunk_size_bytes": 64 * 1024,
        "duration_seconds": round(duration, 6),
        "rows_per_second": rows_per_second(parsed_prices, duration),
        "throughput_mib_per_second": round(
            generated.bytes_emitted / (1024 * 1024) / duration,
            3,
        ),
        "baseline_process_rss_bytes": reading.baseline_rss_bytes,
        "peak_process_rss_bytes": reading.peak_rss_bytes,
        "peak_process_rss_mib": bytes_to_mib(reading.peak_rss_bytes),
        "peak_process_rss_delta_bytes": reading.peak_delta_bytes,
        "peak_process_rss_delta_mib": bytes_to_mib(reading.peak_delta_bytes),
        "memory_scope": "benchmark Python process RSS; PostgreSQL is not involved",
    }
