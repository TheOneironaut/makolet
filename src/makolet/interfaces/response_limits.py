"""Shared byte-accurate limits for compact public JSON responses."""

from __future__ import annotations

import json
from typing import Final

MAXIMUM_PUBLIC_RESPONSE_BYTES: Final = 1024 * 1024


def compact_json_fits(value: object, maximum_bytes: int) -> bool:
    """Return whether compact UTF-8 JSON fits without building the encoded body."""

    if maximum_bytes < 0:
        return False
    encoded_bytes = 0
    encoder = json.JSONEncoder(
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    for chunk in encoder.iterencode(value):
        encoded_bytes += len(chunk.encode("utf-8"))
        if encoded_bytes > maximum_bytes:
            return False
    return True
