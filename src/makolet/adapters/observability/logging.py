"""Secret-safe structured JSON logging configuration."""

from __future__ import annotations

import logging
import re
import sys
import unicodedata
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import Any, TextIO, cast
from uuid import UUID

import structlog
from structlog.contextvars import bound_contextvars, get_contextvars
from structlog.typing import EventDict, Processor, WrappedLogger

from makolet.application.observability import LifecycleEvent, LifecycleLogger

_SECRET_KEYS = frozenset(
    {
        "access_key",
        "api_key",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "password",
        "secret",
        "secret_key",
        "set_cookie",
        "token",
    }
)
_MAX_STRING = 4_096
_MAX_QUERY_KEY = 256
_USERINFO = re.compile(r"(?P<scheme>https?|ftp|ftps)://[^/@\s]+@", re.IGNORECASE)
_QUERY_PARAMETER = re.compile(rf'(?P<separator>[?&])(?P<key>[^?&=#\s"<>]{{1,{_MAX_STRING}}})=')
_QUERY_VALUE_END = re.compile(r'[&#\s"<>]')
_QUERY_SECRET_NAMES = frozenset(
    {
        "accesskey",
        "accesstoken",
        "apikey",
        "auth",
        "authentication",
        "authorization",
        "authtoken",
        "credential",
        "credentials",
        "key",
        "password",
        "secret",
        "secretkey",
        "sharedaccesssignature",
        "sig",
        "signature",
        "token",
    }
)
_QUERY_SECRET_TERMINALS = frozenset(
    {
        "auth",
        "authentication",
        "authorization",
        "credential",
        "credentials",
        "key",
        "password",
        "secret",
        "sig",
        "signature",
        "token",
    }
)
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")
_ASSIGNED_SECRET = re.compile(r"(?i)\b(password|secret|token|api[_-]?key)\s*[:=]\s*[^,;\s]+")
_MAX_COLLECTION = 50
_MAX_IDENTIFIER = 200
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}\Z")
_TOKEN = re.compile(r"[a-z0-9][a-z0-9._:-]{0,127}\Z")
_IDENTIFIER_FIELDS = frozenset(
    {
        "correlation_id",
        "rebuild_run_id",
        "replay_id",
        "retailer_id",
        "run_id",
        "source_file_id",
        "source_id",
        "source_run_id",
        "worker_id",
        "worker_run_id",
        "portal_id",
    }
)
_TOKEN_FIELDS = frozenset(
    {
        "compression",
        "document_type",
        "error_code",
        "operation",
        "status",
    }
)
_COUNT_FIELDS = frozenset(
    {
        "accepted_records",
        "active_source_count",
        "attempt",
        "attempt_limit",
        "content_length",
        "discovered_count",
        "duplicate_count",
        "failed_count",
        "failed_source_count",
        "file_count",
        "healthy_source_count",
        "history_event_count",
        "inserted_count",
        "limit",
        "metadata_records",
        "page_file_count",
        "page_index",
        "price_records",
        "promotion_records",
        "queue_depth",
        "recovered_count",
        "rejected_records",
        "reported_count",
        "scheduled_source_count",
        "selected_count",
        "sequence",
        "skipped_unknown_count",
        "source_files_completed",
        "source_files_total",
        "store_records",
        "successful_count",
        "unchanged_count",
        "unavailable_count",
        "updated_count",
        "warning_count",
    }
)
_BOOLEAN_FIELDS = frozenset(
    {
        "archive_only",
        "complete",
        "created",
        "duplicate",
        "replayed",
        "running",
        "stopping",
        "truncated",
    }
)
_DURATION_FIELDS = frozenset({"duration_seconds"})
_ALLOWED_LIFECYCLE_FIELDS = (
    _IDENTIFIER_FIELDS | _TOKEN_FIELDS | _COUNT_FIELDS | _BOOLEAN_FIELDS | _DURATION_FIELDS
)
_LIFECYCLE_EVENT_NAMES = frozenset(event.value for event in LifecycleEvent)
_STANDARD_EVENT_FIELDS = frozenset(
    {"_from_structlog", "_record", "event", "level", "logger", "timestamp"}
)


def configure_logging(
    *,
    level: str = "INFO",
    stream: TextIO | None = None,
) -> None:
    """Configure stdlib and structlog as one process-local JSON pipeline."""

    level_names = logging.getLevelNamesMapping()
    normalized_level = level.upper()
    if normalized_level not in level_names:
        raise ValueError(f"Unknown log level: {level}")
    numeric_level = level_names[normalized_level]
    output = stream or sys.stderr
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _restrict_lifecycle_event,
        _sanitize_event,
    ]
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(sort_keys=True, ensure_ascii=False),
        ],
    )
    handler = logging.StreamHandler(output)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    for existing in tuple(root.handlers):
        root.removeHandler(existing)
        existing.close()
    root.addHandler(handler)
    root.setLevel(numeric_level)


def get_logger(name: str = "makolet") -> structlog.stdlib.BoundLogger:
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))


def get_lifecycle_logger(name: str = "makolet.lifecycle") -> LifecycleLogger:
    """Return the constrained lifecycle facade over the configured structlog logger."""

    return StructlogLifecycleLogger(get_logger(name))


class StructlogLifecycleLogger:
    """Allow only bounded operational metadata through lifecycle events."""

    def __init__(self, logger: structlog.stdlib.BoundLogger) -> None:
        self._logger = logger

    def context(self, **fields: object) -> AbstractContextManager[None]:
        return bound_contextvars(**_lifecycle_fields(fields))

    def context_if_absent(self, **fields: object) -> AbstractContextManager[None]:
        safe_fields = _lifecycle_fields(fields)
        existing = get_contextvars()
        return bound_contextvars(
            **{key: value for key, value in safe_fields.items() if key not in existing}
        )

    def info(self, event: LifecycleEvent, **fields: object) -> None:
        self._emit("info", event, fields)

    def warning(self, event: LifecycleEvent, **fields: object) -> None:
        self._emit("warning", event, fields)

    def _emit(
        self,
        level: str,
        event: LifecycleEvent,
        fields: Mapping[str, object],
    ) -> None:
        if not isinstance(event, LifecycleEvent):
            raise TypeError("Lifecycle event must use the LifecycleEvent enum")
        getattr(self._logger, level)(event.value, **_lifecycle_fields(fields))


def _restrict_lifecycle_event(
    logger: WrappedLogger,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    del logger, method_name
    if event_dict.get("event") not in _LIFECYCLE_EVENT_NAMES:
        return event_dict
    restricted: EventDict = {}
    for key, value in event_dict.items():
        if key in _STANDARD_EVENT_FIELDS:
            restricted[key] = value
        elif key in _ALLOWED_LIFECYCLE_FIELDS:
            try:
                restricted[key] = _lifecycle_value(key, value)
            except ValueError:
                restricted[key] = "[INVALID]"
    return restricted


def _sanitize_event(
    logger: WrappedLogger,
    method_name: str,
    event_dict: EventDict,
) -> EventDict:
    del logger, method_name
    sanitized: EventDict = {}
    for key, value in event_dict.items():
        text_key = str(key)
        if text_key in {"_record", "_from_structlog"}:
            sanitized[text_key] = value
        elif text_key in {"exc_info", "exception", "stack_info"}:
            sanitized["exception"] = "[SUPPRESSED]"
        else:
            safe_key = _clean_string(text_key)[:128]
            sanitized[safe_key] = _sanitize_value(safe_key, value, depth=0)
    return sanitized


def _sanitize_value(key: str, value: Any, *, depth: int) -> Any:
    normalized_key = key.casefold().replace("-", "_")
    if normalized_key in _SECRET_KEYS or normalized_key.endswith(
        ("_password", "_secret", "_token")
    ):
        return "[REDACTED]"
    if depth >= 4:
        return "[TRUNCATED]"
    if isinstance(value, str):
        cleaned = _clean_string(value)
        cleaned = _USERINFO.sub(r"\g<scheme>://[REDACTED]@", cleaned)
        cleaned = _redact_query_secrets(cleaned[:_MAX_STRING])
        cleaned = _ASSIGNED_SECRET.sub(r"\1=[REDACTED]", cleaned)
        return cleaned[:_MAX_STRING]
    if isinstance(value, Mapping):
        return {
            str(child_key): _sanitize_value(str(child_key), child_value, depth=depth + 1)
            for child_key, child_value in list(value.items())[:_MAX_COLLECTION]
        }
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [
            _sanitize_value("item", child, depth=depth + 1)
            for child in list(value)[:_MAX_COLLECTION]
        ]
    if isinstance(value, bytes):
        return f"[BYTES:{len(value)}]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _sanitize_value(key, str(value), depth=depth + 1)


def _lifecycle_fields(fields: Mapping[str, object]) -> dict[str, object]:
    unknown = set(fields) - _ALLOWED_LIFECYCLE_FIELDS
    if unknown:
        names = ", ".join(sorted(unknown))
        raise ValueError(f"Unsupported lifecycle log fields: {names}")
    return {key: _lifecycle_value(key, value) for key, value in fields.items()}


def _lifecycle_value(key: str, value: object) -> object:
    if value is None:
        return None
    if key in _IDENTIFIER_FIELDS:
        text = str(value) if isinstance(value, UUID) else value
        if not isinstance(text, str) or _IDENTIFIER.fullmatch(text) is None:
            raise ValueError(f"Lifecycle identifier {key} is invalid or exceeds {_MAX_IDENTIFIER}")
        return text
    if key in _TOKEN_FIELDS:
        if not isinstance(value, str) or _TOKEN.fullmatch(value) is None:
            raise ValueError(f"Lifecycle token {key} is invalid")
        return value
    if key in _COUNT_FIELDS:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Lifecycle count {key} must be a non-negative integer")
        return value
    if key in _BOOLEAN_FIELDS:
        if not isinstance(value, bool):
            raise ValueError(f"Lifecycle flag {key} must be boolean")
        return value
    if key in _DURATION_FIELDS:
        if isinstance(value, bool) or not isinstance(value, int | float) or value < 0:
            raise ValueError(f"Lifecycle duration {key} must be non-negative")
        return round(float(value), 6)
    raise AssertionError(f"Unhandled lifecycle field: {key}")


def _clean_string(value: str) -> str:
    escaped: list[str] = []
    for character in value:
        codepoint = ord(character)
        if character == "\r":
            escaped.append("\\r")
        elif character == "\n":
            escaped.append("\\n")
        elif character == "\t":
            escaped.append("\\t")
        elif character == "\x00":
            escaped.append("\\0")
        elif unicodedata.category(character) in {"Cc", "Cs", "Cf", "Zl", "Zp"}:
            prefix, width = ("\\u", 4) if codepoint <= 0xFFFF else ("\\U", 8)
            escaped.append(f"{prefix}{codepoint:0{width}x}")
        else:
            escaped.append(character)
    return "".join(escaped)


def _redact_query_secrets(value: str) -> str:
    redacted: list[str] = []
    preserved_from = 0
    for match in _QUERY_PARAMETER.finditer(value):
        if match.start() < preserved_from or not _is_secret_query_key(match.group("key")):
            continue
        value_end_match = _QUERY_VALUE_END.search(value, match.end())
        value_end = value_end_match.start() if value_end_match is not None else len(value)
        redacted.extend((value[preserved_from : match.end()], "[REDACTED]"))
        preserved_from = value_end
    redacted.append(value[preserved_from:])
    return "".join(redacted)


def _is_secret_query_key(raw_key: str) -> bool:
    if len(raw_key) > _MAX_QUERY_KEY:
        return True
    decoded_key = _strict_percent_decode(raw_key)
    if decoded_key is None:
        return True
    casefolded_key = decoded_key.casefold()
    canonical_key = re.sub(r"[-._:+\s\[\]]+", "", casefolded_key)
    if canonical_key in _QUERY_SECRET_NAMES:
        return True
    components = tuple(filter(None, re.split(r"[-._:+\s\[\]]+", casefolded_key)))
    return bool(components and components[-1] in _QUERY_SECRET_TERMINALS)


def _strict_percent_decode(value: str) -> str | None:
    decoded = bytearray()
    index = 0
    while index < len(value):
        character = value[index]
        if character != "%":
            decoded.extend(character.encode())
            index += 1
            continue
        if (
            index + 2 >= len(value)
            or value[index + 1] not in _HEX_DIGITS
            or value[index + 2] not in _HEX_DIGITS
        ):
            return None
        decoded.append(int(value[index + 1 : index + 3], 16))
        index += 3
    try:
        decoded_text = decoded.decode()
    except UnicodeDecodeError:
        return None
    if any(unicodedata.category(character).startswith("C") for character in decoded_text):
        return None
    return decoded_text
