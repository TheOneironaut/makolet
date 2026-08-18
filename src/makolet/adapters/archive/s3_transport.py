"""Pre-parse response bounds for Botocore S3 clients."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from functools import partial
from typing import Any, Final, NoReturn, cast

from botocore.awsrequest import AWSResponse

DEFAULT_MAXIMUM_S3_CONTROL_RESPONSE_BYTES: Final = 8 * 1024 * 1024
DEFAULT_S3_RESPONSE_CHUNK_BYTES: Final = 64 * 1024

_RESPONSE_BUDGET_CONTEXT_KEY: Final = "makolet.s3-response-byte-budget.v1"

type ErrorFactory = Callable[[str], Exception]


@dataclass(slots=True)
class _ResponseByteBudget:
    maximum_bytes: int
    remaining_bytes: int


class _BoundedResponseBody:
    """Expose one already-bounded response through Botocore's raw-body contract."""

    def __init__(
        self,
        payload: bytes,
        *,
        operation_name: str,
        error_factory: ErrorFactory,
    ) -> None:
        self._payload = payload
        self._operation_name = operation_name
        self._error_factory = error_factory

    def stream(
        self,
        amount: int | None = None,
        *,
        decode_content: bool = False,
    ) -> Iterator[bytes]:
        if decode_content:
            _raise_response_error(
                self._error_factory,
                self._operation_name,
                "decoding is not permitted",
            )
        if amount is None:
            chunk_size = len(self._payload) or 1
        elif isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
            _raise_response_error(
                self._error_factory,
                self._operation_name,
                "requested an invalid chunk size",
            )
        else:
            chunk_size = amount
        for offset in range(0, len(self._payload), chunk_size):
            yield self._payload[offset : offset + chunk_size]

    def close(self) -> None:
        return


def _raise_response_error(
    error_factory: ErrorFactory,
    operation_name: str,
    detail: str,
) -> NoReturn:
    raise error_factory(f"S3 {operation_name} response {detail}")


def _close_ignoring_errors(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception:
        return


def _response_header(headers: Mapping[str, Any], name: str) -> str | None:
    sought = name.casefold()
    for raw_name, raw_value in headers.items():
        if str(raw_name).casefold() != sought:
            continue
        if isinstance(raw_value, bytes):
            return raw_value.decode("latin-1")
        return str(raw_value)
    return None


def _response_budget(
    request: Any,
    *,
    operation_name: str,
    maximum_bytes: int,
    error_factory: ErrorFactory,
) -> _ResponseByteBudget:
    context = getattr(request, "context", None)
    if not isinstance(context, MutableMapping):
        _raise_response_error(
            error_factory,
            operation_name,
            "cannot enforce its retry byte budget",
        )
    existing = context.get(_RESPONSE_BUDGET_CONTEXT_KEY)
    if existing is None:
        budget = _ResponseByteBudget(maximum_bytes, maximum_bytes)
        context[_RESPONSE_BUDGET_CONTEXT_KEY] = budget
        return budget
    if not isinstance(existing, _ResponseByteBudget) or existing.maximum_bytes != maximum_bytes:
        _raise_response_error(
            error_factory,
            operation_name,
            "has invalid retry byte-budget state",
        )
    return existing


def bounded_s3_response(
    http_session: Any,
    request: Any,
    *,
    operation_name: str,
    stream_success: bool,
    error_factory: ErrorFactory,
    maximum_bytes: int = DEFAULT_MAXIMUM_S3_CONTROL_RESPONSE_BYTES,
    chunk_bytes: int = DEFAULT_S3_RESPONSE_CHUNK_BYTES,
    **_kwargs: Any,
) -> AWSResponse:
    """Send one signed request and bound every body Botocore would materialize.

    Successful streaming operations keep their raw response untouched. Error
    responses and every non-streaming success are consumed here under one byte
    budget shared by all retries in the Botocore request context.
    """

    if maximum_bytes <= 0 or chunk_bytes <= 0:
        raise error_factory("S3 response byte limits must be positive")
    budget = _response_budget(
        request,
        operation_name=operation_name,
        maximum_bytes=maximum_bytes,
        error_factory=error_factory,
    )
    original_stream_output = request.stream_output
    request.stream_output = True
    response: Any | None = None
    preserve_raw_response = False
    try:
        response = http_session.send(request)
        status_code = getattr(response, "status_code", None)
        if stream_success and isinstance(status_code, int) and 200 <= status_code < 300:
            preserve_raw_response = True
            return cast(AWSResponse, response)
        raw = getattr(response, "raw", None)
        headers = getattr(response, "headers", None)
        if raw is None or not isinstance(headers, Mapping):
            _raise_response_error(
                error_factory,
                operation_name,
                "is missing its raw body or headers",
            )
        request_method = getattr(request, "method", None)
        response_has_body_semantics = not (
            isinstance(request_method, str) and request_method.casefold() == "head"
        ) and status_code not in {204, 304}
        content_encoding = (
            _response_header(headers, "content-encoding") if response_has_body_semantics else None
        )
        if content_encoding is not None and content_encoding.strip().casefold() not in {
            "",
            "identity",
        }:
            _raise_response_error(
                error_factory,
                operation_name,
                "uses unsupported content encoding",
            )
        declared_length_text = (
            _response_header(headers, "content-length") if response_has_body_semantics else None
        )
        declared_length: int | None = None
        if declared_length_text is not None:
            try:
                declared_length = int(declared_length_text)
            except ValueError as error:
                raise error_factory(
                    f"S3 {operation_name} response has invalid content length"
                ) from error
            if declared_length < 0:
                _raise_response_error(
                    error_factory,
                    operation_name,
                    "has invalid content length",
                )
            if declared_length > budget.remaining_bytes:
                _raise_response_error(
                    error_factory,
                    operation_name,
                    "exceeds its byte limit",
                )
        stream = getattr(raw, "stream", None)
        if not callable(stream):
            _raise_response_error(
                error_factory,
                operation_name,
                "has an invalid raw body",
            )
        payload = bytearray()
        for chunk in stream(chunk_bytes, decode_content=False):
            if not isinstance(chunk, bytes):
                _raise_response_error(
                    error_factory,
                    operation_name,
                    "returned non-byte content",
                )
            if len(chunk) > budget.remaining_bytes:
                _raise_response_error(
                    error_factory,
                    operation_name,
                    "exceeds its byte limit",
                )
            budget.remaining_bytes -= len(chunk)
            payload.extend(chunk)
        if declared_length is not None and declared_length != len(payload):
            _raise_response_error(
                error_factory,
                operation_name,
                "length is inconsistent",
            )
        return AWSResponse(
            response.url,
            response.status_code,
            response.headers,
            _BoundedResponseBody(
                bytes(payload),
                operation_name=operation_name,
                error_factory=error_factory,
            ),
        )
    finally:
        request.stream_output = original_stream_output
        if response is not None and not preserve_raw_response:
            _close_ignoring_errors(getattr(response, "raw", None))


def install_bounded_s3_response_transport(
    client: Any,
    *,
    operations: Mapping[str, bool],
    error_factory: ErrorFactory,
    maximum_bytes: int = DEFAULT_MAXIMUM_S3_CONTROL_RESPONSE_BYTES,
) -> Any:
    """Install operation-specific pre-parse bounds on one Botocore S3 client.

    Mapping values identify operations whose successful response body must stay
    streaming. All errors remain bounded regardless of the mapping value.
    """

    if maximum_bytes <= 0:
        raise error_factory("S3 response byte limit must be positive")
    if not operations:
        raise error_factory("S3 response transport requires at least one operation")
    endpoint = getattr(client, "_endpoint", None)
    http_session = getattr(endpoint, "http_session", None)
    events = getattr(getattr(client, "meta", None), "events", None)
    register_first = getattr(events, "register_first", None)
    if http_session is None or not callable(register_first):
        raise error_factory("S3 client cannot enforce bounded responses")
    for operation_name, stream_success in operations.items():
        if not operation_name or not isinstance(stream_success, bool):
            raise error_factory("S3 response transport operation configuration is invalid")
        register_first(
            f"before-send.s3.{operation_name}",
            partial(
                bounded_s3_response,
                http_session,
                operation_name=operation_name,
                stream_success=stream_success,
                error_factory=error_factory,
                maximum_bytes=maximum_bytes,
            ),
        )
    return client
