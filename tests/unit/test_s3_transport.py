"""Direct security-boundary tests for Botocore's pre-parse response transport."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from types import SimpleNamespace
from typing import Any, cast

import pytest
from botocore.awsrequest import AWSResponse

from makolet.adapters.archive import s3_transport
from makolet.domain.errors import ArchiveIntegrityError


def _error(message: str) -> Exception:
    return ArchiveIntegrityError(message)


class _RawBody:
    def __init__(
        self,
        chunks: tuple[object, ...] = (),
        *,
        close_error: bool = False,
    ) -> None:
        self._chunks = chunks
        self._close_error = close_error
        self.closed = False
        self.stream_calls: list[tuple[int, bool]] = []

    def stream(self, amount: int, *, decode_content: bool) -> Iterator[object]:
        self.stream_calls.append((amount, decode_content))
        yield from self._chunks

    def close(self) -> None:
        self.closed = True
        if self._close_error:
            raise RuntimeError("close failed")


class _HttpSession:
    def __init__(self, response: object) -> None:
        self.response = response
        self.requests: list[object] = []

    def send(self, request: object) -> object:
        self.requests.append(request)
        return self.response


class _Events:
    def __init__(self) -> None:
        self.handlers: list[tuple[str, object]] = []

    def register_first(self, name: str, handler: object) -> None:
        self.handlers.append((name, handler))


def _request(
    *,
    method: str = "GET",
    context: object | None = None,
    stream_output: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        context={} if context is None else context,
        method=method,
        stream_output=stream_output,
    )


def _response(
    raw: object,
    *,
    status_code: object = 200,
    headers: object | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        url="https://objects.example.test/raw/object",
        status_code=status_code,
        headers={} if headers is None else headers,
        raw=raw,
    )


def _bounded(
    response: object,
    request: SimpleNamespace,
    **overrides: object,
) -> AWSResponse:
    arguments: dict[str, object] = {
        "operation_name": "HeadObject",
        "stream_success": False,
        "error_factory": _error,
        "maximum_bytes": 16,
        "chunk_bytes": 4,
    }
    arguments.update(overrides)
    return s3_transport.bounded_s3_response(
        _HttpSession(response),
        request,
        **cast(Any, arguments),
    )


def test_bounded_body_replays_exact_bytes_without_decoding() -> None:
    body = s3_transport._BoundedResponseBody(
        b"abcdef",
        operation_name="ListObjectsV2",
        error_factory=_error,
    )

    assert list(body.stream()) == [b"abcdef"]
    assert list(body.stream(2)) == [b"ab", b"cd", b"ef"]
    assert (
        list(
            s3_transport._BoundedResponseBody(
                b"",
                operation_name="ListObjectsV2",
                error_factory=_error,
            ).stream()
        )
        == []
    )
    body.close()


@pytest.mark.parametrize("amount", [True, 0, -1, "1"])
def test_bounded_body_rejects_invalid_chunk_sizes(amount: object) -> None:
    body = s3_transport._BoundedResponseBody(
        b"value",
        operation_name="GetObject",
        error_factory=_error,
    )

    with pytest.raises(ArchiveIntegrityError, match="invalid chunk size"):
        list(body.stream(cast(Any, amount)))


def test_bounded_body_rejects_content_decoding() -> None:
    body = s3_transport._BoundedResponseBody(
        b"value",
        operation_name="GetObject",
        error_factory=_error,
    )

    with pytest.raises(ArchiveIntegrityError, match="decoding is not permitted"):
        list(body.stream(decode_content=True))


def test_response_header_is_case_insensitive_and_preserves_bytes() -> None:
    headers = {"Content-Length": b"5", "X-Other": "value"}

    assert s3_transport._response_header(headers, "content-length") == "5"
    assert s3_transport._response_header(headers, "x-other") == "value"
    assert s3_transport._response_header(headers, "missing") is None


def test_response_budget_requires_mutable_request_context() -> None:
    request = SimpleNamespace(method="GET", stream_output=False)

    with pytest.raises(ArchiveIntegrityError, match="cannot enforce its retry byte budget"):
        _bounded(_response(_RawBody()), request)

    with pytest.raises(ArchiveIntegrityError, match="cannot enforce its retry byte budget"):
        _bounded(_response(_RawBody()), _request(context=cast(Any, ())))


def test_response_budget_rejects_tampered_or_mismatched_retry_state() -> None:
    request = _request()
    first_raw = _RawBody()
    _bounded(_response(first_raw), request)
    assert first_raw.closed is True

    request.context[s3_transport._RESPONSE_BUDGET_CONTEXT_KEY] = object()
    with pytest.raises(ArchiveIntegrityError, match="invalid retry byte-budget state"):
        _bounded(_response(_RawBody()), request)

    request = _request()
    _bounded(_response(_RawBody()), request)
    with pytest.raises(ArchiveIntegrityError, match="invalid retry byte-budget state"):
        _bounded(_response(_RawBody()), request, maximum_bytes=17)


def test_response_budget_is_reused_across_valid_retry_attempts() -> None:
    request = _request()

    first = _bounded(_response(_RawBody((b"1234",))), request)
    second = _bounded(_response(_RawBody((b"5678",))), request)

    assert list(first.raw.stream()) == [b"1234"]
    assert list(second.raw.stream()) == [b"5678"]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"maximum_bytes": 0}, "limits must be positive"),
        ({"chunk_bytes": 0}, "limits must be positive"),
    ],
)
def test_response_transport_rejects_nonpositive_limits(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ArchiveIntegrityError, match=message):
        _bounded(_response(_RawBody()), _request(), **overrides)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (_response(None), "missing its raw body or headers"),
        (_response(_RawBody(), headers=[]), "missing its raw body or headers"),
        (_response(SimpleNamespace(), headers={}), "invalid raw body"),
    ],
)
def test_response_transport_rejects_missing_sdk_contract(
    response: object,
    message: str,
) -> None:
    with pytest.raises(ArchiveIntegrityError, match=message):
        _bounded(response, _request())


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({"content-encoding": "gzip"}, "unsupported content encoding"),
        ({"content-length": "invalid"}, "invalid content length"),
        ({"content-length": "-1"}, "invalid content length"),
        ({"content-length": "17"}, "exceeds its byte limit"),
    ],
)
def test_response_transport_rejects_unsafe_representation_headers(
    headers: Mapping[str, str],
    message: str,
) -> None:
    raw = _RawBody()

    with pytest.raises(ArchiveIntegrityError, match=message):
        _bounded(_response(raw, headers=headers), _request())

    assert raw.closed is True


def test_response_transport_accepts_identity_encoding_and_exact_length() -> None:
    raw = _RawBody((b"ab", b"c"))
    request = _request(stream_output=True)

    response = _bounded(
        _response(
            raw,
            headers={"Content-Encoding": " identity ", "Content-Length": "3"},
        ),
        request,
    )

    assert list(response.raw.stream(2, decode_content=False)) == [b"ab", b"c"]
    assert raw.stream_calls == [(4, False)]
    assert raw.closed is True
    assert request.stream_output is True


@pytest.mark.parametrize(
    ("chunks", "maximum_bytes", "message"),
    [
        (("not-bytes",), 16, "returned non-byte content"),
        ((b"12345",), 4, "exceeds its byte limit"),
        ((b"abc",), 16, "length is inconsistent"),
    ],
)
def test_response_transport_rejects_unsafe_stream_content(
    chunks: tuple[object, ...],
    maximum_bytes: int,
    message: str,
) -> None:
    raw = _RawBody(chunks)
    headers = {"content-length": "4"} if message == "length is inconsistent" else {}

    with pytest.raises(ArchiveIntegrityError, match=message):
        _bounded(
            _response(raw, headers=headers),
            _request(),
            maximum_bytes=maximum_bytes,
        )

    assert raw.closed is True


@pytest.mark.parametrize(
    ("method", "status_code"),
    [("HEAD", 200), ("head", 200), ("GET", 204), ("GET", 304)],
)
def test_no_body_semantics_ignore_representation_headers_but_charge_raw_bytes(
    method: str,
    status_code: int,
) -> None:
    raw = _RawBody((b"x",))

    response = _bounded(
        _response(
            raw,
            status_code=status_code,
            headers={"content-encoding": "gzip", "content-length": "999999"},
        ),
        _request(method=method),
    )

    assert list(response.raw.stream()) == [b"x"]
    assert raw.closed is True


def test_streaming_success_preserves_sdk_raw_body_and_restores_request_flag() -> None:
    raw = _RawBody((b"payload",))
    response = cast(
        AWSResponse,
        _response(raw, status_code=206, headers={"content-length": "7"}),
    )
    request = _request(stream_output=False)

    selected = _bounded(
        response,
        request,
        operation_name="GetObject",
        stream_success=True,
    )

    assert selected is response
    assert raw.closed is False
    assert request.stream_output is False


def test_nonstreaming_cleanup_ignores_raw_close_errors() -> None:
    raw = _RawBody(close_error=True)

    response = _bounded(_response(raw), _request())

    assert list(response.raw.stream()) == []
    assert raw.closed is True


def test_close_helper_accepts_resources_without_close() -> None:
    s3_transport._close_ignoring_errors(object())


def test_install_transport_registers_exact_operation_handlers() -> None:
    events = _Events()
    client = SimpleNamespace(
        _endpoint=SimpleNamespace(http_session=object()),
        meta=SimpleNamespace(events=events),
    )

    assert (
        s3_transport.install_bounded_s3_response_transport(
            client,
            operations={"HeadObject": False, "GetObject": True},
            error_factory=_error,
            maximum_bytes=8,
        )
        is client
    )
    assert [name for name, _handler in events.handlers] == [
        "before-send.s3.HeadObject",
        "before-send.s3.GetObject",
    ]
    assert all(callable(handler) for _name, handler in events.handlers)


@pytest.mark.parametrize(
    ("client", "operations", "maximum_bytes", "message"),
    [
        (SimpleNamespace(), {"HeadObject": False}, 8, "cannot enforce bounded responses"),
        (
            SimpleNamespace(
                _endpoint=SimpleNamespace(http_session=object()),
                meta=SimpleNamespace(events=object()),
            ),
            {"HeadObject": False},
            8,
            "cannot enforce bounded responses",
        ),
        (
            SimpleNamespace(
                _endpoint=SimpleNamespace(http_session=object()),
                meta=SimpleNamespace(events=_Events()),
            ),
            {},
            8,
            "requires at least one operation",
        ),
        (
            SimpleNamespace(
                _endpoint=SimpleNamespace(http_session=object()),
                meta=SimpleNamespace(events=_Events()),
            ),
            {"HeadObject": False},
            0,
            "limit must be positive",
        ),
    ],
)
def test_install_transport_rejects_invalid_client_or_top_level_configuration(
    client: object,
    operations: Mapping[str, bool],
    maximum_bytes: int,
    message: str,
) -> None:
    with pytest.raises(ArchiveIntegrityError, match=message):
        s3_transport.install_bounded_s3_response_transport(
            client,
            operations=operations,
            error_factory=_error,
            maximum_bytes=maximum_bytes,
        )


@pytest.mark.parametrize(
    "operations",
    [{"": False}, cast(Any, {"HeadObject": 1})],
)
def test_install_transport_rejects_invalid_operation_configuration(
    operations: Mapping[str, bool],
) -> None:
    client = SimpleNamespace(
        _endpoint=SimpleNamespace(http_session=object()),
        meta=SimpleNamespace(events=_Events()),
    )

    with pytest.raises(ArchiveIntegrityError, match="operation configuration is invalid"):
        s3_transport.install_bounded_s3_response_transport(
            client,
            operations=operations,
            error_factory=_error,
        )
