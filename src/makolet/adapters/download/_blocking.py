"""Cancellation-safe bridge for bounded blocking network operations."""

from __future__ import annotations

import threading
from collections.abc import Callable
from contextlib import suppress
from typing import Protocol

import anyio


class Closeable(Protocol):
    def close(self) -> None: ...


class BlockingOperationCancellation:
    """Thread-safe ownership of every socket used by one blocking operation."""

    def __init__(self) -> None:
        self._cancelled = threading.Event()
        self._lock = threading.Lock()
        self._resources: dict[int, Closeable] = {}

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def bind(self, resource: Closeable) -> bool:
        with self._lock:
            cancelled = self._cancelled.is_set()
            if not cancelled:
                self._resources[id(resource)] = resource
        if cancelled:
            resource.close()
            return False
        return True

    def release(self, resource: Closeable) -> None:
        with self._lock:
            self._resources.pop(id(resource), None)

    def cancel(self) -> None:
        self._cancelled.set()
        with self._lock:
            resources = tuple(self._resources.values())
        for resource in resources:
            with suppress(Exception):
                resource.close()

    def checkpoint(self) -> None:
        if self._cancelled.is_set():
            raise TimeoutError("Blocking operation was cancelled")


async def run_bounded_blocking[Result](
    operation: Callable[[], Result],
    cancel_operation: Callable[[], None],
    *,
    timeout_seconds: float,
    cancellation_join_seconds: float = 1.0,
) -> tuple[Result | None, bool]:
    """Run blocking I/O with socket abort and a bounded cancellation join.

    The operation registers each control/data socket with ``cancel_operation``.
    Timeout or caller cancellation closes those sockets, waits a fixed cleanup
    interval, and only then permits AnyIO to abandon a pathologically unresponsive
    worker thread. Real socket operations therefore finish and release their owned
    resources without allowing a broken close implementation to stall shutdown.
    """

    if timeout_seconds <= 0 or cancellation_join_seconds <= 0:
        raise ValueError("Blocking operation deadlines must be positive")

    results: list[Result] = []
    errors: list[BaseException] = []
    completed = threading.Event()
    completion_signal = anyio.Event()

    def tracked_operation() -> None:
        try:
            results.append(operation())
        except BaseException as error:
            errors.append(error)
        finally:
            completed.set()
            with suppress(RuntimeError):
                anyio.from_thread.run_sync(completion_signal.set)

    async def run_operation() -> None:
        await anyio.to_thread.run_sync(tracked_operation, abandon_on_cancel=True)

    async def wait_for_completion() -> None:
        if not completed.is_set():
            await completion_signal.wait()

    timed_out = False
    async with anyio.create_task_group() as task_group:
        task_group.start_soon(run_operation)
        try:
            with anyio.move_on_after(timeout_seconds) as timeout_scope:
                await wait_for_completion()
            timed_out = timeout_scope.cancel_called
        finally:
            if not completed.is_set():
                cancel_operation()
                with anyio.CancelScope(shield=True):
                    with anyio.move_on_after(cancellation_join_seconds):
                        await wait_for_completion()
            task_group.cancel_scope.cancel()

    if timed_out:
        return (results[0] if results else None), True
    if errors:
        raise errors[0]
    if not results:
        raise RuntimeError("Blocking operation completed without a result")
    return results[0], False
