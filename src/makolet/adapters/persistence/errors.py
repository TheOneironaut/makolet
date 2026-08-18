"""Persistence-boundary failures with actionable, non-secret messages."""


class PersistenceError(RuntimeError):
    """Base class for durable-state failures raised intentionally by the adapter."""


class PersistenceConflictError(PersistenceError):
    """An optimistic state or identity precondition was not satisfied."""


class PersistenceNotFoundError(PersistenceError):
    """A requested durable entity does not exist."""


class SnapshotValidationError(PersistenceError):
    """A full snapshot failed absence-reconciliation safety checks."""
