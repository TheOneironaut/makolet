"""Explicit non-success adapters for blocked or unresolved publisher sources."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from makolet.application.models import DiscoveryCursor, DiscoveryPage, DiscoveryRunBudget
from makolet.domain.errors import SourceBlockedError


class DisabledSourceStatus(StrEnum):
    EXTERNALLY_BLOCKED = "externally_blocked"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class DisabledSourceConfig:
    retailer_id: str
    status: DisabledSourceStatus
    public_lead: str | None
    reason: str


class DisabledSourceAdapter:
    def __init__(self, config: DisabledSourceConfig) -> None:
        self.config = config
        self.source_id = config.retailer_id

    async def discover(
        self,
        cursor: DiscoveryCursor | None,
        *,
        limit: int,
        budget: DiscoveryRunBudget | None = None,
    ) -> DiscoveryPage:
        del cursor, limit, budget
        raise SourceBlockedError(
            f"Source {self.source_id} is {self.config.status.value}: {self.config.reason}"
        )


__all__ = ["DisabledSourceAdapter", "DisabledSourceConfig", "DisabledSourceStatus"]
