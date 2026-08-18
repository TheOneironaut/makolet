"""PostgreSQL persistence adapters."""

from makolet.adapters.persistence.catalog_matching import PostgresCatalogMatchingRepository
from makolet.adapters.persistence.collection import (
    PostgresCollectionLeaseManager,
    PostgresCollectionRepository,
)
from makolet.adapters.persistence.database import Database
from makolet.adapters.persistence.ingestion import PostgresIngestionRepository
from makolet.adapters.persistence.leases import PostgresLeaseManager
from makolet.adapters.persistence.maintenance import PostgresArchiveMaintenanceRepository
from makolet.adapters.persistence.operations import PostgresOperationalRepository
from makolet.adapters.persistence.queries import PostgresQueryRepository
from makolet.adapters.persistence.registry import PostgresRegistryRepository

__all__ = [
    "Database",
    "PostgresArchiveMaintenanceRepository",
    "PostgresCatalogMatchingRepository",
    "PostgresCollectionLeaseManager",
    "PostgresCollectionRepository",
    "PostgresIngestionRepository",
    "PostgresLeaseManager",
    "PostgresOperationalRepository",
    "PostgresQueryRepository",
    "PostgresRegistryRepository",
]
