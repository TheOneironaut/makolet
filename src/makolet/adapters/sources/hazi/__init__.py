"""Hazi Hinam HTML listing and stable Azure Blob discovery."""

from __future__ import annotations

from dataclasses import dataclass

from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.paged_html import PagedHtmlSourceAdapter, PagedHtmlSourceConfig
from makolet.application.ports import Clock


@dataclass(frozen=True, slots=True)
class HaziSourceConfig:
    retailer_id: str = "hazi-hinam"
    portal_id: str = "hazi-hinam:prices"
    listing_url: str = "https://shop.hazi-hinam.co.il/Prices"


class HaziSourceAdapter(PagedHtmlSourceAdapter):
    def __init__(self, config: HaziSourceConfig, http: HttpListingClient, clock: Clock) -> None:
        super().__init__(
            PagedHtmlSourceConfig(
                retailer_id=config.retailer_id,
                portal_id=config.portal_id,
                listing_url=config.listing_url,
                listing_hosts=frozenset({"shop.hazi-hinam.co.il"}),
                download_hosts=frozenset({"hazihinamprod01.blob.core.windows.net"}),
                date_parameter="d",
                page_size_hint=50,
            ),
            http,
            clock,
        )


__all__ = ["HaziSourceAdapter", "HaziSourceConfig"]
