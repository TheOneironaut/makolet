"""City Market server-rendered catalog with opaque UUID downloads."""

from __future__ import annotations

from dataclasses import dataclass

from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.paged_html import PagedHtmlSourceAdapter, PagedHtmlSourceConfig
from makolet.application.ports import Clock


@dataclass(frozen=True, slots=True)
class CitySourceConfig:
    retailer_id: str = "city-market"
    portal_id: str = "city-market:prices"
    listing_url: str = "https://www.citymarket-shops.co.il/"


class CitySourceAdapter(PagedHtmlSourceAdapter):
    def __init__(self, config: CitySourceConfig, http: HttpListingClient, clock: Clock) -> None:
        super().__init__(
            PagedHtmlSourceConfig(
                retailer_id=config.retailer_id,
                portal_id=config.portal_id,
                listing_url=config.listing_url,
                listing_hosts=frozenset({"www.citymarket-shops.co.il"}),
                download_hosts=frozenset({"www.citymarket-shops.co.il"}),
                date_parameter="d",
                page_size_hint=50,
                assume_gzip_when_suffix_missing=True,
            ),
            http,
            clock,
        )


__all__ = ["CitySourceAdapter", "CitySourceConfig"]
