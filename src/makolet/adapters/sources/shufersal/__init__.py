"""Shufersal ASP.NET listing with stable identity for signed Azure URLs."""

from __future__ import annotations

from dataclasses import dataclass

from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.paged_html import (
    HtmlListingPartition,
    PagedHtmlSourceAdapter,
    PagedHtmlSourceConfig,
)
from makolet.application.ports import Clock


@dataclass(frozen=True, slots=True)
class ShufersalSourceConfig:
    retailer_id: str = "shufersal"
    portal_id: str = "shufersal:prices"
    listing_url: str = "https://prices.shufersal.co.il/FileObject/UpdateCategory"


class ShufersalSourceAdapter(PagedHtmlSourceAdapter):
    def __init__(
        self,
        config: ShufersalSourceConfig,
        http: HttpListingClient,
        clock: Clock,
    ) -> None:
        super().__init__(
            PagedHtmlSourceConfig(
                retailer_id=config.retailer_id,
                portal_id=config.portal_id,
                listing_url=config.listing_url,
                listing_hosts=frozenset({"prices.shufersal.co.il"}),
                download_hosts=frozenset({"pricesprodpublic.blob.core.windows.net"}),
                partitions=(HtmlListingPartition((("catID", "0"), ("storeId", "0"))),),
                page_size_hint=20,
            ),
            http,
            clock,
        )


__all__ = ["ShufersalSourceAdapter", "ShufersalSourceConfig"]
