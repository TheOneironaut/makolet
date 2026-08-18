from __future__ import annotations

import pytest

from makolet.adapters.sources.bina import BinaSourceConfig
from makolet.adapters.sources.disabled import DisabledSourceAdapter
from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.ncr import FtpCatalogClient
from makolet.adapters.sources.registry import RETAILER_REGISTRY, SourceRegistry
from makolet.domain.errors import SourceBlockedError
from tests.unit.sources.support import FixedClock, FixtureFtpClient, FixtureHttpClient


def registry() -> SourceRegistry:
    http: HttpListingClient = FixtureHttpClient({})
    ftp: FtpCatalogClient = FixtureFtpClient({})
    return SourceRegistry(http, ftp, FixedClock())


def test_registry_has_exactly_one_ordered_definition_for_every_official_row() -> None:
    definitions = RETAILER_REGISTRY

    assert len(definitions) == 28
    assert tuple(item.position for item in definitions) == tuple(range(1, 29))
    assert len({item.retailer_id for item in definitions}) == 28
    assert definitions[0].retailer_id == "king-store"
    assert definitions[-1].retailer_id == "shuk-hair"
    assert {item.family for item in definitions} == {
        "bina_https_json",
        "city_html_uuid",
        "hazi_html_azure",
        "legacy_http_blocked",
        "matrix_https_json",
        "ncr_ftp_ftps",
        "shufersal_html_azure",
        "static_daily_https",
        "unresolved_disabled",
    }


def test_registry_constructs_all_adapters_with_matching_source_ids() -> None:
    subject = registry()
    adapters = subject.create_all()

    assert len(adapters) == 28
    assert tuple(adapter.source_id for adapter in adapters) == tuple(
        definition.retailer_id for definition in RETAILER_REGISTRY
    )
    assert isinstance(subject.create("nitzat-haduvdevan"), DisabledSourceAdapter)


def test_registry_preserves_the_observed_maayan_price_zip_wrapper() -> None:
    definition = next(item for item in RETAILER_REGISTRY if item.retailer_id == "maayan-2000")

    assert isinstance(definition.config, BinaSourceConfig)
    assert definition.config.zip_wrapped_file_types == frozenset({"2"})


def test_registry_exposes_complete_secret_free_database_registrations() -> None:
    subject = registry()
    retailers = subject.retailer_registrations()
    portals = subject.portal_registrations()

    assert len(retailers) == 28
    assert len({item.source_key for item in retailers}) == 28
    assert len({item.edi for item in retailers if item.edi is not None}) == sum(
        item.edi is not None for item in retailers
    )
    assert len(portals) == 30
    assert len({(item.retailer_source_key, item.source_key) for item in portals}) == 30
    netiv = next(item for item in portals if item.retailer_source_key == "netiv-hahesed")
    assert netiv.is_active is False
    assert netiv.protocol.value == "http"
    assert all("@" not in item.base_url for item in portals)


async def test_disabled_sources_fail_explicitly_instead_of_returning_empty_success() -> None:
    subject = registry()

    for retailer_id in ("nitzat-haduvdevan", "netiv-hahesed"):
        with pytest.raises(SourceBlockedError, match=retailer_id):
            await subject.create(retailer_id).discover(None, limit=1)


def test_ncr_registry_contains_only_credential_keys_not_secret_values() -> None:
    rendered = repr(RETAILER_REGISTRY)

    assert "username=" not in rendered.casefold()
    assert "password=" not in rendered.casefold()
    assert "ftp://" not in rendered.casefold()
    assert "ftps://" not in rendered.casefold()
