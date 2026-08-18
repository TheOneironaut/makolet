"""Authoritative clean-room connector registry for the 28 legal-group rows."""

# The official legal names intentionally contain Hebrew characters that resemble Latin glyphs.
# ruff: noqa: RUF001

from __future__ import annotations

from dataclasses import dataclass, replace
from urllib.parse import urlsplit

from makolet.adapters.download.http import RemoteAccessPolicy
from makolet.adapters.sources.bina import BinaSourceAdapter, BinaSourceConfig
from makolet.adapters.sources.city import CitySourceAdapter, CitySourceConfig
from makolet.adapters.sources.disabled import (
    DisabledSourceAdapter,
    DisabledSourceConfig,
    DisabledSourceStatus,
)
from makolet.adapters.sources.hazi import HaziSourceAdapter, HaziSourceConfig
from makolet.adapters.sources.http import HttpListingClient
from makolet.adapters.sources.matrix import MatrixSourceAdapter, MatrixSourceConfig
from makolet.adapters.sources.ncr import (
    FtpCatalogClient,
    NcrFeedConfig,
    NcrSourceAdapter,
    NcrSourceConfig,
)
from makolet.adapters.sources.shufersal import ShufersalSourceAdapter, ShufersalSourceConfig
from makolet.adapters.sources.static_daily import (
    StaticDailyFeedConfig,
    StaticDailySourceAdapter,
    StaticDailySourceConfig,
)
from makolet.application.models import PortalRegistration, RetailerRegistration
from makolet.application.ports import Clock, SourceAdapter
from makolet.domain.enums import SourceProtocol

type SourceConfig = (
    BinaSourceConfig
    | CitySourceConfig
    | DisabledSourceConfig
    | HaziSourceConfig
    | MatrixSourceConfig
    | NcrSourceConfig
    | ShufersalSourceConfig
    | StaticDailySourceConfig
)


@dataclass(frozen=True, slots=True)
class RetailerSourceDefinition:
    position: int
    retailer_id: str
    official_entity: str
    display_name: str
    family: str
    observed_chain_ids: tuple[str, ...]
    config: SourceConfig


def _bina(
    position: int,
    retailer_id: str,
    official_entity: str,
    display_name: str,
    host_label: str,
    query_identifier: str,
    chain_id: str,
    *,
    zip_wrapped_file_types: frozenset[str] = frozenset(),
) -> RetailerSourceDefinition:
    return RetailerSourceDefinition(
        position,
        retailer_id,
        official_entity,
        display_name,
        "bina_https_json",
        (chain_id,),
        BinaSourceConfig(
            retailer_id=retailer_id,
            portal_id=f"bina:{retailer_id}",
            base_url=f"https://{host_label}.binaprojects.com/",
            query_identifier=query_identifier,
            chain_id=chain_id,
            zip_wrapped_file_types=zip_wrapped_file_types,
        ),
    )


def _matrix(
    position: int,
    retailer_id: str,
    official_entity: str,
    display_name: str,
    edi: str,
) -> RetailerSourceDefinition:
    return RetailerSourceDefinition(
        position,
        retailer_id,
        official_entity,
        display_name,
        "matrix_https_json",
        (edi,),
        MatrixSourceConfig(
            retailer_id=retailer_id,
            portal_id=f"matrix:{retailer_id}",
            edi=edi,
        ),
    )


def _ncr_feed(
    retailer_id: str,
    label: str,
    chain_id: str,
    *,
    protocol: SourceProtocol = SourceProtocol.FTP,
) -> NcrFeedConfig:
    return NcrFeedConfig(
        portal_id=f"ncr:{retailer_id}:{label}",
        chain_id=chain_id,
        credential_key=f"{retailer_id}_{label}",
        protocol=protocol,
    )


def _ncr(
    position: int,
    retailer_id: str,
    official_entity: str,
    display_name: str,
    *feeds: NcrFeedConfig,
) -> RetailerSourceDefinition:
    return RetailerSourceDefinition(
        position,
        retailer_id,
        official_entity,
        display_name,
        "ncr_ftp_ftps",
        tuple(feed.chain_id for feed in feeds),
        NcrSourceConfig(retailer_id, tuple(feeds)),
    )


RETAILER_REGISTRY: tuple[RetailerSourceDefinition, ...] = (
    _bina(
        1,
        "king-store",
        "אלמשדהאוי קינג סטור בע״מ",
        "King Store",
        "kingstore",
        "7290058108879",
        "7290058108879",
    ),
    _bina(
        2,
        "maayan-2000",
        "ג.מ מעיין אלפיים (07) בע״מ",
        "Maayan 2000",
        "maayan2000",
        "7290058159628",
        "7290058159628",
        zip_wrapped_file_types=frozenset({"2"}),
    ),
    RetailerSourceDefinition(
        3,
        "global-retail",
        (
            "גלובל ריטייל ק.י. בע״מ (גלובל ריטייל ק.י. (מ.ר) בע״מ, "
            "גלובל ריטייל ק.י. (ע.ר) בע״מ, גלובל ריטייל ק.י. (ה.ר) בע״מ)"
        ),
        "Carrefour",
        "static_daily_https",
        ("7290055700007", "7290055700014"),
        StaticDailySourceConfig(
            "global-retail",
            (
                StaticDailyFeedConfig(
                    "carrefour:primary",
                    "https://prices.carrefour.co.il/",
                    frozenset({"prices.carrefour.co.il"}),
                    frozenset({"prices.carrefour.co.il"}),
                ),
                StaticDailyFeedConfig(
                    "carrefour:signage",
                    "https://shilut.carrefour.co.il/",
                    frozenset({"shilut.carrefour.co.il"}),
                    frozenset({"shilut.carrefour.co.il"}),
                ),
            ),
        ),
    ),
    _ncr(
        4,
        "dor-alon",
        (
            "דור אלון ניהול מתחמים קמעונאיים בע״מ (דוכן גן שמואל החדש שותפות מוגבלת, "
            "עין שמר-אלון אחזקות בע״מ, משמר השרון אלון אחזקות בע״מ, "
            "עינת אלון החזקות בע״מ)"
        ),
        "Dor Alon / AM:PM",
        _ncr_feed(
            "dor-alon",
            "primary",
            "7290492000005",
            protocol=SourceProtocol.FTPS,
        ),
    ),
    _matrix(5, "hyper-cohen", "היפר כהן בע״מ", "Hyper Cohen", "7290455000004"),
    RetailerSourceDefinition(
        6,
        "wolt-market",
        "וואלט אופריישנס סרוויסס ישראל בע״מ",
        "Wolt Market",
        "static_daily_https",
        ("7290058249350",),
        StaticDailySourceConfig(
            "wolt-market",
            (
                StaticDailyFeedConfig(
                    "wolt-market:public",
                    "https://wm-gateway.wolt.com/isr-prices/public/v1/index.html",
                    frozenset({"wm-gateway.wolt.com"}),
                    frozenset({"wm-gateway.wolt.com"}),
                ),
            ),
        ),
    ),
    _matrix(
        7,
        "victory",
        "ויקטורי רשת סופרמרקטים בע״מ",
        "Victory",
        "7290696200003",
    ),
    _bina(
        8,
        "zol-vegadol",
        "זול ובגדול בע״מ",
        "Zol VeBegadol",
        "zolvebegadol",
        "7290058173198",
        "7290058173198",
    ),
    _ncr(
        9,
        "tiv-taam",
        "טיב טעם רשתות בע״מ",
        "Tiv Taam",
        _ncr_feed("tiv-taam", "primary", "7290873255550"),
    ),
    _matrix(
        10,
        "machsanei-hashuk",
        "כ.נ מחסני השוק בע״מ",
        "Machsanei HaShuk",
        "7290661400001",
    ),
    RetailerSourceDefinition(
        11,
        "hazi-hinam",
        "כל בו חצי חינם בע״מ",
        "Hazi Hinam",
        "hazi_html_azure",
        ("7290700100008",),
        HaziSourceConfig(),
    ),
    _ncr(
        12,
        "yohananof",
        "מ. יוחננוף ובניו (1988) בע״מ",
        "Yohananof",
        _ncr_feed("yohananof", "primary", "7290803800003"),
    ),
    _ncr(
        13,
        "merav-mazon",
        "מרב-מזון כל בע״מ",
        "Osher Ad",
        _ncr_feed("merav-mazon", "primary", "7290103152017"),
    ),
    RetailerSourceDefinition(
        14,
        "nitzat-haduvdevan",
        "ניצת הדובדבן כנפי נשרים בע״מ",
        "Nitzat HaDuvdevan",
        "unresolved_disabled",
        (),
        DisabledSourceConfig(
            "nitzat-haduvdevan",
            DisabledSourceStatus.UNRESOLVED,
            None,
            "no current publisher transparency portal has been located",
        ),
    ),
    RetailerSourceDefinition(
        15,
        "netiv-hahesed",
        "נתיב החסד סופר חסד בע״מ",
        "Netiv HaHesed",
        "legacy_http_blocked",
        (),
        DisabledSourceConfig(
            "netiv-hahesed",
            DisabledSourceStatus.EXTERNALLY_BLOCKED,
            "http://141.226.203.152/",
            "the known publisher endpoint returned HTTP 500 and HTTPS timed out",
        ),
    ),
    _ncr(
        16,
        "dabbah",
        "סאלח דאבח ובניו בע״מ",
        "Dabbah",
        _ncr_feed("dabbah", "primary", "7290526500006"),
    ),
    _bina(
        17,
        "super-sapir",
        "סופר ספיר בע״מ",
        "Super Sapir",
        "supersapir",
        "7290058156016",
        "7290058156016",
    ),
    _bina(
        18,
        "super-bareket",
        "סופר ברקת קמעונאות בע״מ",
        "Super Bareket",
        "superbareket",
        "7290875100001",
        "7290875100001",
    ),
    _ncr(
        19,
        "stop-market",
        "סטופ מרקט בע״מ",
        "Stop Market",
        _ncr_feed("stop-market", "primary", "7290639000004"),
    ),
    _ncr(
        20,
        "pulitzer",
        "פוליצר חדרה (1982) בע״מ",
        "Pulitzer Hadera",
        _ncr_feed("pulitzer", "primary", "7291059100008"),
    ),
    _ncr(
        21,
        "paz-freshmarket",
        "פז חברת נפט בע״מ (פרשמרקט בע״מ)",
        "Paz / Freshmarket / Yellow",
        _ncr_feed("paz-freshmarket", "freshmarket", "7290876100000"),
        _ncr_feed("paz-freshmarket", "paz-yellow", "7290644700005"),
    ),
    RetailerSourceDefinition(
        22,
        "city-market",
        "קבוצת סיטי מרקט – יוסף שוורץ",
        "City Market",
        "city_html_uuid",
        ("7290000000003",),
        CitySourceConfig(),
    ),
    _bina(
        23,
        "kt-import",
        "קיי.טי. יבוא ושיווק בע״מ",
        "Mishnat Yosef / K.T.",
        "ktshivuk",
        "5144744100001",
        "7290058289400",
    ),
    _ncr(
        24,
        "keshet-teamim",
        "קשת טעמים בע״מ",
        "Keshet Teamim",
        _ncr_feed("keshet-teamim", "primary", "7290785400000"),
    ),
    _ncr(
        25,
        "rami-levy",
        ("רשת חנויות רמי לוי שיווק השקמה 2006 בע״מ (ב.ה לוי בע״מ, סופר קופיקס בע״מ, פרש פוד בע״מ)"),
        "Rami Levy / Super Cofix",
        _ncr_feed("rami-levy", "primary", "7290058140886"),
        _ncr_feed("rami-levy", "super-cofix", "7291056200008"),
    ),
    RetailerSourceDefinition(
        26,
        "shufersal",
        "שופרסל בע״מ",
        "Shufersal",
        "shufersal_html_azure",
        ("7290027600007",),
        ShufersalSourceConfig(),
    ),
    _bina(
        27,
        "shefa-birkat-hashem",
        "שפע ברכת השם בע״מ",
        "Shefa Birkat Hashem",
        "shefabirkathashem",
        "7290058134977",
        "7290058134977",
    ),
    _bina(
        28,
        "shuk-hair",
        "שוק העיר (ט.ע.מ.ס) בע״מ",
        "Shuk HaIr",
        "shuk-hayir",
        "7290058148776",
        "7290058148776",
    ),
)


class SourceRegistry:
    def __init__(
        self,
        http: HttpListingClient,
        ftp: FtpCatalogClient,
        clock: Clock,
        definitions: tuple[RetailerSourceDefinition, ...] = RETAILER_REGISTRY,
    ) -> None:
        _validate_definitions(definitions)
        self._http = http
        self._ftp = ftp
        self._clock = clock
        self._definitions = definitions
        self._by_id = {definition.retailer_id: definition for definition in definitions}

    @property
    def definitions(self) -> tuple[RetailerSourceDefinition, ...]:
        return self._definitions

    def definition(self, retailer_id: str) -> RetailerSourceDefinition:
        try:
            return self._by_id[retailer_id]
        except KeyError as error:
            raise KeyError(f"Unknown retailer source: {retailer_id}") from error

    def create(self, retailer_id: str) -> SourceAdapter:
        config = self.definition(retailer_id).config
        if isinstance(config, BinaSourceConfig):
            return BinaSourceAdapter(config, self._http, self._clock)
        if isinstance(config, MatrixSourceConfig):
            return MatrixSourceAdapter(config, self._http, self._clock)
        if isinstance(config, NcrSourceConfig):
            return NcrSourceAdapter(config, self._ftp, self._clock)
        if isinstance(config, StaticDailySourceConfig):
            return StaticDailySourceAdapter(config, self._http, self._clock)
        if isinstance(config, HaziSourceConfig):
            return HaziSourceAdapter(config, self._http, self._clock)
        if isinstance(config, CitySourceConfig):
            return CitySourceAdapter(config, self._http, self._clock)
        if isinstance(config, ShufersalSourceConfig):
            return ShufersalSourceAdapter(config, self._http, self._clock)
        return DisabledSourceAdapter(config)

    def create_all(self) -> tuple[SourceAdapter, ...]:
        return tuple(self.create(definition.retailer_id) for definition in self._definitions)

    def http_download_policies(
        self,
        *,
        maximum_response_bytes: int,
    ) -> dict[str, RemoteAccessPolicy]:
        """Return portal-keyed policies for exact source-file downloads."""

        if maximum_response_bytes <= 0:
            raise ValueError("maximum_response_bytes must be positive")
        policies: dict[str, RemoteAccessPolicy] = {}
        for definition in self._definitions:
            config = definition.config
            if isinstance(config, BinaSourceConfig | MatrixSourceConfig):
                policies[config.portal_id] = RemoteAccessPolicy(
                    allowed_hosts=frozenset({config.host}),
                    maximum_response_bytes=maximum_response_bytes,
                )
            elif isinstance(config, StaticDailySourceConfig):
                for feed in config.feeds:
                    policies[feed.portal_id] = RemoteAccessPolicy(
                        allowed_hosts=feed.download_hosts,
                        redirect_hosts=feed.download_hosts,
                        maximum_response_bytes=maximum_response_bytes,
                    )
            elif isinstance(config, HaziSourceConfig):
                policies[config.portal_id] = RemoteAccessPolicy(
                    allowed_hosts=frozenset({"hazihinamprod01.blob.core.windows.net"}),
                    maximum_response_bytes=maximum_response_bytes,
                )
            elif isinstance(config, CitySourceConfig):
                policies[config.portal_id] = RemoteAccessPolicy(
                    allowed_hosts=frozenset({"www.citymarket-shops.co.il"}),
                    maximum_response_bytes=maximum_response_bytes,
                )
            elif isinstance(config, ShufersalSourceConfig):
                policies[config.portal_id] = RemoteAccessPolicy(
                    allowed_hosts=frozenset({"pricesprodpublic.blob.core.windows.net"}),
                    maximum_response_bytes=maximum_response_bytes,
                )
        return policies

    def ftp_feeds(self) -> dict[str, NcrFeedConfig]:
        """Return immutable NCR feed policy values keyed by stable portal ID."""

        return {
            feed.portal_id: replace(feed)
            for definition in self._definitions
            if isinstance((config := definition.config), NcrSourceConfig)
            for feed in config.feeds
        }

    def retailer_registrations(self) -> tuple[RetailerRegistration, ...]:
        return tuple(
            RetailerRegistration(
                source_key=definition.retailer_id,
                legal_name=definition.official_entity,
                display_name=definition.display_name,
                edi=definition.observed_chain_ids[0] if definition.observed_chain_ids else None,
                is_active=not isinstance(definition.config, DisabledSourceConfig),
            )
            for definition in self._definitions
        )

    def portal_registrations(self) -> tuple[PortalRegistration, ...]:
        return tuple(
            registration
            for definition in self._definitions
            for registration in _portal_registrations(definition)
        )


def _validate_definitions(definitions: tuple[RetailerSourceDefinition, ...]) -> None:
    if tuple(definition.position for definition in definitions) != tuple(range(1, 29)):
        raise ValueError("Retailer registry must contain positions 1 through 28 in order")
    ids = tuple(definition.retailer_id for definition in definitions)
    if len(set(ids)) != len(ids):
        raise ValueError("Retailer registry contains duplicate IDs")
    if any(definition.config.retailer_id != definition.retailer_id for definition in definitions):
        raise ValueError("Retailer registry config does not match its legal-group ID")


def _portal_registrations(
    definition: RetailerSourceDefinition,
) -> tuple[PortalRegistration, ...]:
    config = definition.config
    portals: list[tuple[str, SourceProtocol, str, bool]] = []
    if isinstance(config, BinaSourceConfig):
        portals.append((config.portal_id, SourceProtocol.HTTPS, config.base_url, True))
    elif isinstance(config, MatrixSourceConfig):
        portals.append((config.portal_id, SourceProtocol.HTTPS, config.api_root, True))
    elif isinstance(config, NcrSourceConfig):
        portals.extend(
            (
                feed.portal_id,
                feed.protocol,
                f"{feed.protocol.value}://{feed.host}/{feed.remote_directory.strip('/')}",
                True,
            )
            for feed in config.feeds
        )
    elif isinstance(config, StaticDailySourceConfig):
        portals.extend(
            (feed.portal_id, SourceProtocol.HTTPS, feed.index_url, True) for feed in config.feeds
        )
    elif isinstance(config, HaziSourceConfig | CitySourceConfig | ShufersalSourceConfig):
        portals.append((config.portal_id, SourceProtocol.HTTPS, config.listing_url, True))
    elif config.public_lead is not None:
        protocol = SourceProtocol(urlsplit(config.public_lead).scheme.casefold())
        portals.append(
            (
                f"disabled:{definition.retailer_id}",
                protocol,
                config.public_lead,
                False,
            )
        )
    return tuple(
        PortalRegistration(
            retailer_source_key=definition.retailer_id,
            source_key=source_key,
            family=definition.family,
            protocol=protocol,
            base_url=base_url,
            is_active=is_active,
        )
        for source_key, protocol, base_url, is_active in portals
    )


__all__ = ["RETAILER_REGISTRY", "RetailerSourceDefinition", "SourceConfig", "SourceRegistry"]
