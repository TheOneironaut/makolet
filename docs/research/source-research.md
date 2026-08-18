# Source research

Research matrix verified: 2026-08-11. The credential-free HTTPS listing paths for
`shufersal`, `king-store`, `maayan-2000`, `global-retail`, `hyper-cohen`,
`hazi-hinam`, and `city-market` were each reverified with a bounded live listing test on 2026-08-12.
The 28-row source registry is implemented, and every accessible portal family has an
offline-tested discovery adapter. Live checks remain bounded, explicitly selected
evidence rather than a blanket production-health claim.

## Authoritative scope

The current source of truth is the Israel Competition Authority's [large-retailer
list for fiscal year 2025, corrected 23 July
2026](https://www.gov.il/he/departments/legalInfo/bigretailerslist2025up). It lists
28 retailer entities and notes that it excludes retailers that qualify only under
alternative 2 in the section-14 definition. Makolet records that limitation rather
than quietly treating the annual list as a mathematically exhaustive set of every
entity that could have a price-transparency duty.

The National Legislation Database identifies the current [Food and Pharmacy Sector
Competition Promotion Law](https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawPrimary.aspx?lawitemid=2001381).
The Competition Authority also publishes an [official updated law
PDF](https://www.gov.il/BlobFolder/legalinfo/foodsectorlaw/he/foodlaw_%D7%97%D7%95%D7%A7%20%D7%A7%D7%99%D7%93%D7%95%D7%9D%20%D7%94%D7%AA%D7%97%D7%A8%D7%95%D7%AA%20%D7%91%D7%A2%D7%A0%D7%A3%20%D7%94%D7%9E%D7%96%D7%95%D7%9F%20%D7%95%D7%94%D7%A4%D7%90%D7%A8%D7%9D%20%D7%94%D7%AA%D7%A9%D7%A2%D7%93-2014%20%D7%9E%D7%A2%D7%95%D7%93%D7%9B%D7%9F%200725.pdf).
A verified chamber-hosted copy of the Consumer Protection Authority's
[implementation guidance](https://www.chamber.org.il/media/148016/1002.pdf)
specifies XML/Gzip publication, full and incremental naming, and recommended fields
for stores, prices, and promotions. The authority-authored document is retained as a
regulatory reference; the mirror must not be mistaken for a current government
endpoint.

The Consumer Protection Authority's government [retailer-links
page](https://www.gov.il/he/pages/cpfta_prices_regulations) returned a 403 challenge
to the low-volume automated checks on 2026-08-11. The State Comptroller's [2024 food
price-transparency audit](https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/2024/2024.11-75A-PartB/2024.11-75A-PartB-101-Mazon.pdf)
is primary public evidence that the authority maintained links to 27 retailer sites
and that retailers publish stores, product/price, and promotion files. It does not
supersede the later Competition Authority roster.

### Nitzat chronology

The official [2024 large-retailer
PDF](https://www.gov.il/BlobFolder/legalinfo/bigretailerslist/he/foodlaw_%D7%A8%D7%A9%D7%99%D7%9E%D7%AA%20%D7%A7%D7%9E%D7%A2%D7%95%D7%A0%D7%90%D7%99%D7%9D%20%D7%92%D7%93%D7%95%D7%9C%D7%99%D7%9D-%202024%20.pdf)
did not list Nitzat HaDuvdevan, while the corrected fiscal-2025 list published on
2026-07-23 includes it at position 14. **Inference:** this chronology may explain
why older government/audit material refers to 27 retailer links while the current
roster has 28 entries. It is not proof that Nitzat has no disclosure duty or no
public feed; no current portal was located in this verification.

## Current official retailer entities

1. Almashhadawi King Store Ltd.
2. G.M. Maayan 2000 (07) Ltd.
3. Global Retail K.Y. Ltd. and named related entities.
4. Dor Alon Retail Sites Management Ltd. and named related entities.
5. Hyper Cohen Ltd.
6. Wolt Operations Services Israel Ltd.
7. Victory Supermarket Chain Ltd.
8. Zol VeBegadol Ltd.
9. Tiv Taam Chains Ltd.
10. K.N. Market Warehouses Ltd. (`Machsanei HaShuk`).
11. Kol Bo Hazi Hinam Ltd.
12. M. Yohananof & Sons (1988) Ltd.
13. Merav-Mazon Kol Ltd.
14. Nitzat HaDuvdevan Kanfei Nesharim Ltd.
15. Netiv HaHesed Super Hesed Ltd.
16. Saleh Dabbah & Sons Ltd.
17. Super Sapir Ltd.
18. Super Bareket Retail Ltd.
19. Stop Market Ltd.
20. Pulitzer Hadera (1982) Ltd.
21. Paz Oil Company Ltd. (`Freshmarket` named as related entity).
22. City Market Group — Yosef Schwartz.
23. K.T. Import and Marketing Ltd.
24. Keshet Teamim Ltd.
25. Rami Levy Hashikma Marketing 2006 Ltd. and named related entities.
26. Shufersal Ltd.
27. Shefa Birkat Hashem Ltd.
28. Shuk HaIr (T.A.M.S.) Ltd.

The Hebrew legal names and corporate relationships are preserved verbatim in
`docs/source-coverage.md`; English display names above are working transliterations,
not legal translations.

## Clean-room method

For each source, research begins with the official retailer portal, regulation, and
actual low-volume responses. External repositories are checked separately for license
and may contribute only permitted concepts. Restricted, non-commercial, custom,
missing-license, or incompatible repositories are never a code source. No access
control, CAPTCHA, rate limit, authentication rule, or anti-bot measure is bypassed.

Each observation records verification time, stable listing URL/remote identifier,
protocol, authentication status, file family, compression, filename form, encoding,
and blocker. Expiring download signatures are provenance only and are never stored as
stable configuration.

## Verification summary

The operational matrix below mirrors `docs/source-coverage.md`. Research found a
direct portal mapping for 26 of 28 legal rows. Netiv HaHesed has a known historical
source endpoint that was inaccessible during verification; Nitzat remains unresolved.
Wolt is mapped but stale, and Dor Alon's FTPS control login was verifiable while its
data channel was blocked. All other 24 rows exposed current-day listings or files.
The accessible BINA, Matrix, NCR, static-daily, Hazi Hinam, City Market, and
Shufersal families have clean-room fixtures and implemented discovery adapters;
Netiv HaHesed and Nitzat remain explicitly disabled. No row is claimed healthy
merely because its portal was researched or its fixture passes.

| # | ID | Display | Family / stable source | 2026-08-11 live result |
|---:|---|---|---|---|
| 1 | `king-store` | King Store | BINA — `https://kingstore.binaprojects.com/` | Current listing and sample passed |
| 2 | `maayan-2000` | Maayan 2000 | BINA — `https://maayan2000.binaprojects.com/` | Current; 1,000-row cap observed |
| 3 | `global-retail` | Carrefour | Static daily HTTPS — `https://prices.carrefour.co.il/`; secondary `https://shilut.carrefour.co.il/` | Primary current/sample passed; secondary Stores stale |
| 4 | `dor-alon` | Dor Alon / AM:PM | NCR explicit FTPS — `url.retail.publishedprices.co.il` | TLS login passed; data channel blocked |
| 5 | `hyper-cohen` | Hyper Cohen | Matrix JSON — `https://laibcatalog.co.il/webapi/api/getfiles?edi=7290455000004` | Current listing passed |
| 6 | `wolt-market` | Wolt Market | Static date index — `https://wm-gateway.wolt.com/isr-prices/public/v1/index.html` | Latest 2026-05-29; current-date page HTTP 400 |
| 7 | `victory` | Victory | Matrix JSON — `https://laibcatalog.co.il/webapi/api/getfiles?edi=7290696200003` | Current listing/sample passed |
| 8 | `zol-vegadol` | Zol VeBegadol | BINA — `https://zolvebegadol.binaprojects.com/` | Current listing passed |
| 9 | `tiv-taam` | Tiv Taam | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 10 | `machsanei-hashuk` | Machsanei HaShuk | Matrix JSON — `https://laibcatalog.co.il/webapi/api/getfiles?edi=7290661400001` | Current listing passed |
| 11 | `hazi-hinam` | Hazi Hinam | Custom HTML/Azure — `https://shop.hazi-hinam.co.il/Prices` | Current listing/sample passed |
| 12 | `yohananof` | Yohananof | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 13 | `merav-mazon` | Osher Ad | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 14 | `nitzat-haduvdevan` | Nitzat HaDuvdevan | No disclosure portal located; retailer site `https://www.nizat.com/` | Retail site passed; source unresolved |
| 15 | `netiv-hahesed` | Netiv HaHesed | Legacy HTTP lead — `http://141.226.203.152/` | HTTP 500; HTTPS timeout |
| 16 | `dabbah` | Dabbah | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 17 | `super-sapir` | Super Sapir | BINA — `https://supersapir.binaprojects.com/` | Current listing passed |
| 18 | `super-bareket` | Super Bareket | BINA — `https://superbareket.binaprojects.com/` | Current listing passed |
| 19 | `stop-market` | Stop Market | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 20 | `pulitzer` | Pulitzer Hadera | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 21 | `paz-freshmarket` | Paz / Freshmarket / Yellow | Two NCR FTP feeds — `url.retail.publishedprices.co.il` | Both directories current; Freshmarket Stores anomaly |
| 22 | `city-market` | City Market | Custom HTML/UUID — `https://www.citymarket-shops.co.il/` | Current listing/sample passed |
| 23 | `kt-import` | Mishnat Yosef / K.T. | BINA — `https://ktshivuk.binaprojects.com/` | Current; query/file ID mismatch observed |
| 24 | `keshet-teamim` | Keshet Teamim | NCR FTP — `url.retail.publishedprices.co.il` | Current directory passed |
| 25 | `rami-levy` | Rami Levy / Super Cofix | Two NCR FTP feeds — `url.retail.publishedprices.co.il` | Rami current/sample passed; Super Cofix Stores only |
| 26 | `shufersal` | Shufersal | ASP.NET/Azure — `https://prices.shufersal.co.il/` | Listing and two sample downloads passed |
| 27 | `shefa-birkat-hashem` | Shefa Birkat Hashem | BINA — `https://shefabirkathashem.binaprojects.com/` | Current listing passed |
| 28 | `shuk-hair` | Shuk HaIr | BINA — `https://shuk-hayir.binaprojects.com/` | Current listing passed |

## Verified source family: BINA Projects HTTPS/JSON

Applies to King Store, Maayan 2000, Zol VeBegadol, Super Sapir, Super
Bareket, K.T./Mishnat Yosef, Shefa Birkat Hashem, and Shuk HaIr.

- Listing: `https://<retailer>.binaprojects.com/MainIO_Hok.aspx`.
- Protocol and discovery: anonymous HTTPS returning JSON. Observed filters were `_`
  (chain/query identifier), `wReshet`, `WFileType`, `WDate`, and `WStore`.
- Current listing and resolver responses are direct JSON arrays. An empty `[]` is
  an empty partition, not a broken catalog, so unpublished Stores can precede later
  Price/Promo families on the same date. Ambiguous wrappers, error objects, and
  unexpected shapes still fail closed rather than becoming a successful empty catalog.
- Download resolution: `Download.aspx?FileNm=<name>` returns JSON containing `SPath`;
  the resulting object is under `/Download/<name>`.
- Documents: `Stores`/`StoresFull`, `Price`/`PriceFull`, and
  `Promo`/`PromoFull`, as raw XML or case-varied `.gz`/`.GZ` Gzip XML.
- Compression anomaly: on 2026-08-16 the current Maayan 2000 store-001 Price
  response used a `.gz` name and `application/x-gzip` media type but contained one
  bounded ZIP member whose payload was XML. The Maayan Price partition therefore
  has an explicit ZIP-wrapper override; other BINA partitions retain extension-based
  compression and fail closed on an unobserved mismatch.
- Encoding: a downloaded King Store Stores Gzip object expanded to XML declaring
  UTF-8. This is a family sample, not proof that every BINA object uses UTF-8.
- Scale and pagination: an unfiltered Maayan 2000 request returned exactly 1,000
  current-day rows (609 Price, 76 PriceFull, 237 Promo, 76 PromoFull, and 2
  StoresFull), indicating a response cap. On 2026-08-12 the five separate type
  partitions returned 2 Stores, 228 Price, 67 Promo, 76 PriceFull, and 76 PromoFull
  entries, all below the cap. `WStore=001` returned two PriceFull entries and both
  filenames carried store code `001`, proving that narrower store partitions are
  available. The implementation partitions by stable date and document type now and
  fails closed if any one partition reaches 1,000; it does not infer a complete store
  roster from a capped response.
- Identity anomaly: K.T. accepted query identifier `5144744100001` but returned
  `StoresFull7290058289400-000-202608110500.gz`. Trust validated document ChainID
  and configured aliases rather than assuming the query identifier equals file EDI.

## Verified source family: NCR FTP and FTPS

Applies to Dor Alon, Tiv Taam, Yohananof, Osher Ad, Dabbah, Stop Market,
Pulitzer, the Paz/Freshmarket group, Keshet Teamim, and the Rami Levy group.

- Host: `url.retail.publishedprices.co.il` with a public banner identifying the
  Published Prices Server. Retailers use chain-scoped public accounts. Account
  identifiers and any non-empty public credentials are configuration supplied
  externally; they are deliberately absent from repository documentation.
- Authentication: most verified accounts behaved as public/passwordless or
  public-credential FTP. Dor Alon rejected plain FTP with `530 Secure connection
  required`; its explicit-TLS control login passed.
- Transport behavior: active-mode FTP data transfers worked for sampled accounts;
  passive mode timed out in this environment. Dor Alon's FTPS data channel was not
  usable: passive timed out and active returned `500 Port command invalid`.
- Documents: Stores `.xml`; Price, PriceFull, Promo, and PromoFull `.gz` Gzip XML.
  Filename casing varies and unknown objects must be retained for quarantine rather
  than silently discarded.
- Encoding: encoding varies inside the family. A Rami Levy Stores file was 38,608
  bytes with a UTF-16LE BOM and no XML declaration. A sampled Rami Levy Price Gzip
  expanded from 23,273 to 334,957 bytes and began with a UTF-8 BOM. Parsers must sniff
  compression, BOM, and declaration per object.

Current-day directory counts were obtained with one date-targeted listing per
account; they are availability observations, not completeness guarantees.

| Retailer/feed | Price | PriceFull | Promo | PromoFull | Stores | Total / anomaly |
|---|---:|---:|---:|---:|---:|---|
| Tiv Taam | 156 | 54 | 366 | 54 | 1 | 631 |
| Yohananof | 508 | 47 | 464 | 47 | 1 | 1,067 |
| Osher Ad | 173 | 72 | 80 | 72 | 1 | 398 |
| Dabbah | 32 | 9 | 36 | 9 | 1 | 87 |
| Stop Market | 79 | 11 | 88 | 11 | 1 | 190 |
| Pulitzer | 36 | 8 | 16 | 8 | 1 | 70 including one other Gzip object |
| Freshmarket | 70 | 52 | 1 | 52 | 0 | 175; no same-day Stores object observed |
| Paz/Yellow | 6 | 239 | 521 | 242 | 1 | 1,009 |
| Keshet Teamim | 54 | 28 | 280 | 28 | 1 | 391 |
| Rami Levy | 1,167 | 343 | 538 | 348 | 1 | 2,397 |
| Super Cofix | 0 | 0 | 0 | 0 | 1 | Only a current Stores object observed |
| Dor Alon | — | — | — | — | — | FTPS data listing blocked in this environment |

## Verified source family: Laib/Matrix HTTPS JSON API

Applies to Hyper Cohen, Victory, and Machsanei HaShuk.

- Branches: `https://laibcatalog.co.il/webapi/api/getbranches?edi=<edi>`.
- Files: `https://laibcatalog.co.il/webapi/api/getfiles?edi=<edi>`.
- Downloads: `https://laibcatalog.co.il/webapi/<edi>/<filename>`.
- Authentication: none observed. Listing responses were JSON with UTF-8 content and
  fields including `fileName`, `fileType`, and `fileSize`; observed `lastModified`
  values were blank.
- Current branch and file catalogs are nonempty direct JSON arrays. Empty `[]`,
  empty named wrappers such as `{"files": []}` or `{"data": []}`, ambiguous
  multi-key objects, error objects, and unexpected wrapper shapes fail closed rather
  than becoming a successful empty catalog.
- Documents: Stores, Price/PriceFull, and Promo/PromoFull Gzip XML. A Victory Stores
  sample expanded to UTF-8 XML without a declaration.
- Listing behavior: the API returned a whole-chain catalog with no visible
  pagination. Hyper Cohen exposed 5 branches/149 files, Victory 70/2,214, and
  Machsanei HaShuk 71/1,212. A branch parameter must not be assumed to partition the
  response unless independently verified.

## Verified custom family: Carrefour static daily indexes

- Primary: `https://prices.carrefour.co.il/`; secondary signage:
  `https://shilut.carrefour.co.il/`.
- Protocol and discovery: anonymous HTTPS HTML with an embedded daily path and file
  metadata; direct objects are `/YYYYMMDD/<filename>`.
- Primary current inventory: 1,350 names — 407 Price, 171 PriceFull, 598 Promo, 173
  PromoFull, and 1 Stores. The observed Stores object was
  `Stores7290055700007-000-20260811-000100.xml`.
- Primary Stores encoding: the 61,394-byte raw XML sample used a UTF-16LE BOM.
- Secondary inventory: 8,660 names, mainly EDI `7290055700014`. Despite current
  Price/Promo entries, its sole Stores entry was dated 2026-04-12. Preserve this as a
  separate feed and treat the primary prices portal as canonical until the publisher
  relationship is clarified.

## Verified custom family: Wolt static date index

- Index: `https://wm-gateway.wolt.com/isr-prices/public/v1/index.html`; per-day pages
  use `/YYYY-MM-DD.html`, with objects under `download/YYYY-MM-DD/<filename>`.
- Authentication: none observed.
- Documents at the latest date: Stores, PriceFull, and PromoFull Gzip XML for EDI
  `7290058249350`. The sampled Stores object expanded from 1,574 to 8,742 bytes and
  declared `encoding="UTF-8"`.
- Availability blocker: the index stopped at 2026-05-29 and the 2026-08-11 page
  returned HTTP 400. This is a stale mapped source, not a successful current feed.

## Verified custom family: Hazi Hinam HTML/Azure

- Listing: `https://shop.hazi-hinam.co.il/Prices`, with observed date and type filters
  such as `?d=2026-08-11&t=3` and HTML pagination.
- Downloads: stable public Blob URLs below
  `https://hazihinamprod01.blob.core.windows.net/regulatories/`; no authentication or
  signed query was observed.
- Documents: StoresFull, Price/PriceFull, and Promo/PromoFull Gzip XML for EDI
  `7290700100008`. Current StoresFull objects and current Price objects were visible.
- Encoding: a current Price object expanded from 1,102 to 10,624 bytes and began with
  a UTF-8 BOM.

## Verified custom family: City Market HTML/UUID

- Listing: `https://www.citymarket-shops.co.il/`, server-rendered HTML with store,
  `d=YYYY-MM-DD`, type, full/incremental, and page filters.
- Downloads: same-origin opaque routes `/downloadFile/<uuid>`; no authentication
  observed.
- Documents: Stores, Price/PriceFull, and Promo/PromoFull. Displayed names sometimes
  omit the extension, while Content-Disposition exposes `.xml.gz`.
- Encoding: a current Price response was 463-byte Gzip, expanded to 1,831-byte XML,
  and began with a UTF-8 BOM.
- Identity boundary: `citymarketkiryatgat.binaprojects.com` was not substituted for
  this source because no corporate evidence linked it to the listed Yosef Schwartz
  group.

## Verified source family: Shufersal ASP.NET/Azure Blob portal

- Listing: `https://prices.shufersal.co.il/FileObject/UpdateCategory`
- Protocol: HTTPS listing and HTTPS Azure Blob downloads.
- Authentication: none observed for listing or signed public reads.
- Filters: category (`Prices`, `PricesFull`, `Promos`, `PromosFull`, `Stores`), store,
  sort, and one-based page. The observed unfiltered listing had 85 pages.
- Remote objects: Gzip; links use expiring read-only signatures.
- Observed names: `Price<chain>-<subchain>-<store>-<timestamp>.gz` and
  `PriceFull<chain>-<subchain>-<store>-<timestamp>.gz`; stores follow the regulated
  stores family. Filename recognition must be case-insensitive and tolerant of real
  publisher deviations while preserving the original value.
- Price XML: UTF-8, `Root` metadata followed by `Items/Item`. Observed fields include
  chain/subchain/store, audit number, item/update identifiers, names/manufacturer,
  quantity/unit/weighted/package values, decimal prices, discount flag, and status.
- Store XML: UTF-8, `Chain/SubChains/SubChain/Stores/Store`, with chain names and
  store identifiers, name, address, locality code, and postal code.
- Scale observation: one live full-price file expanded from 330,765 to 5,521,029
  bytes; this is evidence for streaming and decompression limits, not a benchmark.

## Blocked or unresolved sources

### Netiv HaHesed legacy HTTP portal

The mapped historical source is `http://141.226.203.152/`. On 2026-08-11 the root,
`/index.aspx`, and `/Prices` returned HTTP 500; HTTPS timed out. No file bytes,
encoding, or current listing could be verified. A downstream comparison service
showed late-July Netiv data, but that is secondary evidence and cannot replace the
publisher source. Treat the endpoint as temporarily inaccessible and implement no
speculative parser.

### Nitzat HaDuvdevan

`https://www.nizat.com/` was reachable, but its HTML contained no transparency link
or regulated `PriceFull`, `PromoFull`, or `StoresFull` tokens. Targeted public search
did not locate a current disclosure portal, and the government retailer-links page
was inaccessible to automation. Status is `unresolved`, not `no feed`.

## Implementation boundary and continuing research

The repository now implements the 28-row registry, every accessible portal-family
adapter, shared bounded download/parser/archive behavior, clean-room family fixtures,
and an explicitly selected live-smoke workflow. That implementation does not turn a
historical observation into a current availability claim: the exact per-retailer
status and last live evidence remain in [source coverage](../source-coverage.md).

Additional research is still required to resolve Nitzat, retest Netiv and Dor Alon
from compatible networks, monitor Wolt freshness, identify any separate Fresh Food
feed, and capture a new clean-room fixture whenever a genuinely new encoding or
schema variation is observed.
