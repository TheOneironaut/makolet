# Open-source review

Status: clean-room audit completed on 2026-08-11. No external repository code,
documentation, fixtures, data, or other copyrightable material has been copied,
adapted, translated, derived, or incorporated into Makolet.

## Method and decision rules

The review inspected GitHub repository metadata, the default-branch head, root file
tree, README and architecture documentation, package manifests, and license text at
the pinned revisions below. For repositories without a permissive license, review
was limited to publicly documented or otherwise observable behavior; implementation
source was not used as a design source.

License conclusions use the following primary references:

- The [OSI approved-license list](https://opensource.org/licenses) identifies MIT
  and Apache-2.0 as OSI-approved licenses.
- The [Open Source Definition](https://opensource.org/osd) requires free
  redistribution and prohibits restrictions on fields of endeavor. A
  non-commercial condition therefore is not open source.
- The [Apache Software Foundation third-party license policy](https://www.apache.org/legal/resolved.html)
  treats Apache-2.0 and MIT as Category A licenses that may be included in an
  Apache-licensed product, while identifying non-commercial and field-of-use
  restrictions as Category X.
- [GitHub's repository licensing guidance](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository)
  confirms that absent a license, default copyright applies and others may not
  reproduce, distribute, or create derivative works.

`Compatible` below means that repository-authored code could be included in an
Apache-2.0 project if its original license and notice obligations are preserved. It
does not approve transitive dependencies, retailer data, Kaggle datasets, fixtures,
product images, trademarks, or network-access methods. Those require separate
provenance, dependency, and legal review.

## Summary

All inspected default branches were named `main`. Commit dates are committer dates
in UTC.

| Repository | Inspected revision | License/SPDX conclusion | OSI and Apache-2.0 decision | Reuse decision | Code reused |
|---|---|---|---|---|---|
| [OpenIsraeliSupermarkets/israeli-supermarket-scarpers](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers) | [`d34a32ba5bf1630028da68b00702d1f2ab6b2dcc`](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/commit/d34a32ba5bf1630028da68b00702d1f2ab6b2dcc), 2026-08-05 16:35:23Z | Custom non-commercial; GitHub `NOASSERTION`; audit ID `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Not OSI-approved; incompatible | Concepts/observable behavior only | No |
| [OpenIsraeliSupermarkets/daily-publish-supermarket-data](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data) | [`56e4014c0857018baad3023243d2dfeda4262270`](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/commit/56e4014c0857018baad3023243d2dfeda4262270), 2026-08-08 17:15:37Z | Custom non-commercial; GitHub `NOASSERTION`; audit ID `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Not OSI-approved; incompatible | Concepts/observable behavior only | No |
| [OpenIsraeliSupermarkets/product-matching-service](https://github.com/OpenIsraeliSupermarkets/product-matching-service) | [`d64c70aa38dffc16cc5b538472ee743b8e014c76`](https://github.com/OpenIsraeliSupermarkets/product-matching-service/commit/d64c70aa38dffc16cc5b538472ee743b8e014c76), 2026-06-06 18:23:36Z | No license; SPDX conclusion `NONE` | Not licensed; incompatible/not cleared | Concepts/observable behavior only | No |
| [AKorets/israeli-supermarket-data](https://github.com/AKorets/israeli-supermarket-data) | [`a2ac4df3861bba202315e89aeb603abe55a946d4`](https://github.com/AKorets/israeli-supermarket-data/commit/a2ac4df3861bba202315e89aeb603abe55a946d4), 2023-08-17 12:58:38Z | MIT, SPDX `MIT` | OSI-approved; compatible | May reuse with MIT notice; none selected | No |
| [amichai1/israeli-price-comparison](https://github.com/amichai1/israeli-price-comparison) | [`4f103c5a88623f274019f4f5809a45390e60107e`](https://github.com/amichai1/israeli-price-comparison/commit/4f103c5a88623f274019f4f5809a45390e60107e), 2026-02-25 19:50:55Z | No repository-wide license; SPDX conclusion `NONE`; one nested package manifest says `MIT` | Repository-wide scope is not licensed or cleared | Concepts/observable behavior only pending upstream clarification | No |
| [OpenIsraeliSupermarkets/israeli-supermarket-parsers](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers) | [`346ebeace8d0ef7700162339836ae18039f1ae8c`](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers/commit/346ebeace8d0ef7700162339836ae18039f1ae8c), 2026-08-08 21:10:41Z | Custom non-commercial; GitHub `NOASSERTION`; audit ID `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Not OSI-approved; incompatible | Concepts/observable behavior only | No |
| [nitaiaharoni1/super-mcp](https://github.com/nitaiaharoni1/super-mcp) | [`0f0c901247e87a18f4ffc48ea2550c261c18ebaa`](https://github.com/nitaiaharoni1/super-mcp/commit/0f0c901247e87a18f4ffc48ea2550c261c18ebaa), 2026-08-11 13:59:27Z | Apache License 2.0, SPDX `Apache-2.0` | OSI-approved; directly compatible | May reuse under Apache-2.0 obligations; none selected | No |
| [Segalil/israeli-supermarket-prices](https://github.com/Segalil/israeli-supermarket-prices) | [`a858e9c31193637830a997538668bfcdc2a2f055`](https://github.com/Segalil/israeli-supermarket-prices/commit/a858e9c31193637830a997538668bfcdc2a2f055), 2026-08-11 17:23:41Z | MIT, SPDX `MIT` | OSI-approved; repository-authored code is compatible | May reuse its MIT code only; restricted dependency must not be inherited; none selected | No |

## Required reference repositories

### OpenIsraeliSupermarkets/israeli-supermarket-scarpers

- Repository: <https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers>
- Evidence: pinned [README](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/blob/d34a32ba5bf1630028da68b00702d1f2ab6b2dcc/README.md),
  [license](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/blob/d34a32ba5bf1630028da68b00702d1f2ab6b2dcc/LICENSE.txt),
  and [package metadata](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/blob/d34a32ba5bf1630028da68b00702d1f2ab6b2dcc/setup.py).
- License and attribution: the custom agreement grants non-commercial use only,
  is expressly non-transferable, reserves commercial rights, and requires permitted
  users to name Sefi Erlich, link the repository, and indicate changes. Commercial
  use requires written permission. The license has no standard SPDX identifier;
  GitHub reports `Other` / `NOASSERTION`.
- Compatibility and reuse: the commercial-use restriction violates the Open Source
  Definition and imposes restrictions beyond Apache-2.0. No code, tests, fixtures,
  configuration, documentation text, or derived implementation may be reused.
- Metadata conflict: `setup.py` says `license="CUSTOM"` but retains an MIT classifier.
  The pinned license text and explicit custom declaration control; the classifier is
  not evidence of an MIT grant.
- Concepts learned: the public source ecosystem groups into portal/protocol families;
  retailers sometimes expose legacy and replacement sources; some portals block
  non-Israeli or cloud addresses; repeated collection requires duplicate detection;
  scheduled live checks can reveal portal drift; and a collector may support distinct
  disk and queue outputs. These are research leads, not verified source-of-truth facts.
- Code reused: no.

### OpenIsraeliSupermarkets/daily-publish-supermarket-data

- Repository: <https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data>
- Evidence: pinned [README](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/blob/56e4014c0857018baad3023243d2dfeda4262270/README.md),
  [license](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/blob/56e4014c0857018baad3023243d2dfeda4262270/LICENSE.txt),
  and [requirements](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/blob/56e4014c0857018baad3023243d2dfeda4262270/requirements.txt).
- License and attribution: the repository uses the same custom non-commercial
  agreement and attribution duties as the scraper: name Sefi Erlich, link the
  repository, and identify changes for otherwise permitted non-commercial use.
- Compatibility and reuse: not OSI-approved and incompatible with Apache-2.0. The
  project also directly depends on the custom-licensed scraper and parser packages,
  so those restrictions must not be imported indirectly. No code reuse is allowed.
- Concepts learned: periodic collection can feed a separate processing stage,
  short-term serving/storage, and immutable or versioned long-term publication;
  publishing and retry workflows benefit from health checks and independent status.
  Kaggle, Supabase, MongoDB, and Kafka are observed choices, not required architecture.
- Code reused: no.

### OpenIsraeliSupermarkets/product-matching-service

- Repository: <https://github.com/OpenIsraeliSupermarkets/product-matching-service>
- Evidence: pinned [repository tree](https://github.com/OpenIsraeliSupermarkets/product-matching-service/tree/d64c70aa38dffc16cc5b538472ee743b8e014c76)
  and [README](https://github.com/OpenIsraeliSupermarkets/product-matching-service/blob/d64c70aa38dffc16cc5b538472ee743b8e014c76/README.md).
- License and attribution: no license file or package license declaration was present.
  SPDX conclusion is `NONE`. There is no redistribution permission or attribution
  mechanism that can make copied code distributable; the repository is cited only
  for research provenance.
- Compatibility and reuse: not licensed, therefore not an approved code source and
  not compatible with an Apache-2.0 distribution. No code reuse is allowed.
- Concepts learned: globally standardized barcodes and chain-internal identifiers
  need different matching paths; shared exact barcodes can form evaluation ground
  truth; quantity and unit can bound candidate generation; scoring should remain
  separate from acceptance thresholds; and matchers should be benchmarked before
  catalog grouping. Makolet must implement these domain ideas independently and keep
  fuzzy matches reviewable rather than silently merging products.
- Code reused: no.

### AKorets/israeli-supermarket-data

- Repository: <https://github.com/AKorets/israeli-supermarket-data>
- Evidence: pinned [README](https://github.com/AKorets/israeli-supermarket-data/blob/a2ac4df3861bba202315e89aeb603abe55a946d4/README.md)
  and [MIT license](https://github.com/AKorets/israeli-supermarket-data/blob/a2ac4df3861bba202315e89aeb603abe55a946d4/LICENSE.txt).
- License and attribution: MIT, SPDX `MIT`, copyright 2023 Avram Korets. Any copy or
  substantial portion must retain the copyright and MIT permission notice.
- Compatibility and reuse: OSI-approved and Apache-compatible. Repository-authored
  code could be reused if the notice is preserved and file-level reuse is recorded
  here and in `THIRD_PARTY_NOTICES.md`; no code has been selected or reused.
- Concepts learned: retailer XML can vary in schema and encoding; downloading,
  store parsing, price parsing, tabular normalization, retailer configuration, and
  visualization are separable concerns.
- Code reused: no.

### amichai1/israeli-price-comparison

- Repository: <https://github.com/amichai1/israeli-price-comparison>
- Evidence: pinned [repository tree](https://github.com/amichai1/israeli-price-comparison/tree/4f103c5a88623f274019f4f5809a45390e60107e),
  [README](https://github.com/amichai1/israeli-price-comparison/blob/4f103c5a88623f274019f4f5809a45390e60107e/README.md),
  [root package manifest](https://github.com/amichai1/israeli-price-comparison/blob/4f103c5a88623f274019f4f5809a45390e60107e/package.json),
  and [scraper package manifest](https://github.com/amichai1/israeli-price-comparison/blob/4f103c5a88623f274019f4f5809a45390e60107e/scraper/package.json).
- License and attribution: there is no repository-root license. The root package is
  private and has no license declaration; README wording about educational and
  demonstration purposes is not a grant. A nested scraper manifest says `MIT`, but
  no license text, named copyright holder, or unambiguous scope establishes that the
  declaration covers the repository. Repository-wide SPDX conclusion is `NONE`.
- Compatibility and reuse: the repository is not cleared for reuse. Even the nested
  scraper package must remain research-only unless the copyright holder clarifies
  its MIT grant and scope. There is therefore no applicable redistribution
  attribution path today beyond citing the research source.
- Concepts learned: Stores, PriceFull, Price delta, PromoFull, and Promo delta have
  different semantics; Cerberus/FTP and Shufersal HTML/blob delivery are distinct
  portal families; large XML benefits from streaming; city resolution needs
  normalization, aliases, and fallbacks; and full, incremental, and store updates
  can run at different cadences. All portal claims require official/live validation.
- Code reused: no.

## Additional repositories found by the currency search

The audit also checked the
[OpenIsraeliSupermarkets organization repositories](https://github.com/orgs/OpenIsraeliSupermarkets/repositories?type=all)
and an [updated-date GitHub repository search](https://github.com/search?q=%22Israeli+supermarket%22+price+in%3Aname%2Cdescription%2Creadme&type=repositories&s=updated&o=desc).
The following three projects were sufficiently relevant to inspect fully.

### OpenIsraeliSupermarkets/israeli-supermarket-parsers

- Repository: <https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers>
- Evidence: pinned [README](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers/blob/346ebeace8d0ef7700162339836ae18039f1ae8c/README.md),
  [license](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers/blob/346ebeace8d0ef7700162339836ae18039f1ae8c/LICENSE.txt),
  and [package metadata](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers/blob/346ebeace8d0ef7700162339836ae18039f1ae8c/setup.py).
- License and attribution: the same custom non-commercial agreement as the scraper,
  with the same Sefi Erlich name, repository-link, and change-indication duties for
  otherwise permitted non-commercial use. GitHub reports `NOASSERTION`; the audit
  uses `LicenseRef-OpenIsraeliSupermarkets-Custom-NC`.
- Compatibility and reuse: not OSI-approved and incompatible with Apache-2.0. Its
  `setup.py` explicitly says `CUSTOM` while retaining a contradictory stale MIT
  classifier. No code, tests, fixtures, or documentation may be reused.
- Concepts learned: downloading and parsing should be separate tasks; archived
  inputs can be replayed through file-type/parser filters; conversion benefits from
  explicit status and output contracts; and scheduled parser tests can reveal schema
  drift.
- Code reused: no.

### nitaiaharoni1/super-mcp

- Repository: <https://github.com/nitaiaharoni1/super-mcp>
- Evidence: pinned [README](https://github.com/nitaiaharoni1/super-mcp/blob/0f0c901247e87a18f4ffc48ea2550c261c18ebaa/README.md)
  and [Apache-2.0 license](https://github.com/nitaiaharoni1/super-mcp/blob/0f0c901247e87a18f4ffc48ea2550c261c18ebaa/LICENSE).
- License and attribution: Apache License 2.0, SPDX `Apache-2.0`. Redistribution must
  include the license, mark modified files, retain applicable copyright, patent,
  trademark, and attribution notices, and propagate any upstream `NOTICE` content.
  No `NOTICE` file existed at the inspected revision.
- Compatibility and reuse: OSI-approved and directly compatible with Makolet's
  Apache-2.0 license. Code could be reused under Apache section 4, with exact files
  and notices recorded; no code has been selected or reused.
- Concepts learned: one canonical application/query layer can serve REST and MCP;
  regulated-feed ingestion should be operationally distinct from best-effort website
  scraping; every price should expose provenance; fulfillment terms need source,
  verification date, confidence, and expiry; and online-only inventory must not be
  conflated with physical-branch availability. The project's non-official website
  scrapers are outside Makolet's authorized core collection scope regardless of code
  license.
- Code reused: no.

### Segalil/israeli-supermarket-prices

- Repository: <https://github.com/Segalil/israeli-supermarket-prices>
- Evidence: pinned [README](https://github.com/Segalil/israeli-supermarket-prices/blob/a858e9c31193637830a997538668bfcdc2a2f055/README.md),
  [MIT license](https://github.com/Segalil/israeli-supermarket-prices/blob/a858e9c31193637830a997538668bfcdc2a2f055/LICENSE),
  and [requirements](https://github.com/Segalil/israeli-supermarket-prices/blob/a858e9c31193637830a997538668bfcdc2a2f055/requirements.txt).
- License and attribution: MIT, SPDX `MIT`, copyright 2026 Segalil. Any copy or
  substantial portion must retain that copyright and MIT permission notice.
- Compatibility and reuse: repository-authored MIT code is OSI-approved and
  Apache-compatible. However, the project directly depends on
  `il-supermarket-scraper==1.0.8`, whose pinned repository license is the incompatible
  custom non-commercial agreement. Makolet must not copy that dependency relationship
  or restricted implementation. Independently written adapters are required. No code
  has been selected or reused.
- Concepts learned: the Stores document can identify an online branch; PriceFull and
  PromoFull need separate handling; sources may combine gzip, Windows-1255, and
  element-name variants; cloud geoblocking can make live runs incomplete; bundle
  promotions require quantity-aware arithmetic; and legal minimal fixtures enable
  offline parser tests.
- Code reused: no.

## Final clean-room decision

- `AKorets/israeli-supermarket-data`, `nitaiaharoni1/super-mcp`, and the original
  MIT-covered portions of `Segalil/israeli-supermarket-prices` are legally eligible
  for future file-level consideration, subject to notice and dependency review.
- The three custom non-commercial OpenIsraeliSupermarkets repositories are
  research-only and must never supply code, tests, fixtures, configurations, or
  adapted documentation.
- `OpenIsraeliSupermarkets/product-matching-service` and repository-wide
  `amichai1/israeli-price-comparison` are unlicensed/not cleared and are
  research-only.
- No inspected repository code has been reused. If that changes, this document and
  `THIRD_PARTY_NOTICES.md` must identify the exact source files, revision, license,
  modifications, and retained notices before merge.
- Software licenses do not establish rights to retailer files, generated datasets,
  Kaggle content, product images, or other third-party data. Those materials remain
  outside the reuse approvals above.
