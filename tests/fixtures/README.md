# Clean-room XML fixtures

The files under `standard/` were independently authored for Makolet and are covered
by the repository's Apache-2.0 license. They are small synthetic examples of the
publicly specified Stores, PriceFull, and PromoFull XML shapes. No retailer file,
production record, credential, signed URL, or third-party source code was copied.

- `stores.xml` exercises nested chain, subchain, and store context.
- `price-full.xml` contains one accepted and one deliberately rejected price record.
- `promo-full.xml` exercises product, store, club, and promotion-condition relations.
- `promo-malformed-*-relation.xml` are isolated promotion records containing one
  valid and one malformed item, store, or club relation. They prove malformed
  relationship children are rejected instead of silently omitted.

The shapes are based only on documented public behavior and the clean-room research
ledger in `docs/research/source-research.md`.

Additional schema-change cases are authored inline in `tests/unit/test_xml_parser.py`
because each is a single small record. They prove additive leaf tags produce bounded
durable warnings, unknown promotion conditions remain visible in restrictions,
renamed required identifiers reject the record, and every documented discount-kind
classification has an explicit clean-room example.
