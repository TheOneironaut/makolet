# Migration rules

Migrations are forward-only, deterministic, and safe on an empty PostgreSQL database
and the immediately preceding schema. Use durable UUID primary keys plus explicit
business uniqueness constraints; retailer identifiers and GTINs are never primary
keys. Keep data transforms bounded and restartable, name indexes deliberately, and
document any lock-heavy operation and its recovery procedure.

Verify upgrade-to-head, downgrade only when explicitly implemented, schema drift, and
representative query plans. Never edit an already released migration; add a successor.

