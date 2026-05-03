# database/schemas/

Contains the `CREATE SCHEMA` statements that set up the three-layer namespace in PostgreSQL.

---

## File: `01_create_schemas.sql`

Creates:
- `bronze` — raw data schema
- `silver` — cleaned data schema
- `gold` — reporting views schema
- `unix_to_ist(bigint)` — helper function that converts a Unix timestamp (integer) to an IST TIMESTAMPTZ

All statements use `CREATE SCHEMA IF NOT EXISTS` and `CREATE OR REPLACE FUNCTION` so they are safe to re-run on an existing database.

---

## Why three schemas?

PostgreSQL schemas act as namespaces. Using `bronze.`, `silver.`, and `gold.` prefixes:
- Makes the data tier immediately clear in any SQL query
- Prevents accidental cross-layer joins without intent
- Allows schema-level permission grants (e.g., read-only access to `gold.*` for Power BI)
