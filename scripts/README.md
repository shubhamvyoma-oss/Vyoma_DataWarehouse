# scripts/

Utility and migration scripts. These are not part of the live pipeline — they are run manually for one-time operations, database maintenance, and diagnostics.

---

## Subfolders

| Folder | Purpose |
|---|---|
| `migrations/` | One-time CSV import scripts for the historical backfill |
| `external/` | Utility scripts for checking DB state, clearing test data, re-routing events |

---

## migrations/

Run once on a fresh database to populate Bronze and Silver with historical data from CSV exports. See [migrations/README.md](migrations/README.md) for details.

## external/

Maintenance and diagnostic scripts. Safe to run at any time — they are read-only or produce easily reversible effects. See [external/README.md](external/README.md) for details.
