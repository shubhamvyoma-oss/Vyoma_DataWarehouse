# database/

Contains all SQL DDL (Data Definition Language) files that define the warehouse schema. Run `run_all.sql` to create the entire database from scratch.

---

## How to Create the Full Schema

```bash
# Run from the project root directory
psql -U postgres -d edmingle_analytics -f database/run_all.sql
```

This creates all schemas, all Bronze tables, all Silver tables, and all Gold views in the correct dependency order.

---

## Folder Structure

```
database/
├── schemas/          CREATE SCHEMA statements (bronze, silver, gold)
├── bronze/
│   ├── webhook/      Tables for webhook-sourced raw data
│   ├── api/          Tables for API-pulled raw data
│   └── manual/       Tables for CSV / manually imported data
├── silver/
│   ├── webhook/      Cleaned tables built from webhook Bronze
│   ├── api/          Cleaned tables built from API Bronze
│   └── manual/       Cleaned tables built from CSV Bronze
├── gold/
│   ├── webhook/      Views over course and enrollment data
│   └── api/          Views over attendance data
└── run_all.sql       Master runner — executes all files in order
```

---

## Medallion Architecture

| Layer | Purpose | Retention |
|---|---|---|
| **Bronze** | Raw data as received — no transformation, no deletion | Permanent audit log |
| **Silver** | Cleaned, typed, deduplicated records — one row per real entity | Live queryable store |
| **Gold** | SQL views over Silver — shaped for Power BI | Always fresh, no storage cost |

---

## Layer Details

- [Bronze README](bronze/README.md)
- [Silver README](silver/README.md)
- [Gold README](gold/README.md)
- [Schemas README](schemas/README.md)

---

## setup_legacy.sql

Historical file from the initial schema setup. Kept for reference. Do not run it — use `run_all.sql` instead.
