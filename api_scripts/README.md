# api_scripts/

This folder contains all scripts that pull data from the Edmingle REST API on a scheduled basis. Each script is self-contained: it connects to the API, fetches data, writes to Bronze, and promotes clean records to Silver. No orchestration layer is needed — run each script directly from the command line.

---

## Scripts

| File | What it does | Run schedule |
|---|---|---|
| `fetch_attendance.py` | Pulls daily attendance for all students via `report_type=55` | Daily (yesterday by default) |
| `fetch_course_catalogue.py` | Pulls all course bundles (names, categories, funnel position) | Daily or on demand |
| `fetch_course_batches.py` | Pulls all batches under every bundle, including tutor and student counts | Daily or on demand |
| `run_course_pipeline.py` | Orchestrates catalogue → batches in sequence and prints a summary | Daily (runs both scripts) |

---

## Data Flow

```
Edmingle API
  │
  ├── GET /report/csv?report_type=55   → fetch_attendance.py
  │         → bronze.attendance_raw
  │         → silver.class_attendance
  │
  ├── GET /institute/{id}/courses/catalogue  → fetch_course_catalogue.py
  │         → bronze.course_catalogue_raw
  │         → silver.course_metadata
  │
  └── GET /short/masterbatch           → fetch_course_batches.py
            → bronze.course_batches_raw
            → silver.course_batches
            → silver.course_master   (rebuilt on every run)
```

---

## Common CONFIG block

Every script has a `CONFIG` block near the top of the file. Update these before first use:

```python
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "your_password_here"
DB_PORT     = 5432
API_KEY     = "your_api_key_here"
ORG_ID      = 683
INST_ID     = 483
```

The API key expires every 30 days (persistent login) or 5 hours (non-persistent). Regenerate it via `POST /tutor/login` with `persistent_login: true`.

---

## Rate Limiting

The Edmingle API enforces a limit of approximately 25 calls per minute. `fetch_attendance.py` handles this automatically: it sleeps for 60 seconds after every 25 date-based calls. The other scripts fetch everything in a single paginated call and do not hit rate limits.

---

## How to run

```bash
# Run yesterday's attendance (default)
python api_scripts/fetch_attendance.py

# Backfill a date range
python api_scripts/fetch_attendance.py --start 2024-01-01 --end 2026-04-30

# Run for one specific date
python api_scripts/fetch_attendance.py --date 2026-03-16

# Preview what the API returns without writing to DB
python api_scripts/fetch_attendance.py --date 2026-03-16 --dry-run

# Run the full course pipeline (catalogue + batches)
python api_scripts/run_course_pipeline.py
```

---

## File-level documentation

- [fetch_attendance.md](fetch_attendance.md) — attendance script internals
- [fetch_course_catalogue.md](fetch_course_catalogue.md) — catalogue script internals
- [fetch_course_batches.md](fetch_course_batches.md) — batches script internals
- [run_course_pipeline.md](run_course_pipeline.md) — pipeline orchestrator internals
