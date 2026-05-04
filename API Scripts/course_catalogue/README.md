# 02 — Fetch Course Catalogue

## What it does

Downloads the list of all courses (called "bundles" in Edmingle) from the API and saves them to the database in two layers:

1. **Bronze** (`bronze.course_catalogue_raw`) — exact copy of what the API returned
2. **Silver** (`silver.course_metadata`) — cleaned, typed, and deduplicated

## Why we need it

Power BI reads `silver.course_metadata` for course names, subjects, and status. This script must run before the batches script (03) because the master table joins on `bundle_id`.

## How it works — step by step

```
Edmingle API  ──────────────────────────────────────────────────────
  │
  │  GET /institute/483/courses/catalogue
  │
  ▼
bronze.course_catalogue_raw   (raw rows — one per course)
  │
  │  Filter: only course_division = 'Course'
  │  Deduplicate: keep row with known status over NULL status
  │  Clean: convert text fields, integers, floats
  │
  ▼
silver.course_metadata   (one row per unique bundle_id)
```

### Steps in code

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `fetch_catalogue()` | Call the API and extract the list of courses |
| 2 | `upsert_bronze()` | Write each raw course to `bronze.course_catalogue_raw` |
| 3 | `transform_to_silver()` | Clean and type the Bronze rows, write to Silver |

## How to run

```bash
python 02_fetch_course_catalogue/fetch_course_catalogue.py
```

Or run both catalogue and batches together with script 04:

```bash
python 04_run_course_pipeline/run_course_pipeline.py
```

## What to check after

- `bronze.course_catalogue_raw` should have rows
- `silver.course_metadata` should have rows
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Schedule

Run **weekly** (courses change slowly). Or run on demand when a new course is added.

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL is not running | Start PostgreSQL |
| `API error: HTTP 401` | API key is wrong | Check `API_KEY` in the script |
| `WARNING: API returned 0 courses` | Empty response | Check the API endpoint URL |
| `All retries exhausted` | Network unreachable | Check internet connection |
