# 03 — Fetch Course Batches

## What it does

Downloads all course batch records from the Edmingle API and saves them to the database. Then rebuilds the master course-batch table used by Power BI.

1. **Bronze** (`bronze.course_batches_raw`) — exact copy from API
2. **Silver** (`silver.course_batches`) — cleaned and typed
3. **Silver master** (`silver.course_master`) — rebuilt from scratch each run

## Why we need it

`silver.course_master` is the main table Power BI reads for the "courses and batches" report. It combines course metadata with batch details into one wide table. This script always rebuilds it so Power BI always sees fresh data.

## How it works — step by step

```
Edmingle API  ──────────────────────────────────────────────────────
  │
  │  GET /institute/483/bundles (paginated, 100 per page)
  │     Each bundle has a list of batches inside it
  │
  ▼
bronze.course_batches_raw   (one row per batch)
  │
  │  Clean: convert timestamps, integers, text
  │
  ▼
silver.course_batches   (one row per batch, typed correctly)
  │
  │  TRUNCATE + INSERT  (full rebuild every run)
  │  JOIN with silver.course_metadata for course name/subject
  │  ROW_NUMBER() to flag the latest batch per bundle
  │
  ▼
silver.course_master   (one row per batch, with course context)
```

### Steps in code

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `fetch_batches()` | Call the API, paginate until all pages are fetched |
| 2 | `flatten_bundles_to_batches()` | Explode bundle→batch nesting into flat rows |
| 3 | `upsert_bronze()` | Write each raw batch to `bronze.course_batches_raw` |
| 4 | `transform_to_silver()` | Clean and type the Bronze rows, write to Silver |
| 5 | `rebuild_master()` | TRUNCATE + INSERT `silver.course_master` |

## How to run

```bash
python 03_fetch_course_batches/fetch_course_batches.py
```

Or run the full pipeline (catalogue + batches) with script 04:

```bash
python 04_run_course_pipeline/run_course_pipeline.py
```

## What to check after

- `bronze.course_batches_raw` should have rows
- `silver.course_batches` should have rows
- `silver.course_master` should have rows
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Schedule

Run daily at **7:00 AM IST** (part of the course pipeline in script 04).

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL is not running | Start PostgreSQL |
| `API error: HTTP 401` | API key is wrong | Check `API_KEY` in the script |
| `silver.course_master has 0 rows` | Bronze or Silver is empty | Run script 02 first |
