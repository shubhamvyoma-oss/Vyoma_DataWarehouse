# 05 — Build Courses

## What it does

Reads data from several Silver tables and creates two wide reporting tables used by Power BI:

1. **`silver.MasterCourseBatch`** — one row per course batch, with `is_latest_batch` flag
2. **`silver.course_meta_data`** — same as above, but also joined with lifecycle data

## Why we need it

Power BI connects directly to these two tables for the main course reporting dashboard. They combine course metadata, batch details, and lifecycle statistics into one place so Power BI only needs one simple query.

## How it works — step by step

```
silver.course_metadata     (course names, subjects, SSS categories)
silver.course_batches      (batch dates, admitted students)
silver.course_lifecycle    (historical stats: avg attendance, pass %)
         │
         │  Merge 1: JOIN course_metadata + course_batches
         │            ROW_NUMBER() per bundle → is_latest_batch flag
         │
         ▼
silver."MasterCourseBatch"  (one row per batch)
         │
         │  Merge 2: LEFT JOIN with silver.course_lifecycle
         │            Adds historical stats like avg_attendance, pass_percentage
         │
         ▼
silver.course_meta_data     (one row per batch, fully enriched)
```

### Steps in code

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `check_required_tables_exist()` | Verify source tables exist before starting |
| 2 | `run_merge_1()` | DROP + CREATE + INSERT silver."MasterCourseBatch" |
| 3 | `run_merge_2()` | DROP + CREATE + INSERT silver.course_meta_data |

## How to run

```bash
python 05_build_courses/build_courses.py
```

Run this **after** scripts 02, 03 (or 04) have fetched fresh API data.

## What to check after

- `silver."MasterCourseBatch"` should have rows
- `silver.course_meta_data` should have rows
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Required table not found: silver.course_batches` | Scripts 02/03 have not run | Run script 04 first |
| `silver."MasterCourseBatch" has 0 rows` | Join found no matches | Check that bundle_ids match between tables |
