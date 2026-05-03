# 10 — Transform Courses Silver

## What it does

Reads the three Bronze course tables (loaded by script 09) and transforms them into their Silver counterparts.

| Bronze table | Silver table |
|-------------|-------------|
| `bronze.course_catalogue_raw` | `silver.course_metadata` |
| `bronze.course_lifecycle_raw` | `silver.course_lifecycle` |
| `bronze.course_batches_raw` | `silver.course_batches` |

## Why we need it

Bronze tables hold raw CSV text. Silver tables have proper data types (integers, dates, floats) that Power BI can filter and sort correctly. This script performs the type conversion and data cleaning step.

## How it works — step by step

```
bronze.course_catalogue_raw  (all text)
  │
  │  bundle_id    → BIGINT
  │  num_students → INT
  │  cost         → FLOAT
  │  other fields → TEXT (cleaned, NULL for blanks)
  │
  ▼
silver.course_metadata

bronze.course_lifecycle_raw  (all text)
  │
  │  course_id      → BIGINT
  │  first_class_date → DATE  (parsed from multiple formats)
  │  avg_attendance → FLOAT
  │  pass_percentage → FLOAT
  │
  ▼
silver.course_lifecycle

bronze.course_batches_raw  (all text)
  │
  │  bundle_id  → BIGINT
  │  batch_id   → BIGINT
  │  start_date → TIMESTAMP (from Unix seconds)
  │  end_date   → TIMESTAMP (from Unix seconds)
  │
  ▼
silver.course_batches
```

## How to run

```bash
python 10_transform_courses_silver/transform_courses_silver.py
```

**Run after** script 09 has loaded the Bronze tables.

## What to check after

- All three Silver tables should have rows
- Row counts in Silver should be close to (but may be less than) Bronze counts
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `silver.course_metadata: 0 rows` | Bronze is empty | Run script 09 first |
| `Rows processed: 0` | Bronze table is empty | Run script 09 first to load the CSVs |
