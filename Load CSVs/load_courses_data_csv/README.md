# 09 — Load Courses CSV

## What it does

Reads three course-related CSV files from the `CSV files` folder and loads each one into its Bronze table. This is a **one-time migration** script.

| CSV file | Target Bronze table |
|----------|---------------------|
| `course_catalogue_data.csv` | `bronze.course_catalogue_raw` |
| `Elearning MIS Merged Tracker - Course Lifecycle (1).csv` | `bronze.course_lifecycle_raw` |
| `batches_data.csv` | `bronze.course_batches_raw` |

## Why we need it

These CSV exports contain course catalogue, lifecycle statistics (avg attendance, pass rates), and batch details from before the live API pipeline existed. Loading them gives script 10 (`transform_courses_silver`) the raw data it needs to populate Silver course tables.

## How it works — step by step

```
CSV files/course_catalogue_data.csv
  │
  │  Read with pandas
  │  Sanitize column names (spaces → underscores, lowercase)
  │  Handle duplicate column names (add _1, _2 suffix)
  │  INSERT rows with ON CONFLICT DO NOTHING
  │
  ▼
bronze.course_catalogue_raw

(Same process for the other two files)

CSV files/Elearning MIS Merged Tracker ...csv  →  bronze.course_lifecycle_raw
CSV files/batches_data.csv                      →  bronze.course_batches_raw
```

The `sanitize_column_name()` function converts messy CSV headers like `"Course Name (Official)"` into safe SQL column names like `course_name_official`.

## How to run

```bash
python 09_load_courses_csv/load_courses_csv.py
```

**Run ONCE** before running script 10.

## What to check after

- All three Bronze tables should have rows
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: CSV folder not found` | CSV files folder is missing | Place the `CSV files` folder in the project root |
| `SKIPPED -- file not found` | One CSV file is missing | Check the file name in the script matches exactly |
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `UndefinedColumn` error in PostgreSQL | CSV headers changed | Check that sanitized column names match the Bronze table schema |
