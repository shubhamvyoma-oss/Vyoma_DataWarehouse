# 08 — Load Students CSV

## What it does

Reads two CSV files from the `CSV files` folder and loads them into Bronze tables in the database. This is a **one-time migration** script.

| CSV file | Target Bronze table |
|----------|---------------------|
| `studentexport.csv` | `bronze.studentexport_raw` |
| `studentCoursesEnrolled.csv` | `bronze.student_courses_enrolled_raw` |

## Why we need it

Before the live webhook server existed, all student and enrollment data was exported manually to CSV from Edmingle. Loading these CSVs into Bronze lets script 11 (`backfill_transactions`) build Silver users and transactions from them, giving Power BI a complete historical picture.

## How it works — step by step

```
CSV files/studentexport.csv
  │
  │  Read with pandas (skip first decorative title row)
  │  Rename CSV headers to clean SQL column names
  │  INSERT rows with ON CONFLICT DO NOTHING (safe to re-run)
  │
  ▼
bronze.studentexport_raw

CSV files/studentCoursesEnrolled.csv
  │
  │  Read with pandas
  │  Column names already match SQL table columns
  │  INSERT rows with ON CONFLICT DO NOTHING
  │
  ▼
bronze.student_courses_enrolled_raw
```

## How to run

```bash
python 08_load_students_csv/load_students_csv.py
```

**Run ONCE** before running script 11.

## What to check after

- `bronze.studentexport_raw` should have rows
- `bronze.student_courses_enrolled_raw` should have rows
- `Skipped (duplicates)` should be 0 on first run; will equal total on re-runs
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: CSV folder not found` | CSV files folder is missing | Place the `CSV files` folder in the project root |
| `No such file or directory: studentexport.csv` | CSV file is missing | Check the file name matches exactly |
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `ModuleNotFoundError: No module named 'pandas'` | pandas not installed | Run `pip install pandas` |
