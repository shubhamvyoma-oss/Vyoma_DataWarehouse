# 11 — Backfill Transactions

## What it does

Reads the Bronze student CSV tables (loaded by script 08) and creates Silver records from them. This is a **one-time migration** script.

- **Step 1**: `bronze.student_courses_enrolled_raw` → `silver.transactions`
- **Step 2**: `bronze.studentexport_raw` → `silver.users`
  - Students whose email doesn't match any enrollment record go to `bronze.unresolved_students_raw`

## Why we need it

Before the live webhook server existed, all enrollment and student profile data was in CSV exports. This script brings that historical data into Silver so Power BI can see all students and enrollments, not just those who registered after the webhook server was set up.

## How it works — step by step

```
bronze.student_courses_enrolled_raw
  │
  │  For each row with a valid user_id:
  │    - Generate event_id = "csv-enrollment-{source_row}"
  │    - Convert bundle_id, master_batch_id to integers
  │    - Convert Unix timestamps for start/end dates
  │    - UPSERT into silver.transactions
  │
  ▼
silver.transactions  (source = 'csv')

bronze.studentexport_raw
  │
  │  Build email → user_id lookup from enrollment table
  │  For each student row:
  │    - Look up their user_id by email
  │    - If found: UPSERT into silver.users
  │    - If not found: INSERT into bronze.unresolved_students_raw
  │
  ▼
silver.users              (event_type = 'csv.import')
bronze.unresolved_students_raw  (students with no matching user_id)
```

## How to run

```bash
python 11_backfill_transactions/backfill_transactions.py
```

**Run after** script 08 has loaded the student CSV Bronze tables.

## What to check after

- `silver.transactions` should have rows with `source = 'csv'`
- `silver.users` should have rows with `event_type = 'csv.import'`
- `bronze.unresolved_students_raw` shows students we couldn't match to a user_id
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `Errors (bad user_id): N` | Some rows have non-integer user_ids | Check the CSV data quality |
| `bronze.unresolved_students_raw: N rows` | Students whose email is not in the enrollment CSV | Normal — these students registered but never enrolled in a course |
