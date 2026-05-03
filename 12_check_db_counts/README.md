# 12 — Check DB Counts

## What it does

Connects to the database and prints the row count for every Bronze and Silver table. Use this to quickly verify that scripts ran successfully.

## Why we need it

After running any pipeline script, you need to confirm data actually landed in the database. This script gives you a one-line-per-table summary without having to write SQL.

## How to run

```bash
python 12_check_db_counts/check_db_counts.py
```

## Example output

```
=== check_db_counts.py ===

BRONZE TABLES
  bronze.webhook_events              :  12,847 rows
  bronze.failed_events               :       0 rows
  bronze.course_catalogue_raw        :     234 rows
  bronze.course_batches_raw          :   1,102 rows
  bronze.course_lifecycle_raw        :     456 rows
  bronze.attendance_raw              :  98,231 rows
  bronze.studentexport_raw           :   8,432 rows
  bronze.student_courses_enrolled_raw:  24,100 rows
  bronze.unresolved_students_raw     :     213 rows

SILVER TABLES
  silver.users                       :   7,891 rows
  silver.transactions                :  18,654 rows
  silver.sessions                    :   6,102 rows
  silver.assessments                 :   1,234 rows
  silver.certificates                :     987 rows
  silver.courses                     :   2,451 rows
  silver.announcements               :      34 rows
  silver.course_metadata             :     234 rows
  silver.course_batches              :     876 rows
  silver.course_lifecycle            :     456 rows
  silver.class_attendance            :  45,678 rows
```

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| Table shows `0 rows` | Pipeline script hasn't run yet | Run the relevant script |
