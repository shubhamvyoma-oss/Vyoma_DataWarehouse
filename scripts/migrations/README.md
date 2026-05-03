# scripts/migrations/

One-time CSV migration scripts for loading historical data into the database. Run these once on a fresh database after creating the schema with `database/run_all.sql`. Do not run them again on a populated database — they use `ON CONFLICT DO NOTHING` to skip duplicates, but re-running is unnecessary.

---

## Files

| File | What it does |
|---|---|
| `csv_load_bronze.py` | Loads student and enrollment CSVs into Bronze tables |
| `csv_backfill_transactions.py` | Promotes Bronze student/enrollment data into Silver tables |
| `csv_load_course_bronze.py` | Loads course catalogue and batches CSVs into Bronze |
| `csv_transform_course_silver.py` | Promotes course lifecycle Bronze data into `silver.course_lifecycle` |

---

## csv_load_bronze.py

Reads two CSV files from the `CSV files/` folder:
- `studentexport.csv` → `bronze.studentexport_raw`
- `studentCoursesEnrolled.csv` → `bronze.student_courses_enrolled_raw`

Also identifies students whose email cannot be matched to an Edmingle `user_id` and writes them to `bronze.unresolved_students_raw`.

Inserts are done with `ON CONFLICT (source_row) DO NOTHING` so re-running is safe.

```bash
python scripts/migrations/csv_load_bronze.py
```

---

## csv_backfill_transactions.py

Reads from `bronze.studentexport_raw` and `bronze.student_courses_enrolled_raw` and upserts into:
- `silver.users` — one row per student, typed columns
- `silver.transactions` — one row per enrollment, source marked as `csv.import`

Handles duplicate user IDs across the CSV using the same COALESCE merge logic as the webhook receiver.

```bash
python scripts/migrations/csv_backfill_transactions.py
```

---

## csv_load_course_bronze.py

Reads two course-related CSV files:
- `course_catalogue_data.csv` → `bronze.course_catalogue_raw`
- `batches_data.csv` → `bronze.course_batches_raw`

Note: these same Bronze tables are also refreshed daily by `api_scripts/fetch_course_catalogue.py` and `fetch_course_batches.py`. The CSV load was the initial population; the API scripts keep them current.

```bash
python scripts/migrations/csv_load_course_bronze.py
```

---

## csv_transform_course_silver.py

Reads from `bronze.course_lifecycle_raw` and upserts into `silver.course_lifecycle`. Converts date strings, numeric strings, and percentage strings to typed PostgreSQL values.

```bash
python scripts/migrations/csv_transform_course_silver.py
```

---

## Required CSV Files

The migration scripts expect these files in the `CSV files/` folder:

| CSV File | Loaded by |
|---|---|
| `studentexport.csv` | `csv_load_bronze.py` |
| `studentCoursesEnrolled.csv` | `csv_load_bronze.py` |
| `course_catalogue_data.csv` | `csv_load_course_bronze.py` |
| `batches_data.csv` | `csv_load_course_bronze.py` |
| `Elearning MIS Merged Tracker - Course Lifecycle (1).csv` | `csv_load_course_bronze.py` |

CSV files are gitignored because they contain student PII. They must be present on the machine where the migration is run but are never committed to the repository.

---

## Order to Run on a Fresh Database

1. `psql -U postgres -d edmingle_analytics -f database/run_all.sql`
2. `python scripts/migrations/csv_load_bronze.py`
3. `python scripts/migrations/csv_backfill_transactions.py`
4. `python scripts/migrations/csv_load_course_bronze.py`
5. `python scripts/migrations/csv_transform_course_silver.py`
6. `python api_scripts/run_course_pipeline.py`
7. `python api_scripts/fetch_attendance.py --start 2024-01-01 --end <today>`
