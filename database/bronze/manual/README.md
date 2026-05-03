# database/bronze/manual/

Tables loaded once from CSV exports taken from Edmingle's admin console and the course operations MIS tracker. These were the historical backfill performed before the webhook and API pipelines were operational. Do not re-run the migration scripts unless setting up a fresh database.

---

## studentexport_raw.sql

**Table**: `bronze.studentexport_raw`

Verbatim copy of `studentexport.csv`. Every column from the CSV is stored as TEXT to avoid data loss during import. 116,000+ rows.

**Unique key**: `source_row` (0-based row index — prevents duplicate imports)

Contains all student profile fields: name, email, registration number, phone, date of birth, parent details, address, city, state, Vyoma custom fields (why they study Sanskrit, persona, objective, teaching experience, etc.), social media URLs.

Loaded by `scripts/migrations/csv_load_bronze.py`. Promoted to `silver.users` by `scripts/migrations/csv_backfill_transactions.py`.

---

## student_courses_enrolled_raw.sql

**Table**: `bronze.student_courses_enrolled_raw`

Verbatim copy of `studentCoursesEnrolled.csv`. 478,000+ enrollment records covering all historical course enrollments before the webhook pipeline was live.

**Unique key**: `source_row`

Key columns: `user_id`, `name`, `email`, `class_id` (batch identifier), `master_batch_id`, `bundle_id`, `batch_status`, `cu_status`, `cu_state`, `start_date`, `end_date`, historical attendance columns (`present`, `absent`, `late`, `excused`, `total_classes`).

Loaded by `scripts/migrations/csv_load_bronze.py`. Promoted to `silver.transactions` by `scripts/migrations/csv_backfill_transactions.py`.

---

## unresolved_students_raw.sql

**Table**: `bronze.unresolved_students_raw`

Students from `studentexport.csv` whose email address could not be matched to any Edmingle `user_id` at the time of the backfill. These ~22,834 students are real people with historical enrollment records, preserved here for potential future matching (e.g., if Edmingle provides a user export with IDs joinable by email).

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Auto-increment row ID |
| `source_row` | INTEGER | Row index from the original CSV |
| `email` | TEXT | The email address that could not be resolved |
| `raw_row` | JSONB | The complete original CSV row as JSON |
| `inserted_at` | TIMESTAMPTZ | When this row was inserted |

---

## course_lifecycle_raw.sql

**Table**: `bronze.course_lifecycle_raw`

Raw copy of the "Elearning MIS Merged Tracker — Course Lifecycle" spreadsheet. Contains 107 columns tracking every operational milestone of each course batch: launch date, first/last class dates, attendance rates, assessment dates, certification counts, ratings, and YouTube/content production status.

**Unique key**: `source_row`

This table is the source for `silver.course_lifecycle` which powers the `gold.course_summary` view (fields like `first_class_date`, `last_class_date`, `avg_attendance`, `total_certified`, `overall_rating`).

Loaded by `scripts/migrations/csv_load_course_bronze.py`. Promoted to `silver.course_lifecycle` by `scripts/migrations/csv_transform_course_silver.py`.
