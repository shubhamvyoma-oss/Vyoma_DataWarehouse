# CSV files/

Historical CSV exports from the Edmingle admin console and the course operations MIS tracker. These files were used for the one-time historical data backfill. They are **gitignored** because they contain student PII (names, emails, phone numbers).

---

## Files

| File | Rows | Loaded into | Description |
|---|---|---|---|
| `studentexport.csv` | ~116,000 | `bronze.studentexport_raw` | Complete student profile export from Edmingle admin. All registered users. |
| `studentCoursesEnrolled.csv` | ~478,000 | `bronze.student_courses_enrolled_raw` | All course enrollment records. One row per student per batch enrollment. Includes historical attendance columns. |
| `course_catalogue_data.csv` | ~541 | `bronze.course_catalogue_raw` | All course bundles with Vyoma classification fields. Superseded by the daily `fetch_course_catalogue.py` API pull. |
| `batches_data.csv` | ~995 | `bronze.course_batches_raw` | All batch records. Superseded by the daily `fetch_course_batches.py` API pull. |
| `Elearning MIS Merged Tracker - Course Lifecycle (1).csv` | ~1,018 | `bronze.course_lifecycle_raw` | Course operations tracking sheet. Contains first/last class dates, attendance rates, assessment and certification metrics, ratings. |

---

## How to Use

The CSV files must be present in this folder when running the migration scripts. See `scripts/migrations/README.md` for the exact load order and scripts.

```bash
python scripts/migrations/csv_load_bronze.py
python scripts/migrations/csv_backfill_transactions.py
python scripts/migrations/csv_load_course_bronze.py
python scripts/migrations/csv_transform_course_silver.py
```

---

## Why Are These Gitignored?

The `.gitignore` excludes `*.csv` from this folder to prevent student personal data (names, emails, phone numbers, addresses) from being committed to the git repository. The schema definitions in `database/bronze/manual/` describe the column structure of these files without exposing any data.
