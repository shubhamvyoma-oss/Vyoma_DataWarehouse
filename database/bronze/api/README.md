# database/bronze/api/

Tables populated by the API pull scripts in `api_scripts/`. These are refreshed on a schedule (daily or on demand) by running the Python scripts.

---

## course_catalogue_raw.sql

**Table**: `bronze.course_catalogue_raw`

Raw copy of the course catalogue from `GET /institute/{INST_ID}/courses/catalogue`. Contains all 59+ columns from the API response stored as TEXT. Upserted daily by `fetch_course_catalogue.py`.

**Unique key**: `source_row` (0-based index in the API response list)

Key columns:
- `bundle_id` — Edmingle's unique bundle identifier
- `course_name` — human-readable name
- `subject`, `sss_category`, `adhyayanam_category` — Vyoma academic classification
- `position_in_funnel` — Bottom / Lower Middle / Middle / Upper Middle / Top
- `term_of_course` — Very Short / Short / Mid / Long
- `viniyoga`, `division` — additional Vyoma-specific fields
- `status` — published / unpublished / archived

Promoted to `silver.course_metadata` by `fetch_course_catalogue.py`.

---

## course_batches_raw.sql

**Table**: `bronze.course_batches_raw`

Raw batch records from `GET /short/masterbatch`. The API returns bundles containing nested batch arrays; the script flattens this into one row per batch and promotes `bundle_id`/`bundle_name` onto each batch record.

**Unique key**: `source_row` (0-based index in the flattened batch list)

Key columns:
- `batch_id` (stored as `class_id` in the API — Edmingle's naming)
- `batch_name`, `bundle_id`, `bundle_name`
- `batch_status` — active / archived
- `start_date`, `end_date` — as returned by the API
- `tutor_id`, `tutor_name`
- `admitted_students` — total students enrolled in this batch

Promoted to `silver.course_batches` and then to `silver.course_master`.

---

## attendance_raw.sql

**Table**: `bronze.attendance_raw`

One row per student per class session from `GET /report/csv?report_type=55`. This is the most granular attendance dataset — every student's attendance status for every class they are enrolled in.

**Unique key**: `(student_id, class_id)` — one record per student per class. Re-pulling the same date updates the existing row via `ON CONFLICT DO UPDATE`, allowing unmarked sessions (`-`) to be updated to `P`/`L`/`A` after teachers mark attendance.

Key columns:
- `student_id`, `student_name`, `student_email`, `reg_no`
- `batch_id`, `batch_name`, `class_id`, `class_name`
- `bundle_id`, `bundle_name`, `course_id`, `course_name`
- `attendance_status` — P (Present) / L (Late) / A (Absent) / - (Unmarked) / E (Excused) / OL (On Leave) / NA (Not Applicable)
- `class_date` — raw string from API ("16 Mar 2026")
- `class_date_parsed` — parsed DATE for joins and aggregation
- `start_time`, `end_time`, `class_duration`
- `teacher_id`, `teacher_name`, `teacher_email`, `teacher_class_signin_status`
- `student_rating`, `student_comments`
- `raw_payload` — full JSON object from the API

Staff rows (email contains `@vyoma`) are filtered out before Bronze insert.

Promoted to `silver.class_attendance` by `fetch_attendance.py`.
