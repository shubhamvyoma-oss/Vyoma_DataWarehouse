# database/silver/api/

Silver tables built from API-pulled Bronze data. These are populated by the scripts in `api_scripts/` and refreshed daily.

---

## course_metadata.sql

**Table**: `silver.course_metadata`

One row per course bundle. Upsert key: `bundle_id`.

Built from: `bronze.course_catalogue_raw` by `fetch_course_catalogue.py`.

Holds all Vyoma-specific course classification fields in typed form:

| Column | Type | Description |
|---|---|---|
| `bundle_id` | BIGINT | Edmingle bundle identifier |
| `course_name` | TEXT | Human-readable name |
| `course_type` | TEXT | Live / Recorded / Hybrid |
| `status` | TEXT | published / unpublished / archived |
| `subject` | TEXT | Academic subject area |
| `term_of_course` | TEXT | Very Short / Short / Mid / Long |
| `position_in_funnel` | TEXT | Bottom / Lower Middle / Middle / Upper Middle / Top |
| `adhyayanam_category` | TEXT | Bhashadhyayanam / Granthadhyayanam / Shastradhyayanam |
| `sss_category` | TEXT | Samskrta / Samskara / Samskriti |
| `viniyoga` | TEXT | True / False — whether this is a Viniyoga course |
| `division` | TEXT | Organisational division |

---

## course_batches.sql

**Table**: `silver.course_batches`

One row per batch. Upsert key: `batch_id`.

Built from: `bronze.course_batches_raw` by `fetch_course_batches.py`.

| Column | Type | Description |
|---|---|---|
| `batch_id` | BIGINT | Edmingle batch identifier |
| `bundle_id` | BIGINT | Parent bundle |
| `batch_name` | TEXT | Batch name |
| `batch_status` | TEXT | active / archived |
| `start_date_ist` | TIMESTAMPTZ | Batch start date in IST |
| `end_date_ist` | TIMESTAMPTZ | Batch end date in IST |
| `tutor_id` | BIGINT | Primary teacher's Edmingle user ID |
| `tutor_name` | TEXT | Primary teacher name |
| `admitted_students` | INTEGER | Students enrolled in this batch |

---

## course_master.sql

**Table**: `silver.course_master`

Denormalised flat table joining course_metadata + course_batches + course_lifecycle. One row per batch. Fully rebuilt (TRUNCATE + INSERT) on every `run_course_pipeline.py` run. Power BI reads directly from this table.

Built from: join of `silver.course_metadata`, `silver.course_batches`, `silver.course_lifecycle`.

Computed flags:
- `is_latest_batch` — 1 if this is the most recent batch for this bundle (by `end_date`)
- `include_in_course_count` — 1 for active or latest batches
- `has_batch` — 1 if the bundle has at least one batch record

---

## class_attendance.sql

**Table**: `silver.class_attendance`

One row per batch per class date. Upsert key: `(batch_id, class_date)`.

Built from: `bronze.attendance_raw` by `fetch_attendance.py`.

**Attendance formula**: Late students (status `L`) count as present. The `attendance_pct` is `(present + late) / (present + late + absent) * 100`.

| Column | Type | Description |
|---|---|---|
| `batch_id` | BIGINT | Batch this class belongs to |
| `bundle_id` | BIGINT | Parent course bundle |
| `class_date` | DATE | Date of the class |
| `class_number` | INTEGER | Sequential number of this class within the batch (1 = first class ever) |
| `present_count` | INTEGER | Students marked P |
| `late_count` | INTEGER | Students marked L |
| `absent_count` | INTEGER | Students marked A |
| `total_enrolled` | INTEGER | Total students enrolled (from `silver.course_batches`) |
| `attendance_pct` | NUMERIC(5,2) | `(present + late) / (present + late + absent) * 100` |
| `pull_date` | DATE | Which API pull produced this data |

`class_number` is recomputed as a window function (`ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY class_date ASC)`) after every Silver upsert for the affected batches.
