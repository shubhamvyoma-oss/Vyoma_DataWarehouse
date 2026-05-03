# database/gold/api/

Gold views built over Silver tables populated by the attendance API pipeline.

---

## attendance_views.sql

Creates 4 views for attendance analytics. Joins `silver.class_attendance`, `silver.course_metadata`, and `silver.course_batches`.

### gold.batch_attendance_summary

One row per batch. Columns:

| Column | Description |
|---|---|
| `batch_id`, `bundle_id` | Identifiers |
| `course_name` | From `silver.course_metadata` |
| `batch_name`, `tutor_name` | From `silver.course_batches` |
| `batch_start`, `batch_end` | Batch date range |
| `enrolled` | Total students enrolled |
| `total_classes_held` | Count of distinct class dates in `silver.class_attendance` |
| `avg_attendance_pct` | Average attendance percentage across all classes |
| `first_class_date`, `last_class_date` | Date range of classes |
| `total_class_count` | Maximum `class_number` (sequential count of classes) |
| `first_class_present` | Present count on class #1 |
| `last_class_present` | Present count on the final class |

### gold.bundle_attendance_summary

Aggregates `gold.batch_attendance_summary` up to the bundle (course) level. One row per bundle.

Columns: `bundle_id`, `course_name`, `total_batches`, `total_classes_all_batches`, `overall_avg_attendance`, `total_enrolled_all_batches`, `earliest_class`, `latest_class`.

### gold.attendance_by_year

Groups `silver.class_attendance` by calendar year. Shows how many batches were active, how many classes were held, the average attendance percentage, and the total number of students who attended in each year.

### gold.first_vs_last_class

Drop-off analysis. One row per batch showing:
- `first_class_count` — how many students attended the first class
- `last_class_count` — how many attended the final class
- `enrolled` — total enrolled students
- `drop_off_pct` — `(first_class - last_class) / enrolled * 100`

Identifies which batches had the largest student drop-off from start to finish.
