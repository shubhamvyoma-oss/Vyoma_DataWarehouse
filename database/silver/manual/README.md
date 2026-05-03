# database/silver/manual/

Silver tables built from the CSV / manually imported Bronze data. These were populated once during the historical backfill and are not refreshed by any ongoing pipeline.

---

## course_lifecycle.sql

**Table**: `silver.course_lifecycle`

One row per batch from the Course Lifecycle MIS tracker. Upsert key: `course_id` (maps to `bundle_id` in other tables).

Built from: `bronze.course_lifecycle_raw` by `scripts/migrations/csv_transform_course_silver.py`.

This table captures all operational metrics for a completed or ongoing course batch: when it launched, how many students attended the first and last classes, assessment results, certification rates, and quality ratings.

Key columns:

| Column | Type | Description |
|---|---|---|
| `course_id` | BIGINT | Maps to `bundle_id` in silver.course_metadata |
| `course_name` | TEXT | Course name |
| `type_of_launch` | TEXT | Repeat / Reopen / Relaunch |
| `first_class_date` | DATE | Date of the first class |
| `last_class_date` | DATE | Date of the last class |
| `enrollments_on_fc` | INTEGER | Enrollment count on the day of the first class |
| `enrollments_on_lc` | INTEGER | Enrollment count on the last day |
| `avg_attendance` | NUMERIC | Average attendance across all classes |
| `total_certified` | INTEGER | Number of students who received certificates |
| `overall_rating` | NUMERIC | Average student rating for the course |

Used by: `silver.course_master`, `gold.course_summary`, `gold.first_vs_last_class`.
