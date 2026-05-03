# database/silver/

Silver is the cleaned, typed, and deduplicated data layer. Every row in a Silver table represents one real-world entity — one student, one enrollment, one class session, one batch. Silver tables are built from Bronze using UPSERT logic: processing the same event twice produces one row, not two.

---

## Subfolders

| Folder | Source | Tables |
|---|---|---|
| `webhook/` | Promoted from `bronze.webhook_events` by the webhook receiver | `users`, `transactions`, `sessions`, `assessments`, `courses`, `announcements`, `certificates` |
| `api/` | Promoted from API Bronze tables by `api_scripts/` | `course_metadata`, `course_batches`, `course_master`, `class_attendance` |
| `manual/` | Promoted from manual/CSV Bronze tables by migration scripts | `course_lifecycle` |

---

## webhook/

Tables built from real-time webhook events. The webhook receiver writes to these tables immediately after inserting into Bronze.

- **`users.sql`** — One row per student. Upsert key: `user_id`. COALESCE merges partial updates so later nulls never overwrite earlier values.
- **`transactions.sql`** — One row per student-course-batch enrollment. Upsert key: `(user_id, bundle_id, master_batch_id)`. Merges CSV backfill and live webhook data.
- **`sessions.sql`** — One row per live class session instance. Upsert key: `attendance_id`. Multiple events (created, started, cancelled) for the same session merge into one row.
- **`assessments.sql`** — One row per assessment event. Upsert key: `event_id`.
- **`courses.sql`** — One row per course completion. Upsert key: `event_id`.
- **`announcements.sql`** — One row per announcement. Raw JSONB payload stored.
- **`certificates.sql`** — One row per certificate issued. Upsert key: `event_id`.

---

## api/

Tables built from scheduled API pulls.

- **`course_metadata.sql`** — One row per course bundle. All Vyoma classification fields (subject, SSS category, funnel position, etc.) as typed columns. Upsert key: `bundle_id`.
- **`course_batches.sql`** — One row per batch. Typed start/end dates as IST TIMESTAMPTZ. Upsert key: `batch_id`.
- **`course_master.sql`** — Denormalised table joining course_metadata + course_batches + course_lifecycle into one flat table for Power BI. Fully rebuilt on every pipeline run. Contains computed flags: `is_latest_batch`, `include_in_course_count`, `has_batch`.
- **`class_attendance.sql`** — One row per batch per class date. Aggregated from `bronze.attendance_raw`. Columns: `present_count`, `late_count`, `absent_count`, `total_enrolled`, `attendance_pct`, `class_number`. Upsert key: `(batch_id, class_date)`.

---

## manual/

Tables built from CSV migration scripts.

- **`course_lifecycle.sql`** — One row per batch from the Course Lifecycle MIS tracker. Contains milestone dates (launch, first class, last class), attendance averages, assessment and certification counts, and ratings. Upsert key: `course_id` (bundle_id).

---

## What Cleaning Happens in Silver

- Unix timestamps are converted to IST (UTC+5:30) using the `unix_to_ist()` helper function
- Typed columns replace raw JSONB: `BIGINT` for IDs, `NUMERIC` for prices, `BOOLEAN` for flags, `DATE`/`TIMESTAMPTZ` for dates
- Duplicate events for the same entity are merged using COALESCE (existing non-null values are never overwritten by null)
- Staff rows (email contains `@vyoma`) are excluded during Bronze-to-Silver promotion
- `attendance_pct` formula: `(present + late) / (present + late + absent) * 100` — Late students count as attended
