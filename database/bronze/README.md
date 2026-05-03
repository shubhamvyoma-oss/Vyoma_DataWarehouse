# database/bronze/

Bronze is the raw data layer. Every record is stored here exactly as it arrived — no transformation, no cleaning, no deletion. Bronze is the permanent audit log: if a Silver transformation has a bug, you can always re-run it from Bronze without needing to re-fetch from Edmingle.

---

## Subfolders

| Folder | Source | Tables |
|---|---|---|
| `webhook/` | Edmingle webhook events (real-time push) | `webhook_events`, `failed_events` |
| `api/` | Edmingle REST API (scheduled pulls) | `course_catalogue_raw`, `course_batches_raw`, `attendance_raw` |
| `manual/` | CSV exports and manual data loads (one-time) | `studentexport_raw`, `student_courses_enrolled_raw`, `unresolved_students_raw`, `course_lifecycle_raw` |

---

## webhook/

Tables populated by the live webhook receiver (`ingestion/webhook_receiver.py`).

- **`webhook_events.sql`** — Every Edmingle webhook event stored as raw JSONB. The `routed_to_silver` flag is set to `true` once the event has been successfully written to Silver. Unique key: `event_id`.
- **`failed_events.sql`** — Requests that arrived but could not be processed (malformed JSON, missing fields, DB temporarily down). Safety net — nothing is silently dropped.

---

## api/

Tables populated by the API pull scripts in `api_scripts/`.

- **`course_catalogue_raw.sql`** — All course bundles from the catalogue API. 59 columns, all TEXT, upserted daily. Includes Vyoma custom fields (subject, SSS category, funnel position, adhyayanam category).
- **`course_batches_raw.sql`** — All batch records from the masterbatch API. Populated via `fetch_course_batches.py`. Unique key: `source_row`.
- **`attendance_raw.sql`** — One row per student per class session from `report_type=55`. Unique key: `(student_id, class_id)`. Re-pulling the same date updates attendance status in-place (handles `-` → `P`/`A` transitions). Staff rows (email contains `@vyoma`) are excluded.

---

## manual/

Tables loaded once from CSV exports. These were the historical backfill from Edmingle's data before the webhook pipeline was live.

- **`studentexport_raw.sql`** — Verbatim copy of `studentexport.csv`. All 50+ columns stored as TEXT. 116,000+ rows.
- **`student_courses_enrolled_raw.sql`** — Verbatim copy of `studentCoursesEnrolled.csv`. 478,000+ enrollment records.
- **`unresolved_students_raw.sql`** — Students from the CSV whose email could not be matched to an Edmingle `user_id`. Preserved for future matching. ~22,834 rows.
- **`course_lifecycle_raw.sql`** — Raw copy of the Course Lifecycle MIS tracker CSV. 107 columns of course operation metrics (class dates, attendance rates, certification counts, ratings). One row per batch.

---

## Design Principles

- All Bronze tables use `IF NOT EXISTS` so DDL is safe to re-run.
- All Bronze tables have `loaded_at TIMESTAMPTZ DEFAULT NOW()` for insertion tracking.
- Raw payloads are stored as JSONB (`raw_payload`) or full TEXT rows so no data is ever discarded.
- Bronze data is never updated or deleted (except the `routed_to_silver` flag on `webhook_events`).
