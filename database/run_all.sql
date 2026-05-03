-- Run all schema files in order against the edmingle_analytics database.
-- Usage (psql): psql -U postgres -d edmingle_analytics -f database/run_all.sql
-- Run from the project root directory so all \i paths resolve correctly.

-- ── Schemas ──────────────────────────────────────────────────────────────────
\i database/schemas/01_create_schemas.sql

-- ── Bronze: Webhook events ────────────────────────────────────────────────────
\i database/bronze/webhook/webhook_events.sql
\i database/bronze/webhook/failed_events.sql

-- ── Bronze: Manual / CSV imports ─────────────────────────────────────────────
\i database/bronze/manual/studentexport_raw.sql
\i database/bronze/manual/student_courses_enrolled_raw.sql
\i database/bronze/manual/unresolved_students_raw.sql
\i database/bronze/manual/course_lifecycle_raw.sql

-- ── Silver: Webhook-sourced tables ───────────────────────────────────────────
\i database/silver/webhook/users.sql
\i database/silver/webhook/transactions.sql
\i database/silver/webhook/sessions.sql
\i database/silver/webhook/assessments.sql
\i database/silver/webhook/courses.sql
\i database/silver/webhook/announcements.sql
\i database/silver/webhook/certificates.sql

-- ── Bronze: API-pulled tables ─────────────────────────────────────────────────
\i database/bronze/api/course_catalogue_raw.sql
\i database/bronze/api/course_batches_raw.sql

-- ── Silver: Manual / CSV-sourced tables ───────────────────────────────────────
\i database/silver/manual/course_lifecycle.sql

-- ── Silver: API-sourced tables ────────────────────────────────────────────────
\i database/silver/api/course_metadata.sql
\i database/silver/api/course_batches.sql
\i database/silver/api/course_master.sql

-- ── Bronze: Attendance (API) ──────────────────────────────────────────────────
\i database/bronze/api/attendance_raw.sql

-- ── Silver: Attendance (API) ──────────────────────────────────────────────────
\i database/silver/api/class_attendance.sql

-- ── Gold: Course views (from webhook + API silver tables) ─────────────────────
\i database/gold/webhook/gold_views.sql

-- ── Gold: Attendance views (from API silver tables) ───────────────────────────
\i database/gold/api/attendance_views.sql
