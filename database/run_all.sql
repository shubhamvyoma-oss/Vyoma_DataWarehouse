-- This is the master script to run all database setup files in the correct order.
-- You can run this in psql using the command: psql -U postgres -d edmingle_analytics -f database/run_all.sql
-- Make sure you are in the project root directory when you run this.

-- ── 1. Create Schemas ────────────────────────────────────────────────────────
-- This creates the organizational folders (schemas) like 'bronze', 'silver', and 'gold'.
\i database/schemas/01_create_schemas.sql

-- ── 2. Bronze Layer: Raw Data Tables ──────────────────────────────────────────
-- These scripts create tables that hold raw data exactly as it comes from webhooks or files.

-- ── API Sources (Edmingle API)
\i database/bronze/api/course_catalogue_data_raw.sql
\i database/bronze/api/batches_data_raw.sql
\i database/bronze/api/attendance_raw.sql

-- ── Manual Sources (CSV Exports)
\i database/bronze/manual/studentexport_raw.sql
\i database/bronze/manual/student_courses_enrolled_raw.sql
\i database/bronze/manual/unresolved_students_raw.sql
\i database/bronze/manual/course_lifecycle_raw.sql

-- ── Webhook Sources (Real-time events)
\i database/bronze/webhook/webhook_events.sql
\i database/bronze/webhook/failed_events.sql

-- ── 3. Silver Layer: Structured Data Tables ───────────────────────────────────
-- These scripts create tables that are cleaned up and organized for easier use.

-- ── API Sources (Edmingle API)
\i database/silver/api/batches_data.sql
\i database/silver/api/course_batch_merge.sql
\i database/silver/api/course_catalogue.sql
\i database/silver/api/attendance_data.sql
\i database/silver/api/course_completion.sql

-- ── Manual Sources (CSV Exports)
\i database/silver/manual/course_lifecycle.sql

-- ── Webhook Sources (Real-time events)
\i database/silver/webhook/users.sql
\i database/silver/webhook/transactions.sql
\i database/silver/webhook/sessions.sql
\i database/silver/webhook/assessments.sql
\i database/silver/webhook/announcements.sql
\i database/silver/webhook/certificates.sql

-- ── 4. Gold Layer: Reporting Views ───────────────────────────────────────────
-- These scripts create the final views used for Power BI reports and dashboards.

-- ── API Sources (Edmingle API)
\i database/gold/api/course_views.sql
\i database/gold/api/attendance_views.sql
