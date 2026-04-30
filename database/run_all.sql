-- Run all schema files in order against the edmingle_analytics database.
-- Usage (psql): psql -U postgres -d edmingle_analytics -f database/run_all.sql
-- Usage (pgAdmin): open this file in Query Tool and execute.
-- Each \i path is relative to the project root — run from there.

\i database/schemas/01_create_schemas.sql

\i database/bronze/01_webhook_events.sql
\i database/bronze/02_failed_events.sql
\i database/bronze/03_studentexport_raw.sql
\i database/bronze/04_student_courses_enrolled_raw.sql
\i database/bronze/05_unresolved_students_raw.sql

\i database/silver/01_users.sql
\i database/silver/02_transactions.sql
\i database/silver/03_sessions.sql
\i database/silver/04_assessments.sql
\i database/silver/05_courses.sql
\i database/silver/06_announcements.sql
\i database/silver/07_certificates.sql

\i database/bronze/06_course_catalogue_raw.sql
\i database/bronze/07_course_lifecycle_raw.sql
\i database/bronze/08_course_batches_raw.sql

\i database/silver/08_course_metadata.sql
\i database/silver/09_course_lifecycle.sql
\i database/silver/10_course_batches.sql

\i database/gold/01_gold_views.sql
