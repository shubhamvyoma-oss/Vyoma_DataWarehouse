-- Edmingle Analytics — Bronze/Silver schema
-- Run in pgAdmin Query Tool against the 'edmingle_analytics' database.

-- Display all timestamps in IST (UTC+5:30). Must run before table creation.
ALTER DATABASE edmingle_analytics SET timezone = 'Asia/Kolkata';


-- ── SCHEMAS ───────────────────────────────────────────────────────────────────

-- 'bronze' holds raw event tables; 'silver' holds structured analytics tables
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;


-- ── HELPER FUNCTION ───────────────────────────────────────────────────────────

-- Converts Unix integer (seconds since epoch) to TIMESTAMPTZ displayed as IST
CREATE OR REPLACE FUNCTION unix_to_ist(unix_ts BIGINT)
RETURNS TIMESTAMPTZ AS $$
    SELECT to_timestamp(unix_ts);
$$ LANGUAGE SQL IMMUTABLE;


-- ── BRONZE LAYER ──────────────────────────────────────────────────────────────

-- Raw webhook events — every Edmingle event stored here first, never deleted
CREATE TABLE IF NOT EXISTS bronze.webhook_events (
    id               SERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    raw_payload      JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    is_live_mode     BOOLEAN DEFAULT true,
    routed_to_silver BOOLEAN DEFAULT false,  -- set true after Silver insert succeeds
    UNIQUE (event_id)
);


-- Safety net for any request that arrived but could not be parsed or stored
CREATE TABLE IF NOT EXISTS bronze.failed_events (
    id             SERIAL PRIMARY KEY,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    failure_reason TEXT,
    raw_body       TEXT,
    content_type   TEXT
);


-- ── SILVER LAYER ──────────────────────────────────────────────────────────────

-- One row per student; UPSERT on user_id; COALESCE preserves existing values on update
CREATE TABLE IF NOT EXISTS silver.users (
    id             SERIAL PRIMARY KEY,
    event_id       TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    user_id        BIGINT NOT NULL,
    email          TEXT,
    full_name      TEXT,
    user_name      TEXT,
    user_role      TEXT,
    contact_number TEXT,
    institution_id INTEGER,
    city           TEXT,
    state          TEXT,
    address        TEXT,
    pincode        TEXT,
    parent_name    TEXT,
    parent_email   TEXT,
    parent_contact TEXT,
    custom_fields  JSONB,
    created_at_ist TIMESTAMPTZ,   -- set once; never overwritten by user_updated
    updated_at_ist TIMESTAMPTZ,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (user_id)
);


-- One row per student-course-batch enrollment; UPSERT on (user_id, bundle_id, master_batch_id)
-- NOTE: This table is dropped and recreated in the 2026-04-29 migration below.
CREATE TABLE IF NOT EXISTS silver.transactions (
    id                    SERIAL PRIMARY KEY,
    event_id              TEXT NOT NULL,
    event_type            TEXT NOT NULL,
    user_id               BIGINT,
    email                 TEXT,
    full_name             TEXT,
    bundle_id             BIGINT,
    course_name           TEXT,
    institution_bundle_id BIGINT,
    master_batch_id       BIGINT,
    master_batch_name     TEXT,
    original_price        NUMERIC(12,2),
    discount              NUMERIC(12,2),
    final_price           NUMERIC(12,2),
    currency              TEXT,
    credits_applied       NUMERIC(12,2),
    payment_method        TEXT,
    transaction_id        TEXT,
    failure_reason        TEXT,
    error_code            TEXT,
    enrollment_status     TEXT,
    start_date_ist        TIMESTAMPTZ,
    end_date_ist          TIMESTAMPTZ,
    event_timestamp_ist   TIMESTAMPTZ,
    received_at           TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);


-- One row per session instance; UPSERT on attendance_id; later events add fields without overwrite
CREATE TABLE IF NOT EXISTS silver.sessions (
    id                   SERIAL PRIMARY KEY,
    event_id             TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    attendance_id        BIGINT NOT NULL,
    class_id             BIGINT,
    class_name           TEXT,
    class_type_formatted TEXT,
    scheduled_start_ist  TIMESTAMPTZ,
    scheduled_end_ist    TIMESTAMPTZ,
    actual_start_ist     TIMESTAMPTZ,
    duration_minutes     INTEGER,
    teacher_id           BIGINT,
    teacher_name         TEXT,
    teacher_email        TEXT,
    master_batches       JSONB,
    schedule_id          BIGINT,
    is_recurring         BOOLEAN,
    virtual_platform     TEXT,
    zoom_meeting_id      TEXT,
    cancellation_reason  TEXT,
    cancelled_by         BIGINT,
    status               INTEGER,
    is_late_signin       BOOLEAN,
    delay_minutes        INTEGER,
    reminder_type        TEXT,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (attendance_id)
);


-- One row per assessment event; UPSERT on event_id
CREATE TABLE IF NOT EXISTS silver.assessments (
    id               SERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    user_id          BIGINT,
    attempt_id       BIGINT,
    exercise_id      BIGINT,   -- NULL for test events
    mark             NUMERIC(8,2),
    is_evaluated     INTEGER,
    faculty_comments TEXT,     -- NULL until evaluated event arrives
    submitted_at_ist TIMESTAMPTZ,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);


-- One row per course completion event; UPSERT on event_id
CREATE TABLE IF NOT EXISTS silver.courses (
    id               SERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    user_id          BIGINT,
    bundle_id        BIGINT,
    completed_at_ist TIMESTAMPTZ,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);


-- Announcement payload structure not yet documented — store raw JSONB; UPSERT on event_id
CREATE TABLE IF NOT EXISTS silver.announcements (
    id          SERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    raw_data    JSONB,
    received_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);


-- One row per certificate issued; UPSERT on event_id
CREATE TABLE IF NOT EXISTS silver.certificates (
    id               SERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    certificate_id   TEXT,
    user_id          BIGINT,
    issued_at_ist    TIMESTAMPTZ,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);


-- ── MIGRATION 2026-04-29: CSV backfill tables + redesigned silver.transactions ─

-- Raw copy of studentexport.csv; source_row = 0-based pandas index; all columns TEXT
CREATE TABLE IF NOT EXISTS bronze.studentexport_raw (
    id                                 SERIAL PRIMARY KEY,
    source_row                         INTEGER NOT NULL,
    row_number                         TEXT,
    name                               TEXT,
    email                              TEXT,
    registration_number                TEXT,
    contact_number_dial_code           TEXT,
    contact_number                     TEXT,
    alternate_contact_number_dial_code TEXT,
    alternate_contact_number           TEXT,
    date_of_birth                      TEXT,
    parent_name                        TEXT,
    parent_contact                     TEXT,
    parent_email                       TEXT,
    address                            TEXT,
    city                               TEXT,
    state                              TEXT,
    standard                           TEXT,
    date_created                       TEXT,
    username                           TEXT,
    gender                             TEXT,
    status                             TEXT,
    username_1                         TEXT,
    why_study_sanskrit                 TEXT,
    user_nice_name                     TEXT,
    user_last_name                     TEXT,
    would_like_to_teach                TEXT,
    teaching_experience                TEXT,
    is_mainstream_education            TEXT,
    objective                          TEXT,
    user_age                           TEXT,
    persona                            TEXT,
    objective_package                  TEXT,
    time_per_week_hours                TEXT,
    age_                               TEXT,
    facebook_profile_url               TEXT,
    instagram_profile_url              TEXT,
    pinterest_profile_url              TEXT,
    soundcloud_profile_url             TEXT,
    tumblr_profile_url                 TEXT,
    youtube_profile_url                TEXT,
    wikipedia_url                      TEXT,
    twitter_username                   TEXT,
    gst_number                         TEXT,
    myspace_profile_url                TEXT,
    international_phone_number         TEXT,
    website                            TEXT,
    educational_qualification          TEXT,
    linkedin_profile_url               TEXT,
    age_v2                             TEXT,
    gender_                            TEXT,
    sanskrit_qualification             TEXT,
    areas_of_interest                  TEXT,
    studying_sanskrit_currently        TEXT,
    current_education_status           TEXT,
    country_name                       TEXT,
    loaded_at                          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_row)
);


-- Raw copy of studentCoursesEnrolled.csv; timestamps stored as TEXT, cast to BIGINT in Silver
CREATE TABLE IF NOT EXISTS bronze.student_courses_enrolled_raw (
    id                    SERIAL PRIMARY KEY,
    source_row            INTEGER NOT NULL,
    user_id               TEXT,
    name                  TEXT,
    email                 TEXT,
    class_id              TEXT,
    class_name            TEXT,
    tutor_name            TEXT,
    total_classes         TEXT,
    present               TEXT,
    absent                TEXT,
    late                  TEXT,
    excused               TEXT,
    start_date            TEXT,
    end_date              TEXT,
    master_batch_id       TEXT,
    master_batch_name     TEXT,
    classusers_start_date TEXT,
    classusers_end_date   TEXT,
    batch_status          TEXT,
    cu_status             TEXT,
    cu_state              TEXT,
    institution_bundle_id TEXT,
    archived_at           TEXT,
    bundle_id             TEXT,
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_row)
);


-- Students from studentexport.csv whose email could not be matched to a user_id
CREATE TABLE IF NOT EXISTS bronze.unresolved_students_raw (
    id          SERIAL PRIMARY KEY,
    source_row  INTEGER NOT NULL,
    email       TEXT,
    raw_row     JSONB,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);


-- Redesigned silver.transactions: UPSERT key changed to (user_id, bundle_id, master_batch_id)
-- so CSV and webhook enrollments for the same student-course merge into one row.
-- WARNING: DROP TABLE destroys existing rows. Run reprocess_bronze.py after this.
DROP TABLE IF EXISTS silver.transactions;

CREATE TABLE silver.transactions (
    id                    SERIAL PRIMARY KEY,
    event_id              TEXT NOT NULL,
    event_type            TEXT NOT NULL,
    event_timestamp_ist   TIMESTAMPTZ,
    user_id               BIGINT NOT NULL,
    email                 TEXT,
    full_name             TEXT,
    contact_number        TEXT,
    bundle_id             BIGINT,
    course_name           TEXT,
    master_batch_id       BIGINT,
    master_batch_name     TEXT,
    institution_bundle_id BIGINT,
    original_price        NUMERIC(12,2),
    discount              NUMERIC(12,2),
    final_price           NUMERIC(12,2),
    currency              TEXT,
    credits_applied       NUMERIC(12,2),
    payment_method        TEXT,
    transaction_id        TEXT,
    start_date_ist        TIMESTAMPTZ,
    end_date_ist          TIMESTAMPTZ,
    created_at_ist        TIMESTAMPTZ,
    source                TEXT DEFAULT 'webhook',
    inserted_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, bundle_id, master_batch_id)
);
