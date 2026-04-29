-- =============================================================================
-- FILE    : database/setup.sql
-- PROJECT : Edmingle Webhook Data Pipeline — Vyoma Samskrta Pathasala
-- PURPOSE : Creates the Bronze and Silver schemas and all tables.
--
-- HOW TO RUN:
--   Open pgAdmin. Connect to the 'edmingle_analytics' database.
--   Open this file in the Query Tool and click Run.
--   No other steps required.
-- =============================================================================


-- Tell this database to display all timestamps in Indian Standard Time (IST).
-- IST is UTC+5:30. PostgreSQL's official timezone name for it is 'Asia/Kolkata'.
-- This setting is permanent — it survives restarts.
-- Every TIMESTAMPTZ value you query from this database will display as IST.
-- This must run before the tables are created so the DEFAULT expressions work correctly.
ALTER DATABASE edmingle_analytics SET timezone = 'Asia/Kolkata';


-- =============================================================================
-- SCHEMAS
-- A schema works like a folder inside the database.
-- We use two schemas to keep raw data and clean data clearly separated.
-- IF NOT EXISTS means running this file a second time will not cause an error.
-- =============================================================================

-- 'bronze' holds the raw event buffer table
CREATE SCHEMA IF NOT EXISTS bronze;

-- 'silver' holds all seven structured tables
CREATE SCHEMA IF NOT EXISTS silver;


-- =============================================================================
-- HELPER FUNCTION: unix_to_ist(unix_ts BIGINT)
--
-- WHY WE NEED THIS:
--   Edmingle sends all timestamps as Unix integers — the number of seconds
--   that have passed since 1970-01-01 00:00:00 UTC. For example, 1709856600.
--   These are unreadable as-is and cannot be filtered or sorted as dates
--   without converting them to a proper PostgreSQL timestamp first.
--
-- WHAT IT DOES:
--   to_timestamp(n) is a built-in PostgreSQL function that converts a Unix
--   integer into a TIMESTAMPTZ value (timestamp with timezone).
--   PostgreSQL stores TIMESTAMPTZ as UTC internally but displays it in the
--   session timezone. Because we set timezone = 'Asia/Kolkata' above,
--   every value returned by this function will display as IST.
--
-- IMMUTABLE tells PostgreSQL the function always returns the same result for
-- the same input. PostgreSQL uses this to optimise queries.
-- =============================================================================

CREATE OR REPLACE FUNCTION unix_to_ist(unix_ts BIGINT)
RETURNS TIMESTAMPTZ AS $$
    -- Convert a Unix integer (seconds since epoch, UTC) into a real timestamp.
    -- The database timezone setting ensures it displays as IST when queried.
    SELECT to_timestamp(unix_ts);
$$ LANGUAGE SQL IMMUTABLE;


-- =============================================================================
-- BRONZE LAYER
-- Table: bronze.webhook_events
--
-- Every single Edmingle webhook event lands here first, completely raw.
-- No filtering. No transformation. Store exactly what we received.
--
-- WHY: If our Silver processing code has a bug, we still have every original
-- event here. We can fix the bug and reprocess from Bronze at any time.
-- The routed_to_silver column lets us identify events that failed Silver processing.
--
-- RULE: Never delete rows from this table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.webhook_events (

    -- Auto-incrementing internal row number managed by PostgreSQL
    id               SERIAL PRIMARY KEY,

    -- The unique event identifier from Edmingle's top-level "id" field
    -- TEXT stores strings of any length — we never truncate webhook data
    event_id         TEXT NOT NULL,

    -- The event type string, e.g. "user.user_created" or "session.session_created"
    -- Stored here so we can filter by event type without parsing the JSON
    event_type       TEXT NOT NULL,

    -- The complete incoming JSON payload stored as JSONB (binary JSON).
    -- JSONB validates that the data is valid JSON and is faster to query than plain JSON.
    -- We never modify this column — it is the original record exactly as Edmingle sent it.
    raw_payload      JSONB NOT NULL,

    -- When our server received this event, automatically set to the current IST time.
    -- NOW() returns the current moment; AT TIME ZONE 'Asia/Kolkata' gives us IST.
    -- DEFAULT means PostgreSQL fills this in automatically on every INSERT.
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- Whether this event came from Edmingle's live platform (true) or their test sandbox (false)
    is_live_mode     BOOLEAN DEFAULT true,

    -- Tracks whether this event was successfully written to a Silver table.
    -- Starts as false on every insert. Flask sets it to true after Silver insert succeeds.
    -- Any rows where this is still false can be identified and reprocessed into Silver.
    routed_to_silver BOOLEAN DEFAULT false,

    -- Edmingle retries failed deliveries up to 4 times, so we could receive
    -- the same event more than once. This constraint makes the second INSERT fail
    -- gracefully instead of creating a duplicate row.
    UNIQUE (event_id)
);


-- =============================================================================
-- BRONZE LAYER — TABLE 2: bronze.failed_events
-- Receives any HTTP request that arrived at /webhook but could not be
-- parsed as JSON, had missing required fields, or caused a Bronze insert error.
-- This is the final safety net — nothing sent to our server is silently lost.
-- Use GET /failed to inspect these rows without opening pgAdmin.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.failed_events (

    -- Auto-incrementing internal row number
    id             SERIAL PRIMARY KEY,

    -- When our server received the failed request, in IST
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- Short description of why processing failed, e.g. "JSON parse failed"
    failure_reason TEXT,

    -- The raw HTTP body as received, truncated to 10 000 characters if enormous
    raw_body       TEXT,

    -- The Content-Type header from the failed request
    content_type   TEXT
);


-- =============================================================================
-- SILVER LAYER — TABLE 1: silver.users
-- Source events: user.user_created, user.user_updated
--
-- One row per student. New events UPSERT (update-or-insert) on user_id.
-- user.user_created fills all known fields when a student first registers.
-- user.user_updated only sends the fields that CHANGED — not the full record.
-- The upsert logic in Flask will use COALESCE to preserve existing values
-- for any field not included in the update payload.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.users (

    -- Auto-incrementing internal row number
    id             SERIAL PRIMARY KEY,

    -- The Edmingle event ID that last wrote to this row
    event_id       TEXT NOT NULL,

    -- Which event type last touched this row
    -- Will be "user.user_created" on first insert, "user.user_updated" on subsequent upserts
    event_type     TEXT NOT NULL,

    -- Edmingle's unique numeric ID for this student.
    -- This is the column we UPSERT on — if a row with this user_id exists, update it.
    user_id        BIGINT NOT NULL,

    -- Student's email address
    email          TEXT,

    -- Student's full display name
    full_name      TEXT,

    -- Student's chosen username on the platform
    user_name      TEXT,

    -- Role on the platform: "student", "tutor", or "admin".
    -- Important for future analytics — staff accounts should be excluded from student reports.
    user_role      TEXT,

    -- Student's phone number stored as a string.
    -- String (not integer) because phone numbers can have leading zeros and country codes.
    contact_number TEXT,

    -- Edmingle's numeric ID for the institution (483 for Vyoma)
    institution_id INTEGER,

    -- Location fields — extracted from the system_fields object in user.user_updated events.
    -- These are NULL for a student until their first user_updated event arrives.
    city           TEXT,
    state          TEXT,
    address        TEXT,
    pincode        TEXT,

    -- Parent or guardian contact — also from system_fields in user.user_updated events
    parent_name    TEXT,
    parent_email   TEXT,
    parent_contact TEXT,

    -- The entire custom_fields array stored as JSONB.
    -- Edmingle sends platform-specific profile fields here (e.g. occupation, qualification).
    -- We store the full array; individual values are always read by matching field_name string,
    -- never by index position, because the order of fields in the array can change.
    custom_fields  JSONB,

    -- When the student first registered, converted from Unix UTC to IST.
    -- Set by user.user_created events only.
    -- The upsert in Flask uses COALESCE so this value is NEVER overwritten
    -- by a later user.user_updated event.
    created_at_ist TIMESTAMPTZ,

    -- When the student last updated their profile, converted from Unix UTC to IST.
    -- NULL until the first user.user_updated event arrives for this student.
    updated_at_ist TIMESTAMPTZ,

    -- When our server last received an event that wrote to this row
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per student — new events for the same student update the existing row
    UNIQUE (user_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 2: silver.transactions
-- Source events: transaction.user_purchase_initiated
--                transaction.user_purchase_completed
--                transaction.user_purchase_failed
--
-- One row per webhook event. UPSERT on event_id.
-- All three event types share this single table.
-- Fields that do not apply to a specific event type will be NULL in that row.
--
-- IMPORTANT: purchase_completed is the ONLY confirmed enrollment.
--   purchase_initiated = student started checkout but did NOT complete payment.
--   purchase_failed    = payment was attempted and failed, NOT an enrollment.
-- Filter to confirmed enrollments with:
--   WHERE event_type = 'transaction.user_purchase_completed'
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.transactions (

    -- Auto-incrementing internal row number
    id                    SERIAL PRIMARY KEY,

    -- Unique Edmingle event ID — one row per event
    event_id              TEXT NOT NULL,

    -- Which transaction event type this row came from
    event_type            TEXT NOT NULL,

    -- The student involved in this transaction
    user_id               BIGINT,
    email                 TEXT,
    full_name             TEXT,

    -- The course package the student is purchasing
    bundle_id             BIGINT,
    course_name           TEXT,

    -- Internal Edmingle IDs for the institution's copy of this bundle and batch
    institution_bundle_id BIGINT,
    master_batch_id       BIGINT,
    master_batch_name     TEXT,

    -- Price breakdown: original_price minus discount equals final_price
    original_price        NUMERIC(12,2),
    discount              NUMERIC(12,2),
    final_price           NUMERIC(12,2),

    -- Currency code, e.g. "INR"
    currency              TEXT,

    -- Platform credits the student applied toward this purchase
    credits_applied       NUMERIC(12,2),

    -- How the student paid, e.g. "razorpay" — only present in purchase_completed rows
    payment_method        TEXT,

    -- The payment gateway's unique reference number — only in purchase_completed rows
    transaction_id        TEXT,

    -- Plain-English reason the payment failed — only in purchase_failed rows
    failure_reason        TEXT,

    -- Machine-readable error code from the payment gateway — only in purchase_failed rows
    error_code            TEXT,

    -- Enrollment status string, e.g. "active" — only in purchase_completed rows
    enrollment_status     TEXT,

    -- When the student's course access starts and ends, converted to IST.
    -- Present in purchase_initiated and purchase_completed; NULL in purchase_failed.
    start_date_ist        TIMESTAMPTZ,
    end_date_ist          TIMESTAMPTZ,

    -- When this transaction event occurred, converted from top-level event_timestamp to IST
    event_timestamp_ist   TIMESTAMPTZ,

    -- When our server received this event
    received_at           TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per event — if the same event_id arrives twice, update in place
    UNIQUE (event_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 3: silver.sessions
-- Source events: session.session_created
--                session.session_update    (no 'd' — this is Edmingle's exact event name)
--                session.session_cancel    (no 'led' — this is Edmingle's exact event name)
--                session.session_started
--                session.session_reminders
--
-- One row per session instance. UPSERT on attendance_id.
-- session_created fills all known fields when a class is first scheduled.
-- Later events add or overwrite specific fields on the same row — they do
-- not create new rows. For example, session_cancel adds cancellation_reason
-- on top of the row that session_created originally inserted.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.sessions (

    -- Auto-incrementing internal row number
    id                   SERIAL PRIMARY KEY,

    -- The Edmingle event ID that last wrote to this row
    event_id             TEXT NOT NULL,

    -- Which event type last updated this row
    event_type           TEXT NOT NULL,

    -- Edmingle's unique ID for this class session instance.
    -- This is the column we UPSERT on. The same attendance_id appears across
    -- all events for the same session (created, updated, cancelled, started).
    attendance_id        BIGINT NOT NULL,

    -- The class template this session belongs to (the definition, not this occurrence)
    class_id             BIGINT,
    class_name           TEXT,

    -- Human-readable format of the class type, e.g. "Live Class"
    class_type_formatted TEXT,

    -- When the class is scheduled to start and end, converted from gmt_start_time
    -- and gmt_end_time to IST. Updated if a session_update event arrives.
    scheduled_start_ist  TIMESTAMPTZ,
    scheduled_end_ist    TIMESTAMPTZ,

    -- When the class actually began — only populated when a session_started event arrives
    actual_start_ist     TIMESTAMPTZ,

    -- Planned length of the class in minutes
    duration_minutes     INTEGER,

    -- The teacher assigned to run this session (from taken_by / taken_by_name / taken_by_email)
    teacher_id           BIGINT,
    teacher_name         TEXT,
    teacher_email        TEXT,

    -- The full master_batches array stored as JSONB.
    -- Each element contains: master_batch_id, master_batch_name, bundle_id, bundle_name.
    -- We store the whole array because a session can have multiple batches.
    master_batches       JSONB,

    -- Links this session to its recurring schedule definition, if part of a series
    schedule_id          BIGINT,
    is_recurring         BOOLEAN,

    -- The virtual meeting platform students use to join, e.g. "Zoom"
    virtual_platform     TEXT,

    -- The Zoom meeting ID students need to join this class
    zoom_meeting_id      TEXT,

    -- Why the class was cancelled — only populated when a session_cancel event arrives
    cancellation_reason  TEXT,

    -- The user_id of whoever cancelled the class — only from session_cancel event
    cancelled_by         BIGINT,

    -- Numeric status code: 0 = scheduled, 1 = running
    status               INTEGER,

    -- Whether the teacher signed in after the scheduled start time — from session_started
    is_late_signin       BOOLEAN,

    -- How many minutes after scheduled start the teacher signed in — from session_started
    delay_minutes        INTEGER,

    -- The reminder type sent before this class — only from session_reminders event.
    -- Values: "1h_before" or "24h_before"
    reminder_type        TEXT,

    -- When our server received the event that last wrote to this row
    received_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per unique class session — events for the same session update this row
    UNIQUE (attendance_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 4: silver.assessments
-- Source events: assessments.test_submitted
--                assessments.test_evaluated
--                assessments.exercise_submitted
--                assessments.exercise_evaluated
--
-- One row per event. UPSERT on event_id.
-- NULL values are expected — exercise_id is NULL for test events,
-- and faculty_comments is NULL for submitted (not yet evaluated) events.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.assessments (

    -- Auto-incrementing internal row number
    id               SERIAL PRIMARY KEY,

    -- Unique Edmingle event ID
    event_id         TEXT NOT NULL,

    -- Which assessment event type this row came from
    event_type       TEXT NOT NULL,

    -- The student who submitted this assessment
    user_id          BIGINT,

    -- Unique ID for this specific submission attempt
    attempt_id       BIGINT,

    -- The exercise or assignment ID — only present in exercise events, NULL for test events
    exercise_id      BIGINT,

    -- The score given for this submission
    -- NUMERIC(8,2) stores up to 8 digits with 2 decimal places, e.g. 99.50
    mark             NUMERIC(8,2),

    -- Grading status: 0 = not yet graded, 1 = graded
    is_evaluated     INTEGER,

    -- Written feedback from the teacher — only in test_evaluated and exercise_evaluated events
    faculty_comments TEXT,

    -- When the student submitted, converted from submitted_at Unix UTC to IST
    submitted_at_ist TIMESTAMPTZ,

    -- When our server received this event
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per event — upsert on event_id
    UNIQUE (event_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 5: silver.courses
-- Source event: course.user_course_completed
--
-- Recorded when a student finishes an entire course (bundle).
-- One row per event. UPSERT on event_id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.courses (

    -- Auto-incrementing internal row number
    id               SERIAL PRIMARY KEY,

    -- Unique Edmingle event ID
    event_id         TEXT NOT NULL,

    -- Event type — will always be "course.user_course_completed"
    event_type       TEXT NOT NULL,

    -- The student who completed the course
    user_id          BIGINT,

    -- The course package (bundle) they completed
    bundle_id        BIGINT,

    -- When they completed the course, converted from completed_at Unix UTC to IST
    completed_at_ist TIMESTAMPTZ,

    -- When our server received this event
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per event — upsert on event_id
    UNIQUE (event_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 6: silver.announcements
-- Source event: announcement.announcement_created
--
-- The full payload structure for announcements is not yet documented by Edmingle.
-- We store the entire data{} object as raw JSONB so nothing is lost.
-- Once we see real announcement payloads, we can add typed columns and
-- populate them with a backfill query from the raw_data JSONB.
-- One row per event. UPSERT on event_id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.announcements (

    -- Auto-incrementing internal row number
    id          SERIAL PRIMARY KEY,

    -- Unique Edmingle event ID
    event_id    TEXT NOT NULL,

    -- Event type — will always be "announcement.announcement_created"
    event_type  TEXT NOT NULL,

    -- The entire data{} object stored as raw JSONB.
    -- Kept here until the payload fields are known and typed columns can be added.
    raw_data    JSONB,

    -- When our server received this event
    received_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per event — upsert on event_id
    UNIQUE (event_id)
);


-- =============================================================================
-- SILVER LAYER — TABLE 7: silver.certificates
-- Source event: certificate.certificate_issued
--
-- Recorded when Edmingle generates a completion certificate for a student.
-- One row per event. UPSERT on event_id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.certificates (

    -- Auto-incrementing internal row number
    id               SERIAL PRIMARY KEY,

    -- Unique Edmingle event ID
    event_id         TEXT NOT NULL,

    -- Event type — will always be "certificate.certificate_issued"
    event_type       TEXT NOT NULL,

    -- Edmingle's unique identifier for the certificate document itself
    certificate_id   TEXT,

    -- The student who received this certificate
    user_id          BIGINT,

    -- When the certificate was issued, converted from issued_at Unix UTC to IST
    issued_at_ist    TIMESTAMPTZ,

    -- When our server received this event
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- One row per event — upsert on event_id
    UNIQUE (event_id)
);


-- =============================================================================
-- MIGRATION 2026-04-29: CSV backfill tables + redesigned silver.transactions
-- =============================================================================


-- ---------------------------------------------------------------------------
-- BRONZE LAYER — TABLE 3: bronze.studentexport_raw
-- Raw copy of studentexport.csv (one row per student profile export).
-- skiprows=1 skips the decorative title row above the header.
-- All columns stored as TEXT — no transformation, no type casting.
-- source_row is the 0-based pandas index (row 0 = first data row).
-- ---------------------------------------------------------------------------
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


-- ---------------------------------------------------------------------------
-- BRONZE LAYER — TABLE 4: bronze.student_courses_enrolled_raw
-- Raw copy of studentCoursesEnrolled.csv (one row per student-course enrollment).
-- Column names match the CSV headers exactly (already snake_case).
-- All values stored as TEXT. Timestamps (start_date, etc.) are Unix integers
-- stored as strings — cast to BIGINT when transforming to Silver.
-- ---------------------------------------------------------------------------
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


-- ---------------------------------------------------------------------------
-- BRONZE LAYER — TABLE 5: bronze.unresolved_students_raw
-- Rows from studentexport.csv that could not be resolved to a user_id.
-- A student is unresolvable if their email is absent or does not appear
-- in bronze.student_courses_enrolled_raw (no known enrollment record).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.unresolved_students_raw (
    id          SERIAL PRIMARY KEY,
    source_row  INTEGER NOT NULL,
    email       TEXT,
    raw_row     JSONB,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- SILVER LAYER — TABLE 2 (REDESIGNED): silver.transactions
--
-- Schema redesigned on 2026-04-29 for CSV + webhook unified storage:
--   UPSERT key changed to (user_id, bundle_id, master_batch_id) — one row
--   per student-course-batch enrollment, regardless of whether it arrived
--   via a live webhook or a CSV historical backfill.
--
--   Added:   contact_number, course_name, created_at_ist, source, inserted_at
--   Removed: failure_reason, error_code, enrollment_status, received_at
--
-- source = 'webhook' for live events; source = 'csv' for backfilled rows.
--
-- WARNING: DROP TABLE destroys existing rows. Export data first if needed.
--   After running, execute:  python ingestion/reprocess_bronze.py
--   to repopulate Silver from all Bronze webhook events.
-- =============================================================================
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
