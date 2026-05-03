-- Table: bronze.student_courses_enrolled_raw
-- Purpose: raw copy of studentCoursesEnrolled.csv; timestamps stored as TEXT, cast to BIGINT in Silver

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
