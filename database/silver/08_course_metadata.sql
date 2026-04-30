-- Table: silver.course_metadata
-- Purpose: one row per course with cleaned dimensions; UPSERT on bundle_id; source is course_catalogue_data.csv

CREATE TABLE IF NOT EXISTS silver.course_metadata (
    id                  SERIAL PRIMARY KEY,
    bundle_id           BIGINT NOT NULL,
    course_name         TEXT,
    subject             TEXT,
    course_type         TEXT,
    status              TEXT,
    term_of_course      TEXT,
    position_in_funnel  TEXT,
    adhyayanam_category TEXT,
    sss_category        TEXT,
    viniyoga            TEXT,
    course_division     TEXT,
    division            TEXT,
    level               TEXT,
    language            TEXT,
    num_students        INTEGER,
    cost                NUMERIC(10,2),
    imported_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (bundle_id)
);
