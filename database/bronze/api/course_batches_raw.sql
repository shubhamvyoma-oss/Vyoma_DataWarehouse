-- Table: bronze.course_batches_raw
-- Purpose: raw copy of batches_data.csv; all 12 columns stored as TEXT; one-time historical load

CREATE TABLE IF NOT EXISTS bronze.course_batches_raw (
    id                    SERIAL PRIMARY KEY,
    source_row            INTEGER NOT NULL,
    bundle_id             TEXT,
    bundle_name           TEXT,
    batch_id              TEXT,
    batch_name            TEXT,
    batch_status          TEXT,
    start_date            TEXT,
    start_date_converted  TEXT,
    end_date              TEXT,
    end_date_converted    TEXT,
    tutor_id              TEXT,
    tutor_name            TEXT,
    admitted_students     TEXT,
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_row)
);
