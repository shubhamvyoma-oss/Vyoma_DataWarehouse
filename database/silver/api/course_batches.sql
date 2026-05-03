-- Table: silver.course_batches
-- Purpose: one row per bundle-batch pair with enrollment dates; UPSERT on (bundle_id, batch_id); source is batches_data.csv

CREATE TABLE IF NOT EXISTS silver.course_batches (
    id                SERIAL PRIMARY KEY,
    bundle_id         BIGINT,
    bundle_name       TEXT,
    batch_id          BIGINT,
    batch_name        TEXT,
    batch_status      TEXT,
    start_date_ist    TIMESTAMPTZ,
    end_date_ist      TIMESTAMPTZ,
    tutor_name        TEXT,
    admitted_students INTEGER,
    imported_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (bundle_id, batch_id)
);
