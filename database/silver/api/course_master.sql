-- silver.course_master — rebuilt daily by run_course_pipeline.py
-- Joins course_batches + course_metadata; includes computed flags

CREATE TABLE IF NOT EXISTS silver.course_master (
    id                       SERIAL PRIMARY KEY,
    bundle_id                BIGINT,
    bundle_name              TEXT,
    batch_id                 BIGINT,
    batch_name               TEXT,
    batch_status             TEXT,
    start_date               DATE,
    end_date                 DATE,
    tutor_name               TEXT,
    admitted_students        INTEGER,
    course_name              TEXT,
    subject                  TEXT,
    course_type              TEXT,
    term_of_course           TEXT,
    position_in_funnel       TEXT,
    adhyayanam_category      TEXT,
    sss_category             TEXT,
    viniyoga                 TEXT,
    division                 TEXT,
    catalogue_status         TEXT,
    final_status             TEXT,
    is_latest_batch          SMALLINT DEFAULT 0,
    include_in_course_count  SMALLINT DEFAULT 0,
    status_adjustment_reason TEXT,
    has_batch                SMALLINT DEFAULT 0,
    built_at                 TIMESTAMPTZ DEFAULT NOW()
);
