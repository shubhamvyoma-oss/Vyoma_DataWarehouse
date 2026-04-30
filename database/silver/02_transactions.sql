-- Table: silver.transactions
-- Purpose: one row per student-course-batch enrollment; UPSERT on (user_id, bundle_id, master_batch_id); merges CSV and webhook enrollments

CREATE TABLE IF NOT EXISTS silver.transactions (
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
