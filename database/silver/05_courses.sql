-- Table: silver.courses
-- Purpose: one row per course completion event; UPSERT on event_id

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
