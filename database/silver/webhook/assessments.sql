-- Table: silver.assessments
-- Purpose: one row per assessment event; UPSERT on event_id; faculty_comments populated by evaluated event

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
