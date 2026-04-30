-- Table: silver.certificates
-- Purpose: one row per certificate issued; UPSERT on event_id

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
