-- Table: bronze.failed_events
-- Purpose: safety net for requests that arrived but could not be parsed or stored in Bronze

CREATE TABLE IF NOT EXISTS bronze.failed_events (
    id             SERIAL PRIMARY KEY,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    failure_reason TEXT,
    raw_body       TEXT,
    content_type   TEXT
);
