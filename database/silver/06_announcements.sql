-- Table: silver.announcements
-- Purpose: announcement payload structure not yet documented — stores raw JSONB; UPSERT on event_id

CREATE TABLE IF NOT EXISTS silver.announcements (
    id          SERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    raw_data    JSONB,
    received_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (event_id)
);
