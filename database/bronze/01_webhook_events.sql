-- Table: bronze.webhook_events
-- Purpose: stores every raw webhook event as received; never deleted; routed_to_silver flags successful Silver writes

CREATE TABLE IF NOT EXISTS bronze.webhook_events (
    id               SERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    raw_payload      JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    is_live_mode     BOOLEAN DEFAULT true,
    routed_to_silver BOOLEAN DEFAULT false,  -- set true after Silver insert succeeds
    UNIQUE (event_id)
);
