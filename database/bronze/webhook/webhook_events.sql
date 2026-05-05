-- =============================================================================
-- BRONZE TABLE: webhook_events
-- =============================================================================
-- This is a "Loading Bay" table. It stores every single raw message sent to 
-- us by the Edmingle LMS via webhooks. We keep the data exactly as it arrived 
-- so we can re-process it if our Silver cleaning logic ever changes.
-- =============================================================================

-- Create the table to store raw webhook event messages if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.webhook_events (
    -- Internal sequence number for database tracking.
    -- BIGSERIAL chosen to handle billions of events over time.
    id               BIGSERIAL PRIMARY KEY,

    -- The unique ID sent by Edmingle for this event. Example: 'evt_12345'
    -- TEXT chosen because IDs can contain letters and numbers.
    event_id         TEXT NOT NULL,

    -- The type of event (e.g., 'user.created'). Example: 'transaction.completed'
    -- TEXT chosen for maximum flexibility as new event types are added.
    event_type       TEXT NOT NULL,

    -- The entire raw message in JSON format.
    -- JSONB chosen because it is faster to query and takes less space than JSON.
    raw_payload      JSONB NOT NULL,

    -- When we received this data. Default is India Standard Time.
    -- TIMESTAMPTZ chosen to ensure we always know the exact moment in time.
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Whether this was a real event or a test message. Example: TRUE
    -- BOOLEAN chosen for simple true/false tracking.
    is_live_mode     BOOLEAN DEFAULT true,

    -- Tracker to see if this raw data has been moved to the Silver layer.
    -- BOOLEAN chosen to prevent duplicate processing.
    routed_to_silver BOOLEAN DEFAULT false,

    -- Prevents the same event from being stored twice in our database.
    -- This speeds up lookups when checking if an event was already received.
    CONSTRAINT unique_webhook_event_id UNIQUE (event_id)
);

-- Index to speed up the process of finding unrouted events.
-- We frequently look for events where 'routed_to_silver' is false.
CREATE INDEX IF NOT EXISTS idx_webhook_unrouted 
ON bronze.webhook_events(event_id) 
WHERE routed_to_silver = false;
