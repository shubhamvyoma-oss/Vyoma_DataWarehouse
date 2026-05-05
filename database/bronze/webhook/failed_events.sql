-- =============================================================================
-- BRONZE TABLE: failed_events
-- =============================================================================
-- This table is a "Safety Net". It stores data that reached our system 
-- but could not be understood or saved properly in other tables.
-- =============================================================================

-- Create the table to store failed event messages if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.failed_events (
    -- Internal ID for the database to keep track of each failure.
    id             SERIAL PRIMARY KEY,
    
    -- When this failed data was received. Defaults to India Standard Time.
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    
    -- A text explanation of why the data could not be saved correctly.
    failure_reason TEXT,
    
    -- The full original message text that failed to be processed.
    raw_body       TEXT,
    
    -- The type of content received (e.g., 'application/json').
    content_type   TEXT
);
