-- =============================================================================
-- BRONZE TABLE: batches_data_raw
-- =============================================================================
-- This table stores raw data about batches from a CSV file.
-- We keep all columns as TEXT to make sure we don't lose any messy data.
-- =============================================================================

-- Create the table to store raw batch data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.batches_data_raw (
    -- Internal ID for the database to keep track of each row.
    id                    SERIAL PRIMARY KEY,
    
    -- The row number from the original CSV file.
    source_row            INTEGER NOT NULL,
    
    -- The unique ID of the bundle as text.
    bundle_id             TEXT,
    
    -- The name of the bundle as text.
    bundle_name           TEXT,
    
    -- The unique ID of the batch as text.
    batch_id              TEXT,
    
    -- The name of the batch as text.
    batch_name            TEXT,
    
    -- The status of the batch (e.g., Ongoing, Completed) as text.
    batch_status          TEXT,
    
    -- The start date as provided in the source file.
    start_date            TEXT,
    
    -- The start date converted to a standard text format.
    start_date_converted  TEXT,
    
    -- The end date as provided in the source file.
    end_date              TEXT,
    
    -- The end date converted to a standard text format.
    end_date_converted    TEXT,
    
    -- The unique ID of the tutor as text.
    tutor_id              TEXT,
    
    -- The name of the tutor as text.
    tutor_name            TEXT,
    
    -- The number of admitted students as text.
    admitted_students     TEXT,
    
    -- The exact time this row was added to our database.
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't load the same row from the CSV file twice.
    UNIQUE (source_row)
);
