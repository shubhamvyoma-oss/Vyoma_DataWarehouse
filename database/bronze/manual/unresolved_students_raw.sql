-- =============================================================================
-- BRONZE TABLE: unresolved_students_raw
-- =============================================================================
-- This table stores records of students whose emails could not be matched 
-- to a known User ID during the data import process.
-- =============================================================================

-- Create the table to store unresolved student data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.unresolved_students_raw (
    -- Internal ID for the database to keep track of each record.
    id          SERIAL PRIMARY KEY,
    
    -- The row number from the original student export CSV.
    source_row  INTEGER NOT NULL,
    
    -- The email address that could not be matched.
    email       TEXT,
    
    -- The entire raw row of data stored as JSON for later investigation.
    raw_row     JSONB,
    
    -- The exact time this record was created in our database.
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
