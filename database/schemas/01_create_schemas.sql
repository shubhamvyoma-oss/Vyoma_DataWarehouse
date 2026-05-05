-- =============================================================================
-- DATABASE SETUP: SCHEMAS AND HELPERS
-- =============================================================================
-- This script sets up the "folders" (Schemas) for our data warehouse 
-- and a helper tool to handle time conversions.
-- =============================================================================

-- Set the time zone for this database to India Standard Time.
-- We do this so all our timestamps match the local time in India.
ALTER DATABASE edmingle_analytics SET timezone = 'Asia/Kolkata';

-- Create the 'bronze' schema if it does not already exist.
-- The 'bronze' schema acts like a "Loading Bay" for raw, messy data.
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create the 'silver' schema if it does not already exist.
-- The 'silver' schema is our "Cleaning Room" where data is tidied up.
CREATE SCHEMA IF NOT EXISTS silver;

-- Create the 'gold' schema if it does not already exist.
-- The 'gold' schema is the "Display Room" for final reports and charts.
CREATE SCHEMA IF NOT EXISTS gold;

-- Create a helper function to convert Unix timestamps to human-readable time.
-- Unix timestamps are numbers representing seconds; this turns them into real dates.
CREATE OR REPLACE FUNCTION unix_to_ist(unix_timestamp_seconds BIGINT)
RETURNS TIMESTAMPTZ AS $$
    -- The 'to_timestamp' tool takes the number of seconds and makes it a date.
    SELECT to_timestamp(unix_timestamp_seconds);
-- This function is 'IMMUTABLE', meaning it always gives the same output for the same input.
$$ LANGUAGE SQL IMMUTABLE;
