-- Table: (schemas and shared objects)
-- Purpose: create bronze/silver/gold schemas and the unix_to_ist helper function

ALTER DATABASE edmingle_analytics SET timezone = 'Asia/Kolkata';

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Converts Unix integer (seconds since epoch) to TIMESTAMPTZ displayed as IST
CREATE OR REPLACE FUNCTION unix_to_ist(unix_ts BIGINT)
RETURNS TIMESTAMPTZ AS $$
    SELECT to_timestamp(unix_ts);
$$ LANGUAGE SQL IMMUTABLE;
