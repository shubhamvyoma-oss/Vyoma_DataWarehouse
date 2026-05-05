-- =============================================================================
-- SETUP: THE FOLDER STRUCTURE (SCHEMAS)
-- =============================================================================
-- This script creates the different "rooms" in our data warehouse.
-- We use SCHEMAS to keep raw data (Bronze) separate from cleaned data (Silver).
-- =============================================================================

-- Create the Bronze schema.
-- We use this as the "Loading Bay" to keep messy, raw source data in one place.
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create the Silver schema.
-- We use this as the "Prep Station" for all cleaned and organized data.
CREATE SCHEMA IF NOT EXISTS silver;

-- Create the Gold schema.
-- We use this as the "Dining Room" for reporting-ready views used by Power BI.
CREATE SCHEMA IF NOT EXISTS gold;
