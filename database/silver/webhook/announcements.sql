-- This creates the announcements table in the silver schema.
-- Silver tables are used to store cleaned and organized data.
CREATE TABLE IF NOT EXISTS silver.announcements (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each announcement uniquely.
    id          SERIAL PRIMARY KEY,
    
    -- This stores the unique ID provided by the external system for the event.
    -- We use this to prevent duplicate announcements.
    event_id    TEXT NOT NULL,
    
    -- This stores the type of event that occurred.
    -- It helps us categorize different types of announcements.
    event_type  TEXT NOT NULL,
    
    -- This stores the full data of the announcement in a flexible JSON format.
    -- We use this to keep all the original details in case we need them later.
    raw_data    JSONB,
    
    -- This stores the date and time when we received this announcement.
    -- We set it to the current time in the India time zone by default.
    received_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    
    -- This ensures that we don't have two records with the same event_id.
    -- It keeps our data clean and free of duplicates.
    UNIQUE (event_id)
);
