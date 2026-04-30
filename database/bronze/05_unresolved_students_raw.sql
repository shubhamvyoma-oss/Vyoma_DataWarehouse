-- Table: bronze.unresolved_students_raw
-- Purpose: students from studentexport.csv whose email could not be matched to a user_id

CREATE TABLE IF NOT EXISTS bronze.unresolved_students_raw (
    id          SERIAL PRIMARY KEY,
    source_row  INTEGER NOT NULL,
    email       TEXT,
    raw_row     JSONB,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
