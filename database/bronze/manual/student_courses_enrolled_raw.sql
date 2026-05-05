-- =============================================================================
-- BRONZE TABLE: student_courses_enrolled_raw
-- =============================================================================
-- This table stores raw data about which students are enrolled in which courses.
-- It is a direct copy from the "studentCoursesEnrolled" CSV file.
-- =============================================================================

-- Create the table to store raw enrollment data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.student_courses_enrolled_raw (
    -- Internal ID for the database to keep track of each row.
    id                    SERIAL PRIMARY KEY,
    
    -- The row number from the original CSV file.
    source_row            INTEGER NOT NULL,
    
    -- The unique ID of the user.
    user_id               TEXT,
    
    -- The name of the student.
    name                  TEXT,
    
    -- The email address of the student.
    email                 TEXT,
    
    -- The unique ID of the class.
    class_id              TEXT,
    
    -- The name of the class.
    class_name            TEXT,
    
    -- The name of the tutor for this class.
    tutor_name            TEXT,
    
    -- Total number of classes in the course.
    total_classes         TEXT,
    
    -- Number of classes the student attended (Present).
    present               TEXT,
    
    -- Number of classes the student missed (Absent).
    absent                TEXT,
    
    -- Number of times the student was late.
    late                  TEXT,
    
    -- Number of times the student was excused.
    excused               TEXT,
    
    -- The start date of the course.
    start_date            TEXT,
    
    -- The end date of the course.
    end_date              TEXT,
    
    -- The unique ID of the master batch.
    master_batch_id       TEXT,
    
    -- The name of the master batch.
    master_batch_name     TEXT,
    
    -- The date the user started in the class.
    classusers_start_date TEXT,
    
    -- The date the user ended in the class.
    classusers_end_date   TEXT,
    
    -- The status of the batch (e.g., Active).
    batch_status          TEXT,
    
    -- The status of the class user record.
    cu_status             TEXT,
    
    -- The state of the class user record.
    cu_state              TEXT,
    
    -- The unique ID of the institution bundle.
    institution_bundle_id TEXT,
    
    -- The date/time when this record was archived.
    archived_at           TEXT,
    
    -- The unique ID of the bundle.
    bundle_id             TEXT,
    
    -- The exact time this row was added to our database.
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't load the same row from the CSV file twice.
    UNIQUE (source_row)
);
