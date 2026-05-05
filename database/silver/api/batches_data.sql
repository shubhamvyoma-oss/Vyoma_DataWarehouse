-- This table is called 'course_batches' and it lives in the 'silver' schema.
-- It stores information about different groups of students (batches) in a course bundle.
CREATE TABLE IF NOT EXISTS silver.course_batches (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each batch record uniquely.
    id                SERIAL PRIMARY KEY,
    
    -- This is the unique ID for the course bundle.
    -- A bundle can contain multiple batches.
    bundle_id         BIGINT,
    
    -- This is the name of the course bundle.
    -- It helps us identify what the bundle is about (e.g., 'Samskrita Level 1').
    bundle_name       TEXT,
    
    -- This is the unique ID for the specific batch.
    -- Each batch represents a specific group of students.
    batch_id          BIGINT,
    
    -- This is the name of the specific batch.
    -- It helps us distinguish between different groups within the same course.
    batch_name        TEXT,
    
    -- This tells us if the batch is currently 'Active', 'Completed', etc.
    -- It helps us filter for currently running courses.
    batch_status      TEXT,
    
    -- This is the date and time when the batch is scheduled to start.
    -- We use TIMESTAMPTZ to ensure the time is correct for different locations.
    start_date        TIMESTAMPTZ,
    
    -- This is the date and time when the batch is scheduled to end.
    -- It tells us when the course group finishes their studies.
    end_date          TIMESTAMPTZ,
    
    -- This is the unique ID of the teacher (tutor) assigned to this batch.
    -- We use it to link the batch to a specific instructor.
    tutor_id          BIGINT,
    
    -- This is the name of the teacher (tutor) for this batch.
    -- It's kept here for easy reading in reports.
    tutor_name        TEXT,
    
    -- This stores the total number of students enrolled in this specific batch.
    -- It helps us track the size of each group.
    batch_enrollment_count   INTEGER,
    
    -- This records exactly when this batch information was added to our database.
    -- It defaults to the current time.
    imported_at       TIMESTAMPTZ DEFAULT NOW(),
    
    -- This ensures that each combination of bundle and batch is only listed once.
    -- It keeps our data clean and prevents duplicates for the same group.
    UNIQUE (bundle_id, batch_id)
);
