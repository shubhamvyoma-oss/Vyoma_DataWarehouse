-- This table is called 'course_catalogue' and it lives in the 'silver' schema.
-- It stores information about students completing their courses.
CREATE TABLE IF NOT EXISTS silver.course_catalogue (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each course completion record uniquely.
    id               SERIAL PRIMARY KEY,
    
    -- This stores the unique ID for the event from the external system.
    -- It ensures we can track the specific notification that a student finished a course.
    event_id         TEXT NOT NULL,
    
    -- This stores the type of event (e.g., 'course_completed').
    -- It helps us categorize different actions related to course progress.
    event_type       TEXT NOT NULL,
    
    -- This is the ID of the student who completed the course.
    -- We use this to link the completion to the correct person.
    user_id          BIGINT,
    
    -- This is the unique ID for the course bundle that was completed.
    -- It tells us exactly which course the student finished.
    bundle_id        BIGINT,
    
    -- This is the date and time when the student officially completed the course.
    -- It is recorded in India Standard Time (IST).
    completed_at_ist TIMESTAMPTZ,
    
    -- This records exactly when our system received this completion information.
    -- It defaults to the current time in the India time zone.
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    
    -- This ensures that we don't have two records for the same event_id.
    -- It prevents duplicate entries for the same course completion.
    UNIQUE (event_id)
);
