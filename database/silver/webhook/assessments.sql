-- This creates the assessments table in the silver schema.
-- This table stores information about student assessments like tests and exercises.
CREATE TABLE IF NOT EXISTS silver.assessments (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each assessment record uniquely.
    id               SERIAL PRIMARY KEY,
    
    -- This stores the unique ID for the event from the external system.
    -- It ensures we can track every specific update or submission.
    event_id         TEXT NOT NULL,
    
    -- This stores what kind of assessment event happened (e.g., submitted or evaluated).
    -- It helps us understand the stage of the assessment.
    event_type       TEXT NOT NULL,
    
    -- This stores the ID of the user who took the assessment.
    -- We use this to link the assessment to a specific student.
    user_id          BIGINT,
    
    -- This stores the ID of the specific attempt made by the student.
    -- A student might try an assessment multiple times, and this tracks each one.
    attempt_id       BIGINT,
    
    -- This stores the ID of the exercise if it was an exercise.
    -- It can be empty (NULL) if the event was a test instead of an exercise.
    exercise_id      BIGINT,
    
    -- This stores the score or mark obtained by the student.
    -- It uses a decimal format to allow for precise grading.
    mark             NUMERIC(8,2),
    
    -- This flag shows if the assessment has been graded (evaluated) or not.
    -- Usually 1 means yes and 0 means no.
    is_evaluated     INTEGER,
    
    -- This stores any feedback or comments provided by the teacher.
    -- It might be empty until the assessment is actually graded.
    faculty_comments TEXT,
    
    -- This stores the date and time when the student submitted the assessment.
    -- It is recorded in India Standard Time (IST).
    submitted_at_ist TIMESTAMPTZ,
    
    -- This stores the date and time when our system received this data.
    -- It defaults to the current time in the India time zone.
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    
    -- This ensures that we don't have two records with the same event_id.
    -- It prevents duplicate data entries for the same event.
    UNIQUE (event_id)
);
