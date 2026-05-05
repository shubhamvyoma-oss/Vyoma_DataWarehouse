-- =============================================================================
-- BRONZE TABLE: attendance_raw
-- =============================================================================
-- This table stores raw attendance data. It records one row for every student 
-- in every class session. It is the "Digital Filing Cabinet" for raw attendance.
-- =============================================================================

-- Create the table to store raw attendance data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.attendance_raw (
    -- Internal ID for the database to keep track of each row.
    id                          SERIAL PRIMARY KEY,
    
    -- The date when this data was fetched from the source.
    pull_date                   DATE        NOT NULL,
    
    -- The unique ID of the student.
    student_id                  BIGINT,
    
    -- The full name of the student.
    student_name                TEXT,
    
    -- The registration number of the student.
    reg_no                      TEXT,
    
    -- The email address of the student.
    student_email               TEXT,
    
    -- The contact phone number of the student.
    student_contact             TEXT,
    
    -- The current status of the student in the batch (e.g., Active).
    student_batch_status        TEXT,
    
    -- The unique ID of the batch.
    batch_id                    BIGINT,
    
    -- The name of the batch.
    batch_name                  TEXT,
    
    -- The unique ID of the specific class session.
    class_id                    BIGINT,
    
    -- The name of the class.
    class_name                  TEXT,
    
    -- The unique ID of the bundle (group of courses).
    bundle_id                   BIGINT,
    
    -- The name of the bundle.
    bundle_name                 TEXT,
    
    -- The unique ID of the course.
    course_id                   BIGINT,
    
    -- The name of the course.
    course_name                 TEXT,
    
    -- The unique ID for this specific attendance record.
    attendance_id               BIGINT,
    
    -- The name of the session.
    session_name                TEXT,
    
    -- The unique ID of the teacher.
    teacher_id                  BIGINT,
    
    -- The name of the teacher.
    teacher_name                TEXT,
    
    -- The email address of the teacher.
    teacher_email               TEXT,
    
    -- Status of whether the teacher signed into the class.
    teacher_class_signin_status TEXT,
    
    -- The student's attendance status (P = Present, A = Absent, - = Not Marked).
    attendance_status           TEXT,
    
    -- The date of the class as a simple text string (e.g., "16 Mar 2026").
    class_date                  TEXT,
    
    -- The date of the class converted into a proper DATE format for sorting.
    class_date_parsed           DATE,
    
    -- The time the class started.
    start_time                  TEXT,
    
    -- The time the class ended.
    end_time                    TEXT,
    
    -- How long the class lasted.
    class_duration              TEXT,
    
    -- The rating the student gave (as a whole number).
    student_rating              INTEGER,
    
    -- Any comments left by the student.
    student_comments            TEXT,
    
    -- The entire original data message in JSON format.
    raw_payload                 JSONB,
    
    -- The exact time this row was added to our database.
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't have the same student listed twice for the same class.
    UNIQUE (student_id, class_id)
);

-- Index to help the database find data by pull date quickly.
CREATE INDEX IF NOT EXISTS idx_att_raw_pull_date    ON bronze.attendance_raw(pull_date);

-- Index to help the database find data by batch ID quickly.
CREATE INDEX IF NOT EXISTS idx_att_raw_batch        ON bronze.attendance_raw(batch_id);

-- Index to help the database find data by bundle ID quickly.
CREATE INDEX IF NOT EXISTS idx_att_raw_bundle       ON bronze.attendance_raw(bundle_id);

-- Index to help the database find data by the parsed class date quickly.
CREATE INDEX IF NOT EXISTS idx_att_raw_class_date   ON bronze.attendance_raw(class_date_parsed);
