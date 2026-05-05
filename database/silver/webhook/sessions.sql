-- This table stores clean information about each individual class session (live classes).
-- It combines multiple events for the same class (like when it's scheduled and when it starts).
CREATE TABLE IF NOT EXISTS silver.sessions (
    -- This is a unique number for each session record that increases automatically.
    -- It identifies each row in this table uniquely.
    id                   SERIAL PRIMARY KEY,

    -- This is the unique ID for the event that created or updated this session.
    -- Links the record back to the original webhook event.
    event_id             TEXT NOT NULL,

    -- This tells us the type of event (like 'session_started' or 'session_cancelled').
    -- Explains the most recent update to this session record.
    event_type           TEXT NOT NULL,

    -- This is the unique ID for the session used for tracking attendance.
    -- We use this to make sure we don't have duplicate records for the same session.
    attendance_id        BIGINT NOT NULL,

    -- This is the ID of the specific class in the course system.
    -- Links this session to a specific course and batch.
    class_id             BIGINT,

    -- This is the name of the class session.
    -- For example: 'Introduction to Sanskrit Grammar - Part 1'.
    class_name           TEXT,

    -- This describes the type of class in a readable way.
    -- For example: 'Live Online' or 'Recorded Session'.
    class_type_formatted TEXT,

    -- This is the date and time the class was supposed to start, in IST.
    -- Used to measure punctuality and scheduling accuracy.
    scheduled_start_ist  TIMESTAMPTZ,

    -- This is the date and time the class was supposed to end, in IST.
    -- Helps us track the planned length of the session.
    scheduled_end_ist    TIMESTAMPTZ,

    -- This is the actual date and time the class started, in IST.
    -- Used to check if the teacher started the class on time.
    actual_start_ist     TIMESTAMPTZ,

    -- This is how many minutes the class actually lasted.
    -- Calculated based on when the class started and ended.
    duration_minutes     INTEGER,

    -- This is the unique ID of the teacher who conducted the class.
    -- Links the session to a specific staff member.
    teacher_id           BIGINT,

    -- This is the full name of the teacher for this session.
    -- Useful for quick reports on teacher performance.
    teacher_name         TEXT,

    -- This is the email address of the teacher.
    -- Used for contacting the teacher regarding this specific session.
    teacher_email        TEXT,

    -- This stores a list of batches that were supposed to attend this session.
    -- It is stored in a flexible JSON format to handle multiple batches easily.
    master_batches       JSONB,

    -- This is the ID of the schedule this session belongs to.
    -- Helps group sessions that are part of the same recurring timetable.
    schedule_id          BIGINT,

    -- This is true if the class happens regularly (like every Monday).
    -- False if it's a one-time special session.
    is_recurring         BOOLEAN,

    -- This tells us which platform was used (like 'Zoom' or 'Google Meet').
    -- Helps us track technical usage patterns.
    virtual_platform     TEXT,

    -- This is the specific meeting ID for Zoom calls.
    -- Useful for troubleshooting meeting links or technical issues.
    zoom_meeting_id      TEXT,

    -- If the class was cancelled, this explains why.
    -- Helps us analyze the most common reasons for class cancellations.
    cancellation_reason  TEXT,

    -- This is the ID of the person who cancelled the class.
    -- Used for accountability in scheduling changes.
    cancelled_by         BIGINT,

    -- This is a number representing the current status of the session.
    -- Different numbers mean things like 'Scheduled', 'Live', or 'Completed'.
    status               INTEGER,

    -- This is true if the student signed into the class after it already started.
    -- Helps us identify students who are frequently late.
    is_late_signin       BOOLEAN,

    -- This counts how many minutes late the student was to the class.
    -- Used for precise punctuality tracking.
    delay_minutes        INTEGER,

    -- This tells us if a reminder was sent and what type it was (like 'Email' or 'SMS').
    -- Helps us see if reminders improve class attendance.
    reminder_type        TEXT,

    -- This is the exact time our system received this session data.
    -- It defaults to the current time in the India/Kolkata timezone.
    received_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- We ensure that each 'attendance_id' appears only once in this table.
    -- If we get new information about a session, we update the existing row.
    UNIQUE (attendance_id)
);
