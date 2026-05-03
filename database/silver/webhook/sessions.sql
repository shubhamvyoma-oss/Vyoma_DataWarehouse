-- Table: silver.sessions
-- Purpose: one row per session instance; UPSERT on attendance_id; later events add fields without overwrite

CREATE TABLE IF NOT EXISTS silver.sessions (
    id                   SERIAL PRIMARY KEY,
    event_id             TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    attendance_id        BIGINT NOT NULL,
    class_id             BIGINT,
    class_name           TEXT,
    class_type_formatted TEXT,
    scheduled_start_ist  TIMESTAMPTZ,
    scheduled_end_ist    TIMESTAMPTZ,
    actual_start_ist     TIMESTAMPTZ,
    duration_minutes     INTEGER,
    teacher_id           BIGINT,
    teacher_name         TEXT,
    teacher_email        TEXT,
    master_batches       JSONB,
    schedule_id          BIGINT,
    is_recurring         BOOLEAN,
    virtual_platform     TEXT,
    zoom_meeting_id      TEXT,
    cancellation_reason  TEXT,
    cancelled_by         BIGINT,
    status               INTEGER,
    is_late_signin       BOOLEAN,
    delay_minutes        INTEGER,
    reminder_type        TEXT,
    received_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (attendance_id)
);
