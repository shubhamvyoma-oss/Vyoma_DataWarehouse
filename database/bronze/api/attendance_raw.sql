-- Raw attendance from report_type=55 (one row per student per class session)
-- API: GET /report/csv?report_type=55&start_time=<unix>&end_time=<unix>&response_type=1
-- studentAttendanceStatus: P=Present  A=Absent  -=not yet marked
-- Unique key: (student_id, class_id) -- one record per student per class session, updated on re-pull

CREATE TABLE IF NOT EXISTS bronze.attendance_raw (
    id                          SERIAL PRIMARY KEY,
    pull_date                   DATE        NOT NULL,
    student_id                  BIGINT,
    student_name                TEXT,
    reg_no                      TEXT,
    student_email               TEXT,
    student_contact             TEXT,
    student_batch_status        TEXT,
    batch_id                    BIGINT,
    batch_name                  TEXT,
    class_id                    BIGINT,
    class_name                  TEXT,
    bundle_id                   BIGINT,
    bundle_name                 TEXT,
    course_id                   BIGINT,
    course_name                 TEXT,
    attendance_id               BIGINT,
    session_name                TEXT,
    teacher_id                  BIGINT,
    teacher_name                TEXT,
    teacher_email               TEXT,
    teacher_class_signin_status TEXT,
    attendance_status           TEXT,       -- P / A / -
    class_date                  TEXT,       -- raw from API: "16 Mar 2026"
    class_date_parsed           DATE,       -- parsed DATE for joins/aggregation
    start_time                  TEXT,
    end_time                    TEXT,
    class_duration              TEXT,
    student_rating              INTEGER,
    student_comments            TEXT,
    raw_payload                 JSONB,
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (student_id, class_id)
);

CREATE INDEX IF NOT EXISTS idx_att_raw_pull_date    ON bronze.attendance_raw(pull_date);
CREATE INDEX IF NOT EXISTS idx_att_raw_batch        ON bronze.attendance_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_att_raw_bundle       ON bronze.attendance_raw(bundle_id);
CREATE INDEX IF NOT EXISTS idx_att_raw_class_date   ON bronze.attendance_raw(class_date_parsed);
