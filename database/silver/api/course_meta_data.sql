-- =============================================================================
-- SILVER TABLE: course_meta_data
-- =============================================================================
-- This is the 'Master' table for reporting. It combines information from:
-- 1. Course Catalogue (Edmingle API)
-- 2. Course Batches (Edmingle API)
-- 3. Course Lifecycle Tracker (Manual MIS Spreadsheet)
--
-- This table is designed to be read directly by Power BI for dashboards.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.course_meta_data (
    -- Unique ID for each record
    id                                              SERIAL PRIMARY KEY,

    -- ── 1. CORE IDENTIFIERS ──────────────────────────────────────────────────
    -- Unique ID for the course bundle from Edmingle
    bundle_id                                       BIGINT,
    -- Unique ID for the specific batch from Edmingle
    batch_id                                        BIGINT,
    -- Name of the course bundle
    bundle_name                                     TEXT,
    -- Official name of the course
    course_name                                     TEXT,
    -- Name of the specific batch (e.g., 'Batch 1')
    batch_name                                      TEXT,

    -- ── 2. CLASSIFICATION & DETAILS ──────────────────────────────────────────
    -- Main subject area (e.g., Sanskrit, Vedanta)
    subject                                         TEXT,
    -- Difficulty level (e.g., Beginner, Advanced)
    level                                           TEXT,
    -- Primary language of instruction
    language                                        TEXT,
    -- Textbooks or study materials used
    texts                                           TEXT,
    -- Category of the course (e.g., 'Recorded', 'Live')
    type                                            TEXT,
    -- Organizational division
    course_division                                 TEXT,
    -- Whether a certificate is issued
    certificate                                     TEXT,
    -- Name of the organization sponsoring the course
    course_sponsor                                  TEXT,
    -- Current enrollment status (e.g., 'Ongoing', 'Completed')
    status                                          TEXT,
    -- Total number of lectures planned
    number_of_lectures                              TEXT,
    -- Total duration of the course (e.g., '6 months')
    duration                                        TEXT,
    -- Target audience categories
    personas                                        TEXT,
    -- SSS classification category
    sss_category                                    TEXT,
    -- Adhyayanam classification category
    adhyayanam_category                             TEXT,
    -- Term or season of the course
    term_of_course                                  TEXT,
    -- Position in the enrollment funnel
    position_in_funnel                              TEXT,

    -- ── 3. SCHEDULE & ATTENDANCE ─────────────────────────────────────────────
    -- Batch start date
    start_date                                      TIMESTAMPTZ,
    -- Batch end date
    end_date                                        TIMESTAMPTZ,
    -- Number of classes held per week
    classes_per_week                                TEXT,
    -- Specific days classes are held
    class_days                                      TEXT,
    -- Timing of classes in IST
    class_timings                                   TEXT,
    -- Additional teachers (Names and Profile Links)
    additional_teacher                              TEXT,
    -- Early Learning Assessment status
    ela                                             TEXT,
    -- Internal Employee ID associated with the course
    employee_id                                     TEXT,
    -- List of panelists involved in the course
    panelists                                       TEXT,

    -- ── 4. ENROLLMENT METRICS ────────────────────────────────────────────────
    -- Total students currently enrolled in Edmingle
    num_students                                    INTEGER,
    -- Total admitted students (from MIS)
    admitted_students                               INTEGER,
    -- Date the course was launched
    launch_date                                     DATE,
    -- Number of enrollments exactly one day after launch
    enrollments_after_launch                        INTEGER,
    -- Date of the first class session
    first_class_date                                DATE,
    -- Number of enrollments on the day of the first class
    enrollments_on_first_class                      INTEGER,
    -- Attendance count for the first class
    first_class_attendance                          INTEGER,
    -- Attendance count for the second class
    second_class_attendance                         INTEGER,
    -- Date of the last class or valedictory session
    last_class_date                                 DATE,
    -- Number of enrollments on the last day of class
    enrollments_on_last_day                         INTEGER,
    -- Attendance count for the last class
    last_class_attendance                           INTEGER,

    -- ── 5. PERFORMANCE & VOLUME ──────────────────────────────────────────────
    -- Total number of class sessions held
    total_classes_held                              INTEGER,
    -- Total duration of all classes in hours
    total_class_hours                               NUMERIC(10,2),
    -- Average attendance percentage across all classes
    avg_attendance                                  NUMERIC(5,2),
    -- Tutors (Names)
    tutors                                          TEXT,
    -- Tutor IDs
    tutor_ids                                       TEXT,
    -- Course IDs included in this bundle
    course_ids                                      TEXT,

    -- ── 6. ASSESSMENT & CERTIFICATION ────────────────────────────────────────
    -- Method of evaluation (e.g., 'Exam', 'Project')
    assessment_type                                 TEXT,
    -- When the final assessment started
    assessment_start_date                           DATE,
    -- When the final assessment ended
    assessment_end_date                             DATE,
    -- Total number of students who attempted the assessment
    total_assessment_attendees                      INTEGER,
    -- Total students who were successfully certified
    total_certified                                 INTEGER,

    -- ── 7. KPI RATIOS (Calculated Metrics) ───────────────────────────────────
    -- Ratio of certified students vs initial enrollments
    cert_vs_initial_enroll                          NUMERIC(5,2),
    -- Ratio of certified students vs end-of-course enrollments
    cert_vs_end_enroll                              NUMERIC(5,2),
    -- Ratio of certified students vs first class attendees
    cert_vs_first_class_attend                      NUMERIC(5,2),
    -- Ratio of certified students vs average attendees
    cert_vs_avg_attend                              NUMERIC(5,2),
    -- Ratio of first class attendance vs initial enrollments
    first_class_attend_vs_initial                   NUMERIC(5,2),
    -- Ratio of first class attendance vs last class attendance
    first_class_attend_vs_last                      NUMERIC(5,2),
    -- Pass percentage (Certified / Assessment Attendees)
    pass_pct_cert_vs_attendees                      NUMERIC(5,2),
    -- Pass percentage (Last class count / Certified)
    pass_pct_students_vs_cert                       NUMERIC(5,2),

    -- ── 8. RATINGS & FEEDBACK ────────────────────────────────────────────────
    -- Overall student rating for the course
    overall_rating                                  NUMERIC(3,2),
    -- Average rating: Ease of attending live webinars
    avg_rating_ease                                 NUMERIC(3,2),
    -- Average rating: Quality of content and materials
    avg_rating_quality                              NUMERIC(3,2),
    -- Average rating for the teacher
    avg_teacher_rating                              NUMERIC(3,2),
    -- Average rating: Accessing materials on website
    avg_rating_access                               NUMERIC(3,2),
    -- Average ELA rating given by teacher
    avg_ela_rating                                  NUMERIC(3,2),
    -- Average content support rating given by teacher
    avg_content_support_rating                      NUMERIC(3,2),

    -- ── 9. SYSTEM TRACKING ───────────────────────────────────────────────────
    -- Flag: 1 if this is the most recent batch for this course
    is_latest_batch                                 SMALLINT DEFAULT 0,
    -- Flag: 1 if this course should be counted in organization-wide totals
    include_in_course_count                         SMALLINT DEFAULT 0,
    -- Flag: 1 if this course has a valid batch attached
    has_batch                                       SMALLINT DEFAULT 1,
    -- When this record was created
    created_at                                      TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure each bundle and batch combination is only stored once
    UNIQUE (bundle_id, batch_id)
);

-- Add indexes to speed up Power BI filtering
CREATE INDEX IF NOT EXISTS idx_meta_bundle ON silver.course_meta_data(bundle_id);
CREATE INDEX IF NOT EXISTS idx_meta_batch ON silver.course_meta_data(batch_id);
CREATE INDEX IF NOT EXISTS idx_meta_subject ON silver.course_meta_data(subject);
