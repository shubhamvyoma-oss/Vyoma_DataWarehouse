-- Table: silver.course_lifecycle
-- Purpose: one row per course-batch with launch metrics; UPSERT on (course_id, batch_name); source is MIS tracker CSV

CREATE TABLE IF NOT EXISTS silver.course_lifecycle (
    id                  SERIAL PRIMARY KEY,
    course_id           BIGINT,
    course_name         TEXT,
    batch_name          TEXT,
    type_of_launch      TEXT,
    status              TEXT,
    subject             TEXT,
    position_in_funnel  TEXT,
    learning_model      TEXT,
    term_of_course      TEXT,
    sss_category        TEXT,
    persona             TEXT,
    first_class_date    DATE,
    last_class_date     DATE,
    enrollments_on_fc   INTEGER,
    enrollments_on_lc   INTEGER,
    avg_attendance      NUMERIC(8,2),
    total_classes_held  INTEGER,
    total_certified     INTEGER,
    pass_percentage     NUMERIC(5,2),
    overall_rating      NUMERIC(3,2),
    batch_id            BIGINT,
    imported_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (course_id, batch_name)
);
