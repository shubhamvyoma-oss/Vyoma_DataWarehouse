-- =============================================================================
-- SILVER TABLE: course_catalogue
-- =============================================================================
-- This is the 'Master Bundle' table. It stores unique course bundles.
-- =============================================================================

DROP TABLE IF EXISTS silver.course_catalogue CASCADE;

CREATE TABLE silver.course_catalogue (
    id                                              SERIAL PRIMARY KEY,
    bundle_id                                       BIGINT UNIQUE,
    course_name                                     TEXT,
    subject                                         TEXT,
    level                                           TEXT,
    language                                        TEXT,
    texts                                           TEXT,
    type                                            TEXT, -- also referred to as course_type
    course_type                                     TEXT,
    course_division                                 TEXT,
    division                                        TEXT, -- also referred to as course_division
    viniyoga                                        TEXT,
    certificate                                     TEXT,
    course_sponsor                                  TEXT,
    status                                          TEXT,
    number_of_lectures                              TEXT,
    duration                                        TEXT,
    personas                                        TEXT,
    sss_category                                    TEXT,
    adhyayanam_category                             TEXT,
    term_of_course                                  TEXT,
    position_in_funnel                              TEXT,
    num_students                                    INTEGER,
    course_ids                                      TEXT,
    imported_at                                     TIMESTAMPTZ DEFAULT NOW(),
    created_at                                      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_catalogue_bundle ON silver.course_catalogue(bundle_id);
CREATE INDEX IF NOT EXISTS idx_catalogue_subject ON silver.course_catalogue(subject);

