-- =============================================================================
-- SILVER TABLE: course_batch_merge
-- =============================================================================
-- This table is the "Master Report" for Power BI.
-- It combines the Course Catalogue (general info) with the Batches (class info).
--
-- Logic used:
-- 1. Excludes any batch named 'Test batch'.
-- 2. Calculates which batch is the "Latest" for each course.
-- 3. Sets the Final Status (Old batches are 'Completed', Newest use Catalogue Status).
-- 4. Includes courses with zero batches as 'Has_Batch = 0'.
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.course_batch_merge (
    -- Unique ID for each record
    id                                              SERIAL PRIMARY KEY,

    -- ── 1. CORE IDENTIFIERS ──────────────────────────────────────────────────
    -- Unique ID for the course bundle
    bundle_id                                       BIGINT,
    -- Name of the course bundle
    bundle_name                                     TEXT,
    -- Unique ID for the specific batch
    batch_id                                        BIGINT,
    -- Name of the specific batch
    batch_name                                      TEXT,

    -- ── 2. STATUS & FLAGS ────────────────────────────────────────────────────
    -- The final status used for reporting (Completed, Ongoing, or Upcoming)
    final_status                                    TEXT,
    -- The original status from the course catalogue
    catalogue_status                                TEXT,
    -- Flag: 1 if this is the newest batch for this course, 0 otherwise
    is_latest_batch                                 INTEGER,
    -- Flag: 1 if this row should be counted as a unique course in Power BI
    include_in_course_count                         INTEGER,
    -- Reason for any status changes (usually blank)
    status_adjustment_reason                        TEXT,
    -- Copy of the original status for compatibility
    status                                          TEXT,
    -- Status of the batch itself (Active, Archived, etc.)
    batch_status                                    TEXT,
    -- Flag: 1 if the course has a batch, 0 if it is an empty course entry
    has_batch                                       INTEGER,

    -- ── 3. DATES ─────────────────────────────────────────────────────────────
    -- Date the batch starts
    start_date                                      DATE,
    -- Date the batch ends
    end_date                                        DATE,

    -- ── 4. ENROLLMENT & TUTORS ───────────────────────────────────────────────
    -- Number of students in this specific batch
    batch_enrollment_count                          INTEGER,
    -- Total students in the entire course bundle
    bundle_enrollment_count                         INTEGER,
    -- Name of the teacher for this batch
    tutor_name                                      TEXT,
    -- ID number of the teacher
    tutor_id                                        BIGINT,

    -- ── 5. CLASSIFICATION ────────────────────────────────────────────────────
    -- Division (e.g., 'Course')
    course_division                                 TEXT,
    -- Category (e.g., 'Live', 'Recorded')
    type                                            TEXT,
    -- Main subject area
    subject                                         TEXT,
    -- Difficulty level
    level                                           TEXT,
    -- Language of instruction
    language                                        TEXT,
    -- SSS classification
    sss_category                                    TEXT,
    -- Adhyayanam classification
    adhyayanam_category                             TEXT,
    -- Target audience
    personas                                        TEXT,
    -- Marketing funnel position
    position_in_funnel                              TEXT,
    -- Duration term (e.g., '6 Months')
    term_of_course                                  TEXT,

    -- ── 6. ADDITIONAL DETAILS ────────────────────────────────────────────────
    -- Materials used
    texts                                           TEXT,
    -- Whether certificate is issued
    certificate                                     TEXT,
    -- Sponsoring organization
    course_sponsor                                  TEXT,
    -- Planned number of classes
    number_of_lectures                              TEXT,
    -- Total duration string
    duration                                        TEXT,
    -- Evaluation method
    computer_based_assessment                       TEXT,
    -- IDs of sub-courses
    course_ids                                      TEXT,

    -- ── 7. SYSTEM TRACKING ───────────────────────────────────────────────────
    -- When this record was created
    built_at                                        TIMESTAMPTZ DEFAULT NOW(),

    -- Ensure we don't have duplicate bundle-batch rows
    UNIQUE (bundle_id, batch_id)
);

-- Indexing for faster Power BI reports
CREATE INDEX IF NOT EXISTS idx_merge_bundle ON silver.course_batch_merge(bundle_id);
CREATE INDEX IF NOT EXISTS idx_merge_latest ON silver.course_batch_merge(is_latest_batch);
