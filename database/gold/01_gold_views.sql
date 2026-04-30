-- Gold layer: SQL views over Silver tables for Power BI
-- All views filter out Vyoma staff (email NOT LIKE '%@vyoma%')
-- Run after all Silver tables are populated

CREATE SCHEMA IF NOT EXISTS gold;

-- ── VIEW 1: gold.course_summary ───────────────────────────────────────────────
-- One row per course-batch with all dimensions + enrollment counts
CREATE OR REPLACE VIEW gold.course_summary AS
SELECT
    cm.bundle_id,
    cm.course_name,
    cm.course_type,
    cm.status,
    cm.subject,
    cm.term_of_course,
    cm.position_in_funnel,
    cm.adhyayanam_category,
    cm.sss_category,
    cm.viniyoga,
    cm.division,
    cl.type_of_launch,
    cl.first_class_date,
    cl.last_class_date,
    cl.enrollments_on_fc,
    cl.enrollments_on_lc,
    cl.avg_attendance,
    cl.total_certified,
    cl.overall_rating,
    COUNT(DISTINCT t.user_id)                                          AS total_enrollments,
    COUNT(DISTINCT CASE WHEN t.source = 'webhook'     THEN t.user_id END) AS live_enrollments,
    COUNT(DISTINCT CASE WHEN t.source = 'csv.import'  THEN t.user_id END) AS historical_enrollments
FROM silver.course_metadata cm
LEFT JOIN silver.course_lifecycle cl
    ON cm.bundle_id = cl.course_id
LEFT JOIN silver.transactions t
    ON cm.bundle_id = t.bundle_id
   AND t.email NOT LIKE '%@vyoma%'
GROUP BY
    cm.bundle_id, cm.course_name, cm.course_type, cm.status,
    cm.subject, cm.term_of_course, cm.position_in_funnel,
    cm.adhyayanam_category, cm.sss_category, cm.viniyoga,
    cm.division, cl.type_of_launch, cl.first_class_date,
    cl.last_class_date, cl.enrollments_on_fc, cl.enrollments_on_lc,
    cl.avg_attendance, cl.total_certified, cl.overall_rating;


-- ── VIEW 2: gold.learner_summary ─────────────────────────────────────────────
-- New vs returning learners per course.
-- New = this enrollment is the student's very first across all courses.
-- Returning = student enrolled in at least one other course before this one.
CREATE OR REPLACE VIEW gold.learner_summary AS
WITH first_enrollment AS (
    SELECT user_id, MIN(start_date_ist) AS first_ever_enrollment
    FROM silver.transactions
    WHERE email NOT LIKE '%@vyoma%'
    GROUP BY user_id
)
SELECT
    t.bundle_id,
    cm.course_name,
    cm.course_type,
    cm.position_in_funnel,
    COUNT(DISTINCT t.user_id)                                                         AS total_learners,
    COUNT(DISTINCT CASE WHEN t.start_date_ist = fe.first_ever_enrollment THEN t.user_id END) AS new_learners,
    COUNT(DISTINCT CASE WHEN t.start_date_ist > fe.first_ever_enrollment  THEN t.user_id END) AS returning_learners
FROM silver.transactions t
JOIN first_enrollment fe ON t.user_id = fe.user_id
LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
WHERE t.email NOT LIKE '%@vyoma%'
GROUP BY t.bundle_id, cm.course_name, cm.course_type, cm.position_in_funnel;


-- ── VIEW 3: gold.course_type_summary ─────────────────────────────────────────
-- Categorisation by type of launch dashboard page
CREATE OR REPLACE VIEW gold.course_type_summary AS
SELECT
    course_type,
    status,
    COUNT(DISTINCT bundle_id) AS total_courses,
    SUM(total_enrollments)    AS total_enrollments,
    SUM(enrollments_on_fc)    AS enrollments_on_fc,
    SUM(enrollments_on_lc)    AS enrollments_on_lc
FROM gold.course_summary
GROUP BY course_type, status;


-- ── VIEW 4: gold.launch_type_summary ─────────────────────────────────────────
-- Repeat / Reopen / Relaunch dashboard page
CREATE OR REPLACE VIEW gold.launch_type_summary AS
SELECT
    type_of_launch,
    COUNT(DISTINCT bundle_id) AS total_courses,
    SUM(total_enrollments)    AS total_enrollments
FROM gold.course_summary
WHERE type_of_launch IS NOT NULL
GROUP BY type_of_launch;


-- ── VIEW 5: gold.subject_summary ─────────────────────────────────────────────
-- Subject category dashboard
CREATE OR REPLACE VIEW gold.subject_summary AS
SELECT
    subject,
    status,
    COUNT(DISTINCT bundle_id) AS total_courses,
    SUM(total_enrollments)    AS total_enrollments,
    SUM(enrollments_on_fc)    AS enrollments_on_fc,
    SUM(enrollments_on_lc)    AS enrollments_on_lc
FROM gold.course_summary
WHERE subject IS NOT NULL
GROUP BY subject, status;


-- ── VIEW 6: gold.learning_model_summary ──────────────────────────────────────
-- Vyoma pyramid: Bhashadhyayanam / Granthadhyayanam / Shastradhyayanam / Viniyoga
CREATE OR REPLACE VIEW gold.learning_model_summary AS
SELECT
    COALESCE(
        CASE WHEN viniyoga = 'True' THEN 'Viniyoga' END,
        adhyayanam_category
    )                         AS learning_model,
    COUNT(DISTINCT bundle_id) AS total_courses,
    SUM(total_enrollments)    AS total_enrollments
FROM gold.course_summary
GROUP BY 1;


-- ── VIEW 7: gold.term_summary ─────────────────────────────────────────────────
-- Term of Course dashboard (Very Short / Short / Mid / Long)
CREATE OR REPLACE VIEW gold.term_summary AS
SELECT
    gs.term_of_course,
    COUNT(DISTINCT gs.bundle_id)                    AS total_courses,
    SUM(gs.total_enrollments)                       AS total_enrollments,
    SUM(ls.new_learners)                            AS new_learners,
    SUM(ls.returning_learners)                      AS returning_learners,
    SUM(ls.new_learners) + SUM(ls.returning_learners) AS individual_learner_count
FROM gold.course_summary gs
LEFT JOIN gold.learner_summary ls USING (bundle_id)
GROUP BY gs.term_of_course;


-- ── VIEW 8: gold.funnel_summary ───────────────────────────────────────────────
-- Position in Funnel dashboard (Bottom / Lower Middle / Middle / Upper Middle / Top)
CREATE OR REPLACE VIEW gold.funnel_summary AS
SELECT
    gs.position_in_funnel,
    COUNT(DISTINCT gs.bundle_id)                    AS total_courses,
    SUM(gs.total_enrollments)                       AS total_enrollments,
    SUM(ls.new_learners)                            AS new_learners,
    SUM(ls.returning_learners)                      AS returning_learners,
    SUM(ls.new_learners) + SUM(ls.returning_learners) AS individual_learner_count
FROM gold.course_summary gs
LEFT JOIN gold.learner_summary ls USING (bundle_id)
GROUP BY gs.position_in_funnel;


-- ── VIEW 9: gold.sss_domain_summary ──────────────────────────────────────────
-- SSS domain Venn diagram (Samskrta / Samskara / Samskriti)
CREATE OR REPLACE VIEW gold.sss_domain_summary AS
SELECT
    gs.sss_category,
    COUNT(DISTINCT gs.bundle_id)                    AS total_courses,
    SUM(gs.total_enrollments)                       AS total_enrollments,
    SUM(ls.new_learners)                            AS new_learners,
    SUM(ls.returning_learners)                      AS returning_learners,
    SUM(ls.new_learners) + SUM(ls.returning_learners) AS individual_learner_count
FROM gold.course_summary gs
LEFT JOIN gold.learner_summary ls USING (bundle_id)
WHERE gs.sss_category IS NOT NULL
GROUP BY gs.sss_category;
