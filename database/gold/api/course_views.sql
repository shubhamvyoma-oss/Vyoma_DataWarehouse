-- This file creates reporting views for courses and learners.
-- These views are used for dashboards to see course performance and student growth.

-- Ensuring the gold schema exists before creating views in it.
CREATE SCHEMA IF NOT EXISTS gold;

-- ── VIEW 1: gold.course ───────────────────────────────────────────────
-- This view provides a detailed list of courses with their metadata and enrollment counts.
CREATE OR REPLACE VIEW gold.course AS
SELECT
    -- Unique ID for the course bundle.
    course_metadata_table.bundle_id,
    -- The full name of the course.
    course_metadata_table.course_name,
    -- The category or type of the course.
    course_metadata_table.course_type,
    -- The current status (e.g., Active, Completed).
    course_metadata_table.status,
    -- The subject area of the course.
    course_metadata_table.subject,
    -- How long the course lasts (e.g., Short, Long).
    course_metadata_table.term_of_course,
    -- Where this course sits in the marketing funnel.
    course_metadata_table.position_in_funnel,
    -- Category related to Adhyayanam studies.
    course_metadata_table.adhyayanam_category,
    -- Category related to SSS (Samskrta, Samskara, Samskriti).
    course_metadata_table.sss_category,
    -- Whether the course is for Viniyoga.
    course_metadata_table.viniyoga,
    -- The division the course belongs to.
    course_metadata_table.division,
    -- How the course was launched (e.g., Repeat, New).
    course_lifecycle_table.type_of_launch,
    -- Date of the first class held.
    course_lifecycle_table.first_class_date,
    -- Date of the last class held.
    course_lifecycle_table.last_class_date,
    -- Number of students enrolled by the time the first class started.
    course_lifecycle_table.enrollments_on_fc AS enrollments_on_first_class,
    -- Number of students enrolled by the time the last class started.
    course_lifecycle_table.enrollments_on_lc AS enrollments_on_last_class,
    -- The average attendance across all classes.
    course_lifecycle_table.avg_attendance AS average_attendance,
    -- Total number of students who received certificates.
    course_lifecycle_table.total_certified AS total_students_certified,
    -- The overall rating given to the course by students.
    course_lifecycle_table.overall_rating,
    -- Counting unique users who enrolled in this course.
    -- We exclude staff emails ending in '@vyoma'.
    COUNT(DISTINCT transactions_table.user_id) AS total_enrollments_count,
    -- Counting unique users who enrolled via webhooks (live data).
    COUNT(DISTINCT CASE WHEN transactions_table.source = 'webhook' THEN transactions_table.user_id ELSE NULL END) AS live_enrollments_count,
    -- Counting unique users who were imported from historical CSV files.
    COUNT(DISTINCT CASE WHEN transactions_table.source = 'csv.import' THEN transactions_table.user_id ELSE NULL END) AS historical_enrollments_count
-- Starting with the main course metadata table.
FROM silver.course_metadata AS course_metadata_table
-- Joining with course lifecycle to get dates and performance metrics.
LEFT JOIN silver.course_lifecycle AS course_lifecycle_table
    ON course_metadata_table.bundle_id = course_lifecycle_table.course_id
-- Joining with transactions to count students.
LEFT JOIN silver.transactions AS transactions_table
    ON course_metadata_table.bundle_id = transactions_table.bundle_id
   -- Excluding staff members from the counts.
   AND transactions_table.email NOT LIKE '%@vyoma%'
-- Grouping by all descriptive columns so the counts work for each course.
GROUP BY
    course_metadata_table.bundle_id, 
    course_metadata_table.course_name, 
    course_metadata_table.course_type, 
    course_metadata_table.status,
    course_metadata_table.subject, 
    course_metadata_table.term_of_course, 
    course_metadata_table.position_in_funnel,
    course_metadata_table.adhyayanam_category, 
    course_metadata_table.sss_category, 
    course_metadata_table.viniyoga,
    course_metadata_table.division, 
    course_lifecycle_table.type_of_launch, 
    course_lifecycle_table.first_class_date,
    course_lifecycle_table.last_class_date, 
    course_lifecycle_table.enrollments_on_fc, 
    course_lifecycle_table.enrollments_on_lc,
    course_lifecycle_table.avg_attendance, 
    course_lifecycle_table.total_certified, 
    course_lifecycle_table.overall_rating;


-- ── VIEW 2: gold.learner ─────────────────────────────────────────────
-- This view helps distinguish between brand new students and returning students for each course.
CREATE OR REPLACE VIEW gold.learner AS
-- This subquery (CTE) finds the very first enrollment date for every student.
WITH first_enrollment_data AS (
    SELECT 
        user_id, 
        MIN(start_date_ist) AS first_ever_enrollment_date
    FROM silver.transactions
    -- Excluding staff members.
    WHERE email NOT LIKE '%@vyoma%'
    GROUP BY user_id
)
SELECT
    -- Unique ID for the course bundle.
    transactions_table.bundle_id,
    -- The name of the course.
    course_metadata_table.course_name,
    -- The category of the course.
    course_metadata_table.course_type,
    -- Marketing funnel position.
    course_metadata_table.position_in_funnel,
    -- Total unique students in this course.
    COUNT(DISTINCT transactions_table.user_id) AS total_learners_count,
    -- Counting students for whom this course started on their very first enrollment date.
    COUNT(DISTINCT CASE WHEN transactions_table.start_date_ist = first_enrollment_data.first_ever_enrollment_date THEN transactions_table.user_id ELSE NULL END) AS new_learners_count,
    -- Counting students who had already enrolled in something else before this course.
    COUNT(DISTINCT CASE WHEN transactions_table.start_date_ist > first_enrollment_data.first_ever_enrollment_date THEN transactions_table.user_id ELSE NULL END) AS returning_learners_count
-- Starting with the transactions table.
FROM silver.transactions AS transactions_table
-- Joining with our subquery to know each student's history.
JOIN first_enrollment_data 
    ON transactions_table.user_id = first_enrollment_data.user_id
-- Joining with course metadata to get course names and types.
LEFT JOIN silver.course_metadata AS course_metadata_table 
    ON transactions_table.bundle_id = course_metadata_table.bundle_id
-- Excluding staff members.
WHERE transactions_table.email NOT LIKE '%@vyoma%'
-- Grouping to get counts per course.
GROUP BY 
    transactions_table.bundle_id, 
    course_metadata_table.course_name, 
    course_metadata_table.course_type, 
    course_metadata_table.position_in_funnel;


-- ── VIEW 3: gold.course_type ─────────────────────────────────────────
-- This view summarizes performance metrics by course category.
CREATE OR REPLACE VIEW gold.course_type AS
SELECT
    -- The type or category of the course.
    course_type,
    -- Whether the course is currently active or finished.
    status,
    -- Counting how many unique courses fall into this category.
    COUNT(DISTINCT bundle_id) AS total_courses_count,
    -- Total student enrollments in this category.
    SUM(total_enrollments_count) AS total_enrollments_sum,
    -- Total enrollments recorded by the first class date.
    SUM(enrollments_on_first_class) AS total_enrollments_on_first_class,
    -- Total enrollments recorded by the last class date.
    SUM(enrollments_on_last_class) AS total_enrollments_on_last_class
-- Using the gold.course view we created earlier.
FROM gold.course
-- Grouping by type and status.
GROUP BY course_type, status;


-- ── VIEW 4: gold.launch_type ─────────────────────────────────────────
-- This view summarizes enrollment data based on how courses were launched.
CREATE OR REPLACE VIEW gold.launch_type AS
SELECT
    -- The type of launch (e.g., Relaunch, Repeat).
    type_of_launch,
    -- Counting unique courses for each launch type.
    COUNT(DISTINCT bundle_id) AS total_courses_count,
    -- Total student enrollments for each launch type.
    SUM(total_enrollments_count) AS total_enrollments_sum
-- Using the gold.course view.
FROM gold.course
-- We only care about courses that have a launch type specified.
WHERE type_of_launch IS NOT NULL
-- Grouping by launch type.
GROUP BY type_of_launch;


-- ── VIEW 5: gold.subject ─────────────────────────────────────────────
-- This view summarizes enrollment data by the subject matter of the courses.
CREATE OR REPLACE VIEW gold.subject AS
SELECT
    -- The subject area (e.g., Vyakarana, Vedanta).
    subject,
    -- Current status of the courses.
    status,
    -- Counting unique courses per subject.
    COUNT(DISTINCT bundle_id) AS total_courses_count,
    -- Total enrollments per subject.
    SUM(total_enrollments_count) AS total_enrollments_sum,
    -- Enrollments on first class per subject.
    SUM(enrollments_on_first_class) AS total_enrollments_on_first_class,
    -- Enrollments on last class per subject.
    SUM(enrollments_on_last_class) AS total_enrollments_on_last_class
-- Using the gold.course view.
FROM gold.course
-- Only including rows where a subject is actually defined.
WHERE subject IS NOT NULL
-- Grouping by subject and status.
GROUP BY subject, status;


-- ── VIEW 6: gold.learning_model ──────────────────────────────────────
-- This view categorizes courses into Vyoma's learning models (Bhasha, Granths, etc.).
CREATE OR REPLACE VIEW gold.learning_model AS
SELECT
    -- Deciding the learning model name. 
    -- If viniyoga is 'True', we call it 'Viniyoga'. 
    -- Otherwise, we use the adhyayanam_category.
    COALESCE(
        CASE WHEN viniyoga = 'True' THEN 'Viniyoga' ELSE NULL END,
        adhyayanam_category
    ) AS learning_model_name,
    -- Counting unique courses in each model.
    COUNT(DISTINCT bundle_id) AS total_courses_count,
    -- Total enrollments in each model.
    SUM(total_enrollments_count) AS total_enrollments_sum
-- Using the gold.course view.
FROM gold.course
-- Grouping by the calculated model name.
GROUP BY 1;


-- ── VIEW 7: gold.term ─────────────────────────────────────────────────
-- This view summarizes enrollments and learner types based on course duration.
CREATE OR REPLACE VIEW gold.term AS
SELECT
    -- The duration term (e.g., Short Term, Mid Term).
    course_summaries.term_of_course,
    -- Counting unique courses for this term.
    COUNT(DISTINCT course_summaries.bundle_id) AS total_courses_count,
    -- Total enrollments (can include same student in multiple courses).
    SUM(course_summaries.total_enrollments_count) AS total_enrollments_sum,
    -- Total brand new students.
    SUM(learner_summaries.new_learners_count) AS total_new_learners,
    -- Total students who have been here before.
    SUM(learner_summaries.returning_learners_count) AS total_returning_learners,
    -- Total individual students (New + Returning).
    SUM(learner_summaries.new_learners_count) + SUM(learner_summaries.returning_learners_count) AS total_individual_learner_count
-- Joining the course view and learner view together.
FROM gold.course AS course_summaries
LEFT JOIN gold.learner AS learner_summaries 
    ON course_summaries.bundle_id = learner_summaries.bundle_id
-- Grouping by the duration term.
GROUP BY course_summaries.term_of_course;


-- ── VIEW 8: gold.funnel ───────────────────────────────────────────────
-- This view shows where students are entering based on the marketing funnel position.
CREATE OR REPLACE VIEW gold.funnel AS
SELECT
    -- Position in the funnel (e.g., Top, Bottom).
    course_summaries.position_in_funnel,
    -- Counting unique courses at this funnel level.
    COUNT(DISTINCT course_summaries.bundle_id) AS total_courses_count,
    -- Total enrollments.
    SUM(course_summaries.total_enrollments_count) AS total_enrollments_sum,
    -- Total brand new students.
    SUM(learner_summaries.new_learners_count) AS total_new_learners,
    -- Total students who have been here before.
    SUM(learner_summaries.returning_learners_count) AS total_returning_learners,
    -- Total individual students.
    SUM(learner_summaries.new_learners_count) + SUM(learner_summaries.returning_learners_count) AS total_individual_learner_count
-- Joining course and learner views.
FROM gold.course AS course_summaries
LEFT JOIN gold.learner AS learner_summaries 
    ON course_summaries.bundle_id = learner_summaries.bundle_id
-- Grouping by funnel position.
GROUP BY course_summaries.position_in_funnel;


-- ── VIEW 9: gold.sss_domain ──────────────────────────────────────────
-- This view summarizes performance for the SSS domain categories.
CREATE OR REPLACE VIEW gold.sss_domain AS
SELECT
    -- The SSS category (Samskrta, Samskara, or Samskriti).
    course_summaries.sss_category,
    -- Counting unique courses.
    COUNT(DISTINCT course_summaries.bundle_id) AS total_courses_count,
    -- Total enrollments.
    SUM(course_summaries.total_enrollments_count) AS total_enrollments_sum,
    -- Total new students.
    SUM(learner_summaries.new_learners_count) AS total_new_learners,
    -- Total returning students.
    SUM(learner_summaries.returning_learners_count) AS total_returning_learners,
    -- Total individual students.
    SUM(learner_summaries.new_learners_count) + SUM(learner_summaries.returning_learners_count) AS total_individual_learner_count
-- Joining course and learner views.
FROM gold.course AS course_summaries
LEFT JOIN gold.learner AS learner_summaries 
    ON course_summaries.bundle_id = learner_summaries.bundle_id
-- Only including rows where SSS category is defined.
WHERE course_summaries.sss_category IS NOT NULL
-- Grouping by SSS category.
GROUP BY course_summaries.sss_category;
