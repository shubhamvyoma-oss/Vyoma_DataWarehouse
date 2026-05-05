-- This file creates views for attendance reporting.
-- These views help show how many students attend classes across different courses and batches.

-- CREATE OR REPLACE VIEW is used to create a new view or update it if it already exists.
-- A view is like a saved query that you can treat like a table.
CREATE OR REPLACE VIEW gold.batch_attendance AS
-- We are selecting information from several tables to get a complete picture of batch attendance.
SELECT
    -- Unique ID for the batch.
    class_attendance_table.batch_id,
    -- Unique ID for the course bundle.
    class_attendance_table.bundle_id,
    -- The name of the course.
    course_catalogue_table.course_name,
    -- The name of the specific batch.
    course_batches_table.batch_name,
    -- The name of the teacher/tutor for this batch.
    course_batches_table.tutor_name,
    -- The start date of the batch, converted to a simple date format.
    course_batches_table.start_date_ist::DATE AS batch_start_date,
    -- The end date of the batch, converted to a simple date format.
    course_batches_table.end_date_ist::DATE AS batch_end_date,
    -- The number of students who were admitted or enrolled in this batch.
    course_batches_table.admitted_students AS students_enrolled_count,
    -- Counting the total number of classes held for this batch.
    COUNT(*) AS total_classes_held,
    -- Calculating the average attendance percentage and rounding it to 2 decimal places.
    ROUND(AVG(class_attendance_table.attendance_pct), 2) AS average_attendance_percentage,
    -- Finding the date of the very first class held for this batch.
    MIN(class_attendance_table.class_date) AS first_class_date,
    -- Finding the date of the most recent class held for this batch.
    MAX(class_attendance_table.class_date) AS last_class_date,
    -- Finding the highest class number to know the total count of classes.
    MAX(class_attendance_table.class_number) AS total_class_count,
    -- Finding how many students were present in the first class (Class Number 1).
    MAX(CASE WHEN class_attendance_table.class_number = 1 THEN class_attendance_table.present_count ELSE NULL END) AS first_class_present_count,
    -- Finding how many students were present in the very last class held for this batch.
    -- We use a subquery to find what the maximum class number is for this specific batch.
    MAX(CASE WHEN class_attendance_table.class_number = (
        SELECT MAX(internal_attendance.class_number) 
        FROM silver.class_attendance AS internal_attendance 
        WHERE internal_attendance.batch_id = class_attendance_table.batch_id
    ) THEN class_attendance_table.present_count ELSE NULL END) AS last_class_present_count
-- We start with the class attendance data from the silver layer.
FROM silver.class_attendance AS class_attendance_table
-- We join with course catalogue to get the course name.
LEFT JOIN silver.course_catalogue AS course_catalogue_table 
    ON class_attendance_table.bundle_id = course_catalogue_table.bundle_id
-- We join with course batches to get batch details like name and tutor.
LEFT JOIN silver.course_batches AS course_batches_table 
    ON class_attendance_table.batch_id = course_batches_table.batch_id
-- We group the results by these columns so the math (like COUNT and AVG) works correctly for each batch.
GROUP BY
    class_attendance_table.batch_id, 
    class_attendance_table.bundle_id, 
    course_catalogue_table.course_name,
    course_batches_table.batch_name, 
    course_batches_table.tutor_name, 
    course_batches_table.start_date_ist,
    course_batches_table.end_date_ist, 
    course_batches_table.admitted_students;


-- This view summarizes attendance data at the course bundle level.
CREATE OR REPLACE VIEW gold.bundle_attendance AS
SELECT
    -- Unique ID for the course bundle.
    bundle_id,
    -- The name of the course.
    course_name,
    -- Counting how many different batches exist for this course.
    COUNT(DISTINCT batch_id) AS total_batches_count,
    -- Adding up all classes held across all batches of this course.
    SUM(total_classes_held) AS total_classes_all_batches,
    -- Calculating the overall average attendance percentage across all batches.
    ROUND(AVG(average_attendance_percentage), 2) AS overall_average_attendance,
    -- Adding up the total number of students enrolled across all batches.
    SUM(students_enrolled_count) AS total_enrolled_all_batches,
    -- The earliest date any class started for this course.
    MIN(first_class_date) AS earliest_class_date,
    -- The latest date any class was held for this course.
    MAX(last_class_date) AS latest_class_date
-- We use the batch_attendance view we just created above.
FROM gold.batch_attendance
-- We group by bundle and course name to get totals for each course.
GROUP BY bundle_id, course_name;


-- This view shows how attendance looks year by year.
CREATE OR REPLACE VIEW gold.attendance_by_year AS
SELECT
    -- Extracting the year from the class date and converting it to an integer.
    EXTRACT(YEAR FROM class_date)::INT AS attendance_year,
    -- Counting how many different batches were active in that year.
    COUNT(DISTINCT batch_id) AS active_batches_count,
    -- Counting how many different courses were active in that year.
    COUNT(DISTINCT bundle_id) AS active_courses_count,
    -- Counting the total number of classes held in that year.
    COUNT(*) AS total_classes_count,
    -- Calculating the average attendance percentage for the year.
    ROUND(AVG(attendance_pct), 2) AS average_attendance_percentage,
    -- Summing up the total number of student presences recorded in that year.
    SUM(present_count) AS total_students_attended_count
-- Taking data from the silver attendance table.
FROM silver.class_attendance
-- Grouping by the year column.
GROUP BY 1
-- Sorting the results by year starting from the earliest.
ORDER BY 1;


-- This view compares the student count in the first class versus the last class to see the drop-off.
CREATE OR REPLACE VIEW gold.first_vs_last_class AS
SELECT
    -- Unique ID for the batch.
    class_attendance_table.batch_id,
    -- Unique ID for the course bundle.
    class_attendance_table.bundle_id,
    -- The name of the course.
    course_catalogue_table.course_name,
    -- The name of the specific batch.
    course_batches_table.batch_name,
    -- Getting the student count from the first class (Class Number 1).
    MAX(CASE WHEN class_attendance_table.class_number = 1 THEN class_attendance_table.present_count ELSE NULL END) AS first_class_student_count,
    -- Getting the student count from the very last class of the batch.
    MAX(CASE WHEN class_attendance_table.class_number = (
        SELECT MAX(internal_attendance.class_number) 
        FROM silver.class_attendance AS internal_attendance 
        WHERE internal_attendance.batch_id = class_attendance_table.batch_id
    ) THEN class_attendance_table.present_count ELSE NULL END) AS last_class_student_count,
    -- The total number of students who enrolled.
    course_batches_table.admitted_students AS total_students_enrolled,
    -- Calculating the percentage of students who dropped off between the first and last class.
    -- Formula: (First Class Count - Last Class Count) * 100 / Total Enrolled.
    -- NULLIF is used to avoid errors if total students enrolled is zero.
    ROUND(
        (
            MAX(CASE WHEN class_attendance_table.class_number = 1 THEN class_attendance_table.present_count ELSE NULL END)
            - 
            MAX(CASE WHEN class_attendance_table.class_number = (
                SELECT MAX(internal_attendance.class_number) 
                FROM silver.class_attendance AS internal_attendance 
                WHERE internal_attendance.batch_id = class_attendance_table.batch_id
            ) THEN class_attendance_table.present_count ELSE NULL END)
        ) * 100.0 / NULLIF(course_batches_table.admitted_students, 0), 2
    ) AS student_drop_off_percentage
-- Joining the tables similarly to the first view.
FROM silver.class_attendance AS class_attendance_table
LEFT JOIN silver.course_catalogue AS course_catalogue_table 
    ON class_attendance_table.bundle_id = course_catalogue_table.bundle_id
LEFT JOIN silver.course_batches AS course_batches_table 
    ON class_attendance_table.batch_id = course_batches_table.batch_id
-- Grouping by batch, course, and enrollment details.
GROUP BY 
    class_attendance_table.batch_id, 
    class_attendance_table.bundle_id, 
    course_catalogue_table.course_name,
    course_batches_table.batch_name, 
    course_batches_table.admitted_students;
