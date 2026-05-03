-- Gold attendance views for Power BI

CREATE OR REPLACE VIEW gold.batch_attendance_summary AS
SELECT
    ca.batch_id,
    ca.bundle_id,
    cm.course_name,
    cb.batch_name,
    cb.tutor_name,
    cb.start_date_ist::DATE          AS batch_start,
    cb.end_date_ist::DATE            AS batch_end,
    cb.admitted_students             AS enrolled,
    COUNT(*)                         AS total_classes_held,
    ROUND(AVG(ca.attendance_pct), 2) AS avg_attendance_pct,
    MIN(ca.class_date)               AS first_class_date,
    MAX(ca.class_date)               AS last_class_date,
    MAX(ca.class_number)             AS total_class_count,
    MAX(ca.present_count)
        FILTER (WHERE ca.class_number = 1)  AS first_class_present,
    MAX(ca.present_count)
        FILTER (WHERE ca.class_number = (
            SELECT MAX(class_number) FROM silver.class_attendance c2
            WHERE c2.batch_id = ca.batch_id
        ))                                  AS last_class_present
FROM silver.class_attendance ca
LEFT JOIN silver.course_metadata cm ON ca.bundle_id = cm.bundle_id
LEFT JOIN silver.course_batches  cb ON ca.batch_id  = cb.batch_id
GROUP BY
    ca.batch_id, ca.bundle_id, cm.course_name,
    cb.batch_name, cb.tutor_name, cb.start_date_ist,
    cb.end_date_ist, cb.admitted_students;


CREATE OR REPLACE VIEW gold.bundle_attendance_summary AS
SELECT
    bundle_id,
    course_name,
    COUNT(DISTINCT batch_id)          AS total_batches,
    SUM(total_classes_held)           AS total_classes_all_batches,
    ROUND(AVG(avg_attendance_pct), 2) AS overall_avg_attendance,
    SUM(enrolled)                     AS total_enrolled_all_batches,
    MIN(first_class_date)             AS earliest_class,
    MAX(last_class_date)              AS latest_class
FROM gold.batch_attendance_summary
GROUP BY bundle_id, course_name;


CREATE OR REPLACE VIEW gold.attendance_by_year AS
SELECT
    EXTRACT(YEAR FROM class_date)::INT AS year,
    COUNT(DISTINCT batch_id)           AS active_batches,
    COUNT(DISTINCT bundle_id)          AS active_courses,
    COUNT(*)                           AS total_classes,
    ROUND(AVG(attendance_pct), 2)      AS avg_attendance_pct,
    SUM(present_count)                 AS total_students_attended
FROM silver.class_attendance
GROUP BY 1
ORDER BY 1;


CREATE OR REPLACE VIEW gold.first_vs_last_class AS
SELECT
    ca.batch_id,
    ca.bundle_id,
    cm.course_name,
    cb.batch_name,
    MAX(ca.present_count)
        FILTER (WHERE ca.class_number = 1)  AS first_class_count,
    MAX(ca.present_count)
        FILTER (WHERE ca.class_number = (
            SELECT MAX(class_number) FROM silver.class_attendance c2
            WHERE c2.batch_id = ca.batch_id
        ))                                  AS last_class_count,
    cb.admitted_students                    AS enrolled,
    ROUND(
        (MAX(ca.present_count) FILTER (WHERE ca.class_number = 1)
        - MAX(ca.present_count) FILTER (WHERE ca.class_number = (
            SELECT MAX(class_number) FROM silver.class_attendance c2
            WHERE c2.batch_id = ca.batch_id
        ))) * 100.0 / NULLIF(cb.admitted_students, 0), 2
    )                                       AS drop_off_pct
FROM silver.class_attendance ca
LEFT JOIN silver.course_metadata cm ON ca.bundle_id = cm.bundle_id
LEFT JOIN silver.course_batches  cb ON ca.batch_id  = cb.batch_id
GROUP BY ca.batch_id, ca.bundle_id, cm.course_name,
         cb.batch_name, cb.admitted_students;
