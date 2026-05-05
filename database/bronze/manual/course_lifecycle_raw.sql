-- =============================================================================
-- BRONZE TABLE: course_lifecycle_raw
-- =============================================================================
-- This table is a raw copy of the "Course Lifecycle" tracker.
-- it contains over 100 columns capturing every stage of a course.
-- =============================================================================

-- Create the table to store raw course lifecycle data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.course_lifecycle_raw (
    -- Internal ID for the database to keep track of each row.
    id                                                                                                        SERIAL PRIMARY KEY,
    
    -- The row number from the original tracking file.
    source_row                                                                                                INTEGER NOT NULL,
    
    -- The serial number from the tracker.
    sl_no                                                                                                     TEXT,
    
    -- The name of the course.
    course_name                                                                                               TEXT,
    
    -- The unique ID of the course.
    course_id                                                                                                 TEXT,
    
    -- The name of the batch.
    batch_name                                                                                                TEXT,
    
    -- How the course was launched.
    type_of_launch                                                                                            TEXT,
    
    -- Current status of the course.
    status                                                                                                    TEXT,
    
    -- The subject area of the course.
    subject                                                                                                   TEXT,
    
    -- Position in the marketing or sales funnel.
    position_in_funnel                                                                                        TEXT,
    
    -- The model used for Samskritadhyayana.
    samskritadhyayana_model                                                                                   TEXT,
    
    -- The duration or term of the course.
    term_of_course                                                                                            TEXT,
    
    -- Category related to SSS.
    sss_category                                                                                              TEXT,
    
    -- Targeted student persona.
    persona                                                                                                   TEXT,
    
    -- Link to the Google Drive folder for the course.
    drive_folder                                                                                              TEXT,
    
    -- Name and link for the YouTube playlist.
    youtube_playlist_name_and_link                                                                            TEXT,
    
    -- Link to the ClickUp folder.
    clickup_folder_link                                                                                       TEXT,
    
    -- Link to the Canva banner design.
    canva_banner_link                                                                                         TEXT,
    
    -- The date when the course details were finalized.
    course_finalisation_date                                                                                  TEXT,
    
    -- Number of classes held each week.
    no_of_classes_in_a_week                                                                                   TEXT,
    
    -- Which days of the week classes are held.
    days_of_classes                                                                                           TEXT,
    
    -- Timing of classes in India Standard Time.
    class_timings_ist                                                                                         TEXT,
    
    -- The tool used for the webinar (e.g., Zoom).
    webinar_tool                                                                                              TEXT,
    
    -- Name and link for the primary teacher.
    primary_teacher_name_sfh_link                                                                             TEXT,
    
    -- Unique ID of the primary teacher.
    primary_teacher_id                                                                                        TEXT,
    
    -- Name and link for any additional teachers.
    additional_teacher_name_link                                                                              TEXT,
    
    -- Unique ID of the additional teacher.
    additional_teacher_id                                                                                     TEXT,
    
    -- Name of the E-Learning Associate (ELA).
    ela                                                                                                       TEXT,
    
    -- Employee ID of the person responsible.
    employee_id                                                                                               TEXT,
    
    -- List of panelists involved.
    panelists                                                                                                 TEXT,
    
    -- Name of the volunteer for uploading class recordings.
    volunteer_for_class_upload                                                                                TEXT,
    
    -- Name of the volunteer for marking timestamps in videos.
    volunteer_for_timestamping                                                                                TEXT,
    
    -- The date the course was launched.
    course_launch_date                                                                                        TEXT,
    
    -- Number of enrollments the day after launch.
    enrolments_on_next_day_of_launch                                                                          TEXT,
    
    -- Check if enough time was given between finalization and launch.
    time_given_for_course_finaliastion_and_launch_should_be_atleast_3_to_5_days                              TEXT,
    
    -- The date of the very first class.
    first_class_date                                                                                          TEXT,
    
    -- Number of enrollments on the day of the first class.
    enrolments_on_the_day_of_first_class                                                                      TEXT,
    
    -- Attendance count for the first class.
    first_class_attendance                                                                                    TEXT,
    
    -- Attendance count for the second class.
    second_class_attendance                                                                                   TEXT,
    
    -- Check if enough time was given between launch and the first class.
    time_given_for_launch_first_class_should_be_atleast_15_days                                              TEXT,
    
    -- Date of the last class or valedictory session.
    last_class_and_valedictory_date                                                                           TEXT,
    
    -- Number of enrollments on the very last day of the course.
    enrolments_on_last_day                                                                                    TEXT,
    
    -- Attendance count for the last class.
    last_class_attendance                                                                                     TEXT,
    
    -- Date when students were asked for course feedback.
    date_of_request_for_course_feedback_to_students                                                          TEXT,
    
    -- Total count of classes held.
    total_no_of_classes_held                                                                                  TEXT,
    
    -- Total number of hours taught.
    total_hours_of_classes                                                                                    TEXT,
    
    -- Average attendance across all classes.
    average_attendance_of_all_classes                                                                         TEXT,
    
    -- Average attendance in the final 5 classes.
    average_attendance_in_last_5_classes                                                                      TEXT,
    
    -- Check if feedback was requested promptly after the last class.
    time_given_btw_last_class_and_feedack_should_not_exceed_1_day                                            TEXT,
    
    -- The type of assessment used (e.g., Quiz, Oral).
    type_of_assessment                                                                                        TEXT,
    
    -- Date the assessment announcement email was sent.
    date_of_assessment_announcement_mail_sent                                                                 TEXT,
    
    -- Check if assessment was announced promptly after the last class.
    time_given_btw_last_class_assessment_announcement_should_not_exceed_3_days                               TEXT,
    
    -- Date the assessment period started.
    assessment_start_date                                                                                     TEXT,
    
    -- Date the assessment period ended.
    assessment_end_date                                                                                       TEXT,
    
    -- Check if students had enough time to prepare for the assessment.
    time_given_btw_assessment_announcement_start_date_of_assessment_should_be_atleast_15_days                TEXT,
    
    -- Total number of students who attended the assessment.
    total_assessment_attendees                                                                                TEXT,
    
    -- Names of judges for oral exams.
    judges_for_oral_assessment                                                                                TEXT,
    
    -- Date when teacher feedback was requested.
    date_of_request_for_teacher_feeback                                                                       TEXT,
    
    -- Check if teacher surveys were conducted in a timely manner.
    time_gap_given_for_conducting_assessment_teacher_satisfaction_survery_should_not_exceed_7_days_in_case_of_any_assessment  TEXT,
    
    -- Date the results were announced.
    results_announcement_date                                                                                 TEXT,
    
    -- Check if results were announced promptly after assessments.
    time_gap_given_for_completion_of_assessment_results_announcement_should_not_exceed_7_days                TEXT,
    
    -- Date the certificates were issued to students.
    date_of_certificates_issued                                                                               TEXT,
    
    -- Check if certificates were issued promptly after results.
    time_gap_given_for_results_announcement_certificate_issue_should_not_exceed_15_days                      TEXT,
    
    -- Total number of students who received certificates.
    total_students_certified                                                                                  TEXT,
    
    -- Date the final close-out email was sent.
    date_of_close_out_email_sent                                                                              TEXT,
    
    -- Check if the close-out email was sent promptly after certificates.
    time_given_for_certificate_issued_close_out_email_date_should_not_exceed_3_days                          TEXT,
    
    -- Ratio of certified students vs initial enrollments.
    total_certified_vs_initial_enrolments                                                                     TEXT,
    
    -- Ratio of certified students vs final enrollments.
    total_certified_vs_course_end_enrolments                                                                  TEXT,
    
    -- Ratio of certified students vs first class attendees.
    total_certified_vs_first_class_attendees                                                                  TEXT,
    
    -- Ratio of certified students vs average attendees.
    total_certified_vs_average_attendees                                                                      TEXT,
    
    -- Ratio of first class attendance vs initial enrollments.
    first_class_attendance_vs_initial_enrolments                                                              TEXT,
    
    -- Comparison of first class vs last class attendance.
    first_class_attendance_vs_last_class_attendance                                                           TEXT,
    
    -- Pass percentage (certified vs took assessment).
    pass_percentage_total_certified_vs_total_assessment_attendees                                             TEXT,
    
    -- Pass percentage (last class students vs certified).
    pass_percentage_total_students_on_last_class_vs_total_certified                                           TEXT,
    
    -- Total number of feedback forms received.
    total_number_of_feedbacks_received                                                                        TEXT,
    
    -- The average rating given for the entire course.
    overall_course_rating                                                                                     TEXT,
    
    -- Average rating for how easy it was to attend webinars.
    average_rating_for_ease_of_attending_live_webinars                                                        TEXT,
    
    -- Average rating for the quality of materials.
    average_rating_for_quality_of_content_and_course_materials                                               TEXT,
    
    -- Average rating given to the teacher.
    average_teacher_rating                                                                                    TEXT,
    
    -- Average rating for accessing materials on the website.
    average_rating_for_accessing_course_materials_in_the_website                                             TEXT,
    
    -- Average rating given to the ELA by the teacher.
    average_ela_rating_by_teacher                                                                             TEXT,
    
    -- Average rating for content support given by the teacher.
    average_content_support_rating_by_teacher                                                                 TEXT,
    
    -- Overall rating given to the course by the teacher.
    overall_course_rating_by_teacher                                                                          TEXT,
    
    -- Name of the Course Master.
    course_master                                                                                             TEXT,
    
    -- Link to the feedback form.
    feedback_form                                                                                             TEXT,
    
    -- Link to the sheet containing feedback responses.
    feedback_form_responses_sheet                                                                             TEXT,
    
    -- Total hours the teacher worked on this course.
    hours_worked_by_teacher_for_the_course                                                                    TEXT,
    
    -- Total hours the ELA worked on this course.
    hours_worked_by_ela_for_the_course                                                                        TEXT,
    
    -- Total hours the linguist team worked on this course.
    hours_worked_by_linguist_team_for_the_course                                                              TEXT,
    
    -- Average hours worked by volunteers.
    average_hours_worked_by_volunteers                                                                        TEXT,
    
    -- Average hours worked by oral exam judges.
    average_hours_worked_by_judges_for_oral_assessment                                                        TEXT,
    
    -- Status of whether the attendance sheet was downloaded.
    attendance_sheet_download_status                                                                          TEXT,
    
    -- Status of filling the CSV for testimonials.
    status_for_filling_in_csv_sheet_for_testimonials                                                         TEXT,
    
    -- Status of adding testimonials to SFH.
    status_for_adding_testimonials_on_sfh                                                                     TEXT,
    
    -- Whether the playlist description was added.
    playlist_description_added                                                                                TEXT,
    
    -- Whether video titles were standardized.
    video_title_standardised                                                                                  TEXT,
    
    -- Whether video descriptions were standardized.
    video_description_standardised                                                                            TEXT,
    
    -- Whether thumbnail images were uploaded.
    thumbnail_images_uploaded                                                                                 TEXT,
    
    -- Whether tags were added to the videos.
    tags_added                                                                                                TEXT,
    
    -- Status of monetization settings.
    monetization_everything_except_during_video                                                               TEXT,
    
    -- Whether timestamping is complete.
    timestamping_done                                                                                         TEXT,
    
    -- Whether start and end bits were added to videos.
    starting_and_ending_video_bits_added                                                                      TEXT,
    
    -- Whether endscreen links were updated.
    endscreen_links_updated                                                                                   TEXT,
    
    -- Whether video cards were updated.
    cards_updated                                                                                             TEXT,
    
    -- The unique ID of the batch.
    batch_id                                                                                                  TEXT,
    
    -- Placeholder for extra data column 1.
    new_1                                                                                                     TEXT,
    
    -- Placeholder for extra data column 2.
    new_2                                                                                                     TEXT,
    
    -- Enrollment count as of March 31, 2025.
    enrollment_as_on_31_mar_2025                                                                              TEXT,
    
    -- Enrollment count as of August 31, 2025.
    enrollment_as_on_31_aug_2025                                                                              TEXT,
    
    -- The exact time this row was added to our database.
    loaded_at                                                                                                 TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't load the same row from the CSV file twice.
    UNIQUE (source_row)
);
