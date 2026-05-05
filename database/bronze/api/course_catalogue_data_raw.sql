-- =============================================================================
-- BRONZE TABLE: course_catalogue_data_raw
-- =============================================================================
-- This table is a raw copy of the Course Catalogue CSV file.
-- It stores 59 different columns as TEXT to capture the data exactly as it is.
-- =============================================================================

-- Create the table to store raw course catalogue data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.course_catalogue_data_raw (
    -- Internal ID for the database to keep track of each row.
    id                               SERIAL PRIMARY KEY,
    
    -- The row number from the original CSV file.
    source_row                       INTEGER NOT NULL,
    
    -- The unique ID of the bundle.
    bundle_id                        TEXT,
    
    -- The name of the course.
    course_name                      TEXT,
    
    -- A description of what the course is about.
    course_description               TEXT,
    
    -- A general overview of the course.
    overview                         TEXT,
    
    -- The cost of the course.
    cost                             TEXT,
    
    -- Whether the course is an online package.
    is_online_package                TEXT,
    
    -- Whether online registration is allowed.
    online_registration_allowed      TEXT,
    
    -- Whether a free preview is allowed.
    free_preview_allowed             TEXT,
    
    -- A more readable or "pretty" name for the course.
    pretty_name                      TEXT,
    
    -- The number of students enrolled.
    num_students                     TEXT,
    
    -- The names of the tutors.
    tutors                           TEXT,
    
    -- The unique IDs of the tutors.
    tutord_ids                       TEXT,
    
    -- The web address (URL) for the course.
    course_url                       TEXT,
    
    -- A list of related courses.
    course_list                      TEXT,
    
    -- The unique IDs of the courses.
    course_ids                       TEXT,
    
    -- The subject of the course.
    subject                          TEXT,
    
    -- The difficulty level of the course.
    level                            TEXT,
    
    -- The language the course is taught in.
    language                         TEXT,
    
    -- Details about the examination.
    examination                      TEXT,
    
    -- Reference texts used in the course.
    texts                            TEXT,
    
    -- The type of course.
    type                             TEXT,
    
    -- The division the course belongs to.
    course_division                  TEXT,
    
    -- Information about the certificate provided.
    certificate                      TEXT,
    
    -- The sponsor of the course.
    course_sponsor                   TEXT,
    
    -- The title of the course in Sanskrit.
    course_title_sanskrit            TEXT,
    
    -- An older record of the course duration.
    duration_old                     TEXT,
    
    -- The schedule for live sessions.
    live_session_schedule_text       TEXT,
    
    -- General information about the course.
    about_the_course                 TEXT,
    
    -- Extra details to know more about the course.
    know_more_about_the_course       TEXT,
    
    -- Information about this specific learning program.
    about_this_learning_program      TEXT,
    
    -- What this learning program offers (Value Proposition).
    learning_program_value_proposi   TEXT,
    
    -- How this learning program is structured.
    how_learning_program_works       TEXT,
    
    -- More information about the various programs.
    know_more_about_the_programs     TEXT,
    
    -- Whether the course is marked as "Coming Soon".
    coming_soon                      TEXT,
    
    -- The group of people this course is meant for.
    target_audience                  TEXT,
    
    -- The current status of the course.
    status                           TEXT,
    
    -- The total number of lectures in the course.
    number_of_lectures               TEXT,
    
    -- The total duration of the course.
    duration                         TEXT,
    
    -- The types of personas this course targets.
    personas                         TEXT,
    
    -- Notes regarding any ongoing webinars.
    ongoing_webinar_note             TEXT,
    
    -- Requirements to be eligible for the course.
    eligibility                      TEXT,
    
    -- Information on what's new in this course version.
    whats_new                        TEXT,
    
    -- Link or info about the "What's New" poster.
    whats_new_poster                 TEXT,
    
    -- The title used for web search engines (Meta Title).
    meta_title                       TEXT,
    
    -- The description used for web search engines (Meta Description).
    meta_description                 TEXT,
    
    -- Keywords used for web search engines (Meta Keywords).
    meta_keywords                    TEXT,
    
    -- A link to DSG related materials.
    dsg_link                         TEXT,
    
    -- Whether to hide this course in the ongoing webinar list.
    hide_in_ongoing_webinar          TEXT,
    
    -- Information about computer-based assessments.
    computer_based_assessment        TEXT,
    
    -- The order in which the course appears in a list.
    course_ordering                  TEXT,
    
    -- The web address to send students after they enroll.
    post_enrollment_redirect_url     TEXT,
    
    -- The unique product ID.
    product_id                       TEXT,
    
    -- The Category from SSS.
    sss_category                     TEXT,
    
    -- The number of credits the course provides.
    credits                          TEXT,
    
    -- Information about Viniyoga.
    viniyoga                         TEXT,
    
    -- The Category from Adhyayanam.
    adhyayanam_category              TEXT,
    
    -- The duration or term of the course.
    term_of_course                   TEXT,
    
    -- Where this course sits in the sales funnel.
    position_in_funnel               TEXT,
    
    -- The division name.
    division                         TEXT,
    
    -- The exact time this row was added to our database.
    loaded_at                        TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't load the same row from the CSV file twice.
    UNIQUE (source_row)
);
