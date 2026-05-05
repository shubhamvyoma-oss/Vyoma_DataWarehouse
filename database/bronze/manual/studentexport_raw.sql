-- =============================================================================
-- BRONZE TABLE: studentexport_raw
-- =============================================================================
-- This table is a raw copy of the "studentexport" CSV file.
-- It contains all personal and profile information for students exactly as exported.
-- =============================================================================

-- Create the table to store raw student export data if it doesn't already exist.
CREATE TABLE IF NOT EXISTS bronze.studentexport_raw (
    -- Internal ID for the database to keep track of each row.
    id                                 SERIAL PRIMARY KEY,
    
    -- The row number from the original CSV file.
    source_row                         INTEGER NOT NULL,
    
    -- The row number as listed inside the CSV file.
    row_number                         TEXT,
    
    -- The full name of the student.
    name                               TEXT,
    
    -- The email address of the student.
    email                              TEXT,
    
    -- The official registration number.
    registration_number                TEXT,
    
    -- The country dial code for the contact number (e.g., +91).
    contact_number_dial_code           TEXT,
    
    -- The main contact phone number.
    contact_number                     TEXT,
    
    -- The dial code for the alternate contact number.
    alternate_contact_number_dial_code TEXT,
    
    -- An alternative contact phone number.
    alternate_contact_number           TEXT,
    
    -- The student's date of birth.
    date_of_birth                      TEXT,
    
    -- The name of the student's parent or guardian.
    parent_name                        TEXT,
    
    -- The contact number of the parent.
    parent_contact                     TEXT,
    
    -- The email address of the parent.
    parent_email                       TEXT,
    
    -- The physical home address of the student.
    address                            TEXT,
    
    -- The city where the student lives.
    city                               TEXT,
    
    -- The state or province where the student lives.
    state                              TEXT,
    
    -- The academic standard or grade of the student.
    standard                           TEXT,
    
    -- The date when the student record was created.
    date_created                       TEXT,
    
    -- The student's username for logging in.
    username                           TEXT,
    
    -- The gender of the student.
    gender                             TEXT,
    
    -- The account status (e.g., Active, Suspended).
    status                             TEXT,
    
    -- An additional username field.
    username_1                         TEXT,
    
    -- Reason given by the student for studying Sanskrit.
    why_study_sanskrit                 TEXT,
    
    -- A "nice" or display version of the username.
    user_nice_name                     TEXT,
    
    -- The last name of the student.
    user_last_name                     TEXT,
    
    -- Whether the student would like to teach in the future.
    would_like_to_teach                TEXT,
    
    -- Details about the student's teaching experience.
    teaching_experience                TEXT,
    
    -- Whether the student is in mainstream education.
    is_mainstream_education            TEXT,
    
    -- The student's objective for joining.
    objective                          TEXT,
    
    -- The age of the student.
    user_age                           TEXT,
    
    -- The category or persona of the student.
    persona                            TEXT,
    
    -- Objective specifically related to a package.
    objective_package                  TEXT,
    
    -- Number of hours the student can commit per week.
    time_per_week_hours                TEXT,
    
    -- Another age field.
    age_                               TEXT,
    
    -- URL to the student's Facebook profile.
    facebook_profile_url               TEXT,
    
    -- URL to the student's Instagram profile.
    instagram_profile_url              TEXT,
    
    -- URL to the student's Pinterest profile.
    pinterest_profile_url              TEXT,
    
    -- URL to the student's SoundCloud profile.
    soundcloud_profile_url             TEXT,
    
    -- URL to the student's Tumblr profile.
    tumblr_profile_url                 TEXT,
    
    -- URL to the student's YouTube profile.
    youtube_profile_url                TEXT,
    
    -- URL to the student's Wikipedia page.
    wikipedia_url                      TEXT,
    
    -- The student's Twitter username.
    twitter_username                   TEXT,
    
    -- The student's GST tax number.
    gst_number                         TEXT,
    
    -- URL to the student's MySpace profile.
    myspace_profile_url                TEXT,
    
    -- A phone number formatted for international calls.
    international_phone_number         TEXT,
    
    -- The student's personal website.
    website                            TEXT,
    
    -- The student's highest educational qualification.
    educational_qualification          TEXT,
    
    -- URL to the student's LinkedIn profile.
    linkedin_profile_url               TEXT,
    
    -- A third version of the age field.
    age_v2                             TEXT,
    
    -- A second version of the gender field.
    gender_                            TEXT,
    
    -- The student's specific qualification in Sanskrit.
    sanskrit_qualification             TEXT,
    
    -- List of the student's areas of interest.
    areas_of_interest                  TEXT,
    
    -- Whether the student is currently studying Sanskrit elsewhere.
    studying_sanskrit_currently        TEXT,
    
    -- Current status of the student's education.
    current_education_status           TEXT,
    
    -- The name of the student's country.
    country_name                       TEXT,
    
    -- The exact time this row was added to our database.
    loaded_at                          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Ensure we don't load the same row from the CSV file twice.
    UNIQUE (source_row)
);
