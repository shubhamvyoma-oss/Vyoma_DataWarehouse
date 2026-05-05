-- This table tracks the entire life of a course batch, from launch to completion.
-- It is very useful for seeing how successful a particular batch was.
CREATE TABLE IF NOT EXISTS silver.course_lifecycle (
    -- This is a unique number for each record that increases automatically.
    -- It helps us identify each lifecycle record uniquely.
    id                  SERIAL PRIMARY KEY,

    -- This is the unique ID for the course (also called bundle ID).
    -- It tells us which course this record belongs to.
    course_id           BIGINT,

    -- This is the full name of the course.
    -- For example: 'Spoken Sanskrit Level 1'.
    course_name         TEXT,

    -- This is the name of the specific group (batch) of students.
    -- For example: 'January 2024 Evening Batch'.
    batch_name          TEXT,

    -- This describes how the course was launched (like 'New Launch' or 'Relaunch').
    -- Helps us analyze the success of different marketing strategies.
    type_of_launch      TEXT,

    -- This is the current status of the batch (like 'Ongoing', 'Completed', or 'Planned').
    -- Tells us if the batch is still active or finished.
    status              TEXT,

    -- This is the main subject area of the course (like 'Sanskrit', 'Yoga', or 'Philosophy').
    -- Helps us group courses by their topic.
    subject             TEXT,

    -- This tells us where the course sits in our educational funnel (like 'Introductory' or 'Advanced').
    -- Helps us see how students progress through different levels of courses.
    position_in_funnel  TEXT,

    -- This describes how the students learn (like 'Live Classes' or 'Self-paced').
    -- Useful for seeing which learning styles are most popular.
    learning_model      TEXT,

    -- This is the total duration of the course.
    -- For example: '3 Months' or '10 Weeks'.
    term_of_course      TEXT,

    -- This is a special category used for internal reporting and classification.
    -- Helps us organize courses according to institutional standards.
    sss_category        TEXT,

    -- This describes the target audience for the course (like 'Children', 'Adults', or 'Teachers').
    -- Helps us see who our students are.
    persona             TEXT,

    -- This is the date when the very first class of the batch happened.
    -- Marks the official start of teaching.
    first_class_date    DATE,

    -- This is the date when the very last class of the batch happened.
    -- Marks the official end of teaching.
    last_class_date     DATE,

    -- This is the number of students who were enrolled on the first day of class.
    -- Tells us how many people started the course.
    enrollments_on_fc   INTEGER,

    -- This is the number of students who were still enrolled on the last day of class.
    -- Helps us calculate how many students dropped out.
    enrollments_on_lc   INTEGER,

    -- This is the average percentage of students who attended each class.
    -- A high number means students were very engaged.
    avg_attendance      NUMERIC(8,2),

    -- This is the total number of class sessions that were actually conducted.
    -- Tells us how much teaching actually happened.
    total_classes_held  INTEGER,

    -- This is the total number of students who earned a certificate for this batch.
    -- Tells us how many students successfully finished the course.
    total_certified     INTEGER,

    -- This is the percentage of students who passed the final assessments.
    -- Measures how well the students learned the material.
    pass_percentage     NUMERIC(5,2),

    -- This is the average feedback score given by students for this batch.
    -- Helps us measure student satisfaction.
    overall_rating      NUMERIC(3,2),

    -- This is the unique ID for the batch from the main system.
    -- We use this to link this lifecycle data to the 'batches_data' table.
    batch_id            BIGINT,

    -- This is the exact time when this record was added to our system.
    -- Defaults to the current system time.
    imported_at         TIMESTAMPTZ DEFAULT NOW(),

    -- We ensure that each combination of course and batch is unique.
    -- This prevents us from having duplicate lifecycle records for the same group.
    UNIQUE (course_id, batch_name)
);
