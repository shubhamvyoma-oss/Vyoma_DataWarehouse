-- This table is called 'attendance_data' and it lives in the 'silver' schema.
-- It stores clean summaries of student attendance for each class.
CREATE TABLE IF NOT EXISTS silver.attendance_data (
    -- This is a unique number for each row that is generated automatically.
    -- We use this as the primary way to identify each record in this table.
    id               SERIAL PRIMARY KEY,

    -- This stores the ID of the batch (a group of students).
    -- We need this to know which batch this attendance record belongs to.
    batch_id         BIGINT NOT NULL,

    -- This stores the full name of the batch.
    -- We keep this here so it's easy to read the reports without looking up another table.
    batch_name       TEXT,

    -- This is the unique ID for a specific class meeting or session.
    -- It helps us distinguish one class session from another.
    class_id         BIGINT NOT NULL,

    -- This stores the ID of the course bundle.
    -- Bundles help us group related courses together.
    bundle_id        BIGINT NOT NULL,

    -- This stores the name of the course bundle.
    -- It makes the data more descriptive and easier to understand.
    bundle_name      TEXT,

    -- This is the date when the class actually took place.
    -- We use the DATE format because we only need the year, month, and day.
    class_date       DATE NOT NULL,

    -- This is the number of the class in a series (like Class 1, Class 2).
    -- It helps us track the progress of a course over time.
    class_number     INTEGER,

    -- This counts how many students were marked as 'Present'.
    -- If no value is provided, it starts at 0.
    present_count    INTEGER DEFAULT 0,

    -- This counts how many students were marked as 'Late'.
    -- If no value is provided, it starts at 0.
    late_count       INTEGER DEFAULT 0,

    -- This counts how many students were marked as 'Absent'.
    -- If no value is provided, it starts at 0.
    absent_count     INTEGER DEFAULT 0,

    -- This is the total number of students whose attendance was recorded for this class.
    -- It is usually the sum of present, late, and absent students.
    total_records    INTEGER DEFAULT 0,

    -- This is the total number of students officially enrolled in this batch.
    -- We use this to see how many people missed the class entirely.
    total_enrolled   INTEGER,

    -- This is the percentage of students who attended the class.
    -- It uses a decimal format to show the exact percentage (like 85.50%).
    attendance_pct   NUMERIC(5,2),

    -- This is the original ID from the external system (Edmingle).
    -- We keep it so we can double-check the data against the source.
    attendance_id    BIGINT,

    -- This stores the ID of the teacher who taught the class.
    -- It allows us to see which teacher was responsible for each session.
    teacher_id       BIGINT,

    -- This stores the name of the teacher.
    -- It's included here so we can see the teacher's name directly in reports.
    teacher_name     TEXT,

    -- This describes how long the class lasted (e.g., '60 minutes').
    -- It is stored as text to accommodate different ways time might be written.
    class_duration   TEXT,

    -- This is the date when this information was brought into our system.
    -- It helps us know how fresh or old the data is.
    pull_date        DATE,

    -- This automatically records exactly when this record was added to our database.
    -- It defaults to the current time.
    created_at       TIMESTAMPTZ DEFAULT NOW(),

    -- This rule makes sure we don't accidentally add the same class for the same batch twice.
    -- It keeps our data unique and accurate.
    CONSTRAINT unique_batch_class UNIQUE (batch_id, class_id)
);

-- This index helps the database find records for a specific batch faster.
-- It makes reports that filter by batch run much more quickly.
CREATE INDEX IF NOT EXISTS idx_classatt_batch  ON silver.attendance_data(batch_id);

-- This index helps the database find records for a specific date faster.
-- It is very useful for reports that show attendance over time.
CREATE INDEX IF NOT EXISTS idx_class_att_date   ON silver.attendance_data(class_date);

-- This index helps the database find records for a specific class ID faster.
-- It is used for quick lookups and joining with other tables.
CREATE INDEX IF NOT EXISTS idx_class_att_class  ON silver.attendance_data(class_id);
