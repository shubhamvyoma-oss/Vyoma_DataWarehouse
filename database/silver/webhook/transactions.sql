-- This table stores clean records of student enrollments and payments (transactions).
-- It combines data from both live webhooks and historical CSV files.
CREATE TABLE IF NOT EXISTS silver.transactions (
    -- This is a unique number for each transaction record that increases automatically.
    -- It helps us track each individual entry in this table.
    id                    SERIAL PRIMARY KEY,

    -- This is the unique ID for the event that triggered this transaction record.
    -- It links this data back to the original source event.
    event_id              TEXT NOT NULL,

    -- This tells us the type of event (like 'course_enrolled').
    -- It explains why this transaction was recorded.
    event_type            TEXT NOT NULL,

    -- This is the date and time when the event actually happened, in IST.
    -- It tells us exactly when the student enrolled or paid.
    event_timestamp_ist   TIMESTAMPTZ,

    -- This is the unique ID of the user who made the transaction.
    -- It links the transaction to a specific student in the 'users' table.
    user_id               BIGINT NOT NULL,

    -- This is the email of the student at the time of transaction.
    -- Useful for quick reference without joining the users table.
    email                 TEXT,

    -- This is the full name of the student who enrolled.
    -- Helps identify the student in financial reports.
    full_name             TEXT,

    -- This is the phone number of the student.
    -- Recorded here to capture their contact info at the moment of payment.
    contact_number        TEXT,

    -- This is the unique ID for the course package (bundle) they joined.
    -- It tells us exactly what educational product they bought.
    bundle_id             BIGINT,

    -- This is the name of the course they enrolled in.
    -- For example: 'Basic Samskrita Speaking Course'.
    course_name           TEXT,

    -- This is the unique ID for the specific group (batch) they were added to.
    -- A course can have many batches; this identifies which one they joined.
    master_batch_id       BIGINT,

    -- This is the name of the specific batch they are in.
    -- For example: 'Batch A - Jan 2024'.
    master_batch_name     TEXT,

    -- This is a secondary ID for the bundle used by the institution.
    -- Helps in cross-referencing with other institutional systems.
    institution_bundle_id BIGINT,

    -- This was the full price of the course before any discounts.
    -- Useful for calculating total potential revenue.
    original_price        NUMERIC(12,2),

    -- This is the amount of money taken off the original price.
    -- Helps us track how much revenue we are giving up in promotions.
    discount              NUMERIC(12,2),

    -- This is the actual amount the student paid after discounts.
    -- This is the real revenue we received.
    final_price           NUMERIC(12,2),

    -- This is the type of money used (like 'INR' or 'USD').
    -- Crucial for multi-currency financial reporting.
    currency              TEXT,

    -- This is the value of any credits or points the student used to pay.
    -- Helps track the usage of internal scholarship or credit systems.
    credits_applied       NUMERIC(12,2),

    -- This tells us how the student paid (like 'Razorpay', 'Bank Transfer').
    -- Useful for reconciling with our payment gateway reports.
    payment_method        TEXT,

    -- This is the unique ID given by the payment gateway for this payment.
    -- Crucial for finding the payment in Razorpay or other systems.
    transaction_id        TEXT,

    -- This is the date when the student's access to the course starts.
    -- IST time when they can begin learning.
    start_date_ist        TIMESTAMPTZ,

    -- This is the date when the student's access to the course ends.
    -- Helps us manage course expiration and renewals.
    end_date_ist          TIMESTAMPTZ,

    -- This is the date and time when this record was first created in the source system.
    -- Tells us when the enrollment was officially logged.
    created_at_ist        TIMESTAMPTZ,

    -- This tells us where the data came from ('webhook' for live, 'csv' for old data).
    -- Helps us understand the reliability and source of the record.
    source                TEXT DEFAULT 'webhook',

    -- This is the exact time this row was added to our database.
    -- Defaults to the current system time.
    inserted_at           TIMESTAMPTZ DEFAULT NOW(),

    -- We make sure the combination of user, bundle, and batch is unique.
    -- This prevents us from counting the same enrollment more than once.
    UNIQUE (user_id, bundle_id, master_batch_id)
);
