-- This table is called 'certificates' and it lives in the 'silver' schema.
-- It stores information about certificates given to students when they finish a course.
CREATE TABLE IF NOT EXISTS silver.certificates (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each certificate record uniquely.
    id               SERIAL PRIMARY KEY,
    
    -- This stores the unique ID for the event from the external system.
    -- It ensures we can track the specific notification that a certificate was issued.
    event_id         TEXT NOT NULL,
    
    -- This stores the type of event (e.g., 'certificate_issued').
    -- It helps us categorize different actions related to certificates.
    event_type       TEXT NOT NULL,
    
    -- This is the actual ID of the certificate itself.
    -- We use this to refer to the specific document or digital badge.
    certificate_id   TEXT,
    
    -- This is the ID of the student who received the certificate.
    -- We use this to link the certificate to the correct person.
    user_id          BIGINT,
    
    -- This is the date and time when the certificate was officially issued.
    -- It is recorded in India Standard Time (IST).
    issued_at_ist    TIMESTAMPTZ,
    
    -- This records exactly when our system received this certificate information.
    -- It defaults to the current time in the India time zone.
    received_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    
    -- This ensures that we don't have two records for the same event_id.
    -- It prevents duplicate entries for the same certificate issuance.
    UNIQUE (event_id)
);
