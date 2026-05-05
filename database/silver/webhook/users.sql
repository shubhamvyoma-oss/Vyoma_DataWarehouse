-- This table stores clean and polished information about every user (student).
-- It lives in the 'silver' schema, which means the data has been cleaned.
CREATE TABLE IF NOT EXISTS silver.users (
    -- This is a unique number for each record that increases automatically.
    -- We use it to identify each row in this table uniquely.
    id             SERIAL PRIMARY KEY,

    -- This is the unique ID for the event that created or updated this user.
    -- It helps us trace the data back to the original message.
    event_id       TEXT NOT NULL,

    -- This tells us what kind of event happened (like 'user_created' or 'user_updated').
    -- It explains why this record was added or changed.
    event_type     TEXT NOT NULL,

    -- This is the unique ID for the user from the Edmingle system.
    -- We use this to make sure we don't have duplicate users.
    user_id        BIGINT NOT NULL,

    -- This is the email address of the user.
    -- We use it as the primary way to contact them.
    email          TEXT,

    -- This is the full name of the user.
    -- It is what we show in reports and certificates.
    full_name      TEXT,

    -- This is the username the person uses to log in.
    -- It might be different from their email.
    user_name      TEXT,

    -- This tells us what role the user has (usually 'student').
    -- It helps us filter out staff or admins if needed.
    user_role      TEXT,

    -- This is the phone number of the user.
    -- Used for urgent communication or SMS alerts.
    contact_number TEXT,

    -- This is the ID of the school or institution the user belongs to.
    -- It helps us group students by their organization.
    institution_id INTEGER,

    -- This is the city where the user lives.
    -- Used for geographic analysis of our students.
    city           TEXT,

    -- This is the state where the user lives.
    -- Helps us understand which regions our students come from.
    state          TEXT,

    -- This is the full postal address of the user.
    -- Used if we need to send physical materials.
    address        TEXT,

    -- This is the postal code (PIN code) for the user's address.
    -- Useful for precise location grouping.
    pincode        TEXT,

    -- This is the name of the user's parent or guardian.
    -- Important for younger students.
    parent_name    TEXT,

    -- This is the email address of the parent or guardian.
    -- Used for sending progress reports to parents.
    parent_email   TEXT,

    -- This is the contact number of the parent or guardian.
    -- Used to reach parents in case of emergencies.
    parent_contact TEXT,

    -- This stores any extra information about the user in a flexible format.
    -- It allows us to keep track of new data points without changing the table.
    custom_fields  JSONB,

    -- This is the date and time when the user was first created, in IST.
    -- We set this once and don't change it, so we know when they joined.
    created_at_ist TIMESTAMPTZ,

    -- This is the date and time when the user's info was last changed, in IST.
    -- It helps us see how recently their information was updated.
    updated_at_ist TIMESTAMPTZ,

    -- This is the exact time our system received this data.
    -- It defaults to the current time in the India/Kolkata timezone.
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),

    -- We ensure that each 'user_id' appears only once in this table.
    -- If we get new data for the same user, we update the existing row.
    UNIQUE (user_id)
);
