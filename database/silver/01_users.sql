-- Table: silver.users
-- Purpose: one row per student; UPSERT on user_id; COALESCE preserves existing non-null values on update

CREATE TABLE IF NOT EXISTS silver.users (
    id             SERIAL PRIMARY KEY,
    event_id       TEXT NOT NULL,
    event_type     TEXT NOT NULL,
    user_id        BIGINT NOT NULL,
    email          TEXT,
    full_name      TEXT,
    user_name      TEXT,
    user_role      TEXT,
    contact_number TEXT,
    institution_id INTEGER,
    city           TEXT,
    state          TEXT,
    address        TEXT,
    pincode        TEXT,
    parent_name    TEXT,
    parent_email   TEXT,
    parent_contact TEXT,
    custom_fields  JSONB,
    created_at_ist TIMESTAMPTZ,   -- set once; never overwritten by user_updated
    updated_at_ist TIMESTAMPTZ,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT (NOW() AT TIME ZONE 'Asia/Kolkata'),
    UNIQUE (user_id)
);
