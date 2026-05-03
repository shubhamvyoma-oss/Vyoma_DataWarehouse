# ============================================================
# 07 — REPROCESS BRONZE
# ============================================================
# What it does: Reads every event stored in the Bronze table
#               and re-routes them to the correct Silver table.
#               This is a recovery tool — use it when the
#               webhook server was down and Silver is missing
#               events, or after a schema change.
#
# Why we need it: If the webhook server crashes or the DB is
#                 temporarily unavailable, events may sit in
#                 Bronze without ever reaching Silver.
#                 Running this script catches them all up.
#
# How to run:
#   python 07_reprocess_bronze/reprocess_bronze.py
#
# What to check after:
#   - All rows in bronze.webhook_events should have
#     routed_to_silver = true after this runs
#   - Silver table counts should be higher
#   - Run 12_check_db_counts to verify
# ============================================================

import sys
import datetime
import psycopg2
import psycopg2.extras
import psycopg2.pool


# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# ── SQL CONSTANTS ─────────────────────────────────────────────

SQL_UPSERT_USER_CREATED = """
    INSERT INTO silver.users (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at_ist, received_at
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s, %s, %s, %s,
        unix_to_ist(%s),
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (user_id) DO UPDATE SET
        event_id       = EXCLUDED.event_id,
        event_type     = EXCLUDED.event_type,
        email          = COALESCE(EXCLUDED.email,          silver.users.email),
        full_name      = COALESCE(EXCLUDED.full_name,      silver.users.full_name),
        user_name      = COALESCE(EXCLUDED.user_name,      silver.users.user_name),
        user_role      = COALESCE(EXCLUDED.user_role,      silver.users.user_role),
        contact_number = COALESCE(EXCLUDED.contact_number, silver.users.contact_number),
        institution_id = COALESCE(EXCLUDED.institution_id, silver.users.institution_id),
        created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
        received_at    = EXCLUDED.received_at
"""

SQL_UPSERT_USER_UPDATED = """
    INSERT INTO silver.users (
        event_id, event_type, user_id,
        email, full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        custom_fields, updated_at_ist, received_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, unix_to_ist(%s),
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (user_id) DO UPDATE SET
        event_id       = EXCLUDED.event_id,
        event_type     = EXCLUDED.event_type,
        email          = COALESCE(EXCLUDED.email,          silver.users.email),
        full_name      = COALESCE(EXCLUDED.full_name,      silver.users.full_name),
        contact_number = COALESCE(EXCLUDED.contact_number, silver.users.contact_number),
        city           = COALESCE(EXCLUDED.city,           silver.users.city),
        state          = COALESCE(EXCLUDED.state,          silver.users.state),
        address        = COALESCE(EXCLUDED.address,        silver.users.address),
        pincode        = COALESCE(EXCLUDED.pincode,        silver.users.pincode),
        parent_name    = COALESCE(EXCLUDED.parent_name,    silver.users.parent_name),
        parent_email   = COALESCE(EXCLUDED.parent_email,   silver.users.parent_email),
        parent_contact = COALESCE(EXCLUDED.parent_contact, silver.users.parent_contact),
        custom_fields  = COALESCE(EXCLUDED.custom_fields,  silver.users.custom_fields),
        updated_at_ist = EXCLUDED.updated_at_ist,
        created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
        received_at    = EXCLUDED.received_at
"""

SQL_UPSERT_TRANSACTION = """
    INSERT INTO silver.transactions (
        event_id, event_type, event_timestamp_ist,
        user_id, email, full_name, contact_number,
        bundle_id, course_name, institution_bundle_id, master_batch_id, master_batch_name,
        original_price, discount, final_price, currency, credits_applied,
        payment_method, transaction_id,
        start_date_ist, end_date_ist, created_at_ist,
        source
    ) VALUES (
        %s, %s, unix_to_ist(%s),
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s,
        unix_to_ist(%s), unix_to_ist(%s), unix_to_ist(%s),
        'webhook'
    )
    ON CONFLICT (user_id, bundle_id, master_batch_id) DO UPDATE SET
        event_id              = EXCLUDED.event_id,
        event_type            = EXCLUDED.event_type,
        event_timestamp_ist   = EXCLUDED.event_timestamp_ist,
        email                 = COALESCE(EXCLUDED.email,                 silver.transactions.email),
        full_name             = COALESCE(EXCLUDED.full_name,             silver.transactions.full_name),
        contact_number        = COALESCE(EXCLUDED.contact_number,        silver.transactions.contact_number),
        course_name           = COALESCE(EXCLUDED.course_name,           silver.transactions.course_name),
        master_batch_name     = COALESCE(EXCLUDED.master_batch_name,     silver.transactions.master_batch_name),
        institution_bundle_id = COALESCE(EXCLUDED.institution_bundle_id, silver.transactions.institution_bundle_id),
        original_price        = COALESCE(EXCLUDED.original_price,        silver.transactions.original_price),
        discount              = COALESCE(EXCLUDED.discount,              silver.transactions.discount),
        final_price           = COALESCE(EXCLUDED.final_price,           silver.transactions.final_price),
        currency              = COALESCE(EXCLUDED.currency,              silver.transactions.currency),
        credits_applied       = COALESCE(EXCLUDED.credits_applied,       silver.transactions.credits_applied),
        payment_method        = COALESCE(EXCLUDED.payment_method,        silver.transactions.payment_method),
        transaction_id        = COALESCE(EXCLUDED.transaction_id,        silver.transactions.transaction_id),
        start_date_ist        = COALESCE(EXCLUDED.start_date_ist,        silver.transactions.start_date_ist),
        end_date_ist          = COALESCE(EXCLUDED.end_date_ist,          silver.transactions.end_date_ist),
        created_at_ist        = COALESCE(EXCLUDED.created_at_ist,        silver.transactions.created_at_ist),
        source                = 'webhook'
"""

SQL_UPSERT_SESSION = """
    INSERT INTO silver.sessions (
        event_id, event_type, attendance_id,
        class_id, class_name, class_type_formatted,
        scheduled_start_ist, scheduled_end_ist, actual_start_ist,
        duration_minutes, teacher_id, teacher_name, teacher_email,
        master_batches, schedule_id, is_recurring,
        virtual_platform, zoom_meeting_id,
        cancellation_reason, cancelled_by,
        status, is_late_signin, delay_minutes, reminder_type,
        received_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s,
        unix_to_ist(%s), unix_to_ist(%s), unix_to_ist(%s),
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (attendance_id) DO UPDATE SET
        event_id             = EXCLUDED.event_id,
        event_type           = EXCLUDED.event_type,
        class_id             = COALESCE(EXCLUDED.class_id,             silver.sessions.class_id),
        class_name           = COALESCE(EXCLUDED.class_name,           silver.sessions.class_name),
        class_type_formatted = COALESCE(EXCLUDED.class_type_formatted, silver.sessions.class_type_formatted),
        scheduled_start_ist  = COALESCE(EXCLUDED.scheduled_start_ist,  silver.sessions.scheduled_start_ist),
        scheduled_end_ist    = COALESCE(EXCLUDED.scheduled_end_ist,    silver.sessions.scheduled_end_ist),
        actual_start_ist     = COALESCE(EXCLUDED.actual_start_ist,     silver.sessions.actual_start_ist),
        duration_minutes     = COALESCE(EXCLUDED.duration_minutes,     silver.sessions.duration_minutes),
        teacher_id           = COALESCE(EXCLUDED.teacher_id,           silver.sessions.teacher_id),
        teacher_name         = COALESCE(EXCLUDED.teacher_name,         silver.sessions.teacher_name),
        teacher_email        = COALESCE(EXCLUDED.teacher_email,        silver.sessions.teacher_email),
        master_batches       = COALESCE(EXCLUDED.master_batches,       silver.sessions.master_batches),
        schedule_id          = COALESCE(EXCLUDED.schedule_id,          silver.sessions.schedule_id),
        is_recurring         = COALESCE(EXCLUDED.is_recurring,         silver.sessions.is_recurring),
        virtual_platform     = COALESCE(EXCLUDED.virtual_platform,     silver.sessions.virtual_platform),
        zoom_meeting_id      = COALESCE(EXCLUDED.zoom_meeting_id,      silver.sessions.zoom_meeting_id),
        cancellation_reason  = COALESCE(EXCLUDED.cancellation_reason,  silver.sessions.cancellation_reason),
        cancelled_by         = COALESCE(EXCLUDED.cancelled_by,         silver.sessions.cancelled_by),
        status               = COALESCE(EXCLUDED.status,               silver.sessions.status),
        is_late_signin       = COALESCE(EXCLUDED.is_late_signin,       silver.sessions.is_late_signin),
        delay_minutes        = COALESCE(EXCLUDED.delay_minutes,        silver.sessions.delay_minutes),
        reminder_type        = COALESCE(EXCLUDED.reminder_type,        silver.sessions.reminder_type),
        received_at          = EXCLUDED.received_at
"""

SQL_UPSERT_ASSESSMENT = """
    INSERT INTO silver.assessments (
        event_id, event_type, user_id, attempt_id, exercise_id,
        mark, is_evaluated, faculty_comments, submitted_at_ist, received_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, unix_to_ist(%s),
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (event_id) DO UPDATE SET
        event_type       = EXCLUDED.event_type,
        user_id          = EXCLUDED.user_id,
        attempt_id       = EXCLUDED.attempt_id,
        exercise_id      = EXCLUDED.exercise_id,
        mark             = EXCLUDED.mark,
        is_evaluated     = EXCLUDED.is_evaluated,
        faculty_comments = EXCLUDED.faculty_comments,
        submitted_at_ist = EXCLUDED.submitted_at_ist,
        received_at      = EXCLUDED.received_at
"""

SQL_UPSERT_COURSE_COMPLETED = """
    INSERT INTO silver.courses (
        event_id, event_type, user_id, bundle_id, completed_at_ist, received_at
    ) VALUES (
        %s, %s, %s, %s, unix_to_ist(%s),
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (event_id) DO UPDATE SET
        event_type       = EXCLUDED.event_type,
        user_id          = EXCLUDED.user_id,
        bundle_id        = EXCLUDED.bundle_id,
        completed_at_ist = EXCLUDED.completed_at_ist,
        received_at      = EXCLUDED.received_at
"""

SQL_UPSERT_ANNOUNCEMENT = """
    INSERT INTO silver.announcements (event_id, event_type, raw_data, received_at)
    VALUES (%s, %s, %s, NOW() AT TIME ZONE 'Asia/Kolkata')
    ON CONFLICT (event_id) DO UPDATE SET
        event_type  = EXCLUDED.event_type,
        raw_data    = EXCLUDED.raw_data,
        received_at = EXCLUDED.received_at
"""

SQL_UPSERT_CERTIFICATE = """
    INSERT INTO silver.certificates (
        event_id, event_type, certificate_id, user_id, issued_at_ist, received_at
    ) VALUES (
        %s, %s, %s, %s, unix_to_ist(%s),
        NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (event_id) DO UPDATE SET
        event_type     = EXCLUDED.event_type,
        certificate_id = EXCLUDED.certificate_id,
        user_id        = EXCLUDED.user_id,
        issued_at_ist  = EXCLUDED.issued_at_ist,
        received_at    = EXCLUDED.received_at
"""

SQL_MARK_ROUTED = """
    UPDATE bronze.webhook_events
    SET    routed_to_silver = true
    WHERE  event_id = %s
"""

SQL_FETCH_BRONZE = """
    SELECT event_id, event_type, raw_payload
    FROM   bronze.webhook_events
    WHERE  event_type != 'url.validate'
    ORDER  BY id
"""


# ── DATABASE CONNECTION POOL ──────────────────────────────────

# Module-level variable that holds the connection pool
_pool = None


def initialize_pool():
    # Create a pool with 1–5 connections to reuse across events
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=5,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def get_connection():
    # Borrow one connection from the pool
    return _pool.getconn()


def release_connection(conn):
    # Return the connection to the pool so other code can reuse it
    _pool.putconn(conn)


# ── SYSTEM-FIELD HELPER ────────────────────────────────────────

def pluck_system_field(system_fields, *name_variants):
    # Real events use a list of {field_name, field_value} objects
    # Test events use a plain dict — handle both formats
    if isinstance(system_fields, dict):
        for name in name_variants:
            value = system_fields.get(name)
            if value is not None:
                return value
        return None

    if not isinstance(system_fields, list):
        return None

    # Build a lookup dict from the list of field objects
    lookup = {}
    for field in system_fields:
        if not isinstance(field, dict):
            continue
        # Both display name and internal name are valid keys
        display_name = (field.get("field_display_name") or "").lower().strip()
        internal_name = (field.get("field_name") or "").lower().strip()
        field_value = field.get("field_value")
        if display_name:
            lookup[display_name] = field_value
        if internal_name:
            lookup[internal_name] = field_value

    # Try each name variant in the lookup
    for name in name_variants:
        value = lookup.get(name.lower().strip())
        if value is not None:
            return value
    return None


# ── PAYLOAD PARSING HELPERS ───────────────────────────────────

def parse_event_timestamp(payload):
    # Real Edmingle events have an outer 'event' key with a date string
    # Test events (sent by test_webhook_send.py) use 'event_timestamp' (an integer)
    if "event" in payload and "id" not in payload:
        # Real event format: extract the ISO date string and convert to Unix
        event_object = payload.get("event", {})
        timestamp_string = event_object.get("event_ts", "")
        try:
            return int(datetime.datetime.fromisoformat(timestamp_string).timestamp())
        except (ValueError, TypeError):
            return None
    # Test event format: already a Unix integer
    return payload.get("event_timestamp")


def extract_data(payload):
    # Real events nest the data under 'payload'; test events use 'data'
    if "event" in payload and "id" not in payload:
        return payload.get("payload", {})
    return payload.get("data", {})


# ── SILVER ROUTING FUNCTIONS ──────────────────────────────────

def route_user_created(conn, event_id, event_type, data, event_timestamp):
    # Save a new user registration to silver.users
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_USER_CREATED, (
        event_id,
        event_type,
        data.get("user_id"),
        data.get("email"),
        # Accept both 'name' and 'full_name' field names
        data.get("name") or data.get("full_name"),
        data.get("user_name"),
        data.get("user_role"),
        data.get("contact_number"),
        data.get("institution_id"),
        # Use creation timestamp from data if available, else event timestamp
        data.get("created_at") or event_timestamp,
    ))
    cursor.close()


def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    # Save a user profile update to silver.users
    cursor = conn.cursor()

    # Real events nest user fields inside a 'user' object; test events are flat
    user_object = data.get("user") or data
    user_id = user_object.get("user_id") or user_object.get("id")
    full_name = user_object.get("name") or user_object.get("full_name")

    # Real events call the phone field 'phone'; test events call it 'contact_number'
    contact_number = user_object.get("phone") or user_object.get("contact_number")
    updated_at_unix = user_object.get("updated_at") or event_timestamp

    # Pull location fields — may live in system_fields for real events
    system_fields = data.get("system_fields")
    city    = user_object.get("city")    or pluck_system_field(system_fields, "city",    "City")
    state   = user_object.get("state")   or pluck_system_field(system_fields, "state",   "State")
    address = user_object.get("address") or pluck_system_field(system_fields, "address", "Address")
    pincode = user_object.get("pincode") or pluck_system_field(system_fields, "pincode", "Pincode", "pin code")

    # Parent fields only come from system_fields
    parent_name    = pluck_system_field(system_fields, "parent_name",    "Parent Name",    "parent name")
    parent_email   = pluck_system_field(system_fields, "parent_email",   "Parent Email",   "parent email")
    parent_contact = pluck_system_field(system_fields, "parent_contact", "Parent Contact", "Parent Phone", "parent phone")

    # Store the entire custom_fields blob as JSON
    custom_fields_raw = data.get("custom_fields")
    if custom_fields_raw is not None:
        custom_fields_json = psycopg2.extras.Json(custom_fields_raw)
    else:
        custom_fields_json = None

    cursor.execute(SQL_UPSERT_USER_UPDATED, (
        event_id, event_type, user_id,
        user_object.get("email"), full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        custom_fields_json, updated_at_unix,
    ))
    cursor.close()


def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # Save an enrollment/purchase event to silver.transactions
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_TRANSACTION, (
        event_id,
        event_type,
        event_timestamp,
        data.get("user_id"),
        data.get("email"),
        data.get("name") or data.get("full_name"),
        data.get("contact_number"),
        data.get("bundle_id"),
        data.get("course_name"),
        data.get("institution_bundle_id"),
        data.get("master_batch_id"),
        data.get("master_batch_name"),
        data.get("original_price"),
        data.get("discount"),
        data.get("final_price"),
        data.get("currency"),
        data.get("credits_applied"),
        data.get("payment_method"),
        data.get("transaction_id"),
        data.get("start_date"),
        data.get("end_date"),
        data.get("created_at"),
    ))
    cursor.close()


def route_session(conn, event_id, event_type, data, event_timestamp):
    # Save a live session event to silver.sessions
    cursor = conn.cursor()

    # Store master_batches as a JSON blob
    master_batches_raw = data.get("master_batches")
    if master_batches_raw is not None:
        master_batches_json = psycopg2.extras.Json(master_batches_raw)
    else:
        master_batches_json = None

    # Real events call start time 'taken_at'; test events call it 'actual_start_time'
    actual_start = data.get("actual_start_time") or data.get("taken_at")

    cursor.execute(SQL_UPSERT_SESSION, (
        event_id, event_type, data.get("attendance_id"),
        data.get("class_id"), data.get("class_name"), data.get("class_type_formatted"),
        data.get("gmt_start_time"), data.get("gmt_end_time"), actual_start,
        data.get("duration_minutes"), data.get("taken_by"), data.get("taken_by_name"),
        data.get("taken_by_email"), master_batches_json, data.get("schedule_id"),
        data.get("is_recurring"), data.get("virtual_class_type_formatted"),
        data.get("zoom_meeting_id"), data.get("cancellation_reason"),
        data.get("cancelled_by"), data.get("status"), data.get("is_late_signin"),
        data.get("delay_minutes"), data.get("reminder_type"),
    ))
    cursor.close()


def route_assessment(conn, event_id, event_type, data, event_timestamp):
    # Save a test or exercise submission to silver.assessments
    cursor = conn.cursor()

    # Real events use 'test_date'; test events use 'submitted_at'
    submitted_at = data.get("submitted_at") or data.get("test_date")

    cursor.execute(SQL_UPSERT_ASSESSMENT, (
        event_id, event_type,
        data.get("user_id"), data.get("attempt_id"), data.get("exercise_id"),
        data.get("mark"), data.get("is_evaluated"), data.get("faculty_comments"),
        submitted_at,
    ))
    cursor.close()


def route_course_completed(conn, event_id, event_type, data, event_timestamp):
    # Save a course completion event to silver.courses
    cursor = conn.cursor()

    # Real events have no 'completed_at' field — fall back to event_timestamp
    completed_at = data.get("completed_at") or event_timestamp

    cursor.execute(SQL_UPSERT_COURSE_COMPLETED, (
        event_id, event_type,
        data.get("user_id"), data.get("bundle_id"), completed_at,
    ))
    cursor.close()


def route_announcement(conn, event_id, event_type, data, event_timestamp):
    # Save an announcement event to silver.announcements (stores raw data as JSON)
    cursor = conn.cursor()
    if data is not None:
        data_json = psycopg2.extras.Json(data)
    else:
        data_json = None
    cursor.execute(SQL_UPSERT_ANNOUNCEMENT, (event_id, event_type, data_json))
    cursor.close()


def route_certificate(conn, event_id, event_type, data, event_timestamp):
    # Save a certificate issued event to silver.certificates
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_CERTIFICATE, (
        event_id, event_type,
        data.get("certificate_id"), data.get("user_id"), data.get("issued_at"),
    ))
    cursor.close()


# ── EVENT ROUTER ─────────────────────────────────────────────
# Maps each Edmingle event type string to the function that handles it

EVENT_ROUTER = {
    "user.user_created":                   route_user_created,
    "user.user_updated":                   route_user_updated,
    "transaction.user_purchase_initiated": route_transaction,
    "transaction.user_purchase_completed": route_transaction,
    "transaction.user_purchase_failed":    route_transaction,
    "session.session_created":             route_session,
    "session.session_update":              route_session,
    "session.session_cancel":              route_session,
    "session.session_started":             route_session,
    "session.session_start":               route_session,
    "session.session_reminders":           route_session,
    "session.session_reminder":            route_session,
    "assessments.test_submitted":          route_assessment,
    "assessments.test_evaluated":          route_assessment,
    "assessments.exercise_submitted":      route_assessment,
    "assessments.exercise_evaluated":      route_assessment,
    "course.user_course_completed":        route_course_completed,
    "announcement.announcement_created":   route_announcement,
    "certificate.certificate_issued":      route_certificate,
}


# ── MAIN ─────────────────────────────────────────────────────

def fetch_all_bronze_events(conn):
    # Read every event from Bronze (except URL validation events)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(SQL_FETCH_BRONZE)
    # fetchall() loads all rows into memory at once — fine for our data size
    all_rows = cursor.fetchall()
    cursor.close()
    return all_rows


def mark_event_routed(conn, event_id):
    # Mark this Bronze event as successfully routed to Silver
    cursor = conn.cursor()
    cursor.execute(SQL_MARK_ROUTED, (event_id,))
    cursor.close()


def process_one_event(event_row):
    # Try to route a single Bronze event to Silver
    # Returns: "ok", "skip", or "error"
    event_id   = event_row["event_id"]
    event_type = event_row["event_type"]
    payload    = event_row["raw_payload"]

    # Check if we have a handler for this event type
    router_function = EVENT_ROUTER.get(event_type)
    if router_function is None:
        # Unknown event type — skip it, not an error
        return "skip"

    # Parse the data payload and timestamp from the raw JSON
    data            = extract_data(payload)
    event_timestamp = parse_event_timestamp(payload)

    # Borrow a DB connection, run the routing function, commit
    conn = get_connection()
    try:
        router_function(conn, event_id, event_type, data, event_timestamp)
        mark_event_routed(conn, event_id)
        conn.commit()
        return "ok"
    except Exception as error:
        # Roll back so the connection is clean before returning it to the pool
        conn.rollback()
        print("  ERROR " + event_type + " [" + str(event_id) + "]: " + str(error),
              file=sys.stderr)
        return "error"
    finally:
        # Always return the connection to the pool
        release_connection(conn)


def main():
    print("=== reprocess_bronze.py ===")
    print("")

    # Set up the connection pool before we start reading events
    initialize_pool()

    # Use a separate connection just for reading Bronze events
    read_conn = get_connection()
    all_rows = fetch_all_bronze_events(read_conn)
    release_connection(read_conn)

    total_events = len(all_rows)
    ok_count     = 0
    skip_count   = 0
    error_count  = 0

    print("Processing " + str(total_events) + " Bronze events ...")
    print("")

    for row_index in range(total_events):
        event_row = all_rows[row_index]

        # Print progress every 50 events so we can see it is working
        if row_index > 0 and row_index % 50 == 0:
            print("  Progress: " + str(row_index) + "/" + str(total_events)
                  + "  (ok=" + str(ok_count)
                  + " skip=" + str(skip_count)
                  + " err=" + str(error_count) + ")")

        result = process_one_event(event_row)

        if result == "ok":
            ok_count = ok_count + 1
        elif result == "skip":
            skip_count = skip_count + 1
        else:
            error_count = error_count + 1

    print("")
    print("  REPROCESS COMPLETE")
    print("  " + "-" * 41)
    print("  Total Bronze events   : " + str(total_events))
    print("  Routed to Silver      : " + str(ok_count))
    print("  Skipped (unknown type): " + str(skip_count))
    print("  Errors                : " + str(error_count))
    print("")


if __name__ == "__main__":
    main()
