# =============================================================================
# FILE    : ingestion/webhook_receiver.py
# PROJECT : Edmingle Webhook Data Pipeline — Vyoma Samskrta Pathasala
# PURPOSE : Flask web server that receives webhook events from Edmingle.
#           Every event is stored in Bronze (raw JSON) first, then routed
#           to the correct Silver table (structured, typed data).
#
# ENDPOINTS:
#   POST /webhook  — Receives events from Edmingle. Always returns HTTP 200.
#   GET  /health   — Checks that the server and database are reachable.
#   GET  /status   — Shows the last 10 events received (from Bronze).
#
# HOW TO RUN:
#   pip install flask psycopg2-binary
#   python ingestion/webhook_receiver.py
#   Server starts at http://localhost:5000
# =============================================================================


# =============================================================================
# IMPORTS
# flask         : the web framework — lets us define HTTP endpoints
# json          : converts Python dicts/lists to JSON strings
# logging       : writes timestamped messages to the terminal
# psycopg2      : Python driver for connecting to PostgreSQL
# psycopg2.extras: provides RealDictCursor (rows as dicts) and Json (JSONB adapter)
# =============================================================================
import json
import logging
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify


# =============================================================================
# LOGGING SETUP
# Every event we receive and every step we take gets logged to the terminal.
# This lets you see exactly what the server is doing without opening pgAdmin.
# Format: "2024-03-08 05:30:00  INFO  Received event user.user_created"
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# =============================================================================
# FLASK APPLICATION
# Flask(__name__) creates the web server.
# __name__ tells Flask which Python module it is running in.
# =============================================================================
app = Flask(__name__)


# =============================================================================
# DATABASE CONNECTION SETTINGS
# Hardcoded as agreed — no environment variables.
# =============================================================================
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'edmingle_analytics'
DB_USER = 'postgres'
DB_PASS = 'Svyoma'


# =============================================================================
# HELPER: get_db_connection()
# Opens a fresh PostgreSQL connection every time it is called.
# We open one connection for Bronze and a separate one for Silver in each
# request. This keeps them in separate transactions — if Silver fails and
# rolls back, the Bronze commit is already saved and unaffected.
# =============================================================================
def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )


# =============================================================================
# BRONZE FUNCTIONS
# These two functions write to bronze.webhook_events.
# insert_bronze() stores the raw payload.
# mark_routed_to_silver() updates the flag after Silver succeeds.
# =============================================================================

def insert_bronze(conn, event_id, event_type, payload, is_live_mode):
    # ON CONFLICT DO NOTHING: if Edmingle retries and sends the same event_id
    # again, skip the insert without an error instead of creating a duplicate row.
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bronze.webhook_events (event_id, event_type, raw_payload, is_live_mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (
        event_id,
        event_type,
        # psycopg2.extras.Json() converts a Python dict to the correct format
        # for a PostgreSQL JSONB column. It handles all nested objects and arrays.
        psycopg2.extras.Json(payload),
        is_live_mode
    ))
    cur.close()


def mark_routed_to_silver(conn, event_id):
    # Sets routed_to_silver = true after Silver insert succeeds.
    # This runs in the SAME transaction as the Silver insert.
    # If Silver fails and the transaction rolls back, this update also rolls back —
    # which is correct. The event stays marked as unrouted so we can reprocess it.
    cur = conn.cursor()
    cur.execute("""
        UPDATE bronze.webhook_events
        SET    routed_to_silver = true
        WHERE  event_id = %s
    """, (event_id,))
    cur.close()


# =============================================================================
# SILVER ROUTING FUNCTIONS
# One function per event group.
# Each function extracts typed fields from the raw payload dict and writes a
# clean, structured row to the correct Silver table.
#
# All inserts use ON CONFLICT ... DO UPDATE (upsert) — never plain INSERT.
# If Edmingle retries and sends the same event twice, we update in place
# instead of creating a duplicate row.
#
# None of these functions call conn.commit() or conn.close().
# The webhook() route manages the transaction — it commits after all Silver
# work succeeds, or rolls back if anything fails.
# =============================================================================


def route_user_created(conn, event_id, event_type, data, event_timestamp):
    # Handles: user.user_created
    # Inserts one row per new student. UPSERT on user_id.
    cur = conn.cursor()

    # .get() returns None if the key is absent — it never raises a KeyError.
    # This is important because Edmingle may not always send every field.
    user_id        = data.get('user_id')
    email          = data.get('email')
    full_name      = data.get('full_name')
    user_name      = data.get('user_name')
    user_role      = data.get('user_role')
    contact_number = data.get('contact_number')
    institution_id = data.get('institution_id')

    # Use created_at from the data payload if it exists.
    # Fall back to the top-level event_timestamp if created_at is absent.
    # unix_to_ist() is our SQL function that converts a Unix integer to an IST timestamp.
    created_at_unix = data.get('created_at') or event_timestamp

    cur.execute("""
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
            -- COALESCE(new_value, existing_value):
            -- Use the new value if it is not NULL; otherwise keep what is already stored.
            -- This protects existing data when a field is absent from the incoming event.
            email          = COALESCE(EXCLUDED.email,          silver.users.email),
            full_name      = COALESCE(EXCLUDED.full_name,      silver.users.full_name),
            user_name      = COALESCE(EXCLUDED.user_name,      silver.users.user_name),
            user_role      = COALESCE(EXCLUDED.user_role,      silver.users.user_role),
            contact_number = COALESCE(EXCLUDED.contact_number, silver.users.contact_number),
            institution_id = COALESCE(EXCLUDED.institution_id, silver.users.institution_id),
            -- COALESCE(existing_value, new_value) — note the order is REVERSED here.
            -- This means: keep the original value if it exists; only set it if it was never set.
            -- created_at_ist must NEVER be overwritten once it is first set.
            created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
            received_at    = EXCLUDED.received_at
    """, (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at_unix
    ))
    cur.close()


def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    # Handles: user.user_updated
    # Only CHANGED fields are sent in this event — not the full student record.
    # COALESCE in the upsert preserves existing values for any field not included.
    cur = conn.cursor()

    user_id        = data.get('user_id')
    email          = data.get('email')
    full_name      = data.get('full_name')
    contact_number = data.get('contact_number')

    # updated_at is when the profile change happened.
    # Fall back to event_timestamp if updated_at is absent.
    updated_at_unix = data.get('updated_at') or event_timestamp

    # system_fields is a nested object inside the payload.
    # We use 'or {}' so that if system_fields is missing, we get an empty dict
    # instead of None — making the .get() calls below always safe.
    system_fields  = data.get('system_fields') or {}
    city           = system_fields.get('city')
    state          = system_fields.get('state')
    address        = system_fields.get('address')
    pincode        = system_fields.get('pincode')
    parent_name    = system_fields.get('parent_name')
    parent_email   = system_fields.get('parent_email')
    parent_contact = system_fields.get('parent_contact')

    # custom_fields is an array of objects like:
    #   [{"field_name": "occupation", "field_value": "Engineer", ...}, ...]
    # We store the entire array as JSONB. Individual values are ALWAYS read
    # by matching field_name string — NEVER by index position [0], [1], etc.
    # because the order of fields in the array can differ between events.
    custom_fields_raw = data.get('custom_fields')
    # Use psycopg2.extras.Json() if the array is present; use None (SQL NULL) if absent.
    custom_fields_json = psycopg2.extras.Json(custom_fields_raw) if custom_fields_raw is not None else None

    cur.execute("""
        INSERT INTO silver.users (
            event_id, event_type, user_id,
            email, full_name, contact_number,
            city, state, address, pincode,
            parent_name, parent_email, parent_contact,
            custom_fields, updated_at_ist, received_at
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, unix_to_ist(%s),
            NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (user_id) DO UPDATE SET
            event_id       = EXCLUDED.event_id,
            event_type     = EXCLUDED.event_type,
            -- For each field: if this update included it (not NULL), use the new value.
            -- If the field was not in this update (NULL), keep whatever was stored before.
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
            -- updated_at_ist always takes the new value — it records when this change happened.
            updated_at_ist = EXCLUDED.updated_at_ist,
            -- created_at_ist is NEVER overwritten — keep the original registration date.
            created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
            received_at    = EXCLUDED.received_at
    """, (
        event_id, event_type, user_id,
        email, full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        custom_fields_json, updated_at_unix
    ))
    cur.close()


def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # Handles all three transaction event types in one function:
    #   transaction.user_purchase_initiated  — checkout started, NOT a confirmed enrollment
    #   transaction.user_purchase_completed  — payment succeeded, CONFIRMED enrollment
    #   transaction.user_purchase_failed     — payment failed, NOT an enrollment
    #
    # Fields that do not apply to a given event type will be None (SQL NULL) in that row.
    # UPSERT on event_id — one row per event.
    cur = conn.cursor()

    user_id               = data.get('user_id')
    email                 = data.get('email')
    full_name             = data.get('full_name')
    bundle_id             = data.get('bundle_id')
    course_name           = data.get('course_name')
    institution_bundle_id = data.get('institution_bundle_id')
    master_batch_id       = data.get('master_batch_id')
    master_batch_name     = data.get('master_batch_name')
    original_price        = data.get('original_price')
    discount              = data.get('discount')
    final_price           = data.get('final_price')
    currency              = data.get('currency')
    credits_applied       = data.get('credits_applied')

    # Only present in purchase_completed — None (SQL NULL) for all other event types
    payment_method    = data.get('payment_method')
    transaction_id    = data.get('transaction_id')
    enrollment_status = data.get('enrollment_status')

    # Only present in purchase_failed — None for all other event types
    failure_reason = data.get('failure_reason')
    error_code     = data.get('error_code')

    # Present in initiated and completed — None in failed
    start_date_unix = data.get('start_date')
    end_date_unix   = data.get('end_date')

    # unix_to_ist(NULL) returns NULL in PostgreSQL — safe to pass None here
    cur.execute("""
        INSERT INTO silver.transactions (
            event_id, event_type,
            user_id, email, full_name,
            bundle_id, course_name, institution_bundle_id, master_batch_id, master_batch_name,
            original_price, discount, final_price, currency, credits_applied,
            payment_method, transaction_id, enrollment_status,
            failure_reason, error_code,
            start_date_ist, end_date_ist, event_timestamp_ist,
            received_at
        ) VALUES (
            %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            unix_to_ist(%s), unix_to_ist(%s), unix_to_ist(%s),
            NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (event_id) DO UPDATE SET
            event_type            = EXCLUDED.event_type,
            user_id               = EXCLUDED.user_id,
            email                 = EXCLUDED.email,
            full_name             = EXCLUDED.full_name,
            bundle_id             = EXCLUDED.bundle_id,
            course_name           = EXCLUDED.course_name,
            institution_bundle_id = EXCLUDED.institution_bundle_id,
            master_batch_id       = EXCLUDED.master_batch_id,
            master_batch_name     = EXCLUDED.master_batch_name,
            original_price        = EXCLUDED.original_price,
            discount              = EXCLUDED.discount,
            final_price           = EXCLUDED.final_price,
            currency              = EXCLUDED.currency,
            credits_applied       = EXCLUDED.credits_applied,
            payment_method        = EXCLUDED.payment_method,
            transaction_id        = EXCLUDED.transaction_id,
            enrollment_status     = EXCLUDED.enrollment_status,
            failure_reason        = EXCLUDED.failure_reason,
            error_code            = EXCLUDED.error_code,
            start_date_ist        = EXCLUDED.start_date_ist,
            end_date_ist          = EXCLUDED.end_date_ist,
            event_timestamp_ist   = EXCLUDED.event_timestamp_ist,
            received_at           = EXCLUDED.received_at
    """, (
        event_id, event_type,
        user_id, email, full_name,
        bundle_id, course_name, institution_bundle_id, master_batch_id, master_batch_name,
        original_price, discount, final_price, currency, credits_applied,
        payment_method, transaction_id, enrollment_status,
        failure_reason, error_code,
        start_date_unix, end_date_unix, event_timestamp
    ))
    cur.close()


def route_session(conn, event_id, event_type, data, event_timestamp):
    # Handles all five session event types, all sharing one row per attendance_id:
    #   session.session_created   — fills most fields when a class is first scheduled
    #   session.session_update    — updates changed fields (note: no 'd' at the end)
    #   session.session_cancel    — adds cancellation_reason and cancelled_by
    #   session.session_started   — adds actual_start_ist, is_late_signin, delay_minutes
    #   session.session_reminders — adds reminder_type
    #
    # UPSERT on attendance_id. COALESCE means each new event adds its own fields
    # without overwriting unrelated fields set by a previous event.
    cur = conn.cursor()

    attendance_id        = data.get('attendance_id')
    class_id             = data.get('class_id')
    class_name           = data.get('class_name')
    class_type_formatted = data.get('class_type_formatted')

    # Scheduled times: Edmingle calls them gmt_start_time and gmt_end_time (Unix UTC)
    gmt_start_time = data.get('gmt_start_time')
    gmt_end_time   = data.get('gmt_end_time')

    # Actual start time — only in session_started, None otherwise
    actual_start_time = data.get('actual_start_time')

    duration_minutes = data.get('duration_minutes')

    # Teacher fields have different source names in the Edmingle payload than our column names
    teacher_id    = data.get('taken_by')        # payload field: taken_by
    teacher_name  = data.get('taken_by_name')   # payload field: taken_by_name
    teacher_email = data.get('taken_by_email')  # payload field: taken_by_email

    # master_batches is an array — store the whole array as JSONB so no batch data is lost
    master_batches_raw  = data.get('master_batches')
    master_batches_json = psycopg2.extras.Json(master_batches_raw) if master_batches_raw is not None else None

    schedule_id  = data.get('schedule_id')
    is_recurring = data.get('is_recurring')

    # virtual_class_type_formatted is the human-readable meeting platform name, e.g. "Zoom"
    virtual_platform = data.get('virtual_class_type_formatted')
    zoom_meeting_id  = data.get('zoom_meeting_id')

    # Cancellation fields — only in session_cancel, None for all other session events
    cancellation_reason = data.get('cancellation_reason')
    cancelled_by        = data.get('cancelled_by')

    status = data.get('status')

    # Late sign-in fields — only in session_started, None for all other session events
    is_late_signin = data.get('is_late_signin')
    delay_minutes  = data.get('delay_minutes')

    # Reminder type — only in session_reminders ("1h_before" or "24h_before")
    reminder_type = data.get('reminder_type')

    cur.execute("""
        INSERT INTO silver.sessions (
            event_id, event_type, attendance_id,
            class_id, class_name, class_type_formatted,
            scheduled_start_ist, scheduled_end_ist, actual_start_ist,
            duration_minutes,
            teacher_id, teacher_name, teacher_email,
            master_batches,
            schedule_id, is_recurring,
            virtual_platform, zoom_meeting_id,
            cancellation_reason, cancelled_by,
            status, is_late_signin, delay_minutes,
            reminder_type,
            received_at
        ) VALUES (
            %s, %s, %s,
            %s, %s, %s,
            unix_to_ist(%s), unix_to_ist(%s), unix_to_ist(%s),
            %s,
            %s, %s, %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s,
            NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (attendance_id) DO UPDATE SET
            event_id             = EXCLUDED.event_id,
            event_type           = EXCLUDED.event_type,
            -- COALESCE: use the new value if it came with this event (not NULL),
            -- keep the existing value if this event did not include that field (NULL).
            -- This lets session_cancel add its fields without wiping session_created's fields.
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
    """, (
        event_id, event_type, attendance_id,
        class_id, class_name, class_type_formatted,
        gmt_start_time, gmt_end_time, actual_start_time,
        duration_minutes,
        teacher_id, teacher_name, teacher_email,
        master_batches_json,
        schedule_id, is_recurring,
        virtual_platform, zoom_meeting_id,
        cancellation_reason, cancelled_by,
        status, is_late_signin, delay_minutes,
        reminder_type
    ))
    cur.close()


def route_assessment(conn, event_id, event_type, data, event_timestamp):
    # Handles all four assessment event types:
    #   assessments.test_submitted, assessments.test_evaluated
    #   assessments.exercise_submitted, assessments.exercise_evaluated
    #
    # exercise_id is None for test events (not present in the payload).
    # faculty_comments is None until an 'evaluated' event arrives.
    # UPSERT on event_id — one row per event.
    cur = conn.cursor()

    user_id           = data.get('user_id')
    attempt_id        = data.get('attempt_id')
    exercise_id       = data.get('exercise_id')       # None for test events
    mark              = data.get('mark')
    is_evaluated      = data.get('is_evaluated')
    faculty_comments  = data.get('faculty_comments')  # None until evaluated
    submitted_at_unix = data.get('submitted_at')

    cur.execute("""
        INSERT INTO silver.assessments (
            event_id, event_type, user_id, attempt_id, exercise_id,
            mark, is_evaluated, faculty_comments,
            submitted_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            unix_to_ist(%s),
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
    """, (
        event_id, event_type, user_id, attempt_id, exercise_id,
        mark, is_evaluated, faculty_comments,
        submitted_at_unix
    ))
    cur.close()


def route_course(conn, event_id, event_type, data, event_timestamp):
    # Handles: course.user_course_completed
    # Recorded when a student finishes an entire course bundle.
    # UPSERT on event_id — one row per event.
    cur = conn.cursor()

    user_id           = data.get('user_id')
    bundle_id         = data.get('bundle_id')
    completed_at_unix = data.get('completed_at')

    cur.execute("""
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
    """, (event_id, event_type, user_id, bundle_id, completed_at_unix))
    cur.close()


def route_announcement(conn, event_id, event_type, data, event_timestamp):
    # Handles: announcement.announcement_created
    # The full payload is not yet documented — store the entire data{} object as JSONB.
    # Structured columns can be added later once we have seen real payloads.
    # UPSERT on event_id — one row per event.
    cur = conn.cursor()

    # Store the entire data dict as raw JSONB — no field extraction
    raw_data_json = psycopg2.extras.Json(data) if data is not None else None

    cur.execute("""
        INSERT INTO silver.announcements (event_id, event_type, raw_data, received_at)
        VALUES (%s, %s, %s, NOW() AT TIME ZONE 'Asia/Kolkata')
        ON CONFLICT (event_id) DO UPDATE SET
            event_type  = EXCLUDED.event_type,
            raw_data    = EXCLUDED.raw_data,
            received_at = EXCLUDED.received_at
    """, (event_id, event_type, raw_data_json))
    cur.close()


def route_certificate(conn, event_id, event_type, data, event_timestamp):
    # Handles: certificate.certificate_issued
    # Recorded when Edmingle generates a certificate for a student.
    # UPSERT on event_id — one row per event.
    cur = conn.cursor()

    certificate_id = data.get('certificate_id')
    user_id        = data.get('user_id')
    issued_at_unix = data.get('issued_at')

    cur.execute("""
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
    """, (event_id, event_type, certificate_id, user_id, issued_at_unix))
    cur.close()


# =============================================================================
# EVENT ROUTING MAP
# Maps each Edmingle event_name string to its Silver routing function.
# When a webhook arrives, we look up event_type here to find what to call.
# If event_type is not in this dict, we log a warning and keep Bronze only —
# we never silently drop an event.
# =============================================================================
EVENT_ROUTER = {
    # User events → silver.users
    'user.user_created':                   route_user_created,
    'user.user_updated':                   route_user_updated,

    # Transaction events → silver.transactions
    'transaction.user_purchase_initiated': route_transaction,
    'transaction.user_purchase_completed': route_transaction,
    'transaction.user_purchase_failed':    route_transaction,

    # Session events → silver.sessions
    # Note: Edmingle sends 'session_update' (no 'd') and 'session_cancel' (no 'led')
    'session.session_created':             route_session,
    'session.session_update':              route_session,
    'session.session_cancel':              route_session,
    'session.session_started':             route_session,
    'session.session_reminders':           route_session,

    # Assessment events → silver.assessments
    'assessments.test_submitted':          route_assessment,
    'assessments.test_evaluated':          route_assessment,
    'assessments.exercise_submitted':      route_assessment,
    'assessments.exercise_evaluated':      route_assessment,

    # Course completion → silver.courses
    'course.user_course_completed':        route_course,

    # Announcement → silver.announcements
    'announcement.announcement_created':   route_announcement,

    # Certificate → silver.certificates
    'certificate.certificate_issued':      route_certificate,
}


# =============================================================================
# FLASK ROUTE: GET /health
# Quick check that the server is running and can reach the database.
# Returns 200 if healthy, 500 if the database connection fails.
# (The "always return 200" rule only applies to /webhook, not to /health.)
# =============================================================================
@app.route('/health', methods=['GET'])
def health():
    try:
        # Try to open a database connection and immediately close it
        conn = get_db_connection()
        conn.close()
        return jsonify({'status': 'ok', 'database': 'connected'}), 200
    except Exception as e:
        log.error(f"Health check failed — cannot reach database: {e}")
        return jsonify({'status': 'error', 'database': str(e)}), 500


# =============================================================================
# FLASK ROUTE: GET /status
# Shows the last 10 events stored in Bronze, newest first.
# Useful for confirming events are arriving without opening pgAdmin.
# =============================================================================
@app.route('/status', methods=['GET'])
def status():
    conn = None
    try:
        conn = get_db_connection()

        # RealDictCursor returns each row as a dict {column_name: value}
        # instead of the default tuple (value, value, ...) — easier to read
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT event_id, event_type, received_at, is_live_mode, routed_to_silver
            FROM   bronze.webhook_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cur.fetchall()
        cur.close()

        # jsonify() cannot serialize Python datetime objects — convert each to a string.
        # .isoformat() produces a standard format like "2024-03-08T05:30:00+05:30"
        events = []
        for row in rows:
            events.append({
                'event_id':         row['event_id'],
                'event_type':       row['event_type'],
                'received_at':      row['received_at'].isoformat() if row['received_at'] else None,
                'is_live_mode':     row['is_live_mode'],
                'routed_to_silver': row['routed_to_silver'],
            })

        return jsonify({'count': len(events), 'last_10_events': events}), 200

    except Exception as e:
        log.error(f"Status endpoint failed: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        # Always close the connection — whether the query succeeded or failed
        if conn:
            conn.close()


# =============================================================================
# FLASK ROUTE: POST /webhook
# The main endpoint. Edmingle sends all events here as HTTP POST requests.
#
# FLOW:
#   1. Parse the JSON body from the incoming request.
#   2. Write the raw payload to Bronze (always — this is our safety net).
#      Bronze uses its own connection and commits immediately.
#   3. Look up the event_type in EVENT_ROUTER to find the right Silver function.
#   4. Call that Silver function to write structured data to the Silver table.
#   5. Mark the Bronze row as routed_to_silver = true (same transaction as step 4).
#   6. Return HTTP 200 to Edmingle — always, no matter what happened internally.
#
# CRITICAL RULE: Edmingle marks our webhook Inactive permanently if it receives
# anything other than HTTP 200. We must return 200 even if our database is down,
# the payload is malformed, or our Silver code throws an exception.
# =============================================================================
@app.route('/webhook', methods=['POST'])
def webhook():

    # -------------------------------------------------------------------------
    # STEP 1: Capture and parse the raw request body.
    #
    # We log the raw body FIRST before any processing. This means even if our
    # parsing logic fails, we have a record of exactly what Edmingle sent us.
    # This is critical for debugging payload structure differences.
    #
    # force=True tells Flask to parse the body as JSON regardless of the
    # Content-Type header. Without this, get_json() returns None whenever
    # Edmingle sends events without 'Content-Type: application/json', which
    # causes silent data loss — we return 200 but store nothing.
    # -------------------------------------------------------------------------
    raw_body = request.get_data(as_text=True)
    log.info(f"Raw request received — Content-Type: {request.content_type} — Body: {raw_body[:300]}")

    payload = request.get_json(silent=True, force=True)

    if payload is None:
        log.warning(f"Could not parse body as JSON — returning 200. Body was: {raw_body[:300]}")
        return jsonify({'status': 'received'}), 200

    # -------------------------------------------------------------------------
    # Detect which payload structure Edmingle sent.
    #
    # Normal events use top-level fields:
    #   { "id": "...", "event_name": "user.user_created", "data": {...} }
    #
    # The url.validate ping uses a different structure — everything is nested
    # inside an 'event' key and the field names are different:
    #   { "event": { "event_name": "url.validate", "livemode": false,
    #                "event_ts": "2026-04-28T08:05:20+00:00" } }
    #
    # We normalise both into the same local variables so the rest of the
    # function does not need to know which structure arrived.
    # -------------------------------------------------------------------------
    if 'event' in payload and 'id' not in payload:
        # Nested structure — url.validate ping from Edmingle
        event_block     = payload['event']
        event_type      = event_block.get('event_name')
        event_ts_str    = event_block.get('event_ts', '')
        # This format has no 'id' field. Build a deterministic event_id from
        # the event_ts string so Bronze can store it with a valid unique key.
        event_id        = f"{event_type}-{event_ts_str}" if event_ts_str else event_type
        is_live_mode    = event_block.get('livemode', True)
        event_timestamp = None  # event_ts is an ISO string, not a Unix integer
        # Extract data from inside the event block if it exists.
        # Previously this was hardcoded to {} which would silently discard all
        # student/session/transaction data for any event using the nested structure.
        data            = event_block.get('data', {})
    else:
        # Normal structure — used by all real Edmingle event types
        event_id        = payload.get('id')
        event_type      = payload.get('event_name')
        event_timestamp = payload.get('event_timestamp')
        is_live_mode    = payload.get('is_live_mode', True)
        data            = payload.get('data', {})

    if not event_id or not event_type:
        # Edmingle always sends these — log if they are missing and move on
        log.warning(f"Event missing 'id' or 'event_name'. Raw payload: {payload}")
        return jsonify({'status': 'received'}), 200

    log.info(f"Received: {event_type}  [event_id: {event_id}]")

    # -------------------------------------------------------------------------
    # STEP 2: Write to Bronze (our permanent safety net)
    # This uses its own connection and commits immediately.
    # Bronze is completely independent of Silver — a Silver failure cannot
    # affect or undo the Bronze record.
    # -------------------------------------------------------------------------
    conn_bronze = None
    try:
        conn_bronze = get_db_connection()
        insert_bronze(conn_bronze, event_id, event_type, payload, is_live_mode)
        # commit() saves the INSERT permanently — it cannot be undone after this point
        conn_bronze.commit()
        log.info(f"Bronze stored: {event_type}  [event_id: {event_id}]")
    except Exception as e:
        # rollback() cancels any partial changes in this transaction
        if conn_bronze:
            conn_bronze.rollback()
        log.error(f"Bronze insert failed for {event_type} [event_id: {event_id}]: {e}")
    finally:
        # Always close the connection — whether we succeeded or failed
        if conn_bronze:
            conn_bronze.close()

    # -------------------------------------------------------------------------
    # url.validate is Edmingle's infrastructure health check — not a real event.
    # It is already stored in Bronze above as a record that validation occurred.
    # Skip Silver routing entirely: there is no Silver table for this event type
    # and it carries no student or session data.
    # -------------------------------------------------------------------------
    if event_type == 'url.validate':
        log.info("Validation ping stored in Bronze — skipping Silver routing.")
        return jsonify({'status': 'received'}), 200

    # -------------------------------------------------------------------------
    # STEPS 3, 4, 5: Route to Silver + mark Bronze as routed
    # Silver uses a separate connection from Bronze.
    # The Silver insert and the mark_routed_to_silver update share one transaction.
    # If Silver fails, both roll back together — the Bronze flag stays false,
    # which lets us identify unrouted events and reprocess them later.
    # -------------------------------------------------------------------------
    conn_silver = None
    try:
        conn_silver = get_db_connection()

        # Look up which routing function handles this event type
        router_fn = EVENT_ROUTER.get(event_type)

        if router_fn:
            # STEP 3+4: Call the Silver routing function for this event type
            router_fn(conn_silver, event_id, event_type, data, event_timestamp)

            # STEP 5: Mark this event as successfully routed in Bronze
            # This is in the same transaction as the Silver insert above
            mark_routed_to_silver(conn_silver, event_id)

            # Save both the Silver insert and the Bronze flag update together
            conn_silver.commit()
            log.info(f"Silver routed: {event_type}  [event_id: {event_id}]")

        else:
            # event_type is not in our router — unknown event from Edmingle
            # It is already stored safely in Bronze. Log a warning, do not fail.
            log.warning(f"Unknown event type '{event_type}' — stored in Bronze only")

    except Exception as e:
        if conn_silver:
            conn_silver.rollback()
        log.error(f"Silver routing failed for {event_type} [event_id: {event_id}]: {e}")
    finally:
        if conn_silver:
            conn_silver.close()

    # -------------------------------------------------------------------------
    # STEP 6: Always return HTTP 200 to Edmingle
    # This line runs regardless of what happened above.
    # If Edmingle gets any other response code, it retries and eventually
    # marks our webhook Inactive permanently — we never let that happen.
    # -------------------------------------------------------------------------
    return jsonify({'status': 'received'}), 200


# =============================================================================
# ENTRY POINT
# This block runs only when you start the file directly:
#   python ingestion/webhook_receiver.py
#
# host='0.0.0.0' means the server listens on all network interfaces,
# not just localhost. This is needed when Edmingle sends webhooks from the internet.
#
# debug=True means Flask restarts automatically when you edit this file,
# and prints full error tracebacks in the terminal. Disable this in production.
# =============================================================================
if __name__ == '__main__':
    log.info("Starting Edmingle webhook receiver on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)
