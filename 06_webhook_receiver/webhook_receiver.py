# ============================================================
# 06 — WEBHOOK RECEIVER (FLASK SERVER)
# ============================================================
# What it does: Runs a web server that listens for real-time
#               events sent by Edmingle (enrollments, sessions,
#               assessments, etc.) and saves them to the database.
#
# Why we need it: Edmingle sends event notifications instantly
#                 when students enroll, teachers start classes,
#                 certificates are issued, etc. This server
#                 catches those notifications 24/7.
#
# How to run:
#   python 06_webhook_receiver/webhook_receiver.py
#
# The server starts on port 5000. Keep it running at all times.
#
# Endpoints:
#   POST /webhook       — receives events from Edmingle
#   GET  /health        — checks if server is healthy
#   GET  /status        — shows last 10 events received
#   GET  /failed        — shows last 10 failed events
#   POST /retry-failed  — recovers events saved to disk during DB outage
#
# What to check after starting:
#   Visit http://localhost:5000/health in your browser.
#   It should say: {"status": "ok", "database": "connected"}
# ============================================================

import atexit
import datetime
import json
import logging
import logging.handlers
import os
import re
import signal
import sys
import threading

import psycopg2
import psycopg2.extras
import psycopg2.pool
from flask import Flask, request, jsonify

# ── DATABASE AND API SETTINGS ─────────────────────────────────
DB_HOST           = "localhost"
DB_NAME           = "edmingle_analytics"
DB_USER           = "postgres"
DB_PASSWORD       = "Svyoma"
DB_PORT           = 5432
WEBHOOK_SECRET    = "your_webhook_secret_here"
EDMINGLE_API_KEY  = "859b19531f4b149a605679c5ea21eeb8"
ORG_ID            = 683
INSTITUTION_ID    = 483
API_BASE_URL      = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# ─────────────────────────────────────────────────────────────


# ── LOGGING SETUP ─────────────────────────────────────────────
# We log to both the terminal AND a rotating file.
# The file rotates at 10 MB and keeps the last 5 backups.
LOG_FORMAT = "%(asctime)s  %(levelname)s  %(message)s"
LOG_DATE   = "%Y-%m-%d %H:%M:%S"

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Log to the terminal (console) so you can see events in real time
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE))
log.addHandler(stream_handler)

# Log to a file so events are recorded permanently
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webhook_receiver.log")
file_handler  = logging.handlers.RotatingFileHandler(
    log_file_path,
    maxBytes=10 * 1024 * 1024,  # 10 MB per file
    backupCount=5,               # Keep 5 old log files
)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE))
log.addHandler(file_handler)


# ── PII MASKING ───────────────────────────────────────────────
# We must not log real email addresses or phone numbers.
# These patterns detect and replace them with [email] and [phone].
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
PHONE_PATTERN = re.compile(r"(\+91[\-\s]?|0)?[6-9]\d{9}")


def mask_pii(text):
    # Replace any emails and phone numbers in the text with placeholders
    text = EMAIL_PATTERN.sub("[email]", text)
    text = PHONE_PATTERN.sub("[phone]", text)
    return text


# ── DISK FALLBACK ─────────────────────────────────────────────
# When the database is completely down, we write events to disk
# so they can be recovered later via POST /retry-failed.
FALLBACK_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fallback_queue.jsonl")
fallback_lock  = threading.Lock()


def write_to_disk_fallback(failure_reason, raw_body, content_type):
    # Save the event to a local file when the database is unreachable
    try:
        entry = json.dumps({
            "failure_reason": failure_reason,
            "raw_body":       raw_body,
            "content_type":   content_type,
        })
        # Use a thread lock so two requests don't write at the same time
        with fallback_lock:
            with open(FALLBACK_FILE, "a", encoding="utf-8") as file_handle:
                file_handle.write(entry + "\n")
        log.warning("DB unreachable — event written to disk fallback: " + FALLBACK_FILE)
    except Exception as error:
        log.error("Disk fallback write also failed: " + str(error))


# ── FLASK APP ─────────────────────────────────────────────────
# Flask is the web framework that handles HTTP requests.
app = Flask(__name__)


# ── DATABASE CONNECTION POOL ──────────────────────────────────
# A connection pool keeps multiple database connections open and
# ready, so each request doesn't have to reconnect from scratch.
db_pool = None


def initialize_connection_pool():
    # Create the connection pool with 2 to 20 simultaneous connections
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2, maxconn=20,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    log.info("Database connection pool initialized.")


def get_db_connection():
    # Get a connection from the pool, retrying up to 5 times if busy
    last_error = None
    for attempt in range(5):
        try:
            connection = db_pool.getconn()
            # Set a 5-second timeout so slow queries don't block the server
            cursor = connection.cursor()
            cursor.execute("SET statement_timeout = '5s'")
            cursor.close()
            connection.commit()
            return connection
        except psycopg2.pool.PoolError as error:
            last_error = error
            import time as _time
            _time.sleep(0.05 * (attempt + 1))
    raise last_error


def release_db_connection(connection):
    # Return the connection to the pool so it can be reused
    db_pool.putconn(connection)


# Initialize the pool when the server starts
initialize_connection_pool()


# ── SHUTDOWN HANDLERS ─────────────────────────────────────────

def close_connection_pool():
    # Close all connections when the server shuts down
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        log.info("Database connection pool closed.")


def handle_shutdown_signal(signal_number, frame):
    # Handle Ctrl+C and system shutdown signals gracefully
    close_connection_pool()
    sys.exit(0)


# Register shutdown handlers
atexit.register(close_connection_pool)
signal.signal(signal.SIGTERM, handle_shutdown_signal)
signal.signal(signal.SIGINT,  handle_shutdown_signal)


# ── BRONZE INSERT HELPERS ─────────────────────────────────────

def insert_bronze_event(connection, event_id, event_type, payload, is_live_mode):
    # Save the raw event to Bronze — ON CONFLICT DO NOTHING handles
    # duplicate events that Edmingle might resend
    cursor = connection.cursor()
    cursor.execute("""
        INSERT INTO bronze.webhook_events (event_id, event_type, raw_payload, is_live_mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (event_id, event_type, psycopg2.extras.Json(payload), is_live_mode))
    cursor.close()


def mark_event_routed_to_silver(connection, event_id):
    # Update the Bronze row to record that this event was sent to Silver
    cursor = connection.cursor()
    cursor.execute("""
        UPDATE bronze.webhook_events
        SET    routed_to_silver = true
        WHERE  event_id = %s
    """, (event_id,))
    cursor.close()


def save_failed_event(failure_reason, raw_body, content_type):
    # Save the failed event to bronze.failed_events (separate connection)
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        # Truncate raw_body to 10000 chars to avoid database limits
        safe_body = raw_body[:10000] if raw_body else None
        cursor.execute("""
            INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type)
            VALUES (%s, %s, %s)
        """, (failure_reason, safe_body, content_type))
        cursor.close()
        connection.commit()
        log.info("Failed event saved to bronze.failed_events: " + failure_reason)
    except Exception as error:
        log.error("Could not write to bronze.failed_events: " + str(error))
        if connection:
            connection.rollback()
        # Last resort: write to disk so we don't lose the event
        write_to_disk_fallback(failure_reason, raw_body, content_type)
    finally:
        if connection:
            release_db_connection(connection)


# ── SILVER ROUTING: SQL CONSTANTS ─────────────────────────────

SQL_UPSERT_USER_CREATED = """
    INSERT INTO silver.users (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at_ist, received_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
        start_date_ist, end_date_ist, created_at_ist, source
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


# ── SILVER ROUTING FUNCTIONS ──────────────────────────────────
# Each function reads fields from the event data dict and saves
# them to the appropriate Silver table.
# IMPORTANT: These functions do NOT call conn.commit().
# The /webhook route owns the transaction and commits at the end.

def pluck_system_field(system_fields, *field_name_variants):
    # Extract a value from Edmingle's system_fields list or dict.
    # Real events send system_fields as a list; test events send a dict.
    if isinstance(system_fields, dict):
        for field_name in field_name_variants:
            value = system_fields.get(field_name)
            if value is not None:
                return value
        return None
    if not isinstance(system_fields, list):
        return None
    # Build a lookup dict from the list of field objects
    lookup = {}
    for field_object in system_fields:
        if not isinstance(field_object, dict):
            continue
        display_name = (field_object.get("field_display_name") or "").lower().strip()
        field_name   = (field_object.get("field_name") or "").lower().strip()
        field_value  = field_object.get("field_value")
        if display_name:
            lookup[display_name] = field_value
        if field_name:
            lookup[field_name] = field_value
    # Try each variant name in the lookup
    for name in field_name_variants:
        value = lookup.get(name.lower().strip())
        if value is not None:
            return value
    return None


def route_user_created(conn, event_id, event_type, data, event_timestamp):
    # Extract all the user fields from the event data
    user_id        = data.get("user_id")
    email          = data.get("email")
    full_name      = data.get("name")
    if full_name is None:
        full_name = data.get("full_name")
    user_name      = data.get("user_name")
    user_role      = data.get("user_role")
    contact_number = data.get("contact_number")
    institution_id = data.get("institution_id")
    created_at     = data.get("created_at")
    if created_at is None:
        created_at = event_timestamp
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_USER_CREATED, (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at
    ))
    cursor.close()


def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    # Real events nest user fields under data['user']; test events are flat
    user_object    = data.get("user") or data
    user_id        = user_object.get("user_id") or user_object.get("id")
    email          = user_object.get("email")
    full_name      = user_object.get("name") or user_object.get("full_name")
    contact_number = user_object.get("phone") or user_object.get("contact_number")
    updated_at     = user_object.get("updated_at") or event_timestamp
    # Location and parent fields may come from system_fields
    sys_fields = data.get("system_fields")
    city    = user_object.get("city")    or pluck_system_field(sys_fields, "city",    "City")
    state   = user_object.get("state")   or pluck_system_field(sys_fields, "state",   "State")
    address = user_object.get("address") or pluck_system_field(sys_fields, "address", "Address")
    pincode = user_object.get("pincode") or pluck_system_field(sys_fields, "pincode", "Pincode", "pin code")
    parent_name    = pluck_system_field(sys_fields, "parent_name",    "Parent Name")
    parent_email   = pluck_system_field(sys_fields, "parent_email",   "Parent Email")
    parent_contact = pluck_system_field(sys_fields, "parent_contact", "Parent Contact", "Parent Phone")
    custom_fields_raw  = data.get("custom_fields")
    if custom_fields_raw is not None:
        custom_fields_json = psycopg2.extras.Json(custom_fields_raw)
    else:
        custom_fields_json = None
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_USER_UPDATED, (
        event_id, event_type, user_id,
        email, full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        custom_fields_json, updated_at
    ))
    cursor.close()


def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # Extract all transaction fields (purchase/enrollment details)
    cursor = conn.cursor()
    cursor.execute(SQL_UPSERT_TRANSACTION, (
        event_id, event_type, event_timestamp,
        data.get("user_id"), data.get("email"),
        data.get("name") or data.get("full_name"), data.get("contact_number"),
        data.get("bundle_id"), data.get("course_name"),
        data.get("institution_bundle_id"), data.get("master_batch_id"),
        data.get("master_batch_name"),
        data.get("original_price"), data.get("discount"), data.get("final_price"),
        data.get("currency"), data.get("credits_applied"),
        data.get("payment_method"), data.get("transaction_id"),
        data.get("start_date"), data.get("end_date"), data.get("created_at"),
    ))
    cursor.close()


def route_session(conn, event_id, event_type, data, event_timestamp):
    # Real events use 'taken_at' for actual start time; test events use 'actual_start_time'
    actual_start = data.get("actual_start_time") or data.get("taken_at")
    master_batches_raw = data.get("master_batches")
    if master_batches_raw is not None:
        master_batches_json = psycopg2.extras.Json(master_batches_raw)
    else:
        master_batches_json = None
    cursor = conn.cursor()
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
    # Real events use 'test_date'; test events use 'submitted_at'
    submitted_at = data.get("submitted_at") or data.get("test_date")
    cursor = conn.cursor()
    cursor.execute("""
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
    """, (
        event_id, event_type,
        data.get("user_id"), data.get("attempt_id"), data.get("exercise_id"),
        data.get("mark"), data.get("is_evaluated"), data.get("faculty_comments"),
        submitted_at,
    ))
    cursor.close()


def route_course(conn, event_id, event_type, data, event_timestamp):
    # Real events have no 'completed_at' field — use the event timestamp instead
    completed_at = data.get("completed_at") or event_timestamp
    cursor = conn.cursor()
    cursor.execute("""
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
    """, (event_id, event_type, data.get("user_id"), data.get("bundle_id"), completed_at))
    cursor.close()


def route_announcement(conn, event_id, event_type, data, event_timestamp):
    # Announcement payload structure is not documented — store the whole data dict as JSON
    if data is not None:
        raw_data_json = psycopg2.extras.Json(data)
    else:
        raw_data_json = None
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO silver.announcements (event_id, event_type, raw_data, received_at)
        VALUES (%s, %s, %s, NOW() AT TIME ZONE 'Asia/Kolkata')
        ON CONFLICT (event_id) DO UPDATE SET
            event_type  = EXCLUDED.event_type,
            raw_data    = EXCLUDED.raw_data,
            received_at = EXCLUDED.received_at
    """, (event_id, event_type, raw_data_json))
    cursor.close()


def route_certificate(conn, event_id, event_type, data, event_timestamp):
    # Extract certificate fields and save to Silver
    cursor = conn.cursor()
    cursor.execute("""
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
    """, (
        event_id, event_type,
        data.get("certificate_id"), data.get("user_id"), data.get("issued_at"),
    ))
    cursor.close()


# ── EVENT ROUTER ──────────────────────────────────────────────
# This dictionary maps each Edmingle event type name to the
# function that should handle it.
# Note: Edmingle uses 'session_update' (no 'd') and
#       'session_cancel' (no 'led') — exact spelling matters.
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
    "course.user_course_completed":        route_course,
    "announcement.announcement_created":   route_announcement,
    "certificate.certificate_issued":      route_certificate,
}


# ── PAYLOAD NORMALIZER ────────────────────────────────────────

def extract_event_fields(payload):
    # Edmingle sends two formats:
    # Real events: {"event": {...}, "payload": {...}}  — nested format
    # Test events: {"id": "...", "event_name": "...", "data": {...}} — flat format
    if "event" in payload and "id" not in payload:
        # This is a real Edmingle event in nested format
        event_object = payload.get("event", {})
        event_type   = event_object.get("event") or event_object.get("event_name")
        event_ts_str = event_object.get("event_ts", "")
        if event_ts_str:
            event_id = event_type + "-" + event_ts_str
        else:
            event_id = event_type
        is_live_mode = event_object.get("livemode", True)
        try:
            event_timestamp = int(datetime.datetime.fromisoformat(event_ts_str).timestamp())
        except (ValueError, TypeError):
            event_timestamp = None
        # The actual data fields are in 'payload', NOT in 'event'
        data = payload.get("payload", {})
    else:
        # This is a flat test event format
        event_id        = payload.get("id")
        event_type      = payload.get("event_name")
        event_timestamp = payload.get("event_timestamp")
        is_live_mode    = payload.get("is_live_mode", True)
        data            = payload.get("data", {})
    return event_id, event_type, event_timestamp, is_live_mode, data


# ── FLASK ERROR HANDLERS ──────────────────────────────────────
# CRITICAL: Edmingle will mark our webhook as Inactive if it
# ever receives anything other than HTTP 200. So we always
# return 200 even for errors.

def handle_404(error):
    # Log that an unknown URL was requested
    log.warning("404 Not Found: " + request.path)
    return jsonify({"status": "received"}), 200


def handle_405(error):
    # Log that the wrong HTTP method was used (e.g. GET instead of POST)
    log.warning("405 Method Not Allowed: " + request.method + " " + request.path)
    return jsonify({"status": "received"}), 200


def handle_413(error):
    # Request body was too large — log it as a failed event
    log.warning("413 Request Entity Too Large on " + request.path)
    save_failed_event("413 request too large", "", request.content_type)
    return jsonify({"status": "received"}), 200


def handle_500(error):
    # Internal server error — log it but still return 200 to Edmingle
    log.error("500 Internal Server Error: " + str(error))
    return jsonify({"status": "received"}), 200


# Register the error handlers with Flask
app.register_error_handler(404, handle_404)
app.register_error_handler(405, handle_405)
app.register_error_handler(413, handle_413)
app.register_error_handler(500, handle_500)


# ── FLASK ROUTE HANDLERS ──────────────────────────────────────

def handle_health():
    # Check if we can get a database connection — if yes, we are healthy
    conn = None
    try:
        conn = get_db_connection()
        release_db_connection(conn)
        conn = None
        return jsonify({"status": "ok", "database": "connected"}), 200
    except Exception as error:
        log.error("Health check failed — cannot reach database: " + str(error))
        if conn:
            release_db_connection(conn)
        return jsonify({"status": "error", "database": str(error)}), 500


def handle_status():
    # Return the last 10 events received so you can monitor activity
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT event_id, event_type, received_at, is_live_mode, routed_to_silver
            FROM   bronze.webhook_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cursor.fetchall()
        cursor.close()
        # Build the JSON response from the database rows
        events_list = []
        for row in rows:
            event_item = {
                "event_id":         row["event_id"],
                "event_type":       row["event_type"],
                "received_at":      row["received_at"].isoformat() if row["received_at"] else None,
                "is_live_mode":     row["is_live_mode"],
                "routed_to_silver": row["routed_to_silver"],
            }
            events_list.append(event_item)
        return jsonify({"count": len(events_list), "last_10_events": events_list}), 200
    except Exception as error:
        log.error("Status endpoint failed: " + str(error))
        return jsonify({"error": str(error)}), 500
    finally:
        if conn:
            release_db_connection(conn)


def handle_failed():
    # Return the last 10 failed events for inspection
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT id, received_at, failure_reason, content_type,
                   LEFT(raw_body, 500) AS raw_body_preview
            FROM   bronze.failed_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cursor.fetchall()
        cursor.close()
        events_list = []
        for row in rows:
            event_item = {
                "id":               row["id"],
                "received_at":      row["received_at"].isoformat() if row["received_at"] else None,
                "failure_reason":   row["failure_reason"],
                "content_type":     row["content_type"],
                "raw_body_preview": row["raw_body_preview"],
            }
            events_list.append(event_item)
        return jsonify({"count": len(events_list), "last_10_failed": events_list}), 200
    except Exception as error:
        log.error("Failed endpoint error: " + str(error))
        return jsonify({"error": str(error)}), 500
    finally:
        if conn:
            release_db_connection(conn)


def handle_retry_failed():
    # Read events from the disk fallback file and try to save them to Bronze
    if not os.path.exists(FALLBACK_FILE):
        return jsonify({"retried": 0, "remaining": 0}), 200
    # Read the fallback file with a thread lock to prevent concurrent access
    with fallback_lock:
        try:
            with open(FALLBACK_FILE, "r", encoding="utf-8") as file_handle:
                raw_lines = file_handle.readlines()
            # Strip blank lines
            lines = []
            for line in raw_lines:
                if line.strip():
                    lines.append(line.strip())
        except Exception as error:
            return jsonify({"error": str(error)}), 200
    retried      = 0
    failed_lines = []
    for line in lines:
        try:
            entry      = json.loads(line)
            raw_body   = entry.get("raw_body", "")
            if raw_body:
                payload = json.loads(raw_body)
            else:
                payload = None
            if not payload:
                failed_lines.append(line)
                continue
            event_id, event_type, event_ts, is_live_mode, data = extract_event_fields(payload)
            if not event_id or not event_type:
                failed_lines.append(line)
                continue
            conn = None
            try:
                conn = get_db_connection()
                insert_bronze_event(conn, event_id, event_type, payload, is_live_mode)
                conn.commit()
                retried = retried + 1
                log.info("retry-failed: recovered " + event_type + " [" + str(event_id) + "]")
            except Exception as error:
                failed_lines.append(line)
                log.error("retry-failed: could not recover " + str(event_id) + ": " + str(error))
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    release_db_connection(conn)
        except Exception:
            failed_lines.append(line)
    # Rewrite the fallback file with only the lines that still failed
    with fallback_lock:
        if failed_lines:
            with open(FALLBACK_FILE, "w", encoding="utf-8") as file_handle:
                for remaining_line in failed_lines:
                    file_handle.write(remaining_line + "\n")
        else:
            try:
                os.remove(FALLBACK_FILE)
            except FileNotFoundError:
                pass
    log.info("retry-failed: retried=" + str(retried) + " remaining=" + str(len(failed_lines)))
    return jsonify({"retried": retried, "remaining": len(failed_lines)}), 200


def handle_webhook():
    # CRITICAL: We MUST return HTTP 200 no matter what.
    # Edmingle marks our webhook as Inactive if it gets any other response.

    # Capture the raw request body and content type for error logging
    raw_body     = request.get_data(as_text=True)
    content_type = request.content_type
    log.info("Incoming request — Content-Type: " + str(content_type) +
             " — Body: " + mask_pii(raw_body[:300]))

    # Parse JSON from the request body (force=True works even if Content-Type is wrong)
    payload = request.get_json(silent=True, force=True)

    if payload is None:
        # Could not parse JSON — save as failed event and return 200
        failure_reason = "JSON parse failed"
        log.warning(failure_reason + " — body: " + mask_pii(raw_body[:300]))
        save_failed_event(failure_reason, raw_body, content_type)
        return jsonify({"status": "received"}), 200

    # Extract the event fields (handles both real and test event formats)
    event_id, event_type, event_timestamp, is_live_mode, data = extract_event_fields(payload)

    if not event_id or not event_type:
        failure_reason = "Missing event_id or event_type — keys: " + str(list(payload.keys()))
        log.warning(failure_reason)
        save_failed_event(failure_reason, raw_body, content_type)
        return jsonify({"status": "received"}), 200

    log.info("Received: " + str(event_type) + "  [event_id: " + str(event_id) + "]")

    # ── SAVE TO BRONZE ────────────────────────────────────────
    # Bronze uses its own connection so it never rolls back due to Silver errors.
    conn_bronze = None
    try:
        conn_bronze = get_db_connection()
        insert_bronze_event(conn_bronze, event_id, event_type, payload, is_live_mode)
        conn_bronze.commit()
        log.info("Bronze stored: " + str(event_type) + "  [" + str(event_id) + "]")
    except Exception as error:
        if conn_bronze:
            conn_bronze.rollback()
        log.error("Bronze insert failed for " + str(event_type) + ": " + str(error))
        save_failed_event("Bronze insert failed: " + str(error), raw_body, content_type)
    finally:
        if conn_bronze:
            release_db_connection(conn_bronze)

    # Edmingle uses 'url.validate' to check our server is alive — skip Silver routing
    if event_type == "url.validate":
        log.info("Validation ping stored in Bronze — skipping Silver routing.")
        return jsonify({"status": "received"}), 200

    # ── ROUTE TO SILVER ───────────────────────────────────────
    # Silver routing uses its own connection so Bronze is never undone.
    conn_silver = None
    try:
        conn_silver = get_db_connection()
        # Look up the routing function for this event type
        router_function = EVENT_ROUTER.get(event_type)
        if router_function:
            # Call the routing function to save to the correct Silver table
            router_function(conn_silver, event_id, event_type, data, event_timestamp)
            # Mark in Bronze that this event was routed to Silver
            mark_event_routed_to_silver(conn_silver, event_id)
            conn_silver.commit()
            log.info("Silver routed: " + str(event_type) + "  [" + str(event_id) + "]")
        else:
            log.warning("Unknown event type '" + str(event_type) + "' — stored in Bronze only")
    except Exception as error:
        if conn_silver:
            conn_silver.rollback()
        log.error("Silver routing failed for " + str(event_type) + ": " + str(error))
    finally:
        if conn_silver:
            release_db_connection(conn_silver)

    return jsonify({"status": "received"}), 200


# ── REGISTER FLASK ROUTES ─────────────────────────────────────
# We use add_url_rule instead of @app.route decorators so each
# function definition stays simple.
app.add_url_rule("/health",       "health",       handle_health,       methods=["GET"])
app.add_url_rule("/status",       "status",       handle_status,       methods=["GET"])
app.add_url_rule("/failed",       "failed",       handle_failed,       methods=["GET"])
app.add_url_rule("/retry-failed", "retry_failed", handle_retry_failed, methods=["POST"])
app.add_url_rule("/webhook",      "webhook",      handle_webhook,      methods=["POST"])


# ── GUNICORN HOOK ─────────────────────────────────────────────
def post_fork(server, worker):
    # When Gunicorn creates a new worker process, give it a fresh connection pool.
    # Without this, workers would share the same pool across fork boundaries.
    global db_pool
    if db_pool:
        db_pool.closeall()
    initialize_connection_pool()


# ── START THE SERVER ──────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting Edmingle webhook receiver on port 5000 ...")
    app.run(host="0.0.0.0", port=5000, debug=True)
