# =============================================================================
# FILE    : ingestion/webhook_receiver.py
# PROJECT : Edmingle Webhook Data Pipeline — Vyoma Samskrta Pathasala
# PURPOSE : Flask web server that receives webhook events from Edmingle.
#           Every event is stored in Bronze (raw JSON) first, then routed
#           to the correct Silver table (structured, typed data).
#           Any request that cannot be parsed or stored is written to
#           bronze.failed_events so nothing is silently lost.
#
# ENDPOINTS:
#   POST /webhook  — Receives events from Edmingle. Always returns HTTP 200.
#   GET  /health   — Checks that the server and database are reachable.
#   GET  /status   — Shows the last 10 events received (from Bronze).
#   GET  /failed   — Shows the last 10 events in bronze.failed_events.
#
# HOW TO RUN (development):
#   pip install flask psycopg2-binary
#   python ingestion/webhook_receiver.py
#   Server starts at http://localhost:5000
#
# HOW TO RUN (production with gunicorn):
#   gunicorn -w 4 -b 0.0.0.0:5000 ingestion.webhook_receiver:app
# =============================================================================


# =============================================================================
# IMPORTS
# atexit            : registers the pool-close function to run on interpreter exit
# logging           : writes timestamped messages to terminal and log file
# logging.handlers  : RotatingFileHandler — log files with automatic size cap
# os                : os.getenv() reads environment variables loaded by dotenv
# re                : regular expressions used by the PII masking function
# signal            : catches SIGTERM / SIGINT for graceful pool shutdown
# sys               : sys.exit() called from the signal handler
# dotenv            : loads variables from .env into os.environ at startup
# psycopg2          : Python driver for PostgreSQL
# psycopg2.extras   : RealDictCursor (rows as dicts) and Json (JSONB adapter)
# psycopg2.pool     : ThreadedConnectionPool — reuse connections across requests
# =============================================================================
import atexit
import datetime
import logging
import logging.handlers
import os
import re
import signal
import sys

from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
import psycopg2.pool
from flask import Flask, request, jsonify

# Load .env from the project root (one directory above ingestion/).
# override=False means existing shell environment variables take precedence,
# so production deployments can set variables without a .env file.
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'), override=False)


# =============================================================================
# LOGGING SETUP
# Two handlers run simultaneously:
#   StreamHandler       — writes to the terminal (visible during development)
#   RotatingFileHandler — writes to ingestion/webhook_receiver.log
#                         10 MB per file; the last 5 files are kept
# Both use the same format: "2024-03-08 05:30:00  INFO  message here"
# =============================================================================
_LOG_FORMAT = '%(asctime)s  %(levelname)s  %(message)s'
_LOG_DATE   = '%Y-%m-%d %H:%M:%S'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

# Terminal handler
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE))
log.addHandler(_stream_handler)

# Rotating file handler — 10 MB max per file, 5 backup files kept
_file_handler = logging.handlers.RotatingFileHandler(
    'ingestion/webhook_receiver.log',
    maxBytes=10 * 1024 * 1024,   # 10 MB
    backupCount=5,
)
_file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE))
log.addHandler(_file_handler)


# =============================================================================
# PII MASKING
# mask_pii() replaces email addresses and Indian mobile numbers with tokens
# before any user-supplied data is written to the log file.
# Applied to raw request bodies and any field-level log messages that could
# contain student contact information.
# =============================================================================
_EMAIL_RE = re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
_PHONE_RE = re.compile(r'(\+91[\-\s]?|0)?[6-9]\d{9}')


def mask_pii(text: str) -> str:
    text = _EMAIL_RE.sub('[email]', text)
    text = _PHONE_RE.sub('[phone]', text)
    return text


# =============================================================================
# FLASK APPLICATION
# =============================================================================
app = Flask(__name__)


# =============================================================================
# DATABASE — ThreadedConnectionPool
# A pool keeps minconn=2 connections open at all times and grows to maxconn=10
# under load. Connections are reused across requests — no open/close overhead.
#
# IMPORTANT: gunicorn forks worker processes at startup. Each worker inherits
# the parent's open file descriptors, including pool sockets. Workers sharing
# those sockets corrupts PostgreSQL sessions. The post_fork() function below
# closes the inherited pool and opens a fresh one inside each worker.
# =============================================================================
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'edmingle_analytics')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', '')

_pool = None   # module-level reference; replaced by _init_pool() at startup and in post_fork()


def _init_pool():
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=20,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )
    log.info("Database connection pool initialised.")


def get_db_connection():
    # Retry up to 5 times with short back-off when the pool is exhausted under
    # burst traffic. Each attempt waits a little longer before retrying.
    import time as _time
    last_exc = None
    for attempt in range(5):
        try:
            conn = _pool.getconn()
            cur = conn.cursor()
            cur.execute("SET statement_timeout = '5s'")
            cur.close()
            conn.commit()
            return conn
        except psycopg2.pool.PoolError as exc:
            last_exc = exc
            _time.sleep(0.05 * (attempt + 1))   # 50 ms, 100 ms, 150 ms, 200 ms, 250 ms
    raise last_exc


def release_db_connection(conn):
    # Return the connection to the pool — do NOT close it.
    # Closing would destroy it; the pool reuses it for the next request.
    _pool.putconn(conn)


# Initialise the pool at import time.
# This covers the development server and single-worker gunicorn.
_init_pool()


# =============================================================================
# GRACEFUL SHUTDOWN
# When gunicorn sends SIGTERM to stop a worker, or when the development server
# receives Ctrl-C (SIGINT), close all pool connections cleanly so PostgreSQL
# does not accumulate orphaned connections between restarts.
#
# _close_pool() does the actual work.
# _signal_handler() calls it and then exits — atexit will call _close_pool()
# again, but the _pool = None guard prevents a double close.
# =============================================================================
def _close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
        log.info("Database connection pool closed.")


def _signal_handler(signum, frame):
    _close_pool()
    sys.exit(0)


atexit.register(_close_pool)
signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT,  _signal_handler)


# =============================================================================
# GUNICORN POST-FORK HOOK
# Called by gunicorn inside each worker process immediately after forking.
# Closes the pool inherited from the parent and opens a fresh one for this
# worker so no two workers share the same PostgreSQL socket.
#
# Wire this up by adding to gunicorn.conf.py:
#   from ingestion.webhook_receiver import post_fork
# =============================================================================
def post_fork(server, worker):
    global _pool
    if _pool:
        _pool.closeall()
    _init_pool()


# =============================================================================
# BRONZE FUNCTIONS
# insert_bronze         — stores the raw payload in bronze.webhook_events
# mark_routed_to_silver — flips routed_to_silver = true after Silver succeeds
# insert_failed_event   — writes to bronze.failed_events (the safety net)
# =============================================================================

def insert_bronze(conn, event_id, event_type, payload, is_live_mode):
    # ON CONFLICT DO NOTHING: if Edmingle retries and sends the same event_id
    # again, skip silently instead of creating a duplicate row.
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bronze.webhook_events (event_id, event_type, raw_payload, is_live_mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (
        event_id,
        event_type,
        # psycopg2.extras.Json() serialises a Python dict to the PostgreSQL JSONB format.
        # It handles all nested objects and arrays without any manual json.dumps().
        psycopg2.extras.Json(payload),
        is_live_mode,
    ))
    cur.close()


def mark_routed_to_silver(conn, event_id):
    # Runs in the SAME transaction as the Silver insert.
    # If Silver rolls back, this update rolls back too — event stays unrouted
    # so we can identify and reprocess it later.
    cur = conn.cursor()
    cur.execute("""
        UPDATE bronze.webhook_events
        SET    routed_to_silver = true
        WHERE  event_id = %s
    """, (event_id,))
    cur.close()


def insert_failed_event(failure_reason, raw_body, content_type):
    # Writes to bronze.failed_events — the final safety net.
    # Called whenever a request arrives but cannot be parsed or stored.
    # Uses its own independent connection so it is never affected by
    # Bronze or Silver transaction failures.
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type)
            VALUES (%s, %s, %s)
        """, (
            failure_reason,
            raw_body[:10000] if raw_body else None,   # cap at 10 000 chars
            content_type,
        ))
        cur.close()
        conn.commit()
        log.info(f"Failed event saved to bronze.failed_events: {failure_reason}")
    except Exception as e:
        log.error(f"Could not write to bronze.failed_events: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            release_db_connection(conn)


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
    full_name      = data.get('name') or data.get('full_name')
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
            -- Keep the original value if it exists; only set it if it was never set.
            -- created_at_ist must NEVER be overwritten once it is first set.
            created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
            received_at    = EXCLUDED.received_at
    """, (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at_unix
    ))
    cur.close()


def _pluck_system_field(system_fields, *name_variants):
    # Real Edmingle sends system_fields as a list of field objects:
    #   [{"field_display_name": "City", "field_value": "Mumbai", ...}, ...]
    # This function searches the list for any element whose field_display_name
    # or field_name matches one of the supplied variants (case-insensitive).
    # It also handles the old flat-dict format used by test events so test_all_events.py
    # keeps working without changes.
    if isinstance(system_fields, dict):
        for name in name_variants:
            val = system_fields.get(name)
            if val is not None:
                return val
        return None

    if not isinstance(system_fields, list):
        return None

    # Build a lookup: lowercased name → value, from both field_display_name and field_name
    lookup = {}
    for field in system_fields:
        if not isinstance(field, dict):
            continue
        display = (field.get('field_display_name') or '').lower().strip()
        fname   = (field.get('field_name')         or '').lower().strip()
        value   = field.get('field_value')
        if display:
            lookup[display] = value
        if fname:
            lookup[fname] = value

    for name in name_variants:
        val = lookup.get(name.lower().strip())
        if val is not None:
            return val
    return None


def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    # Handles: user.user_updated
    # Only CHANGED fields are sent in this event — not the full student record.
    # COALESCE in the upsert preserves existing values for any field not included.
    cur = conn.cursor()

    # Real Edmingle user.user_updated payloads nest all user fields under a 'user' key:
    #   data = {"user": {"user_id": 123, "email": "...", "city": "...", ...}, "system_fields": [...]}
    # Test events use a flat structure with fields directly at the root of data.
    # data.get('user') or data: if 'user' key exists and is non-empty use it,
    # otherwise fall back to data itself (covers test event format).
    user_obj       = data.get('user') or data
    user_id        = user_obj.get('user_id') or user_obj.get('id')
    email          = user_obj.get('email')
    # Real events use 'name'; test events use 'full_name'
    full_name      = user_obj.get('name') or user_obj.get('full_name')
    # Real events use 'phone'; test events use 'contact_number'
    contact_number = user_obj.get('phone') or user_obj.get('contact_number')

    # updated_at is when the profile change happened.
    # Fall back to event_timestamp if updated_at is absent.
    updated_at_unix = user_obj.get('updated_at') or event_timestamp

    # Location fields: try user_obj first (real events have them directly on the user object),
    # then fall back to system_fields list (some Edmingle versions put them there instead).
    system_fields_raw = data.get('system_fields')
    city    = user_obj.get('city')    or _pluck_system_field(system_fields_raw, 'city',    'City')
    state   = user_obj.get('state')   or _pluck_system_field(system_fields_raw, 'state',   'State')
    address = user_obj.get('address') or _pluck_system_field(system_fields_raw, 'address', 'Address')
    pincode = user_obj.get('pincode') or _pluck_system_field(system_fields_raw, 'pincode', 'Pincode', 'pin code')
    # Parent fields are only in system_fields — not on the user object itself
    parent_name    = _pluck_system_field(system_fields_raw, 'parent_name',    'Parent Name',    'parent name')
    parent_email   = _pluck_system_field(system_fields_raw, 'parent_email',   'Parent Email',   'parent email')
    parent_contact = _pluck_system_field(system_fields_raw, 'parent_contact', 'Parent Contact', 'Parent Phone', 'parent phone')

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
    # UPSERT key: (user_id, bundle_id, master_batch_id) — one row per enrollment.
    # Multiple events for the same enrollment (e.g., initiated → completed) update
    # the same row, with COALESCE preserving non-null values from earlier events.
    cur = conn.cursor()

    user_id               = data.get('user_id')
    email                 = data.get('email')
    full_name             = data.get('name') or data.get('full_name')
    contact_number        = data.get('contact_number')
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
    payment_method        = data.get('payment_method')
    transaction_id        = data.get('transaction_id')
    start_date_unix       = data.get('start_date')
    end_date_unix         = data.get('end_date')
    created_at_unix       = data.get('created_at')

    # unix_to_ist(NULL) returns NULL in PostgreSQL — safe to pass None here
    cur.execute("""
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
    """, (
        event_id, event_type, event_timestamp,
        user_id, email, full_name, contact_number,
        bundle_id, course_name, institution_bundle_id, master_batch_id, master_batch_name,
        original_price, discount, final_price, currency, credits_applied,
        payment_method, transaction_id,
        start_date_unix, end_date_unix, created_at_unix
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

    # Actual start time — only in session_started/session_start events.
    # Real Edmingle events use 'taken_at'; test events use 'actual_start_time'.
    actual_start_time = data.get('actual_start_time') or data.get('taken_at')

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
    # Real Edmingle events use 'test_date'; test events use 'submitted_at'.
    submitted_at_unix = data.get('submitted_at') or data.get('test_date')

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
    # Real Edmingle payloads have no 'completed_at' field — fall back to event_timestamp,
    # which is parsed from the top-level event_ts ISO string for real events.
    completed_at_unix = data.get('completed_at') or event_timestamp

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

    # Store the entire data dict as raw JSONB — no field extraction yet
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
    # Edmingle's exact event name spellings (confirmed from live traffic):
    #   session_update    — no 'd' at the end
    #   session_cancel    — no 'led' at the end
    #   session_reminder  — no 's' at the end (live events); reminders — test events
    #   session_start     — no 'd' at the end (live events); session_started — test events
    'session.session_created':             route_session,
    'session.session_update':              route_session,
    'session.session_cancel':              route_session,
    'session.session_started':             route_session,   # test events
    'session.session_start':               route_session,   # real Edmingle events
    'session.session_reminders':           route_session,   # test events
    'session.session_reminder':            route_session,   # real Edmingle events

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
# FLASK ERROR HANDLERS
# Edmingle marks our webhook Inactive permanently if it receives anything other
# than HTTP 200. These handlers ensure that Flask-level errors (wrong path,
# wrong method, payload too large, unhandled exception) also return 200.
# =============================================================================

@app.errorhandler(404)
def handle_404(e):
    log.warning(f"404 Not Found: {request.path}")
    return jsonify({'status': 'received'}), 200


@app.errorhandler(405)
def handle_405(e):
    log.warning(f"405 Method Not Allowed: {request.method} {request.path}")
    return jsonify({'status': 'received'}), 200


@app.errorhandler(413)
def handle_413(e):
    # Request body was too large to parse — log to failed_events and return 200
    # so Edmingle does not mark the webhook Inactive.
    log.warning(f"413 Request Entity Too Large on {request.path}")
    insert_failed_event('413 request too large', '', request.content_type)
    return jsonify({'status': 'received'}), 200


@app.errorhandler(500)
def handle_500(e):
    log.error(f"500 Internal Server Error: {e}")
    return jsonify({'status': 'received'}), 200


# =============================================================================
# FLASK ROUTE: GET /health
# Quick check that the server is running and can reach the database.
# Returns 200 if healthy, 500 if the database connection fails.
# (The "always return 200" rule only applies to /webhook, not to /health.)
# =============================================================================
@app.route('/health', methods=['GET'])
def health():
    conn = None
    try:
        conn = get_db_connection()
        release_db_connection(conn)
        conn = None
        return jsonify({'status': 'ok', 'database': 'connected'}), 200
    except Exception as e:
        log.error(f"Health check failed — cannot reach database: {e}")
        if conn:
            release_db_connection(conn)
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
        # instead of the default tuple (value, value, ...) — easier to serialise
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT event_id, event_type, received_at, is_live_mode, routed_to_silver
            FROM   bronze.webhook_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cur.fetchall()
        cur.close()

        # jsonify() cannot serialise Python datetime objects — convert each to a string.
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
        if conn:
            release_db_connection(conn)


# =============================================================================
# FLASK ROUTE: GET /failed
# Shows the last 10 entries in bronze.failed_events, newest first.
# Use this to inspect any request that arrived at /webhook but could not be
# parsed as JSON, had missing required fields, or caused a Bronze insert error.
# =============================================================================
@app.route('/failed', methods=['GET'])
def failed():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, received_at, failure_reason, content_type,
                   LEFT(raw_body, 500) AS raw_body_preview
            FROM   bronze.failed_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cur.fetchall()
        cur.close()

        events = []
        for row in rows:
            events.append({
                'id':               row['id'],
                'received_at':      row['received_at'].isoformat() if row['received_at'] else None,
                'failure_reason':   row['failure_reason'],
                'content_type':     row['content_type'],
                'raw_body_preview': row['raw_body_preview'],
            })

        return jsonify({'count': len(events), 'last_10_failed': events}), 200

    except Exception as e:
        log.error(f"Failed endpoint error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        if conn:
            release_db_connection(conn)


# =============================================================================
# FLASK ROUTE: POST /webhook
# The main endpoint. Edmingle sends all events here as HTTP POST requests.
#
# FLOW:
#   1. Capture the raw request body and mask PII before logging.
#   2. Parse the JSON body. If parsing fails, write to bronze.failed_events.
#   3. Detect the payload structure (real Edmingle nested vs. test flat) and
#      extract event_id, event_type, is_live_mode, data, event_timestamp.
#   4. Write the raw payload to Bronze (always — this is our safety net).
#      Bronze uses its own connection and commits immediately.
#   5. Look up the event_type in EVENT_ROUTER to find the right Silver function.
#   6. Call that Silver function to write structured data to the Silver table.
#   7. Mark the Bronze row as routed_to_silver = true (same transaction as step 6).
#   8. Return HTTP 200 to Edmingle — always, no matter what happened internally.
#
# CRITICAL RULE: Edmingle marks our webhook Inactive permanently if it receives
# anything other than HTTP 200. We must return 200 even if our database is down,
# the payload is malformed, or our Silver code throws an exception.
# =============================================================================
@app.route('/webhook', methods=['POST'])
def webhook():

    # -------------------------------------------------------------------------
    # STEP 1: Capture the raw request body.
    # We capture this FIRST before any parsing. If parsing fails, we still have
    # the raw body to write to bronze.failed_events for investigation.
    #
    # PII is masked before the body is logged so that student email addresses
    # and phone numbers are never written to the log file in plain text.
    # -------------------------------------------------------------------------
    raw_body     = request.get_data(as_text=True)
    content_type = request.content_type
    log.info(f"Raw request — Content-Type: {content_type} — Body: {mask_pii(raw_body[:300])}")

    # force=True parses the body as JSON regardless of the Content-Type header.
    # Without this, get_json() returns None whenever Edmingle sends events
    # without 'Content-Type: application/json', causing silent data loss.
    payload = request.get_json(silent=True, force=True)

    if payload is None:
        reason = 'JSON parse failed'
        log.warning(f"{reason} — body was: {mask_pii(raw_body[:300])}")
        insert_failed_event(reason, raw_body, content_type)
        return jsonify({'status': 'received'}), 200

    # -------------------------------------------------------------------------
    # STEP 2: Detect which payload structure Edmingle sent and normalise it.
    #
    # Real Edmingle events use a nested structure:
    #   {
    #     "event":   { "event_name": "user.user_created",
    #                  "livemode":   true,
    #                  "event_ts":   "2026-04-28T08:05:20+00:00" },
    #     "payload": { "user_id": 123, "email": "...", ... }
    #   }
    #
    # Test events from test_all_events.py use a flat structure:
    #   { "id": "...", "event_name": "...", "event_timestamp": 1234, "data": {...} }
    #
    # We normalise both into the same local variables so the rest of the
    # function does not need to know which structure arrived.
    # -------------------------------------------------------------------------
    if 'event' in payload and 'id' not in payload:
        # Nested structure — real Edmingle events.
        event_obj    = payload.get('event', {})
        # Some Edmingle versions use the key 'event' inside event_obj instead of 'event_name'
        event_type   = event_obj.get('event') or event_obj.get('event_name')
        event_ts_str = event_obj.get('event_ts', '')
        # Build a deterministic event_id from the ISO timestamp string.
        # Real Edmingle events have no top-level 'id' field.
        event_id        = f"{event_type}-{event_ts_str}" if event_ts_str else event_type
        is_live_mode    = event_obj.get('livemode', True)
        # Parse the ISO timestamp string to a Unix integer so routing functions
        # can use it as a fallback when the payload carries no explicit timestamp field.
        try:
            event_timestamp = int(datetime.datetime.fromisoformat(event_ts_str).timestamp())
        except (ValueError, TypeError):
            event_timestamp = None

        # CRITICAL: the actual data payload is at the TOP-LEVEL 'payload' key,
        # NOT inside the 'event' block. Using event_obj.get('data') would return {}
        # for every real event, producing NULL in every Silver column.
        data = payload.get('payload', {})

    else:
        # Flat structure — used by test_all_events.py.
        event_id        = payload.get('id')
        event_type      = payload.get('event_name')
        event_timestamp = payload.get('event_timestamp')
        is_live_mode    = payload.get('is_live_mode', True)
        data            = payload.get('data', {})

    if not event_id or not event_type:
        reason = f"Missing event_id or event_type — keys present: {list(payload.keys())}"
        log.warning(reason)
        insert_failed_event(reason, raw_body, content_type)
        return jsonify({'status': 'received'}), 200

    log.info(f"Received: {event_type}  [event_id: {event_id}]")

    # -------------------------------------------------------------------------
    # STEP 3: Write to Bronze (our permanent safety net).
    # This uses its own connection and commits immediately.
    # Bronze is completely independent of Silver — a Silver failure cannot
    # affect or undo the Bronze record.
    # -------------------------------------------------------------------------
    conn_bronze = None
    try:
        conn_bronze = get_db_connection()
        insert_bronze(conn_bronze, event_id, event_type, payload, is_live_mode)
        conn_bronze.commit()
        log.info(f"Bronze stored: {event_type}  [event_id: {event_id}]")
    except Exception as e:
        if conn_bronze:
            conn_bronze.rollback()
        log.error(f"Bronze insert failed for {event_type} [event_id: {event_id}]: {e}")
        insert_failed_event(f"Bronze insert failed: {e}", raw_body, content_type)
    finally:
        if conn_bronze:
            release_db_connection(conn_bronze)

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
    # STEPS 4, 5, 6: Route to Silver + mark Bronze as routed.
    # Silver uses a separate connection from Bronze.
    # The Silver insert and the mark_routed_to_silver update share one transaction.
    # If Silver fails, both roll back together — the Bronze flag stays false,
    # which lets us identify unrouted events and reprocess them later.
    # -------------------------------------------------------------------------
    conn_silver = None
    try:
        conn_silver = get_db_connection()

        router_fn = EVENT_ROUTER.get(event_type)

        if router_fn:
            router_fn(conn_silver, event_id, event_type, data, event_timestamp)
            mark_routed_to_silver(conn_silver, event_id)
            conn_silver.commit()
            log.info(f"Silver routed: {event_type}  [event_id: {event_id}]")
        else:
            # event_type is not in our router — unknown event from Edmingle.
            # Already stored safely in Bronze. Log a warning, do not fail.
            log.warning(f"Unknown event type '{event_type}' — stored in Bronze only")

    except Exception as e:
        if conn_silver:
            conn_silver.rollback()
        log.error(f"Silver routing failed for {event_type} [event_id: {event_id}]: {e}")
    finally:
        if conn_silver:
            release_db_connection(conn_silver)

    # -------------------------------------------------------------------------
    # STEP 7: Always return HTTP 200 to Edmingle.
    # This line runs regardless of what happened above.
    # If Edmingle receives any other response code, it retries and eventually
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
# debug=True restarts Flask automatically when you edit this file and prints
# full error tracebacks in the terminal. Disable this in production.
# =============================================================================
if __name__ == '__main__':
    _port = int(os.getenv('FLASK_PORT', 5000))
    log.info(f"Starting Edmingle webhook receiver on port {_port}...")
    app.run(host='0.0.0.0', port=_port, debug=True)
