import atexit
import datetime
import json as _json
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

# ── CONFIG ──────────────────────────────────────
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
# ─────────────────────────────────────────────────

# ── LOGGING ──────────────────────────────────────────────────────────────────
_LOG_FORMAT = '%(asctime)s  %(levelname)s  %(message)s'
_LOG_DATE   = '%Y-%m-%d %H:%M:%S'

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE))
log.addHandler(_stream_handler)

# Rotating log — 10 MB max, 5 backups
_file_handler = logging.handlers.RotatingFileHandler(
    'ingestion/webhook_receiver.log',
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
)
_file_handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_LOG_DATE))
log.addHandler(_file_handler)


# ── PII MASKING ───────────────────────────────────────────────────────────────
_EMAIL_RE = re.compile(r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+')
_PHONE_RE = re.compile(r'(\+91[\-\s]?|0)?[6-9]\d{9}')


def mask_pii(text: str) -> str:
    text = _EMAIL_RE.sub('[email]', text)
    text = _PHONE_RE.sub('[phone]', text)
    return text


# ── DISK FALLBACK ─────────────────────────────────────────────────────────────
# When both Bronze and failed_events inserts fail (DB fully down), events are
# written here so they can be recovered via POST /retry-failed after DB comes back.
FALLBACK_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fallback_queue.jsonl')
_fallback_lock = threading.Lock()


def _write_fallback(failure_reason, raw_body, content_type):
    try:
        entry = _json.dumps({
            'failure_reason': failure_reason,
            'raw_body':       raw_body,
            'content_type':   content_type,
        })
        with _fallback_lock:
            with open(FALLBACK_FILE, 'a', encoding='utf-8') as f:
                f.write(entry + '\n')
        log.warning(f"DB unreachable — event written to disk fallback ({FALLBACK_FILE})")
    except Exception as e:
        log.error(f"Disk fallback write also failed: {e}")


# ── FLASK APP ─────────────────────────────────────────────────────────────────
app = Flask(__name__)


# ── DATABASE POOL ─────────────────────────────────────────────────────────────
_pool = None


def _init_pool():
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=2, maxconn=20,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    log.info("Database connection pool initialised.")


def get_db_connection():
    # Retry up to 5 times with back-off when the pool is exhausted under burst traffic.
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
            _time.sleep(0.05 * (attempt + 1))
    raise last_exc


def release_db_connection(conn):
    _pool.putconn(conn)


_init_pool()


# ── GRACEFUL SHUTDOWN ─────────────────────────────────────────────────────────
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


# Gunicorn post-fork: close inherited pool and open a fresh one per worker.
def post_fork(server, worker):
    global _pool
    if _pool:
        _pool.closeall()
    _init_pool()


# ── BRONZE HELPERS ────────────────────────────────────────────────────────────

def insert_bronze(conn, event_id, event_type, payload, is_live_mode):
    # ON CONFLICT DO NOTHING handles Edmingle retries that send the same event_id.
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO bronze.webhook_events (event_id, event_type, raw_payload, is_live_mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """, (event_id, event_type, psycopg2.extras.Json(payload), is_live_mode))
    cur.close()


def mark_routed_to_silver(conn, event_id):
    # Runs in the SAME transaction as the Silver insert so they roll back together.
    cur = conn.cursor()
    cur.execute("""
        UPDATE bronze.webhook_events
        SET    routed_to_silver = true
        WHERE  event_id = %s
    """, (event_id,))
    cur.close()


def insert_failed_event(failure_reason, raw_body, content_type):
    # Independent connection — never affected by Bronze or Silver failures.
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type)
            VALUES (%s, %s, %s)
        """, (failure_reason, raw_body[:10000] if raw_body else None, content_type))
        cur.close()
        conn.commit()
        log.info(f"Failed event saved to bronze.failed_events: {failure_reason}")
    except Exception as e:
        log.error(f"Could not write to bronze.failed_events: {e}")
        if conn:
            conn.rollback()
        _write_fallback(failure_reason, raw_body, content_type)
    finally:
        if conn:
            release_db_connection(conn)


# ── SILVER ROUTING FUNCTIONS ──────────────────────────────────────────────────
# Each function: extracts typed fields from raw payload → writes to Silver table.
# None call conn.commit() — the webhook() route owns the transaction.

def route_user_created(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    user_id         = data.get('user_id')
    email           = data.get('email')
    full_name       = data.get('name') or data.get('full_name')
    user_name       = data.get('user_name')
    user_role       = data.get('user_role')
    contact_number  = data.get('contact_number')
    institution_id  = data.get('institution_id')
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
            email          = COALESCE(EXCLUDED.email,          silver.users.email),
            full_name      = COALESCE(EXCLUDED.full_name,      silver.users.full_name),
            user_name      = COALESCE(EXCLUDED.user_name,      silver.users.user_name),
            user_role      = COALESCE(EXCLUDED.user_role,      silver.users.user_role),
            contact_number = COALESCE(EXCLUDED.contact_number, silver.users.contact_number),
            institution_id = COALESCE(EXCLUDED.institution_id, silver.users.institution_id),
            created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist),
            received_at    = EXCLUDED.received_at
    """, (
        event_id, event_type, user_id,
        email, full_name, user_name, user_role, contact_number, institution_id,
        created_at_unix
    ))
    cur.close()


def _pluck_system_field(system_fields, *name_variants):
    # Handles both list format (real Edmingle) and dict format (test events).
    if isinstance(system_fields, dict):
        for name in name_variants:
            val = system_fields.get(name)
            if val is not None:
                return val
        return None

    if not isinstance(system_fields, list):
        return None

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
    cur = conn.cursor()

    # Real events nest fields under data['user']; test events are flat.
    user_obj        = data.get('user') or data
    user_id         = user_obj.get('user_id') or user_obj.get('id')
    email           = user_obj.get('email')
    full_name       = user_obj.get('name') or user_obj.get('full_name')
    # Real events use 'phone'; test events use 'contact_number'.
    contact_number  = user_obj.get('phone') or user_obj.get('contact_number')
    updated_at_unix = user_obj.get('updated_at') or event_timestamp

    system_fields_raw = data.get('system_fields')
    city    = user_obj.get('city')    or _pluck_system_field(system_fields_raw, 'city',    'City')
    state   = user_obj.get('state')   or _pluck_system_field(system_fields_raw, 'state',   'State')
    address = user_obj.get('address') or _pluck_system_field(system_fields_raw, 'address', 'Address')
    pincode = user_obj.get('pincode') or _pluck_system_field(system_fields_raw, 'pincode', 'Pincode', 'pin code')
    # Parent fields come only from system_fields, never from the user object.
    parent_name    = _pluck_system_field(system_fields_raw, 'parent_name',    'Parent Name',    'parent name')
    parent_email   = _pluck_system_field(system_fields_raw, 'parent_email',   'Parent Email',   'parent email')
    parent_contact = _pluck_system_field(system_fields_raw, 'parent_contact', 'Parent Contact', 'Parent Phone', 'parent phone')

    custom_fields_raw  = data.get('custom_fields')
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
    """, (
        event_id, event_type, user_id,
        email, full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        custom_fields_json, updated_at_unix
    ))
    cur.close()


def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # UPSERT key: (user_id, bundle_id, master_batch_id) — one row per enrollment.
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
    cur = conn.cursor()

    attendance_id        = data.get('attendance_id')
    class_id             = data.get('class_id')
    class_name           = data.get('class_name')
    class_type_formatted = data.get('class_type_formatted')
    gmt_start_time       = data.get('gmt_start_time')
    gmt_end_time         = data.get('gmt_end_time')
    # Real events use 'taken_at'; test events use 'actual_start_time'.
    actual_start_time    = data.get('actual_start_time') or data.get('taken_at')
    duration_minutes     = data.get('duration_minutes')
    teacher_id           = data.get('taken_by')
    teacher_name         = data.get('taken_by_name')
    teacher_email        = data.get('taken_by_email')
    master_batches_raw   = data.get('master_batches')
    master_batches_json  = psycopg2.extras.Json(master_batches_raw) if master_batches_raw is not None else None
    schedule_id          = data.get('schedule_id')
    is_recurring         = data.get('is_recurring')
    virtual_platform     = data.get('virtual_class_type_formatted')
    zoom_meeting_id      = data.get('zoom_meeting_id')
    cancellation_reason  = data.get('cancellation_reason')
    cancelled_by         = data.get('cancelled_by')
    status               = data.get('status')
    is_late_signin       = data.get('is_late_signin')
    delay_minutes        = data.get('delay_minutes')
    reminder_type        = data.get('reminder_type')

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
    cur = conn.cursor()

    user_id           = data.get('user_id')
    attempt_id        = data.get('attempt_id')
    exercise_id       = data.get('exercise_id')
    mark              = data.get('mark')
    is_evaluated      = data.get('is_evaluated')
    faculty_comments  = data.get('faculty_comments')
    # Real events use 'test_date'; test events use 'submitted_at'.
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
    cur = conn.cursor()

    user_id           = data.get('user_id')
    bundle_id         = data.get('bundle_id')
    # Real events have no 'completed_at' field — fall back to event_timestamp.
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
    # Payload structure not yet documented — store entire data{} as JSONB.
    cur = conn.cursor()
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


# ── EVENT ROUTER ──────────────────────────────────────────────────────────────
# Maps each Edmingle event_name to its Silver routing function.
# Edmingle's exact event name spellings (confirmed from live traffic):
#   session_update — no 'd'; session_cancel — no 'led'; session_start — no 'd'
EVENT_ROUTER = {
    'user.user_created':                   route_user_created,
    'user.user_updated':                   route_user_updated,
    'transaction.user_purchase_initiated': route_transaction,
    'transaction.user_purchase_completed': route_transaction,
    'transaction.user_purchase_failed':    route_transaction,
    'session.session_created':             route_session,
    'session.session_update':              route_session,
    'session.session_cancel':              route_session,
    'session.session_started':             route_session,
    'session.session_start':               route_session,
    'session.session_reminders':           route_session,
    'session.session_reminder':            route_session,
    'assessments.test_submitted':          route_assessment,
    'assessments.test_evaluated':          route_assessment,
    'assessments.exercise_submitted':      route_assessment,
    'assessments.exercise_evaluated':      route_assessment,
    'course.user_course_completed':        route_course,
    'announcement.announcement_created':   route_announcement,
    'certificate.certificate_issued':      route_certificate,
}


# ── FLASK ERROR HANDLERS ──────────────────────────────────────────────────────
# Edmingle marks our webhook Inactive permanently if it receives anything other than 200.

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
    log.warning(f"413 Request Entity Too Large on {request.path}")
    insert_failed_event('413 request too large', '', request.content_type)
    return jsonify({'status': 'received'}), 200


@app.errorhandler(500)
def handle_500(e):
    log.error(f"500 Internal Server Error: {e}")
    return jsonify({'status': 'received'}), 200


# ── FLASK ROUTE: GET /health ──────────────────────────────────────────────────
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


# ── FLASK ROUTE: GET /status ──────────────────────────────────────────────────
@app.route('/status', methods=['GET'])
def status():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT event_id, event_type, received_at, is_live_mode, routed_to_silver
            FROM   bronze.webhook_events
            ORDER  BY received_at DESC
            LIMIT  10
        """)
        rows = cur.fetchall()
        cur.close()

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


# ── FLASK ROUTE: GET /failed ──────────────────────────────────────────────────
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


# ── FLASK ROUTE: POST /retry-failed ──────────────────────────────────────────
# Processes events from the disk fallback file written during DB outage.
@app.route('/retry-failed', methods=['POST'])
def retry_failed():
    if not os.path.exists(FALLBACK_FILE):
        return jsonify({'retried': 0, 'remaining': 0}), 200

    with _fallback_lock:
        try:
            with open(FALLBACK_FILE, 'r', encoding='utf-8') as f:
                lines = [l.strip() for l in f if l.strip()]
        except Exception as e:
            return jsonify({'error': str(e)}), 200

    retried      = 0
    failed_lines = []

    for line in lines:
        try:
            entry      = _json.loads(line)
            raw_body   = entry.get('raw_body', '')
            payload    = _json.loads(raw_body) if raw_body else None
            if not payload:
                failed_lines.append(line)
                continue

            if 'event' in payload and 'id' not in payload:
                event_obj    = payload.get('event', {})
                event_type   = event_obj.get('event') or event_obj.get('event_name')
                event_ts_str = event_obj.get('event_ts', '')
                event_id     = f"{event_type}-{event_ts_str}" if event_ts_str else event_type
                is_live_mode = event_obj.get('livemode', True)
            else:
                event_id     = payload.get('id')
                event_type   = payload.get('event_name')
                is_live_mode = payload.get('is_live_mode', True)

            if not event_id or not event_type:
                failed_lines.append(line)
                continue

            conn = None
            try:
                conn = get_db_connection()
                insert_bronze(conn, event_id, event_type, payload, is_live_mode)
                conn.commit()
                retried += 1
                log.info(f"retry-failed: recovered {event_type} [{event_id}]")
            except Exception as e:
                failed_lines.append(line)
                log.error(f"retry-failed: could not recover {event_id}: {e}")
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    release_db_connection(conn)
        except Exception as e:
            failed_lines.append(line)

    with _fallback_lock:
        if failed_lines:
            with open(FALLBACK_FILE, 'w', encoding='utf-8') as f:
                for ln in failed_lines:
                    f.write(ln + '\n')
        else:
            try:
                os.remove(FALLBACK_FILE)
            except FileNotFoundError:
                pass

    log.info(f"retry-failed: retried={retried} remaining={len(failed_lines)}")
    return jsonify({'retried': retried, 'remaining': len(failed_lines)}), 200


# ── FLASK ROUTE: POST /webhook ────────────────────────────────────────────────
# CRITICAL: Always return HTTP 200 — Edmingle marks webhook Inactive on any other response.
@app.route('/webhook', methods=['POST'])
def webhook():

    # ── CAPTURE RAW BODY ─────────────────────────────────────────────────────
    raw_body     = request.get_data(as_text=True)
    content_type = request.content_type
    log.info(f"Raw request — Content-Type: {content_type} — Body: {mask_pii(raw_body[:300])}")

    # force=True parses JSON regardless of Content-Type — prevents silent data loss.
    payload = request.get_json(silent=True, force=True)

    if payload is None:
        reason = 'JSON parse failed'
        log.warning(f"{reason} — body was: {mask_pii(raw_body[:300])}")
        insert_failed_event(reason, raw_body, content_type)
        return jsonify({'status': 'received'}), 200

    # ── NORMALISE PAYLOAD ─────────────────────────────────────────────────────
    # Real Edmingle nested: {"event": {...}, "payload": {...}}
    # Test events flat:     {"id": "...", "event_name": "...", "data": {...}}
    if 'event' in payload and 'id' not in payload:
        event_obj    = payload.get('event', {})
        event_type   = event_obj.get('event') or event_obj.get('event_name')
        event_ts_str = event_obj.get('event_ts', '')
        event_id     = f"{event_type}-{event_ts_str}" if event_ts_str else event_type
        is_live_mode = event_obj.get('livemode', True)
        try:
            event_timestamp = int(datetime.datetime.fromisoformat(event_ts_str).timestamp())
        except (ValueError, TypeError):
            event_timestamp = None
        # CRITICAL: data is at top-level 'payload', NOT inside 'event'.
        data = payload.get('payload', {})
    else:
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

    # ── SAVE TO BRONZE ────────────────────────────────────────────────────────
    # Independent connection — Bronze is never rolled back by Silver failures.
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

    # url.validate is Edmingle's infrastructure health check — skip Silver routing.
    if event_type == 'url.validate':
        log.info("Validation ping stored in Bronze — skipping Silver routing.")
        return jsonify({'status': 'received'}), 200

    # ── ROUTE TO SILVER ───────────────────────────────────────────────────────
    # Silver insert and mark_routed_to_silver share one transaction.
    # Silver failure rolls back both — Bronze flag stays false for reprocessing.
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
            log.warning(f"Unknown event type '{event_type}' — stored in Bronze only")

    except Exception as e:
        if conn_silver:
            conn_silver.rollback()
        log.error(f"Silver routing failed for {event_type} [event_id: {event_id}]: {e}")
    finally:
        if conn_silver:
            release_db_connection(conn_silver)

    return jsonify({'status': 'received'}), 200


if __name__ == '__main__':
    log.info("Starting Edmingle webhook receiver on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)
