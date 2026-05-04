import sys
import datetime
import psycopg2
import psycopg2.extras
import psycopg2.pool

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

# ── DATABASE ──────────────────────────────────────────────────────────────────
_pool = None


def _init_pool():
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=5,
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def get_conn():
    return _pool.getconn()


def put_conn(conn):
    _pool.putconn(conn)


# ── BRONZE HELPERS ────────────────────────────────────────────────────────────

def mark_routed_to_silver(conn, event_id):
    cur = conn.cursor()
    cur.execute(
        "UPDATE bronze.webhook_events SET routed_to_silver = true WHERE event_id = %s",
        (event_id,)
    )
    cur.close()


# ── SILVER ROUTING FUNCTIONS ──────────────────────────────────────────────────

def _pluck_system_field(system_fields, *name_variants):
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


def route_user_created(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
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
        event_id, event_type, data.get('user_id'),
        data.get('email'), data.get('name') or data.get('full_name'),
        data.get('user_name'), data.get('user_role'), data.get('contact_number'),
        data.get('institution_id'),
        data.get('created_at') or event_timestamp
    ))
    cur.close()


def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    user_obj        = data.get('user') or data
    user_id         = user_obj.get('user_id') or user_obj.get('id')
    full_name       = user_obj.get('name') or user_obj.get('full_name')
    # Real events use 'phone'; test events use 'contact_number'.
    contact_number  = user_obj.get('phone') or user_obj.get('contact_number')
    updated_at_unix = user_obj.get('updated_at') or event_timestamp
    sf = data.get('system_fields')
    city    = user_obj.get('city')    or _pluck_system_field(sf, 'city',    'City')
    state   = user_obj.get('state')   or _pluck_system_field(sf, 'state',   'State')
    address = user_obj.get('address') or _pluck_system_field(sf, 'address', 'Address')
    pincode = user_obj.get('pincode') or _pluck_system_field(sf, 'pincode', 'Pincode', 'pin code')
    parent_name    = _pluck_system_field(sf, 'parent_name',    'Parent Name',    'parent name')
    parent_email   = _pluck_system_field(sf, 'parent_email',   'Parent Email',   'parent email')
    parent_contact = _pluck_system_field(sf, 'parent_contact', 'Parent Contact', 'Parent Phone', 'parent phone')
    cfr = data.get('custom_fields')
    cf  = psycopg2.extras.Json(cfr) if cfr is not None else None
    cur.execute("""
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
    """, (
        event_id, event_type, user_id,
        user_obj.get('email'), full_name, contact_number,
        city, state, address, pincode,
        parent_name, parent_email, parent_contact,
        cf, updated_at_unix
    ))
    cur.close()


def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # UPSERT key: (user_id, bundle_id, master_batch_id) — one row per enrollment.
    cur = conn.cursor()
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
        data.get('user_id'), data.get('email'),
        data.get('name') or data.get('full_name'), data.get('contact_number'),
        data.get('bundle_id'), data.get('course_name'),
        data.get('institution_bundle_id'), data.get('master_batch_id'),
        data.get('master_batch_name'),
        data.get('original_price'), data.get('discount'), data.get('final_price'),
        data.get('currency'), data.get('credits_applied'),
        data.get('payment_method'), data.get('transaction_id'),
        data.get('start_date'), data.get('end_date'), data.get('created_at')
    ))
    cur.close()


def route_session(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    mbr = data.get('master_batches')
    mb  = psycopg2.extras.Json(mbr) if mbr is not None else None
    # Real events use 'taken_at'; test events use 'actual_start_time'.
    actual_start = data.get('actual_start_time') or data.get('taken_at')
    cur.execute("""
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
    """, (
        event_id, event_type, data.get('attendance_id'),
        data.get('class_id'), data.get('class_name'), data.get('class_type_formatted'),
        data.get('gmt_start_time'), data.get('gmt_end_time'), actual_start,
        data.get('duration_minutes'), data.get('taken_by'), data.get('taken_by_name'),
        data.get('taken_by_email'), mb, data.get('schedule_id'), data.get('is_recurring'),
        data.get('virtual_class_type_formatted'), data.get('zoom_meeting_id'),
        data.get('cancellation_reason'), data.get('cancelled_by'),
        data.get('status'), data.get('is_late_signin'), data.get('delay_minutes'),
        data.get('reminder_type')
    ))
    cur.close()


def route_assessment(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    # Real events use 'test_date'; test events use 'submitted_at'.
    submitted_at = data.get('submitted_at') or data.get('test_date')
    cur.execute("""
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
        data.get('user_id'), data.get('attempt_id'), data.get('exercise_id'),
        data.get('mark'), data.get('is_evaluated'), data.get('faculty_comments'),
        submitted_at
    ))
    cur.close()


def route_course(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    # Real events have no 'completed_at' field — fall back to event_timestamp.
    completed_at = data.get('completed_at') or event_timestamp
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
    """, (event_id, event_type, data.get('user_id'), data.get('bundle_id'), completed_at))
    cur.close()


def route_announcement(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO silver.announcements (event_id, event_type, raw_data, received_at)
        VALUES (%s, %s, %s, NOW() AT TIME ZONE 'Asia/Kolkata')
        ON CONFLICT (event_id) DO UPDATE SET
            event_type  = EXCLUDED.event_type,
            raw_data    = EXCLUDED.raw_data,
            received_at = EXCLUDED.received_at
    """, (event_id, event_type,
          psycopg2.extras.Json(data) if data is not None else None))
    cur.close()


def route_certificate(conn, event_id, event_type, data, event_timestamp):
    cur = conn.cursor()
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
    """, (event_id, event_type,
          data.get('certificate_id'), data.get('user_id'), data.get('issued_at')))
    cur.close()


# ── EVENT ROUTER ──────────────────────────────────────────────────────────────
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


# ── PAYLOAD HELPERS ────────────────────────────────────────────────────────────

def _parse_event_timestamp(payload):
    if 'event' in payload and 'id' not in payload:
        event_obj    = payload.get('event', {})
        event_ts_str = event_obj.get('event_ts', '')
        try:
            return int(datetime.datetime.fromisoformat(event_ts_str).timestamp())
        except (ValueError, TypeError):
            return None
    return payload.get('event_timestamp')


def _extract_data(payload):
    if 'event' in payload and 'id' not in payload:
        return payload.get('payload', {})
    return payload.get('data', {})


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    _init_pool()

    conn_read = get_conn()
    cur = conn_read.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT event_id, event_type, raw_payload
        FROM   bronze.webhook_events
        WHERE  event_type != 'url.validate'
        ORDER  BY id
    """)
    rows = cur.fetchall()
    cur.close()
    put_conn(conn_read)

    total   = len(rows)
    ok      = 0
    skipped = 0
    errors  = 0

    print(f"Reprocessing {total} Bronze events...")

    for i, row in enumerate(rows):
        if i > 0 and i % 50 == 0:
            print(f"  Progress: {i}/{total}  (ok={ok} skip={skipped} err={errors})")

        event_id   = row['event_id']
        event_type = row['event_type']
        payload    = row['raw_payload']

        router_fn = EVENT_ROUTER.get(event_type)
        if not router_fn:
            skipped += 1
            continue

        data            = _extract_data(payload)
        event_timestamp = _parse_event_timestamp(payload)

        conn = get_conn()
        try:
            router_fn(conn, event_id, event_type, data, event_timestamp)
            mark_routed_to_silver(conn, event_id)
            conn.commit()
            ok += 1
        except Exception as e:
            conn.rollback()
            print(f"  ERROR  {event_type}  [{event_id}]: {e}", file=sys.stderr)
            errors += 1
        finally:
            put_conn(conn)

    print(f"\nProcessed: {total}  Success: {ok}  Failed: {errors}  Skipped: {skipped}")


if __name__ == '__main__':
    main()
