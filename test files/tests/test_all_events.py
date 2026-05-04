# =============================================================================
# FILE    : tests/test_all_events.py
# PURPOSE : Sends one test webhook event for every untested event type,
#           then queries every Silver table for row counts.
#           Run with: python tests/test_all_events.py
# =============================================================================

import requests
import psycopg2

WEBHOOK_URL = "http://localhost:5000/webhook"

DB_CONFIG = {
    'host':     'localhost',
    'port':     5432,
    'dbname':   'edmingle_analytics',
    'user':     'postgres',
    'password': 'Svyoma',
}

# =============================================================================
# TEST EVENTS
# One payload per untested event type.
# All session events use the same attendance_id (8914546) so they upsert onto
# one row in silver.sessions — matching how real Edmingle events behave.
# All assessment events have unique event_ids so they produce 4 separate rows.
# =============================================================================
EVENTS = [

    # ── TRANSACTIONS ──────────────────────────────────────────────────────────

    {
        "id": "txn-initiated-001",
        "event_name": "transaction.user_purchase_initiated",
        "event_timestamp": 1709853000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "email": "test.student@example.com",
            "full_name": "Test Student",
            "bundle_id": 12477,
            "course_name": "Sanskrit Foundation Course",
            "institution_bundle_id": 363,
            "master_batch_id": 1281,
            "master_batch_name": "Batch A 2024",
            "original_price": 5000.00,
            "discount": 500.00,
            "final_price": 4500.00,
            "currency": "INR",
            "credits_applied": 0.00,
            "start_date": 1709856600,
            "end_date": 1741392600,
            "created_at": 1709853000,
        },
    },

    {
        "id": "txn-failed-001",
        "event_name": "transaction.user_purchase_failed",
        "event_timestamp": 1709854000,
        "is_live_mode": False,
        "data": {
            "user_id": 99002,
            "email": "another.student@example.com",
            "full_name": "Another Student",
            "bundle_id": 12477,
            "course_name": "Sanskrit Foundation Course",
            "final_price": 4500.00,
            "currency": "INR",
            "payment_method": "razorpay",
            "failure_reason": "Payment gateway timeout",
            "error_code": "GATEWAY_TIMEOUT",
            "attempted_at": 1709854000,
        },
    },

    # ── SESSIONS ──────────────────────────────────────────────────────────────
    # All five events share attendance_id 8914546.
    # session_created inserts the row; the next four upsert on top of it.
    # Final result: 1 row in silver.sessions with all fields populated.

    {
        "id": "session-created-001",
        "event_name": "session.session_created",
        "event_timestamp": 1709856600,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar — Lesson 1",
            "class_type": 1,
            "class_type_formatted": "Live Class",
            "gmt_start_time": 1709870400,
            "gmt_end_time": 1709874000,
            "duration_minutes": 60,
            "taken_by": 15,
            "taken_by_name": "Prof. Ramesh Sharma",
            "taken_by_email": "ramesh@example.com",
            "master_batches": [
                {
                    "master_batch_id": 1281,
                    "master_batch_name": "Batch A 2024",
                    "bundle_id": 12477,
                    "bundle_name": "Sanskrit Foundation Course",
                }
            ],
            "organization_id": 683,
            "institution_id": 483,
            "schedule_id": 5001,
            "is_recurring": True,
            "status": 0,
            "is_live": False,
            "virtual_class_type": 1,
            "virtual_class_type_formatted": "Zoom",
            "zoom_meeting_id": "123456789",
            "created_at": 1709856600,
            "created_by": 15,
        },
    },

    {
        "id": "session-update-001",
        "event_name": "session.session_update",
        "event_timestamp": 1709857000,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar — Lesson 1",
            "gmt_start_time": 1709874000,
            "gmt_end_time": 1709877600,
            "taken_by": 15,
            "taken_by_name": "Prof. Ramesh Sharma",
            "updated_at": 1709857000,
            "updated_by": 15,
        },
    },

    {
        "id": "session-cancel-001",
        "event_name": "session.session_cancel",
        "event_timestamp": 1709858000,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "cancellation_reason": "Teacher unavailable",
            "cancelled_by": 15,
        },
    },

    {
        "id": "session-started-001",
        "event_name": "session.session_started",
        "event_timestamp": 1709874300,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar — Lesson 1",
            "gmt_start_time": 1709874000,
            "actual_start_time": 1709874300,
            "taken_at": 1709874300,
            "signin_by": 15,
            "signin_by_name": "Prof. Ramesh Sharma",
            "signin_by_email": "ramesh@example.com",
            "status": 1,
            "is_live_formatted": "Running",
            "is_late_signin": True,
            "delay_minutes": 5,
        },
    },

    {
        "id": "session-reminder-001",
        "event_name": "session.session_reminders",
        "event_timestamp": 1709870400,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "reminder_type": "1h_before",
        },
    },

    # ── ASSESSMENTS ───────────────────────────────────────────────────────────
    # Each event has a unique event_id → 4 separate rows in silver.assessments.

    {
        "id": "assess-test-sub-001",
        "event_name": "assessments.test_submitted",
        "event_timestamp": 1709860000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55001,
            "mark": 0,
            "is_evaluated": 0,
            "submitted_at": 1709860000,
        },
    },

    {
        "id": "assess-test-eval-001",
        "event_name": "assessments.test_evaluated",
        "event_timestamp": 1709861000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55001,
            "mark": 85.5,
            "is_evaluated": 1,
            "faculty_comments": "Good work. Review chapter 3.",
            "submitted_at": 1709860000,
        },
    },

    {
        "id": "assess-ex-sub-001",
        "event_name": "assessments.exercise_submitted",
        "event_timestamp": 1709862000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55002,
            "exercise_id": 7001,
            "mark": 0,
            "is_evaluated": 0,
            "submitted_at": 1709862000,
        },
    },

    {
        "id": "assess-ex-eval-001",
        "event_name": "assessments.exercise_evaluated",
        "event_timestamp": 1709863000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55002,
            "exercise_id": 7001,
            "mark": 90.0,
            "is_evaluated": 1,
            "faculty_comments": "Excellent exercise work.",
            "submitted_at": 1709862000,
        },
    },

    # ── COURSES ───────────────────────────────────────────────────────────────

    {
        "id": "course-completed-001",
        "event_name": "course.user_course_completed",
        "event_timestamp": 1741392600,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "bundle_id": 12477,
            "completed_at": 1741392600,
        },
    },

    # ── ANNOUNCEMENTS ─────────────────────────────────────────────────────────

    {
        "id": "announce-001",
        "event_name": "announcement.announcement_created",
        "event_timestamp": 1709857000,
        "is_live_mode": False,
        "data": {
            "announcement_id": 301,
            "title": "System Maintenance Notice",
            "message": "The platform will be down for maintenance on Sunday.",
            "created_by": 1,
            "created_at": 1709857000,
        },
    },

    # ── CERTIFICATES ──────────────────────────────────────────────────────────

    {
        "id": "cert-001",
        "event_name": "certificate.certificate_issued",
        "event_timestamp": 1741392700,
        "is_live_mode": False,
        "data": {
            "certificate_id": "CERT-2024-99001-12477",
            "user_id": 99001,
            "issued_at": 1741392700,
        },
    },
]


def send_all_events():
    print("=" * 60)
    print("SENDING TEST EVENTS")
    print("=" * 60)
    results = {}
    for event in EVENTS:
        name = event["event_name"]
        eid  = event["id"]
        try:
            resp = requests.post(WEBHOOK_URL, json=event, timeout=5)
            ok   = resp.status_code == 200
        except Exception as e:
            print(f"  FAIL  {name}  — could not reach server: {e}")
            results[name] = False
            continue

        status = "OK  " if ok else "FAIL"
        print(f"  {status}  {name}  [{eid}]")
        results[name] = ok

    failed = [k for k, v in results.items() if not v]
    if failed:
        print(f"\n  {len(failed)} event(s) got a non-200 response: {failed}")
    else:
        print(f"\n  All {len(EVENTS)} events returned HTTP 200.")
    return results


def check_silver_counts():
    print("\n" + "=" * 60)
    print("SILVER TABLE ROW COUNTS")
    print("=" * 60)

    tables = [
        "silver.users",
        "silver.transactions",
        "silver.sessions",
        "silver.assessments",
        "silver.courses",
        "silver.announcements",
        "silver.certificates",
    ]

    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    all_ok = True
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count  = cur.fetchone()[0]
        status = "OK   " if count > 0 else "EMPTY"
        print(f"  {status}  {table}: {count} row(s)")
        if count == 0:
            all_ok = False

    cur.close()
    conn.close()

    print()
    if all_ok:
        print("  All 7 Silver tables have data.")
    else:
        print("  WARNING: one or more Silver tables are empty — check errors above.")
    return all_ok


if __name__ == "__main__":
    send_all_events()
    check_silver_counts()
