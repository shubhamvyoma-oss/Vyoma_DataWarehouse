# ============================================================
# 16 — TEST ALL EVENTS
# ============================================================
# What it does: Sends one test webhook event for every
#               supported event type to the running webhook
#               server, then checks that all Silver tables
#               have data in them.
#
# Why we need it: Quick smoke test to verify the webhook
#                 server is working correctly for all event
#                 types end-to-end, from HTTP request all
#                 the way to the database.
#
# How to run:
#   1. Start the webhook server first:
#      python 06_webhook_receiver/webhook_receiver.py
#   2. Then run this test:
#      python 16_test_all_events/test_all_events.py
#
# What to check after:
#   - All events should show "OK"
#   - All Silver tables should have data
# ============================================================

import requests
import psycopg2


# ── SETTINGS ──────────────────────────────────────────────────
WEBHOOK_URL = "http://localhost:5000/webhook"

DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
# ─────────────────────────────────────────────────────────────

# ── TEST EVENTS ───────────────────────────────────────────────
# One complete event payload for each supported event type.
# All session events share attendance_id=8914546 so they upsert
# onto one row in silver.sessions — matching real Edmingle behaviour.
# Each assessment event has a unique event_id to produce separate rows.

TEST_EVENTS = [

    # ── TRANSACTIONS ─────────────────────────────────────────

    {
        "id":              "txn-initiated-001",
        "event_name":      "transaction.user_purchase_initiated",
        "event_timestamp": 1709853000,
        "is_live_mode":    False,
        "data": {
            "user_id":              99001,
            "email":                "test.student@example.com",
            "full_name":            "Test Student",
            "bundle_id":            12477,
            "course_name":          "Sanskrit Foundation Course",
            "institution_bundle_id": 363,
            "master_batch_id":      1281,
            "master_batch_name":    "Batch A 2024",
            "original_price":       5000.00,
            "discount":             500.00,
            "final_price":          4500.00,
            "currency":             "INR",
            "credits_applied":      0.00,
            "start_date":           1709856600,
            "end_date":             1741392600,
            "created_at":           1709853000,
        },
    },

    {
        "id":              "txn-failed-001",
        "event_name":      "transaction.user_purchase_failed",
        "event_timestamp": 1709854000,
        "is_live_mode":    False,
        "data": {
            "user_id":       99002,
            "email":         "another.student@example.com",
            "full_name":     "Another Student",
            "bundle_id":     12477,
            "course_name":   "Sanskrit Foundation Course",
            "final_price":   4500.00,
            "currency":      "INR",
            "payment_method": "razorpay",
        },
    },

    # ── SESSIONS ─────────────────────────────────────────────
    # All 5 session events share attendance_id 8914546.
    # session_created inserts the row; the next 4 upsert onto it.

    {
        "id":              "session-created-001",
        "event_name":      "session.session_created",
        "event_timestamp": 1709856600,
        "is_live_mode":    False,
        "data": {
            "attendance_id":               8914546,
            "class_id":                    121333,
            "class_name":                  "Sanskrit Grammar - Lesson 1",
            "class_type_formatted":        "Live Class",
            "gmt_start_time":              1709870400,
            "gmt_end_time":                1709874000,
            "duration_minutes":            60,
            "taken_by":                    15,
            "taken_by_name":               "Prof. Ramesh Sharma",
            "taken_by_email":              "ramesh@example.com",
            "master_batches": [
                {
                    "master_batch_id":   1281,
                    "master_batch_name": "Batch A 2024",
                    "bundle_id":         12477,
                    "bundle_name":       "Sanskrit Foundation Course",
                }
            ],
            "schedule_id":   5001,
            "is_recurring":  True,
            "status":        0,
            "virtual_class_type_formatted": "Zoom",
            "zoom_meeting_id": "123456789",
        },
    },

    {
        "id":              "session-update-001",
        "event_name":      "session.session_update",
        "event_timestamp": 1709857000,
        "is_live_mode":    False,
        "data": {
            "attendance_id": 8914546,
            "class_id":      121333,
            "class_name":    "Sanskrit Grammar - Lesson 1",
            "gmt_start_time": 1709874000,
            "gmt_end_time":   1709877600,
        },
    },

    {
        "id":              "session-cancel-001",
        "event_name":      "session.session_cancel",
        "event_timestamp": 1709858000,
        "is_live_mode":    False,
        "data": {
            "attendance_id":       8914546,
            "cancellation_reason": "Teacher unavailable",
            "cancelled_by":        15,
        },
    },

    {
        "id":              "session-started-001",
        "event_name":      "session.session_started",
        "event_timestamp": 1709874300,
        "is_live_mode":    False,
        "data": {
            "attendance_id":    8914546,
            "class_id":         121333,
            "class_name":       "Sanskrit Grammar - Lesson 1",
            "gmt_start_time":   1709874000,
            "actual_start_time": 1709874300,
            "taken_at":          1709874300,
            "status":            1,
            "is_late_signin":    True,
            "delay_minutes":     5,
        },
    },

    {
        "id":              "session-reminder-001",
        "event_name":      "session.session_reminders",
        "event_timestamp": 1709870400,
        "is_live_mode":    False,
        "data": {
            "attendance_id": 8914546,
            "reminder_type": "1h_before",
        },
    },

    # ── ASSESSMENTS ──────────────────────────────────────────
    # Each event has a unique id, so 4 separate rows are created.

    {
        "id":              "assess-test-sub-001",
        "event_name":      "assessments.test_submitted",
        "event_timestamp": 1709860000,
        "is_live_mode":    False,
        "data": {
            "user_id":      99001,
            "attempt_id":   55001,
            "mark":         0,
            "is_evaluated": 0,
            "submitted_at": 1709860000,
        },
    },

    {
        "id":              "assess-test-eval-001",
        "event_name":      "assessments.test_evaluated",
        "event_timestamp": 1709861000,
        "is_live_mode":    False,
        "data": {
            "user_id":          99001,
            "attempt_id":       55001,
            "mark":             85.5,
            "is_evaluated":     1,
            "faculty_comments": "Good work. Review chapter 3.",
            "submitted_at":     1709860000,
        },
    },

    {
        "id":              "assess-ex-sub-001",
        "event_name":      "assessments.exercise_submitted",
        "event_timestamp": 1709862000,
        "is_live_mode":    False,
        "data": {
            "user_id":      99001,
            "attempt_id":   55002,
            "exercise_id":  7001,
            "mark":         0,
            "is_evaluated": 0,
            "submitted_at": 1709862000,
        },
    },

    {
        "id":              "assess-ex-eval-001",
        "event_name":      "assessments.exercise_evaluated",
        "event_timestamp": 1709863000,
        "is_live_mode":    False,
        "data": {
            "user_id":          99001,
            "attempt_id":       55002,
            "exercise_id":      7001,
            "mark":             90.0,
            "is_evaluated":     1,
            "faculty_comments": "Excellent exercise work.",
            "submitted_at":     1709862000,
        },
    },

    # ── COURSES ──────────────────────────────────────────────

    {
        "id":              "course-completed-001",
        "event_name":      "course.user_course_completed",
        "event_timestamp": 1741392600,
        "is_live_mode":    False,
        "data": {
            "user_id":      99001,
            "bundle_id":    12477,
            "completed_at": 1741392600,
        },
    },

    # ── ANNOUNCEMENTS ────────────────────────────────────────

    {
        "id":              "announce-001",
        "event_name":      "announcement.announcement_created",
        "event_timestamp": 1709857000,
        "is_live_mode":    False,
        "data": {
            "announcement_id": 301,
            "title":           "System Maintenance Notice",
            "message":         "Platform will be down for maintenance on Sunday.",
        },
    },

    # ── CERTIFICATES ─────────────────────────────────────────

    {
        "id":              "cert-001",
        "event_name":      "certificate.certificate_issued",
        "event_timestamp": 1741392700,
        "is_live_mode":    False,
        "data": {
            "certificate_id": "CERT-2024-99001-12477",
            "user_id":        99001,
            "issued_at":      1741392700,
        },
    },
]


# ── STEP 1: SEND ALL TEST EVENTS ──────────────────────────────

def send_all_events():
    # Send each event one at a time to the webhook server
    print("=" * 60)
    print("SENDING TEST EVENTS")
    print("=" * 60)

    # Track which event types passed and which failed
    results = {}

    for event in TEST_EVENTS:
        event_name = event["event_name"]
        event_id   = event["id"]

        try:
            # Send the event as a JSON POST request
            response = requests.post(WEBHOOK_URL, json=event, timeout=5)
            http_ok = (response.status_code == 200)
        except Exception as error:
            print("  FAIL  " + event_name + "  -- server unreachable: " + str(error))
            results[event_name] = False
            continue

        # Print one line per event showing pass/fail
        if http_ok:
            print("  OK    " + event_name + "  [" + event_id + "]")
        else:
            print("  FAIL  " + event_name + "  [" + event_id + "]  HTTP " + str(response.status_code))

        results[event_name] = http_ok

    # Count how many event types failed
    failed_list = []
    for event_name, passed in results.items():
        if not passed:
            failed_list.append(event_name)

    print("")
    if len(failed_list) > 0:
        print("  " + str(len(failed_list)) + " event(s) got a non-200 response:")
        for name in failed_list:
            print("    - " + name)
    else:
        print("  All " + str(len(TEST_EVENTS)) + " events returned HTTP 200.")

    return results


# ── STEP 2: CHECK SILVER TABLE COUNTS ────────────────────────

def check_silver_counts():
    # Connect to the database and print the row count for each Silver table
    print("")
    print("=" * 60)
    print("SILVER TABLE ROW COUNTS")
    print("=" * 60)

    # These are the Silver tables that the test events should populate
    silver_tables = [
        "silver.users",
        "silver.transactions",
        "silver.sessions",
        "silver.assessments",
        "silver.courses",
        "silver.announcements",
        "silver.certificates",
    ]

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        return False

    cursor = conn.cursor()
    all_tables_have_data = True

    for table_name in silver_tables:
        cursor.execute("SELECT COUNT(*) FROM " + table_name)
        count = cursor.fetchone()[0]

        if count > 0:
            status_label = "OK   "
        else:
            status_label = "EMPTY"
            all_tables_have_data = False

        print("  " + status_label + "  " + table_name + ": " + str(count) + " row(s)")

    cursor.close()
    conn.close()

    print("")
    if all_tables_have_data:
        print("  All 7 Silver tables have data.")
    else:
        print("  WARNING: one or more Silver tables are empty.")
        print("  Check that the webhook server is running and events are being routed.")

    return all_tables_have_data


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== test_all_events.py ===")
    print("")
    print("NOTE: The webhook server must be running before this test.")
    print("      Start it with: python 06_webhook_receiver/webhook_receiver.py")
    print("")

    # Step 1: send all the test events
    send_results = send_all_events()

    # Step 2: check that Silver tables were populated
    check_silver_counts()


if __name__ == "__main__":
    main()
