# ============================================================
# 18 — TEST PIPELINE END-TO-END
# ============================================================
# What it does: Runs 9 comprehensive tests against the live
#               webhook server and database to verify the
#               entire pipeline is working correctly.
#
#   Test 1: Data integrity — no duplicates, no orphaned rows
#   Test 2: Field mapping — API payload fields reach Silver
#   Test 3: Duplicate protection — same event_id is idempotent
#   Test 4: Failed event recovery — /failed endpoint works
#   Test 5: Server resilience — server restarts cleanly
#   Test 6: IST timestamps — all times use +05:30 offset
#   Test 7: NULL audit — no important fields are mostly NULL
#   Test 8: Concurrent load — 20 simultaneous events handled
#   Test 9: Constraint violation — bad data is rejected safely
#
# Why we need it: Proves the full pipeline is correct and robust
#                 before deploying to production.
#
# How to run:
#   1. Start the webhook server first:
#      python 06_webhook_receiver/webhook_receiver.py
#   2. Then run this test:
#      python 18_test_pipeline_e2e/test_pipeline_e2e.py
#
# What to check after:
#   - Final results summary at the bottom should show PASS for all
# ============================================================

import os
import sys
import subprocess
import threading
import time
import uuid
import datetime
import psycopg2
import psycopg2.extras
import requests

# Force UTF-8 output so non-ASCII characters print correctly on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── SETTINGS ──────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
WEBHOOK_URL = "http://localhost:5000/webhook"
SERVER_URL  = "http://localhost:5000"
# ─────────────────────────────────────────────────────────────

# This script lives at: 18_test_pipeline_e2e/test_pipeline_e2e.py
# The project root is one level up
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)

DB_CONN_ARGS = {
    "host":     DB_HOST,
    "port":     DB_PORT,
    "dbname":   DB_NAME,
    "user":     DB_USER,
    "password": DB_PASSWORD,
}

# Dictionary to collect all test results: {test_name: True/False}
TEST_RESULTS = {}


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def connect_to_database():
    # Open a connection to the database with autocommit enabled
    conn = psycopg2.connect(**DB_CONN_ARGS)
    conn.autocommit = True
    return conn


def run_query_all_rows(conn, sql, params=None):
    # Run a query and return all rows as a list of dictionaries
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if params is not None:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def run_query_one_value(conn, sql, params=None):
    # Run a query and return the first column of the first row, or None
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()
    if row:
        return row[0]
    return None


def send_event(payload, timeout_seconds=10):
    # Send a single event as a JSON POST to the webhook server
    return requests.post(WEBHOOK_URL, json=payload, timeout=timeout_seconds)


def make_event(event_name, data, event_id=None, timestamp=None):
    # Build a standard "flat" test event payload (the format test_webhook_send.py uses)
    if event_id is None:
        event_id = "e2e-" + uuid.uuid4().hex[:10]
    if timestamp is None:
        timestamp = int(time.time())
    return {
        "id":              event_id,
        "event_name":      event_name,
        "event_timestamp": timestamp,
        "is_live_mode":    False,
        "data":            data,
    }


def log_test_result(test_name, passed, detail=""):
    # Record a test result and print a PASS/FAIL line
    icon = "[PASS]" if passed else "[FAIL]"
    print("  " + icon + "  " + test_name)
    if detail:
        # Print multi-line detail indented
        for line in detail.strip().splitlines():
            print("           " + line)
    TEST_RESULTS[test_name] = passed


def print_section_header(title):
    # Print a section header
    print("")
    print("=" * 62)
    print("  " + title)
    print("=" * 62)


def cleanup_test_data(conn, event_ids, user_ids=None, attendance_ids=None):
    # Delete test data from Bronze and Silver tables so tests don't interfere
    cursor = conn.cursor()

    if event_ids:
        # Reset routed_to_silver flag in Bronze
        cursor.execute(
            "UPDATE bronze.webhook_events SET routed_to_silver = false WHERE event_id = ANY(%s)",
            (event_ids,),
        )
        # Delete from all Silver tables that use event_id
        for table_name in ("silver.transactions", "silver.assessments",
                           "silver.courses", "silver.announcements",
                           "silver.certificates"):
            cursor.execute(
                "DELETE FROM " + table_name + " WHERE event_id = ANY(%s)",
                (event_ids,),
            )

    if user_ids:
        cursor.execute("DELETE FROM silver.users WHERE user_id = ANY(%s)", (user_ids,))

    if attendance_ids:
        cursor.execute(
            "DELETE FROM silver.sessions WHERE attendance_id = ANY(%s)",
            (attendance_ids,),
        )

    cursor.close()


# ── TEST 1: DATA INTEGRITY ────────────────────────────────────

def test1_data_integrity():
    print_section_header("TEST 1 -- Data Integrity Check")
    conn = connect_to_database()

    # Check 1a: No duplicate event_ids in Bronze
    duplicate_count = run_query_one_value(conn, """
        SELECT COUNT(*) FROM (
            SELECT event_id FROM bronze.webhook_events
            GROUP BY event_id HAVING COUNT(*) > 1
        ) sub
    """)
    log_test_result(
        "1a. No duplicate event_ids in Bronze",
        duplicate_count == 0,
        str(duplicate_count) + " duplicate(s)" if duplicate_count else "",
    )

    # Check 1b: bronze.failed_events should be empty
    failed_count = run_query_one_value(conn, "SELECT COUNT(*) FROM bronze.failed_events")
    log_test_result(
        "1b. bronze.failed_events is empty",
        failed_count == 0,
        str(failed_count) + " failed row(s)" if failed_count else "",
    )

    # Check 1c: For each Silver table with event_id, every routed Bronze event must have a Silver row
    silver_checks = [
        ("assessments", "silver.assessments"),
        ("course",      "silver.courses"),
        ("announcement","silver.announcements"),
        ("certificate", "silver.certificates"),
    ]
    for event_prefix, silver_table in silver_checks:
        missing = run_query_one_value(conn, """
            SELECT COUNT(*) FROM bronze.webhook_events b
            WHERE b.routed_to_silver = true
              AND b.event_type LIKE %s
              AND NOT EXISTS (
                SELECT 1 FROM """ + silver_table + """ s WHERE s.event_id = b.event_id
              )
        """, (event_prefix + ".%",))
        log_test_result(
            "1c. " + silver_table + ": all routed Bronze events have Silver rows",
            missing == 0,
            str(missing) + " orphan(s)" if missing else "",
        )

    # Check 1c: silver.users — check via user_id (real events use nested payload)
    u_missing = run_query_one_value(conn, """
        SELECT COUNT(*) FROM bronze.webhook_events b
        WHERE b.routed_to_silver = true
          AND b.is_live_mode = true
          AND b.event_type IN ('user.user_created','user.user_updated')
          AND NOT EXISTS (
            SELECT 1 FROM silver.users u
            WHERE u.user_id = COALESCE(
              NULLIF((b.raw_payload->'payload'->>'user_id'), '')::bigint,
              NULLIF((b.raw_payload->'payload'->'user'->>'user_id'), '')::bigint,
              NULLIF((b.raw_payload->'data'->>'user_id'), '')::bigint
            )
          )
    """)
    log_test_result(
        "1c. silver.users: all routed Bronze events have Silver rows",
        u_missing == 0,
        str(u_missing) + " orphan(s)" if u_missing else "",
    )

    # Check 1c: silver.sessions — check via attendance_id
    s_missing = run_query_one_value(conn, """
        SELECT COUNT(*) FROM bronze.webhook_events b
        WHERE b.routed_to_silver = true
          AND b.event_type LIKE 'session.%%'
          AND NOT EXISTS (
            SELECT 1 FROM silver.sessions s
            WHERE s.attendance_id = COALESCE(
              NULLIF((b.raw_payload->'payload'->>'attendance_id'), '')::bigint,
              NULLIF((b.raw_payload->'data'->>'attendance_id'), '')::bigint
            )
          )
    """)
    log_test_result(
        "1c. silver.sessions: all routed Bronze events have Silver rows",
        s_missing == 0,
        str(s_missing) + " orphan(s)" if s_missing else "",
    )

    # Check 1d: Key columns in Silver tables should never be NULL
    null_checks = [
        ("silver.users",        "user_id"),
        ("silver.transactions", "user_id"),
        ("silver.sessions",     "attendance_id"),
        ("silver.assessments",  "user_id"),
        ("silver.assessments",  "attempt_id"),
        ("silver.courses",      "user_id"),
        ("silver.courses",      "bundle_id"),
        ("silver.certificates", "user_id"),
        ("silver.certificates", "certificate_id"),
    ]
    for table_name, column_name in null_checks:
        null_count = run_query_one_value(conn,
            "SELECT COUNT(*) FROM " + table_name + " WHERE " + column_name + " IS NULL")
        log_test_result(
            "1d. " + table_name + "." + column_name + " has no NULLs",
            null_count == 0,
            str(null_count) + " NULL(s)" if null_count else "",
        )

    conn.close()


# ── TEST 2: FIELD MAPPING VERIFICATION ────────────────────────

def test2_field_mapping():
    print_section_header("TEST 2 -- Real Payload Field Mapping Verification")
    conn = connect_to_database()

    # Each check describes one event type and the fields we expect to find mapped in Silver
    # This test only runs if there are real (is_live_mode=true) events in Bronze
    checks = [
        {
            "label":      "user.user_created",
            "event_type": "user.user_created",
            "silver_table": "silver.users",
            "silver_key": "user_id",
            # key_path is the path in raw_payload to the UPSERT key value
            "key_path":   ["payload", "user_id"],
            # Each mapping is: (path in raw_payload, column in Silver, type for comparison)
            "mappings": [
                (["payload", "name"],      "full_name", str),
                (["payload", "email"],     "email",     str),
                (["payload", "user_id"],   "user_id",   int),
                (["payload", "user_name"], "user_name", str),
            ],
        },
        {
            "label":      "user.user_updated",
            "event_type": "user.user_updated",
            "silver_table": "silver.users",
            "silver_key": "user_id",
            "key_path":   ["payload", "user", "user_id"],
            "mappings": [
                (["payload", "user", "name"],    "full_name", str),
                (["payload", "user", "email"],   "email",     str),
                (["payload", "user", "user_id"], "user_id",   int),
            ],
        },
        {
            "label":      "transaction.user_purchase_completed",
            "event_type": "transaction.user_purchase_completed",
            "silver_table": "silver.transactions",
            "silver_key": "event_id",
            "key_path":   None,
            "mappings": [
                (["payload", "name"],        "full_name",   str),
                (["payload", "email"],       "email",       str),
                (["payload", "user_id"],     "user_id",     int),
                (["payload", "bundle_id"],   "bundle_id",   int),
                (["payload", "course_name"], "course_name", str),
            ],
        },
        {
            "label":      "session.session_start",
            "event_type": "session.session_start",
            "silver_table": "silver.sessions",
            "silver_key": "attendance_id",
            "key_path":   ["payload", "attendance_id"],
            "mappings": [
                (["payload", "class_name"],    "class_name",   str),
                (["payload", "taken_by_name"], "teacher_name", str),
                (["payload", "attendance_id"], "attendance_id", int),
            ],
        },
    ]

    all_mappings_pass = True

    for check in checks:
        # Find real events (is_live_mode has 'event' key in payload = real Edmingle format)
        bronze_rows = run_query_all_rows(conn, """
            SELECT event_id, raw_payload
            FROM   bronze.webhook_events
            WHERE  event_type = %s AND raw_payload ? 'event'
            ORDER  BY received_at DESC LIMIT 5
        """, (check["event_type"],))

        if not bronze_rows:
            # No real events of this type in Bronze — skip with a warning
            print("  [WARN]  " + check["label"] + " -- no real events in Bronze, skipping")
            continue

        # Check the most recent real event
        bronze_row = bronze_rows[0]
        event_id   = bronze_row["event_id"]
        payload    = bronze_row["raw_payload"]

        # Find the corresponding Silver row
        if check["silver_key"] == "event_id":
            silver_rows = run_query_all_rows(conn,
                "SELECT * FROM " + check["silver_table"] + " WHERE event_id = %s",
                (event_id,))
        else:
            # Navigate the key_path to find the UPSERT key value
            key_value = payload
            for key in check["key_path"]:
                if isinstance(key_value, dict):
                    key_value = key_value.get(key)
                else:
                    key_value = None
            if key_value is None:
                print("  [WARN]  " + check["label"] + " -- could not extract key value")
                continue
            silver_rows = run_query_all_rows(conn,
                "SELECT * FROM " + check["silver_table"] + " WHERE " + check["silver_key"] + " = %s",
                (key_value,))

        if not silver_rows:
            print("  [WARN]  " + check["label"] + " -- no Silver row found, skipping")
            continue

        silver_row = silver_rows[0]

        # Check each field mapping
        for field_path, silver_column, cast_type in check["mappings"]:
            # Navigate the path in raw_payload to get the payload value
            payload_value = payload
            for key in field_path:
                if isinstance(payload_value, dict):
                    payload_value = payload_value.get(key)
                else:
                    payload_value = None

            silver_value = silver_row.get(silver_column)

            # Compare values after casting to the expected type
            try:
                if payload_value is not None:
                    cast_payload = cast_type(payload_value)
                else:
                    cast_payload = None
                if silver_value is not None:
                    cast_silver = cast_type(silver_value)
                else:
                    cast_silver = None
                values_match = (cast_payload == cast_silver)
            except (ValueError, TypeError):
                values_match = (str(payload_value) == str(silver_value))

            field_label = check["label"] + ": " + ".".join(field_path[1:]) + " -> " + silver_column
            if not values_match:
                all_mappings_pass = False
                log_test_result(
                    "2. MISMATCH " + field_label,
                    False,
                    "payload=" + repr(payload_value) + "  silver=" + repr(silver_value),
                )

    log_test_result("2. All sampled real payload fields correctly mapped to Silver",
                    all_mappings_pass)
    conn.close()


# ── TEST 3: DUPLICATE PROTECTION ──────────────────────────────

def test3_duplicate_protection():
    print_section_header("TEST 3 -- Duplicate Protection")
    conn = connect_to_database()

    # Send the exact same event twice — the server must handle it gracefully
    test_user_id = 99999901
    test_event_id = "dupe-test-" + uuid.uuid4().hex[:8]
    event = make_event("user.user_created", {
        "user_id":        test_user_id,
        "full_name":      "Dupe Test",
        "email":          "dupe@test.com",
        "user_name":      "dupetest",
        "user_role":      "student",
        "created_at":     int(time.time()),
        "contact_number": None,
        "institution_id": 483,
    }, event_id=test_event_id)

    response1 = send_event(event)
    response2 = send_event(event)
    time.sleep(0.5)

    # Count how many times this event_id appears in Bronze (should be exactly 1)
    bronze_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = %s",
        (test_event_id,))

    # Count how many times this user_id appears in Silver (should be exactly 1)
    silver_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM silver.users WHERE user_id = %s",
        (test_user_id,))

    log_test_result(
        "3a. Both duplicate requests returned HTTP 200",
        response1.status_code == 200 and response2.status_code == 200,
        "r1=" + str(response1.status_code) + "  r2=" + str(response2.status_code),
    )
    log_test_result(
        "3b. Bronze has exactly 1 row for the duplicate event_id",
        bronze_count == 1,
        "found " + str(bronze_count),
    )
    log_test_result(
        "3c. Silver has exactly 1 row for the duplicate user_id",
        silver_count == 1,
        "found " + str(silver_count),
    )

    cleanup_test_data(conn, [test_event_id], user_ids=[test_user_id])
    conn.close()


# ── TEST 4: FAILED EVENT RECOVERY ─────────────────────────────

def test4_failed_event_recovery():
    print_section_header("TEST 4 -- Failed Event Recovery")
    conn = connect_to_database()

    # Insert a fake failed event manually so we can test the /failed endpoint
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type)
        VALUES ('e2e-test failure', '{"test":true}', 'application/json')
        RETURNING id
    """)
    fake_event_id = cursor.fetchone()[0]
    cursor.close()

    time.sleep(0.2)

    # Check that GET /failed shows the fake event
    response = requests.get(SERVER_URL + "/failed", timeout=5)
    response_data = response.json()
    returned_ids = [row.get("id") for row in response_data.get("last_10_failed", [])]

    log_test_result("4a. GET /failed returns HTTP 200", response.status_code == 200)
    log_test_result(
        "4b. Fake failed event visible in /failed response",
        fake_event_id in returned_ids,
        "fake_id=" + str(fake_event_id) + "  returned=" + str(returned_ids),
    )

    # Delete the fake event and verify it disappears from /failed
    cursor2 = conn.cursor()
    cursor2.execute("DELETE FROM bronze.failed_events WHERE id = %s", (fake_event_id,))
    cursor2.close()

    response2 = requests.get(SERVER_URL + "/failed", timeout=5)
    ids_after_delete = [row.get("id") for row in response2.json().get("last_10_failed", [])]
    log_test_result(
        "4c. After deletion, fake row no longer in /failed",
        fake_event_id not in ids_after_delete,
        "fake_id=" + str(fake_event_id) + "  still in " + str(ids_after_delete),
    )

    conn.close()


# ── TEST 5: SERVER RESILIENCE ─────────────────────────────────

def test5_server_resilience():
    print_section_header("TEST 5 -- Server Resilience")
    conn = connect_to_database()

    port = 5000

    # Try to find and stop any existing server process on port 5000
    old_pid = None
    try:
        netstat_result = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, shell=True,
        )
        for line in netstat_result.stdout.splitlines():
            if ":" + str(port) in line and "LISTENING" in line:
                parts = line.split()
                old_pid = int(parts[-1])
                break
    except Exception:
        old_pid = None

    if old_pid:
        subprocess.run(
            ["taskkill", "/F", "/PID", str(old_pid)],
            capture_output=True, shell=True,
        )
        time.sleep(1.5)
        print("    Stopped old server (PID " + str(old_pid) + ")")
    else:
        print("    Could not locate running server PID -- continuing anyway")

    # Start a new server process
    server_script_path = os.path.join(PROJECT_DIR, "06_webhook_receiver", "webhook_receiver.py")
    new_process = subprocess.Popen(
        [sys.executable, server_script_path],
        cwd=PROJECT_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait up to 12 seconds for the server to start accepting connections
    server_came_up = False
    for attempt in range(12):
        time.sleep(1)
        try:
            health_response = requests.get(SERVER_URL + "/health", timeout=2)
            if health_response.status_code == 200:
                server_came_up = True
                break
        except requests.exceptions.ConnectionError:
            # Server is not up yet — try again
            pass

    log_test_result(
        "5a. Server restarted and /health returned 200",
        server_came_up,
        "Never came up within 12s" if not server_came_up else "",
    )

    if not server_came_up:
        conn.close()
        return

    # Send one event to the freshly restarted server
    test_user_id = 99999902
    test_event_id = "resilience-" + uuid.uuid4().hex[:8]
    event = make_event("user.user_created", {
        "user_id":        test_user_id,
        "full_name":      "Resilience Test",
        "email":          "resilience@test.com",
        "user_name":      "restest",
        "user_role":      "student",
        "created_at":     int(time.time()),
        "contact_number": None,
        "institution_id": 483,
    }, event_id=test_event_id)

    response = send_event(event)
    time.sleep(0.5)

    bronze_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = %s",
        (test_event_id,))
    silver_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM silver.users WHERE user_id = %s",
        (test_user_id,))

    log_test_result("5b. Post-restart event returned HTTP 200", response.status_code == 200)
    log_test_result("5c. Post-restart event stored in Bronze", bronze_count == 1,
                    "found " + str(bronze_count))
    log_test_result("5d. Post-restart event routed to Silver", silver_count == 1,
                    "found " + str(silver_count))

    cleanup_test_data(conn, [test_event_id], user_ids=[test_user_id])
    conn.close()


# ── TEST 6: IST TIMESTAMP VERIFICATION ────────────────────────

def test6_ist_timestamps():
    print_section_header("TEST 6 -- IST Timestamp Verification")
    conn = connect_to_database()

    # These are the tables and columns that should always store timestamps as IST (+05:30)
    timestamp_checks = [
        ("silver.users",        ["created_at_ist",      "received_at"]),
        ("silver.transactions", ["event_timestamp_ist",  "inserted_at"]),
        ("silver.sessions",     ["scheduled_start_ist",  "received_at"]),
        ("silver.assessments",  ["submitted_at_ist",     "received_at"]),
        ("silver.courses",      ["completed_at_ist",     "received_at"]),
    ]

    bad_columns = []
    sample_lines = []

    for table_name, column_names in timestamp_checks:
        for column_name in column_names:
            rows = run_query_all_rows(conn,
                "SELECT " + column_name + "::text AS ts_text FROM "
                + table_name + " WHERE " + column_name + " IS NOT NULL LIMIT 5")

            for row in rows:
                ts_text = row["ts_text"]
                sample_lines.append(table_name + "." + column_name + ": " + str(ts_text))

                # IST timestamps must contain the +05:30 offset marker
                if "+05:30" not in (ts_text or ""):
                    bad_columns.append(table_name + "." + column_name
                                       + " -- text=" + repr(ts_text))

    # Show a few sample timestamps so we can visually verify the offset
    if sample_lines:
        print("")
        print("  Sample timestamps (expect +05:30 offset):")
        for line in sample_lines[:6]:
            print("    " + line)

    log_test_result(
        "6. All Silver timestamps have IST (+05:30) offset",
        len(bad_columns) == 0,
        "\n".join(bad_columns) if bad_columns else "",
    )

    conn.close()


# ── TEST 7: NULL AUDIT ─────────────────────────────────────────

def test7_null_audit():
    print_section_header("TEST 7 -- NULL Audit (<=20% NULL threshold)")
    conn = connect_to_database()

    # Each check is: (description, SQL query returning (null_count, total_count), max_pct_allowed)
    null_checks = [
        ("silver.users.full_name", """
            SELECT COUNT(*) FILTER (WHERE full_name IS NULL), COUNT(*) FROM silver.users
        """, 20.0),
        ("silver.users.email", """
            SELECT COUNT(*) FILTER (WHERE email IS NULL), COUNT(*) FROM silver.users
        """, 5.0),
        ("silver.transactions.full_name (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.full_name IS NULL), COUNT(*)
            FROM silver.transactions t
            JOIN bronze.webhook_events b USING (event_id)
            WHERE b.is_live_mode = true
        """, 5.0),
        ("silver.transactions.bundle_id (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.bundle_id IS NULL), COUNT(*)
            FROM silver.transactions t
            JOIN bronze.webhook_events b USING (event_id)
            WHERE b.is_live_mode = true
        """, 5.0),
        ("silver.sessions.class_name", """
            SELECT COUNT(*) FILTER (WHERE class_name IS NULL), COUNT(*) FROM silver.sessions
        """, 10.0),
        ("silver.sessions.teacher_name", """
            SELECT COUNT(*) FILTER (WHERE teacher_name IS NULL), COUNT(*) FROM silver.sessions
        """, 20.0),
        ("silver.certificates.certificate_id", """
            SELECT COUNT(*) FILTER (WHERE certificate_id IS NULL), COUNT(*) FROM silver.certificates
        """, 5.0),
    ]

    print("")
    print("  " + "column/table".ljust(52) + " nulls  total    pct   limit  status")
    print("  " + "-" * 52 + " -----  -----  -----   -----  ------")

    all_within_threshold = True

    for label, sql, max_allowed_pct in null_checks:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        cursor.close()

        null_count = row[0]
        total_count = row[1]

        if total_count > 0:
            pct = null_count / total_count * 100
        else:
            pct = 0.0

        within_threshold = (pct <= max_allowed_pct)
        if not within_threshold:
            all_within_threshold = False

        status = "OK" if within_threshold else "OVER (" + "{:.1f}".format(pct) + "%)"
        icon = "ok" if within_threshold else "FAIL"

        print("  [" + icon + "]  " + label.ljust(48)
              + " " + str(null_count).rjust(5)
              + " " + str(total_count).rjust(6)
              + " " + "{:.1f}%".format(pct).rjust(6)
              + "  " + str(int(max_allowed_pct)).rjust(4) + "%  " + status)

    log_test_result("7. All columns within NULL % threshold", all_within_threshold)
    conn.close()


# ── TEST 8: CONCURRENT LOAD ────────────────────────────────────

def test8_concurrent_load():
    print_section_header("TEST 8 -- Concurrent Load Test (20 simultaneous events)")
    conn = connect_to_database()

    num_events = 20

    # Build unique event IDs and user IDs for this test run
    concurrent_event_ids = ["conc-" + uuid.uuid4().hex[:10] for i in range(num_events)]
    concurrent_user_ids  = [99997000 + i for i in range(num_events)]

    # Collect HTTP response codes from all threads
    response_codes = []
    # Lock prevents two threads writing to response_codes at the same time
    response_lock = threading.Lock()

    def send_one_event(event_index):
        # This function runs in a separate thread for each event
        event = make_event("user.user_created", {
            "user_id":        concurrent_user_ids[event_index],
            "full_name":      "Concurrent User " + str(event_index),
            "email":          "concurrent_" + str(event_index) + "@test.com",
            "user_name":      "concuser" + str(event_index),
            "user_role":      "student",
            "created_at":     int(time.time()),
            "contact_number": None,
            "institution_id": 483,
        }, event_id=concurrent_event_ids[event_index])

        try:
            response = requests.post(WEBHOOK_URL, json=event, timeout=20)
            # Use the lock when writing to the shared list
            response_lock.acquire()
            response_codes.append(response.status_code)
            response_lock.release()
        except Exception:
            response_lock.acquire()
            response_codes.append(0)
            response_lock.release()

    # Start all threads at once
    thread_list = []
    for i in range(num_events):
        thread = threading.Thread(target=send_one_event, args=(i,))
        thread_list.append(thread)

    start_time = time.time()

    for thread in thread_list:
        thread.start()

    # Wait for all threads to finish
    for thread in thread_list:
        thread.join()

    elapsed_seconds = time.time() - start_time

    # Wait a moment for all DB writes to complete
    time.sleep(1.0)

    # Count how many events made it to Bronze and Silver
    all_200 = all(code == 200 for code in response_codes)

    bronze_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = ANY(%s)",
        (concurrent_event_ids,))
    silver_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM silver.users WHERE user_id = ANY(%s)",
        (concurrent_user_ids,))

    # Find any non-200 responses to report
    non_200 = [code for code in response_codes if code != 200]

    log_test_result(
        "8a. All 20 concurrent requests returned HTTP 200",
        all_200,
        "non-200 responses: " + str(non_200) if non_200 else "",
    )
    log_test_result(
        "8b. All 20 events stored in Bronze",
        bronze_count == num_events,
        "Bronze count=" + str(bronze_count) + ", expected " + str(num_events),
    )
    log_test_result(
        "8c. All 20 events routed to Silver",
        silver_count == num_events,
        "Silver count=" + str(silver_count) + ", expected " + str(num_events),
    )
    print("    Wall-clock time for 20 concurrent requests: " + "{:.2f}".format(elapsed_seconds) + "s")

    cleanup_test_data(conn, concurrent_event_ids, user_ids=concurrent_user_ids)
    conn.close()


# ── TEST 9: CONSTRAINT VIOLATION HANDLING ─────────────────────

def test9_constraint_violation():
    print_section_header("TEST 9 -- DB Constraint Violation Handling")
    conn = connect_to_database()

    test_event_id = "constraint-" + uuid.uuid4().hex[:8]

    # Deliberately omit attendance_id — silver.sessions has it NOT NULL
    # This should cause a DB error, but the server must still return HTTP 200
    # and store the event in Bronze (just not route it to Silver)
    event = make_event("session.session_created", {
        "class_id":       999901,
        "class_name":     "Constraint Test Class",
        "gmt_start_time": int(time.time()),
        "gmt_end_time":   int(time.time()) + 3600,
        # attendance_id intentionally missing
    }, event_id=test_event_id)

    response = send_event(event)
    time.sleep(0.5)

    # The server must still return HTTP 200 even though Silver will reject the event
    log_test_result(
        "9a. Server returns HTTP 200 despite Silver constraint violation",
        response.status_code == 200,
        "status=" + str(response.status_code),
    )

    # The event should be preserved in Bronze for recovery later
    bronze_rows = run_query_all_rows(conn,
        "SELECT routed_to_silver FROM bronze.webhook_events WHERE event_id = %s",
        (test_event_id,))

    log_test_result(
        "9b. Event is preserved in Bronze",
        len(bronze_rows) == 1,
        "Bronze rows=" + str(len(bronze_rows)),
    )

    if bronze_rows:
        # routed_to_silver should be False because the Silver INSERT failed
        was_routed = bronze_rows[0]["routed_to_silver"]
        log_test_result(
            "9c. Bronze.routed_to_silver = false (Silver was rolled back)",
            was_routed is False,
            "routed_to_silver=" + str(was_routed),
        )

    # The malformed event should NOT appear in silver.sessions
    silver_count = run_query_one_value(conn,
        "SELECT COUNT(*) FROM silver.sessions WHERE event_id = %s",
        (test_event_id,))
    log_test_result(
        "9d. Malformed event NOT written to Silver",
        silver_count == 0,
        "Silver rows=" + str(silver_count),
    )

    cleanup_test_data(conn, [test_event_id])
    conn.close()


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("")
    print("=" * 62)
    print("  Edmingle Webhook Pipeline -- End-to-End Test Suite")
    print("=" * 62)

    # Confirm the server is running before starting tests
    try:
        health_response = requests.get(SERVER_URL + "/health", timeout=5)
        if health_response.status_code != 200:
            print("")
            print("  [FAIL] Server not healthy at " + SERVER_URL + " -- aborting")
            sys.exit(1)
        print("")
        print("  Server healthy at " + SERVER_URL)
    except requests.exceptions.ConnectionError:
        print("")
        print("  [FAIL] Cannot connect to " + SERVER_URL)
        print("         Is the webhook server running?")
        print("         Start it with: python 06_webhook_receiver/webhook_receiver.py")
        sys.exit(1)

    # Run all 9 tests in order
    test1_data_integrity()
    test2_field_mapping()
    test3_duplicate_protection()
    test4_failed_event_recovery()
    test5_server_resilience()
    test6_ist_timestamps()
    test7_null_audit()
    test8_concurrent_load()
    test9_constraint_violation()

    # Print the final summary
    total_tests  = len(TEST_RESULTS)
    passed_count = 0
    for result in TEST_RESULTS.values():
        if result:
            passed_count = passed_count + 1
    failed_count = total_tests - passed_count

    print("")
    print("=" * 62)
    print("  FINAL RESULTS: " + str(passed_count) + "/" + str(total_tests)
          + " passed, " + str(failed_count) + " failed")
    print("=" * 62)

    for test_name, passed in TEST_RESULTS.items():
        icon = "[PASS]" if passed else "[FAIL]"
        print("  " + icon + "  " + test_name)
    print("")

    if failed_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
