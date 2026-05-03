# ============================================================
# 17 — TEST DB UNAVAILABILITY
# ============================================================
# What it does: Tests that the webhook server handles database
#               outages gracefully — events must NOT be lost
#               when the DB is temporarily down.
#
#               The test runs in 5 phases:
#               Phase 1: Baseline — send 5 events while DB is up
#               Phase 2: Block all new DB connections (simulate outage)
#               Phase 3: Send 10 events while DB is blocked
#               Phase 4: Restore DB connections
#               Phase C: Call /retry-failed to recover the queued events
#               Phase 5: Verify all 10 outage events are in Bronze
#
# Why we need it: Confirms the disk-fallback queue in the webhook
#                 server is working. If this test passes, zero events
#                 are lost during a DB outage.
#
# IMPORTANT: This test uses ALTER DATABASE which affects all connections.
#            Only run this in a development environment, never on production.
#
# How to run:
#   1. Start the webhook server first:
#      python 06_webhook_receiver/webhook_receiver.py
#   2. Then run this test:
#      python 17_test_db_unavailability/test_db_unavailability.py
#
# What to check after:
#   - All 5 phases should say PASS
#   - "Data lost: 0" in the summary
# ============================================================

import time
import uuid
import psycopg2
import requests


# ── SETTINGS ──────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
WEBHOOK_URL = "http://localhost:5000/webhook"
SERVER_URL  = "http://localhost:5000"
# ─────────────────────────────────────────────────────────────

DB_CONN_ARGS = {
    "host":     DB_HOST,
    "port":     DB_PORT,
    "dbname":   DB_NAME,
    "user":     DB_USER,
    "password": DB_PASSWORD,
}


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def count_rows_in_silver_tables():
    # Connect and count rows in the main Silver and Bronze tables
    conn = psycopg2.connect(**DB_CONN_ARGS)
    cursor = conn.cursor()

    table_list = [
        "bronze.webhook_events",
        "bronze.failed_events",
        "silver.users",
        "silver.transactions",
        "silver.sessions",
        "silver.courses",
        "silver.assessments",
    ]

    counts = {}
    for table_name in table_list:
        cursor.execute("SELECT COUNT(*) FROM " + table_name)
        counts[table_name] = cursor.fetchone()[0]

    cursor.close()
    conn.close()
    return counts


def build_user_created_event(label, request_number):
    # Build a simple user.user_created event payload for testing
    user_id = 92000000 + request_number
    timestamp = int(time.time())
    # Use a random hex suffix so each event_id is unique
    event_id = "dbtest-" + label + "-" + uuid.uuid4().hex[:8]

    return {
        "id":              event_id,
        "event_name":      "user.user_created",
        "event_timestamp": timestamp,
        "is_live_mode":    False,
        "data": {
            "user_id":        user_id,
            "email":          "dbtest" + str(request_number) + "@test.com",
            "name":           "DB Test " + str(request_number),
            "user_name":      "dbtest" + str(request_number),
            "user_role":      "student",
            "created_at":     timestamp,
            "institution_id": 483,
        },
    }


def send_events_batch(label, start_number, event_count):
    # Send multiple events to the webhook server and collect the HTTP status codes
    all_results = []
    for i in range(event_count):
        event = build_user_created_event(label, start_number + i)
        try:
            response = requests.post(WEBHOOK_URL, json=event, timeout=15)
            all_results.append({"status": response.status_code, "id": event["id"]})
        except Exception as error:
            all_results.append({"status": 0, "id": event["id"], "error": str(error)})
    return all_results


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("DB UNAVAILABILITY TEST")
    print("=" * 60)
    print("")
    print("WARNING: This test temporarily blocks all new connections")
    print("         to the edmingle_analytics database.")
    print("         Only run in a development environment.")
    print("")

    # ── PHASE 1: BASELINE ────────────────────────────────────
    print("Phase 1 -- Baseline (DB up): sending 5 events ...")
    phase1_results = send_events_batch("baseline", 0, 5)

    # Give the server a moment to process the events
    time.sleep(0.5)
    counts_after_phase1 = count_rows_in_silver_tables()

    # Count how many of the 5 events got HTTP 200
    phase1_ok_count = 0
    for result in phase1_results:
        if result["status"] == 200:
            phase1_ok_count = phase1_ok_count + 1

    phase1_pass = (phase1_ok_count == 5)

    print("  HTTP 200s received        : " + str(phase1_ok_count) + "/5")
    print("  bronze.webhook_events     : " + str(counts_after_phase1["bronze.webhook_events"]))
    print("  silver.users              : " + str(counts_after_phase1["silver.users"]))
    print("  Phase 1: " + ("PASS" if phase1_pass else "FAIL"))
    print("")

    # ── PHASE 2: BLOCK NEW DB CONNECTIONS ────────────────────
    print("Phase 2 -- Blocking new connections to " + DB_NAME + " ...")

    # Connect to the system 'postgres' database (not the app database)
    # so we can still issue admin commands even after blocking the app DB
    admin_conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname="postgres",
        user=DB_USER, password=DB_PASSWORD,
    )
    # autocommit=True is needed so ALTER DATABASE takes effect immediately
    admin_conn.autocommit = True
    admin_cursor = admin_conn.cursor()

    # Terminate any existing connections to the app database
    # so the Flask connection pool is completely cleared
    admin_cursor.execute("""
        SELECT pg_terminate_backend(pid)
        FROM   pg_stat_activity
        WHERE  datname = 'edmingle_analytics'
          AND  pid <> pg_backend_pid()
    """)
    terminated_count = admin_cursor.rowcount
    print("  Terminated existing pool connections : " + str(terminated_count))

    # Block all new connections to the app database
    admin_cursor.execute("ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS false")
    print("  ALLOW_CONNECTIONS set to false")

    # Verify the block is actually working by trying to connect
    db_is_blocked = False
    try:
        test_conn = psycopg2.connect(**DB_CONN_ARGS)
        test_conn.close()
        print("  WARNING: DB is still reachable — block may not have taken effect")
    except psycopg2.OperationalError as error:
        db_is_blocked = True
        print("  DB confirmed unreachable: " + str(error)[:80])
    print("")

    # ── PHASE 3: SEND EVENTS WHILE DB IS DOWN ────────────────
    print("Phase 3 -- Sending 10 events while DB is blocked ...")

    phase3_events = []
    for i in range(10):
        user_id = 92000100 + i
        timestamp = int(time.time())
        event_id = "dbtest-down-" + uuid.uuid4().hex[:8]

        event = {
            "id":              event_id,
            "event_name":      "user.user_created",
            "event_timestamp": timestamp,
            "is_live_mode":    False,
            "data": {
                "user_id":        user_id,
                "email":          "dbdown" + str(i) + "@test.com",
                "name":           "DB Down " + str(i),
                "user_name":      "dbdown" + str(i),
                "user_role":      "student",
                "created_at":     timestamp,
                "institution_id": 483,
            },
        }

        try:
            response = requests.post(WEBHOOK_URL, json=event, timeout=15)
            phase3_events.append({"status": response.status_code, "id": event_id})
        except Exception as error:
            phase3_events.append({"status": 0, "id": event_id, "error": str(error)})

    # Count HTTP 200 responses — server must NOT crash even when DB is down
    phase3_ok_count = 0
    for result in phase3_events:
        if result["status"] == 200:
            phase3_ok_count = phase3_ok_count + 1

    phase3_pass = (phase3_ok_count == 10)

    print("  HTTP 200s while DB blocked   : " + str(phase3_ok_count) + "/10")
    if phase3_pass:
        print("  Server did NOT crash: YES -- events were queued to disk fallback")
    else:
        print("  Server DID crash or returned errors -- FAIL")
    print("")

    # ── PHASE 4: RESTORE DB CONNECTIONS ──────────────────────
    print("Phase 4 -- Restoring DB connections ...")

    admin_cursor.execute("ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS true")
    admin_conn.close()

    # Wait for the change to take effect
    time.sleep(2)

    db_restored = False
    try:
        test_conn = psycopg2.connect(**DB_CONN_ARGS)
        test_conn.close()
        db_restored = True
        print("  DB connection restored successfully.")
    except Exception as error:
        print("  DB still unreachable: " + str(error))
    print("")

    # ── PHASE C: TRIGGER DISK-FALLBACK RECOVERY ──────────────
    print("Phase C -- Calling /retry-failed to recover disk-fallback events ...")
    time.sleep(1)

    retry_ok = False
    retried_count   = 0
    remaining_count = 0

    try:
        retry_response = requests.post(SERVER_URL + "/retry-failed", timeout=30)
        response_data = retry_response.json()
        retried_count   = response_data.get("retried",   0)
        remaining_count = response_data.get("remaining", 0)
        retry_ok = (retry_response.status_code == 200)
        print("  /retry-failed HTTP status : " + str(retry_response.status_code))
        print("  retried                   : " + str(retried_count))
        print("  remaining                 : " + str(remaining_count))
    except Exception as error:
        print("  /retry-failed call failed : " + str(error))
    print("")

    # ── PHASE 5: VERIFY ALL OUTAGE EVENTS ARE IN BRONZE ──────
    print("Phase 5 -- Verifying all 10 outage events are in Bronze ...")
    time.sleep(1)

    # Collect all the event IDs from Phase 3 to search for in Bronze
    phase3_event_ids = []
    for result in phase3_events:
        phase3_event_ids.append(result["id"])

    conn2 = psycopg2.connect(**DB_CONN_ARGS)
    cursor2 = conn2.cursor()

    # Count how many of the Phase-3 events made it to Bronze
    # ANY(%s) matches any value in the Python list
    cursor2.execute(
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = ANY(%s)",
        (phase3_event_ids,),
    )
    phase3_in_bronze = cursor2.fetchone()[0]
    cursor2.close()
    conn2.close()

    events_lost = 10 - phase3_in_bronze
    phase5_pass = (phase3_in_bronze == 10)

    print("  Phase-3 events found in bronze: " + str(phase3_in_bronze) + "/10")
    print("")

    # Print final row counts with delta from Phase 1
    counts_final = count_rows_in_silver_tables()
    print("Final counts (change from Phase-1 baseline):")
    for table_name, final_count in counts_final.items():
        baseline_count = counts_after_phase1[table_name]
        delta = final_count - baseline_count
        if delta > 0:
            delta_string = "  (+" + str(delta) + ")"
        else:
            delta_string = ""
        print("  " + table_name + ": " + str(final_count) + delta_string)
    print("")

    # ── PRINT PASS/FAIL SUMMARY ───────────────────────────────
    print("Test checks:")
    print("  Phase 1 -- 5/5 HTTP 200 baseline       : " + ("PASS" if phase1_pass else "FAIL"))
    print("  Phase 3 -- 10/10 HTTP 200 while DB down : " + ("PASS" if phase3_pass else "FAIL"))
    print("  Phase 4 -- DB restored successfully     : " + ("PASS" if db_restored else "FAIL"))
    print("  Phase C -- /retry-failed returned 200   : " + ("PASS" if retry_ok    else "FAIL"))
    print("  Phase 5 -- All 10 events in Bronze      : " + ("PASS" if phase5_pass else "FAIL")
          + "  (" + str(phase3_in_bronze) + "/10 recovered)")
    print("  Data lost                               : " + str(events_lost)
          + "  (must be zero)")
    print("")

    all_passed = phase1_pass and phase3_pass and db_restored and retry_ok and phase5_pass

    if all_passed:
        print("DB UNAVAILABILITY TEST: PASS")
    else:
        print("DB UNAVAILABILITY TEST: FAIL")
        if not phase5_pass:
            print("  REASON: " + str(events_lost) + "/10 events sent during outage were lost.")
    print("")


if __name__ == "__main__":
    main()
