"""
DB Unavailability Test
Simulates DB-down by using ALTER DATABASE ALLOW_CONNECTIONS false.
This blocks all new connections from the Flask pool while keeping
our admin session (connected to 'postgres' DB) alive.
"""
import psycopg2
import time
import requests
import uuid

CONN_ARGS = dict(host='localhost', port=5432, dbname='edmingle_analytics',
                 user='postgres', password='Svyoma')
BASE_URL  = "http://localhost:5000"


def count_rows(conn_args=CONN_ARGS):
    conn = psycopg2.connect(**conn_args)
    cur  = conn.cursor()
    results = {}
    for tbl in ['bronze.webhook_events', 'bronze.failed_events',
                'silver.users', 'silver.transactions',
                'silver.sessions', 'silver.courses', 'silver.assessments']:
        cur.execute(f'SELECT COUNT(*) FROM {tbl}')
        results[tbl] = cur.fetchone()[0]
    conn.close()
    return results


def make_event(label, req_num):
    uid = 92000000 + req_num
    ts  = int(time.time())
    return {
        'id':              f'dbtest-{label}-{uuid.uuid4().hex[:8]}',
        'event_name':      'user.user_created',
        'event_timestamp': ts,
        'is_live_mode':    False,
        'data': {
            'user_id':      uid,
            'email':        f'dbtest{req_num}@test.com',
            'name':         f'DB Test {req_num}',
            'user_name':    f'dbtest{req_num}',
            'user_role':    'student',
            'created_at':   ts,
            'institution_id': 483,
        }
    }


def send_events(label, start, count):
    results = []
    for i in range(count):
        ev = make_event(label, start + i)
        try:
            r = requests.post(f"{BASE_URL}/webhook", json=ev, timeout=15)
            results.append({'status': r.status_code, 'id': ev['id']})
        except Exception as ex:
            results.append({'status': 0, 'id': ev['id'], 'err': str(ex)})
    return results


def main():
    print("=" * 60)
    print("DB UNAVAILABILITY TEST")
    print("=" * 60)
    print()

    # ── PHASE 1 — Baseline ────────────────────────────────────────────
    print("Phase 1 -- Baseline (DB up): sending 5 events")
    r1 = send_events('baseline', 0, 5)
    time.sleep(0.5)
    b1 = count_rows()

    ok_baseline = all(e['status'] == 200 for e in r1)
    print(f"  HTTP 200s: {sum(1 for e in r1 if e['status']==200)}/5")
    print(f"  bronze.webhook_events: {b1['bronze.webhook_events']:,}")
    print(f"  silver.users:          {b1['silver.users']:,}")
    print(f"  Phase 1: {'OK' if ok_baseline else 'FAIL'}")
    print()

    # ── PHASE 2 — Block new connections ──────────────────────────────
    print("Phase 2 -- Blocking new connections to edmingle_analytics ...")
    admin_conn = psycopg2.connect(
        host='localhost', port=5432, dbname='postgres',
        user='postgres', password='Svyoma'
    )
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()

    admin_cur.execute("""
        SELECT pg_terminate_backend(pid)
        FROM   pg_stat_activity
        WHERE  datname = 'edmingle_analytics'
          AND  pid <> pg_backend_pid()
    """)
    terminated = admin_cur.rowcount
    admin_cur.execute("ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS false")
    print(f"  Terminated {terminated} active pool connections.")
    print(f"  ALLOW_CONNECTIONS = false  (new connections will be rejected)")

    db_blocked = False
    try:
        test_conn = psycopg2.connect(**CONN_ARGS)
        test_conn.close()
        print("  WARNING: still able to connect -- block not in effect yet")
    except psycopg2.OperationalError as e:
        db_blocked = True
        print(f"  DB confirmed unreachable: {str(e)[:80]}")
    print()

    # ── PHASE 3 — Send events while DB is down ────────────────────────
    print("Phase 3 -- Sending 10 events while DB is blocked ...")
    phase3_events = []
    for i in range(10):
        uid = 92000100 + i
        ts  = int(time.time())
        ev  = {
            'id':              f'dbtest-down-{uuid.uuid4().hex[:8]}',
            'event_name':      'user.user_created',
            'event_timestamp': ts,
            'is_live_mode':    False,
            'data': {
                'user_id':      uid,
                'email':        f'dbdown{i}@test.com',
                'name':         f'DB Down {i}',
                'user_name':    f'dbdown{i}',
                'user_role':    'student',
                'created_at':   ts,
                'institution_id': 483,
            }
        }
        try:
            r = requests.post(f"{BASE_URL}/webhook", json=ev, timeout=15)
            phase3_events.append({'status': r.status_code, 'id': ev['id']})
        except Exception as ex:
            phase3_events.append({'status': 0, 'id': ev['id'], 'err': str(ex)})

    all_200_while_down = all(e['status'] == 200 for e in phase3_events)
    print(f"  HTTP 200s returned while DB blocked: "
          f"{sum(1 for e in phase3_events if e['status']==200)}/10")
    print(f"  Server did NOT crash: {'YES' if all_200_while_down else 'NO - CRASH DETECTED'}")
    print()

    # ── PHASE 4 — Restore DB ─────────────────────────────────────────
    print("Phase 4 -- Restoring DB connections ...")
    admin_cur.execute("ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS true")
    admin_conn.close()
    time.sleep(2)

    db_restored = False
    try:
        test_conn = psycopg2.connect(**CONN_ARGS)
        test_conn.close()
        db_restored = True
        print("  DB connection restored successfully.")
    except Exception as e:
        print(f"  DB still unreachable: {e}")
    print()

    # ── PHASE 5 — Recovery check ─────────────────────────────────────
    print("Phase 5 -- Recovery: checking what was captured during outage ...")
    time.sleep(1)

    conn2 = psycopg2.connect(**CONN_ARGS)
    cur2  = conn2.cursor()

    phase3_ids = [e['id'] for e in phase3_events]
    cur2.execute(
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = ANY(%s)",
        (phase3_ids,)
    )
    phase3_in_bronze = cur2.fetchone()[0]

    cur2.execute("""
        SELECT COUNT(*) FROM bronze.failed_events
        WHERE  failure_reason LIKE '%%Bronze insert failed%%'
           OR  failure_reason LIKE '%%pool%%'
           OR  failure_reason LIKE '%%connect%%'
           OR  failure_reason LIKE '%%ALLOW_CONNECTIONS%%'
    """)
    failed_in_table = cur2.fetchone()[0]

    cur2.execute(
        "SELECT failure_reason FROM bronze.failed_events ORDER BY id DESC LIMIT 5"
    )
    recent_failures = [row[0] for row in cur2.fetchall()]
    conn2.close()

    b2 = count_rows()
    print(f"  Phase-3 event IDs found in bronze.webhook_events: {phase3_in_bronze}/10")
    print(f"  Entries in bronze.failed_events (connection errors): {failed_in_table}")
    if recent_failures:
        print(f"  Most recent failure reasons:")
        for fr in recent_failures:
            print(f"    - {str(fr)[:90]}")
    print()
    print("Final counts (delta from Phase-1 baseline):")
    for k, v in b2.items():
        delta = v - b1[k]
        marker = f"  (+{delta})" if delta else ""
        print(f"  {k}: {v:,}{marker}")

    # ── PASS / FAIL ──────────────────────────────────────────────────
    print()
    events_captured = phase3_in_bronze + failed_in_table
    pass1 = ok_baseline
    pass2 = all_200_while_down
    pass3 = db_restored
    pass4 = (events_captured >= 10)

    print("Checks:")
    print(f"  Phase 1 -- Baseline 5/5 HTTP 200:      {'PASS' if pass1 else 'FAIL'}")
    print(f"  Phase 3 -- 10/10 HTTP 200 while down:  {'PASS' if pass2 else 'FAIL'}")
    print(f"  Phase 4 -- DB restored successfully:   {'PASS' if pass3 else 'FAIL'}")
    print(f"  Phase 5 -- All 10 events captured:     "
          f"{'PASS' if pass4 else 'FAIL'}  ({events_captured}/10 in Bronze or failed_events)")
    print()

    if pass1 and pass2 and pass3 and pass4:
        print("DB UNAVAILABILITY TEST PASS")
    else:
        print("DB UNAVAILABILITY TEST FAIL")
        if not pass4:
            lost = 10 - events_captured
            print()
            print(f"  REASON: {lost}/10 events sent during outage were silently lost.")
            print(f"  Root cause: the Flask connection pool has no in-memory queue")
            print(f"  or disk fallback for events that arrive when both the Bronze")
            print(f"  insert AND the bronze.failed_events insert fail (DB fully down).")
            print(f"  The event body IS in the application log (ingestion/webhook_receiver.log)")
            print(f"  but there is no automated recovery path from log to database.")


if __name__ == '__main__':
    main()
