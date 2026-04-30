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

    # ── PHASE C — Trigger disk-fallback recovery ─────────────────────
    print("Phase C -- Triggering /retry-failed to recover disk-fallback events ...")
    time.sleep(1)
    try:
        rc = requests.post(f"{BASE_URL}/retry-failed", timeout=30)
        rc_data = rc.json()
        retried   = rc_data.get('retried', 0)
        remaining = rc_data.get('remaining', 0)
        print(f"  /retry-failed HTTP {rc.status_code}")
        print(f"  retried={retried}  remaining={remaining}")
        phase_c_ok = (rc.status_code == 200)
    except Exception as ex:
        print(f"  /retry-failed failed: {ex}")
        retried, remaining, phase_c_ok = 0, 0, False
    print()

    # ── PHASE 5 — Final count check ──────────────────────────────────
    print("Phase 5 -- Verifying all 10 outage events are in Bronze ...")
    time.sleep(1)

    conn2 = psycopg2.connect(**CONN_ARGS)
    cur2  = conn2.cursor()

    phase3_ids = [e['id'] for e in phase3_events]
    cur2.execute(
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = ANY(%s)",
        (phase3_ids,)
    )
    phase3_in_bronze = cur2.fetchone()[0]
    conn2.close()

    b2 = count_rows()
    print(f"  Phase-3 event IDs found in bronze.webhook_events: {phase3_in_bronze}/10")
    print()
    print("Final counts (delta from Phase-1 baseline):")
    for k, v in b2.items():
        delta = v - b1[k]
        marker = f"  (+{delta})" if delta else ""
        print(f"  {k}: {v:,}{marker}")

    # ── PASS / FAIL ──────────────────────────────────────────────────
    print()
    lost  = 10 - phase3_in_bronze
    pass1 = ok_baseline
    pass2 = all_200_while_down
    pass3 = db_restored
    passc = phase_c_ok
    pass5 = (phase3_in_bronze == 10)

    print("Checks:")
    print(f"  Phase 1 -- Baseline 5/5 HTTP 200:           {'PASS' if pass1 else 'FAIL'}")
    print(f"  Phase 3 -- 10/10 HTTP 200 while down:       {'PASS' if pass2 else 'FAIL'}")
    print(f"  Phase 4 -- DB restored successfully:        {'PASS' if pass3 else 'FAIL'}")
    print(f"  Phase C -- /retry-failed responded HTTP 200:{'PASS' if passc else 'FAIL'}")
    print(f"  Phase 5 -- All 10 events in Bronze:         "
          f"{'PASS' if pass5 else 'FAIL'}  ({phase3_in_bronze}/10 recovered)")
    print(f"  Data lost: {lost} (must be zero)")
    print()

    if pass1 and pass2 and pass3 and passc and pass5:
        print("DB UNAVAILABILITY TEST PASS")
    else:
        print("DB UNAVAILABILITY TEST FAIL")
        if not pass5:
            print(f"  REASON: {lost}/10 events sent during outage were not recovered.")


if __name__ == '__main__':
    main()
