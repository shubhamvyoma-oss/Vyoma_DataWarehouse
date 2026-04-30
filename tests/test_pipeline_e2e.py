#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import io
import json
import os
import subprocess
import sys
import threading
import time
import uuid

# Force UTF-8 output so non-ASCII characters in labels print cleanly on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
import requests

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

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_URL  = "http://localhost:5000"
WEBHOOK_URL = f"{SERVER_URL}/webhook"

DB_DSN = dict(
    host     = DB_HOST,
    port     = DB_PORT,
    dbname   = DB_NAME,
    user     = DB_USER,
    password = DB_PASSWORD,
)

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"

_results: dict[str, bool] = {}


# ── HELPERS ───────────────────────────────────────────────────────────────────

def db_connect():
    conn = psycopg2.connect(**DB_DSN)
    conn.autocommit = True
    return conn


def q(conn, sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        return cur.fetchall()


def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params) if params is not None else cur.execute(sql)
        row = cur.fetchone()
        return row[0] if row else None


def post(payload, timeout=10):
    return requests.post(WEBHOOK_URL, json=payload, timeout=timeout)


def make_flat_event(event_name, data, event_id=None, ts=None):
    return {
        'id':              event_id or f"e2e-{uuid.uuid4().hex[:10]}",
        'event_name':      event_name,
        'event_timestamp': ts or int(time.time()),
        'is_live_mode':    False,
        'data':            data,
    }


def log_result(name: str, passed: bool, detail: str = ''):
    icon = f"[{PASS}]" if passed else f"[{FAIL}]"
    print(f"  {icon}  {name}")
    if detail:
        for line in detail.strip().splitlines():
            print(f"           {line}")
    _results[name] = passed


def section(title: str):
    print(f"\n{'='*62}")
    print(f"  {title}")
    print(f"{'='*62}")


def cleanup(conn, event_ids: list[str], user_ids: list[int] = None,
            attendance_ids: list[int] = None):
    with conn.cursor() as cur:
        if event_ids:
            # Reset Bronze flag so integrity checks don't see stale routed_to_silver=true
            cur.execute(
                "UPDATE bronze.webhook_events SET routed_to_silver = false WHERE event_id = ANY(%s)",
                (event_ids,),
            )
            for tbl in ('silver.transactions', 'silver.assessments',
                        'silver.courses', 'silver.announcements',
                        'silver.certificates'):
                cur.execute(f"DELETE FROM {tbl} WHERE event_id = ANY(%s)", (event_ids,))
        if user_ids:
            cur.execute("DELETE FROM silver.users WHERE user_id = ANY(%s)", (user_ids,))
        if attendance_ids:
            cur.execute(
                "DELETE FROM silver.sessions WHERE attendance_id = ANY(%s)",
                (attendance_ids,),
            )


# ── TEST 1 — Data integrity check ─────────────────────────────────────────────

def test1_data_integrity():
    section("TEST 1 — Data Integrity Check")
    conn = db_connect()

    dupes = scalar(conn, """
        SELECT COUNT(*) FROM (
            SELECT event_id FROM bronze.webhook_events
            GROUP BY event_id HAVING COUNT(*) > 1
        ) x
    """)
    log_result("1a. No duplicate event_ids in Bronze",
               dupes == 0, f"{dupes} duplicate(s)" if dupes else "")

    fc = scalar(conn, "SELECT COUNT(*) FROM bronze.failed_events")
    log_result("1b. bronze.failed_events is empty",
               fc == 0, f"{fc} failed row(s)" if fc else "")

    for prefix, silver in [
        ('assessments', 'silver.assessments'),
        ('course',      'silver.courses'),
        ('announcement','silver.announcements'),
        ('certificate', 'silver.certificates'),
    ]:
        missing = scalar(conn, f"""
            SELECT COUNT(*) FROM bronze.webhook_events b
            WHERE b.routed_to_silver = true
              AND b.event_type LIKE %s
              AND NOT EXISTS (
                SELECT 1 FROM {silver} s WHERE s.event_id = b.event_id
              )
        """, (f"{prefix}.%",))
        log_result(f"1c. {silver}: all routed Bronze events have Silver rows",
                   missing == 0, f"{missing} orphan(s)" if missing else "")

    # silver.transactions uses (user_id, bundle_id, master_batch_id) as its UPSERT key.
    # Multiple Bronze events for the same enrollment collapse into one Silver row,
    # so checking event_id would spuriously flag earlier events as "missing".
    t_missing = scalar(conn, """
        SELECT COUNT(*) FROM bronze.webhook_events b
        WHERE b.routed_to_silver = true
          AND b.is_live_mode = true
          AND b.event_type LIKE %s
          AND NOT EXISTS (
            SELECT 1 FROM silver.transactions s
            WHERE s.user_id = COALESCE(
              NULLIF((b.raw_payload->'payload'->>'user_id'), '')::bigint,
              NULLIF((b.raw_payload->'data'->>'user_id'),    '')::bigint
            )
          )
    """, ('transaction.%',))
    log_result("1c. silver.transactions: all routed Bronze events covered by Silver",
               t_missing == 0, f"{t_missing} orphan(s)" if t_missing else "")

    u_missing = scalar(conn, """
        SELECT COUNT(*) FROM bronze.webhook_events b
        WHERE b.routed_to_silver = true
          AND b.is_live_mode = true
          AND b.event_type IN ('user.user_created','user.user_updated')
          AND NOT EXISTS (
            SELECT 1 FROM silver.users u
            WHERE u.user_id = COALESCE(
              NULLIF((b.raw_payload->'payload'->>'user_id'), '')::bigint,
              NULLIF((b.raw_payload->'payload'->'user'->>'user_id'), '')::bigint,
              NULLIF((b.raw_payload->'payload'->'user'->>'id'), '')::bigint,
              NULLIF((b.raw_payload->'data'->>'user_id'), '')::bigint
            )
          )
    """)
    log_result("1c. silver.users: all routed Bronze events have Silver rows",
               u_missing == 0, f"{u_missing} orphan(s)" if u_missing else "")

    s_missing = scalar(conn, """
        SELECT COUNT(*) FROM bronze.webhook_events b
        WHERE b.routed_to_silver = true
          AND b.event_type LIKE %s
          AND NOT EXISTS (
            SELECT 1 FROM silver.sessions s
            WHERE s.attendance_id = COALESCE(
              NULLIF((b.raw_payload->'payload'->>'attendance_id'), '')::bigint,
              NULLIF((b.raw_payload->'data'->>'attendance_id'), '')::bigint
            )
          )
    """, ('session.%',))
    log_result("1c. silver.sessions: all routed Bronze events have Silver rows",
               s_missing == 0, f"{s_missing} orphan(s)" if s_missing else "")

    id_checks = [
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
    for tbl, col in id_checks:
        n = scalar(conn, f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL")
        log_result(f"1d. {tbl}.{col} has no NULLs",
                   n == 0, f"{n} NULL(s)" if n else "")

    conn.close()


# ── TEST 2 — Real payload field mapping verification ──────────────────────────

def test2_field_mapping():
    section("TEST 2 — Real Payload Field Mapping Verification")
    conn = db_connect()

    checks = [
        dict(
            label       = "user.user_created",
            event_type  = "user.user_created",
            silver_tbl  = "silver.users",
            silver_key  = "user_id",
            key_path    = ["payload", "user_id"],
            mappings    = [
                (["payload", "name"],           "full_name",  str),
                (["payload", "email"],          "email",      str),
                (["payload", "user_id"],        "user_id",    int),
                (["payload", "user_name"],      "user_name",  str),
            ],
        ),
        dict(
            label       = "user.user_updated (user.name)",
            event_type  = "user.user_updated",
            silver_tbl  = "silver.users",
            silver_key  = "user_id",
            key_path    = ["payload", "user", "user_id"],
            mappings    = [
                (["payload", "user", "name"],   "full_name",  str),
                (["payload", "user", "email"],  "email",      str),
                (["payload", "user", "user_id"],"user_id",    int),
            ],
        ),
        dict(
            label       = "transaction.user_purchase_completed",
            event_type  = "transaction.user_purchase_completed",
            silver_tbl  = "silver.transactions",
            silver_key  = "event_id",
            key_path    = None,
            mappings    = [
                (["payload", "name"],           "full_name",  str),
                (["payload", "email"],          "email",      str),
                (["payload", "user_id"],        "user_id",    int),
                (["payload", "bundle_id"],      "bundle_id",  int),
                (["payload", "course_name"],    "course_name",str),
            ],
        ),
        dict(
            label       = "session.session_start (taken_at → actual_start_ist)",
            event_type  = "session.session_start",
            silver_tbl  = "silver.sessions",
            silver_key  = "attendance_id",
            key_path    = ["payload", "attendance_id"],
            mappings    = [
                (["payload", "class_name"],     "class_name",   str),
                (["payload", "taken_by_name"],  "teacher_name", str),
                (["payload", "attendance_id"],  "attendance_id",int),
            ],
        ),
        dict(
            label       = "assessments.exercise_submitted (test_date → submitted_at_ist)",
            event_type  = "assessments.exercise_submitted",
            silver_tbl  = "silver.assessments",
            silver_key  = "event_id",
            key_path    = None,
            mappings    = [
                (["payload", "user_id"],        "user_id",    int),
                (["payload", "attempt_id"],     "attempt_id", int),
                (["payload", "mark"],           "mark",       float),
                (["payload", "is_evaluated"],   "is_evaluated",int),
            ],
        ),
        dict(
            label       = "course.user_course_completed",
            event_type  = "course.user_course_completed",
            silver_tbl  = "silver.courses",
            silver_key  = "event_id",
            key_path    = None,
            mappings    = [
                (["payload", "user_id"],        "user_id",    int),
                (["payload", "bundle_id"],      "bundle_id",  int),
            ],
        ),
    ]

    print(f"\n  {'event_type (label)':<48} {'payload_field':<22} {'silver_col':<20} result")
    print(f"  {'-'*48} {'-'*22} {'-'*20} {'-'*6}")

    all_pass = True

    for chk in checks:
        rows = q(conn, """
            SELECT event_id, raw_payload
            FROM   bronze.webhook_events
            WHERE  event_type = %s AND raw_payload ? 'event'
            ORDER  BY received_at DESC LIMIT 5
        """, (chk['event_type'],))

        if not rows:
            print(f"  [WARN]  {chk['label']:<46} -- no real events in Bronze, skipping")
            continue

        for bronze_row in rows:
            eid     = bronze_row['event_id']
            payload = bronze_row['raw_payload']

            if chk['silver_key'] == 'event_id':
                silver_rows = q(conn,
                    f"SELECT * FROM {chk['silver_tbl']} WHERE event_id = %s", (eid,))
            else:
                kval = payload
                for k in chk['key_path']:
                    kval = kval.get(k) if isinstance(kval, dict) else None
                if kval is None:
                    continue
                silver_rows = q(conn,
                    f"SELECT * FROM {chk['silver_tbl']} WHERE {chk['silver_key']} = %s",
                    (kval,))

            if not silver_rows:
                continue
            silver = silver_rows[0]

            for path, col, cast in chk['mappings']:
                pval = payload
                for k in path:
                    pval = pval.get(k) if isinstance(pval, dict) else None

                sval = silver.get(col)

                try:
                    pval_cast = cast(pval) if pval is not None else None
                    sval_cast = cast(sval) if sval is not None else None
                    match = (pval_cast == sval_cast)
                except (ValueError, TypeError):
                    match = str(pval) == str(sval)

                ok_flag = match
                if not match:
                    all_pass = False
                field_path = ".".join(path[1:])
                icon = "ok" if ok_flag else "MISMATCH"
                print(f"  [{icon}]  {chk['label']:<44} {field_path:<22} {col:<20} "
                      f"pval={pval_cast!r} sval={sval_cast!r}")
            break

    log_result("2. All sampled real payload fields correctly mapped to Silver", all_pass)
    conn.close()


# ── TEST 3 — Duplicate protection ─────────────────────────────────────────────

def test3_duplicate_protection():
    section("TEST 3 — Duplicate Protection")
    conn  = db_connect()
    uid   = 99999901
    eid   = f"dupe-test-{uuid.uuid4().hex[:8]}"
    event = make_flat_event('user.user_created', {
        'user_id': uid, 'full_name': 'Dupe Test', 'email': 'dupe@test.com',
        'user_name': 'dupetest', 'user_role': 'student',
        'created_at': int(time.time()), 'contact_number': None, 'institution_id': 483,
    }, event_id=eid)

    r1 = post(event)
    r2 = post(event)
    time.sleep(0.5)

    b_count = scalar(conn, "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = %s", (eid,))
    s_count = scalar(conn, "SELECT COUNT(*) FROM silver.users WHERE user_id = %s", (uid,))

    log_result("3a. Both duplicate requests returned HTTP 200",
               r1.status_code == 200 and r2.status_code == 200,
               f"r1={r1.status_code} r2={r2.status_code}")
    log_result("3b. Bronze has exactly 1 row for the duplicate event_id",
               b_count == 1, f"found {b_count}")
    log_result("3c. Silver has exactly 1 row for the duplicate user_id",
               s_count == 1, f"found {s_count}")

    cleanup(conn, [eid], user_ids=[uid])
    conn.close()


# ── TEST 4 — Failed event recovery test ───────────────────────────────────────

def test4_failed_event_recovery():
    section("TEST 4 — Failed Event Recovery Test")
    conn = db_connect()

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type)
            VALUES ('e2e-test failure', '{"test":true}', 'application/json')
            RETURNING id
        """)
        fake_id = cur.fetchone()[0]

    time.sleep(0.2)
    resp = requests.get(f"{SERVER_URL}/failed", timeout=5)
    data = resp.json()
    ids_in_resp = [r.get('id') for r in data.get('last_10_failed', [])]

    log_result("4a. GET /failed returns HTTP 200", resp.status_code == 200)
    log_result("4b. Fake failed event visible in /failed response",
               fake_id in ids_in_resp,
               f"fake_id={fake_id}, returned ids={ids_in_resp}")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.failed_events WHERE id = %s", (fake_id,))

    resp2 = requests.get(f"{SERVER_URL}/failed", timeout=5)
    ids_after = [r.get('id') for r in resp2.json().get('last_10_failed', [])]
    log_result("4c. After deletion, fake row no longer in /failed",
               fake_id not in ids_after, f"fake_id={fake_id} still in {ids_after}")

    conn.close()


# ── TEST 5 — Server resilience test ──────────────────────────────────────────

def test5_server_resilience():
    section("TEST 5 — Server Resilience Test")
    conn = db_connect()

    port = 5000
    try:
        netstat = subprocess.run(
            ['netstat', '-ano'], capture_output=True, text=True, shell=True
        )
        old_pid = None
        for line in netstat.stdout.splitlines():
            if f':{port}' in line and 'LISTENING' in line:
                old_pid = int(line.split()[-1])
                break
    except Exception:
        old_pid = None

    if old_pid:
        subprocess.run(['taskkill', '/F', '/PID', str(old_pid)],
                       capture_output=True, shell=True)
        time.sleep(1.5)
        print(f"    Stopped old server (PID {old_pid})")
    else:
        print(f"    Could not locate running server PID — continuing anyway")

    proc = subprocess.Popen(
        [sys.executable, os.path.join(BASE_DIR, 'ingestion', 'webhook_receiver.py')],
        cwd=BASE_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    came_up = False
    for _ in range(12):
        time.sleep(1)
        try:
            if requests.get(f"{SERVER_URL}/health", timeout=2).status_code == 200:
                came_up = True
                break
        except requests.exceptions.ConnectionError:
            pass

    log_result("5a. Server restarted and /health returned 200",
               came_up, "Never came up within 12 s" if not came_up else "")

    if not came_up:
        conn.close()
        return

    uid = 99999902
    eid = f"resilience-{uuid.uuid4().hex[:8]}"
    event = make_flat_event('user.user_created', {
        'user_id': uid, 'full_name': 'Resilience Test', 'email': 'resilience@test.com',
        'user_name': 'restest', 'user_role': 'student',
        'created_at': int(time.time()), 'contact_number': None, 'institution_id': 483,
    }, event_id=eid)

    resp = post(event)
    time.sleep(0.5)

    b = scalar(conn, "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = %s", (eid,))
    s = scalar(conn, "SELECT COUNT(*) FROM silver.users WHERE user_id = %s", (uid,))

    log_result("5b. Post-restart event returned HTTP 200", resp.status_code == 200)
    log_result("5c. Post-restart event stored in Bronze", b == 1, f"found {b}")
    log_result("5d. Post-restart event routed to Silver", s == 1, f"found {s}")

    cleanup(conn, [eid], user_ids=[uid])
    conn.close()


# ── TEST 6 — IST timestamp verification ──────────────────────────────────────

def test6_ist_timestamps():
    section("TEST 6 — IST Timestamp Verification")
    conn = db_connect()

    checks = [
        ("silver.users",        ["created_at_ist",       "received_at"]),
        ("silver.transactions", ["event_timestamp_ist",   "inserted_at"]),
        ("silver.sessions",     ["scheduled_start_ist",   "received_at"]),
        ("silver.assessments",  ["submitted_at_ist",      "received_at"]),
        ("silver.courses",      ["completed_at_ist",      "received_at"]),
    ]

    bad_rows = []
    samples  = []

    for tbl, cols in checks:
        for col in cols:
            rows = q(conn, f"""
                SELECT {col}::text AS ts_text, {col} AS ts_obj
                FROM   {tbl}
                WHERE  {col} IS NOT NULL
                LIMIT  5
            """)
            for row in rows:
                ts_text = row['ts_text']
                ts_obj  = row['ts_obj']
                samples.append(f"{tbl}.{col}: {ts_text}")

                if '+05:30' not in (ts_text or ''):
                    bad_rows.append(f"{tbl}.{col} — text={ts_text!r}")
                    continue

                if hasattr(ts_obj, 'tzinfo') and ts_obj.tzinfo is None:
                    bad_rows.append(f"{tbl}.{col} — timezone-naive Python datetime")

    if samples:
        print(f"\n  Sample timestamps (expect +05:30 offset):")
        for s in samples[:6]:
            print(f"    {s}")

    log_result("6. All Silver timestamps have IST (+05:30) offset",
               len(bad_rows) == 0,
               "\n".join(bad_rows) if bad_rows else "")

    conn.close()


# ── TEST 7 — NULL audit ────────────────────────────────────────────────────────

def test7_null_audit():
    section("TEST 7 — NULL Audit (≤20 % NULL threshold on real events)")
    conn = db_connect()

    checks = [
        ("silver.users.full_name", """
            SELECT COUNT(*) FILTER (WHERE u.full_name IS NULL),
                   COUNT(*)
            FROM   silver.users u
        """, 20.0),
        ("silver.users.email", """
            SELECT COUNT(*) FILTER (WHERE u.email IS NULL),
                   COUNT(*)
            FROM   silver.users u
        """, 5.0),
        ("silver.transactions.full_name (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.full_name IS NULL),
                   COUNT(*)
            FROM   silver.transactions t
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.transactions.email (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.email IS NULL),
                   COUNT(*)
            FROM   silver.transactions t
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.transactions.bundle_id (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.bundle_id IS NULL),
                   COUNT(*)
            FROM   silver.transactions t
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.transactions.course_name (real only)", """
            SELECT COUNT(*) FILTER (WHERE t.course_name IS NULL),
                   COUNT(*)
            FROM   silver.transactions t
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.sessions.class_name", """
            SELECT COUNT(*) FILTER (WHERE class_name IS NULL),
                   COUNT(*)
            FROM   silver.sessions
        """, 10.0),
        ("silver.sessions.teacher_name", """
            SELECT COUNT(*) FILTER (WHERE teacher_name IS NULL),
                   COUNT(*)
            FROM   silver.sessions
        """, 20.0),
        ("silver.assessments.submitted_at_ist (real only)", """
            SELECT COUNT(*) FILTER (WHERE a.submitted_at_ist IS NULL),
                   COUNT(*)
            FROM   silver.assessments a
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.courses.completed_at_ist (real only)", """
            SELECT COUNT(*) FILTER (WHERE c.completed_at_ist IS NULL),
                   COUNT(*)
            FROM   silver.courses c
            JOIN   bronze.webhook_events b USING (event_id)
            WHERE  b.is_live_mode = true
        """, 5.0),
        ("silver.certificates.certificate_id", """
            SELECT COUNT(*) FILTER (WHERE certificate_id IS NULL),
                   COUNT(*)
            FROM   silver.certificates
        """, 5.0),
    ]

    print(f"\n  {'column/table':<50} {'nulls':>5} {'total':>6} {'pct':>6}  limit  status")
    print(f"  {'-'*50} {'-'*5} {'-'*6} {'-'*6}  {'-'*5}  {'-'*6}")

    all_pass = True
    for label, sql, max_pct in checks:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        nulls, total = row[0], row[1]
        pct   = (nulls / total * 100) if total > 0 else 0.0
        ok    = pct <= max_pct
        if not ok:
            all_pass = False
        status = "OK" if ok else f"OVER ({pct:.1f}%)"
        icon   = "ok" if ok else "FAIL"
        print(f"  [{icon}]  {label:<46} {nulls:>5} {total:>6} {pct:>5.1f}%  {max_pct:>4.0f}%  {status}")

    log_result("7. All columns within NULL % threshold", all_pass)
    conn.close()


# ── TEST 8 — Concurrent load test ────────────────────────────────────────────

def test8_concurrent_load():
    section("TEST 8 — Concurrent Load Test (20 simultaneous events)")
    conn = db_connect()
    N    = 20

    event_ids = [f"conc-{uuid.uuid4().hex[:10]}" for _ in range(N)]
    user_ids  = [99997000 + i for i in range(N)]
    responses: list[int] = []
    lock       = threading.Lock()

    def send_one(i: int):
        payload = make_flat_event('user.user_created', {
            'user_id':        user_ids[i],
            'full_name':      f'Concurrent User {i}',
            'email':          f'concurrent_{i}@test.com',
            'user_name':      f'concuser{i}',
            'user_role':      'student',
            'created_at':     int(time.time()),
            'contact_number': None,
            'institution_id': 483,
        }, event_id=event_ids[i])
        try:
            r = requests.post(WEBHOOK_URL, json=payload, timeout=20)
            with lock:
                responses.append(r.status_code)
        except Exception:
            with lock:
                responses.append(0)

    threads = [threading.Thread(target=send_one, args=(i,)) for i in range(N)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0

    time.sleep(1.0)

    all_200 = all(r == 200 for r in responses)
    bronze_n = scalar(conn,
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE event_id = ANY(%s)",
        (event_ids,))
    silver_n = scalar(conn,
        "SELECT COUNT(*) FROM silver.users WHERE user_id = ANY(%s)",
        (user_ids,))

    non_200 = [r for r in responses if r != 200]
    log_result("8a. All 20 concurrent requests returned HTTP 200",
               all_200, f"non-200 responses: {non_200}" if non_200 else "")
    log_result("8b. All 20 events stored in Bronze",
               bronze_n == N, f"Bronze count={bronze_n}, expected {N}")
    log_result("8c. All 20 events routed to Silver",
               silver_n == N, f"Silver count={silver_n}, expected {N}")
    print(f"    Wall-clock time for 20 concurrent requests: {elapsed:.2f}s")

    cleanup(conn, event_ids, user_ids=user_ids)
    conn.close()


# ── TEST 9 — DB constraint violation handling ─────────────────────────────────

def test9_constraint_violation():
    section("TEST 9 — DB Constraint Violation Handling")
    conn = db_connect()

    eid = f"constraint-{uuid.uuid4().hex[:8]}"

    # attendance_id deliberately omitted — silver.sessions has it NOT NULL
    event = make_flat_event('session.session_created', {
        'class_id':   999901,
        'class_name': 'Constraint Test Class',
        'gmt_start_time': int(time.time()),
        'gmt_end_time':   int(time.time()) + 3600,
    }, event_id=eid)

    resp = post(event)
    time.sleep(0.5)

    log_result("9a. Server returns HTTP 200 despite Silver constraint violation",
               resp.status_code == 200, f"status={resp.status_code}")

    bronze_rows = q(conn,
        "SELECT routed_to_silver FROM bronze.webhook_events WHERE event_id = %s", (eid,))
    log_result("9b. Event is preserved in Bronze",
               len(bronze_rows) == 1, f"Bronze rows={len(bronze_rows)}")

    if bronze_rows:
        routed = bronze_rows[0]['routed_to_silver']
        log_result("9c. Bronze.routed_to_silver = false (Silver rolled back correctly)",
                   routed is False, f"routed_to_silver={routed}")

    silver_n = scalar(conn,
        "SELECT COUNT(*) FROM silver.sessions WHERE event_id = %s", (eid,))
    log_result("9d. Malformed event NOT written to Silver",
               silver_n == 0, f"Silver rows={silver_n}")

    cleanup(conn, [eid])
    conn.close()


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*62)
    print("  Edmingle Webhook Pipeline -- End-to-End Test Suite")
    print("="*62)

    try:
        r = requests.get(f"{SERVER_URL}/health", timeout=5)
        if r.status_code != 200:
            print(f"\n  {FAIL} Server not healthy at {SERVER_URL} — aborting")
            sys.exit(1)
        print(f"\n  Server healthy at {SERVER_URL}")
    except requests.exceptions.ConnectionError:
        print(f"\n  {FAIL} Cannot connect to {SERVER_URL} — is the server running?")
        sys.exit(1)

    test1_data_integrity()
    test2_field_mapping()
    test3_duplicate_protection()
    test4_failed_event_recovery()
    test5_server_resilience()
    test6_ist_timestamps()
    test7_null_audit()
    test8_concurrent_load()
    test9_constraint_violation()

    total  = len(_results)
    passed = sum(1 for v in _results.values() if v)
    failed = total - passed

    print(f"\n{'='*62}")
    print(f"  FINAL RESULTS: {passed}/{total} passed, {failed} failed")
    print(f"{'='*62}")
    for name, ok in _results.items():
        print(f"  [{'PASS' if ok else 'FAIL'}]  {name}")
    print()

    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
