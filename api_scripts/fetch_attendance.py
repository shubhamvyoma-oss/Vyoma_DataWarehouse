# ATTENDANCE PIPELINE -- report_type=55
# Pulls all-student attendance for ONE date per call
# API: GET /report/csv with Unix start_time/end_time + response_type=1 (JSON)
# studentAttendanceStatus: P=Present  A=Absent  -=not yet marked
#
# Run manually:  python api_scripts/fetch_attendance.py --date 2026-03-16
# Run backfill:  python api_scripts/fetch_attendance.py --start 2023-01-01 --end 2025-12-31
# Dry run:       python api_scripts/fetch_attendance.py --date 2026-03-16 --dry-run
# Rate limit:    25 calls/minute (sleep 60s after every 25 dates)

import argparse
import datetime
import sys
import time

import psycopg2
import psycopg2.extras
import requests

# -- CONFIG --------------------------------------------------
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
API_KEY     = "590605228a847624e065a76e986803fa"
ORG_ID      = 683
INST_ID     = 483
API_BASE    = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# ------------------------------------------------------------

HEADERS          = {"apikey": API_KEY, "ORGID": str(ORG_ID)}
REPORT_URL       = f"{API_BASE}/report/csv"
RATE_LIMIT_CALLS = 25
RATE_LIMIT_SLEEP = 60
IST              = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


# -- API CALL ------------------------------------------------

def fetch_attendance_for_date(date_str, dry_run=False):
    """Call report_type=55 for one date. Returns list of row dicts."""
    day   = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
    start = int(day.replace(hour=0,  minute=0,  second=0).timestamp())
    end   = int(day.replace(hour=23, minute=59, second=59).timestamp())

    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(
                REPORT_URL,
                headers=HEADERS,
                params={
                    "report_type":     55,
                    "organization_id": ORG_ID,
                    "start_time":      start,
                    "end_time":        end,
                    "response_type":   1,      # JSON response
                },
                timeout=60,
            )
            if r.status_code == 401:
                print(f"  [{date_str}] 401 Unauthorized — API key expired")
                return []
            if r.status_code != 200:
                last_exc = f"HTTP {r.status_code}: {r.text[:150]}"
                if attempt < 2:
                    time.sleep(10)
                continue
            rows = r.json().get("data") or r.json().get("rows") or r.json().get("report") or []
            last_exc = None
            break
        except Exception as e:
            last_exc = e
            if attempt < 2:
                time.sleep(10)
    else:
        print(f"  [{date_str}] All retries failed: {last_exc}")
        return None

    if dry_run:
        from collections import Counter
        statuses = Counter(row.get("studentAttendanceStatus") for row in rows)
        print(f"  Format       : JSON (report_type=55)")
        print(f"  Rows returned: {len(rows)}")
        print(f"  Statuses     : {dict(statuses)}")
        if rows:
            print(f"  Fields ({len(rows[0])}): {list(rows[0].keys())}")
            print(f"  First 5 rows:")
            for row in rows[:5]:
                print(f"    student={row.get('studentName')!r}  "
                      f"batch={row.get('batchName','')[:35]!r}  "
                      f"class_date={row.get('classDate')!r}  "
                      f"status={row.get('studentAttendanceStatus')!r}  "
                      f"email={row.get('studentEmail')!r}")

    return rows


# -- PARSE DATE ----------------------------------------------

def _parse_class_date(val):
    """Parse '16 Mar 2026' or standard formats to a date object."""
    if not val:
        return None
    s = str(val).strip()
    for fmt in ("%d %b %Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _int(val):
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


# -- SAVE TO BRONZE ------------------------------------------

def save_to_bronze(conn, rows, pull_date, dry_run=False):
    if not rows or dry_run:
        return 0, 0

    inserted   = 0
    staff_skip = 0
    cur        = conn.cursor()

    for row in rows:
        email = row.get("studentEmail") or row.get("student_email")
        if email and "@vyoma" in str(email).lower():
            staff_skip += 1
            continue

        cur.execute("""
            INSERT INTO bronze.attendance_raw (
                pull_date,
                student_id, student_name, reg_no,
                student_email, student_contact, student_batch_status,
                batch_id, batch_name,
                class_id, class_name,
                bundle_id, bundle_name,
                course_id, course_name,
                attendance_id, session_name,
                teacher_id, teacher_name, teacher_email,
                teacher_class_signin_status,
                attendance_status,
                class_date, class_date_parsed,
                start_time, end_time, class_duration,
                student_rating, student_comments,
                raw_payload
            ) VALUES (
                %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s,
                %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s
            )
            ON CONFLICT (student_id, class_id) DO UPDATE SET
                pull_date                   = EXCLUDED.pull_date,
                attendance_status           = EXCLUDED.attendance_status,
                teacher_class_signin_status = EXCLUDED.teacher_class_signin_status,
                student_rating              = EXCLUDED.student_rating,
                student_comments            = EXCLUDED.student_comments,
                loaded_at                   = NOW()
        """, (
            pull_date,
            _int(row.get("student_Id") or row.get("student_id")),
            row.get("studentName"),
            row.get("regNo"),
            email,
            row.get("studentContact"),
            row.get("studentBatchStatus"),
            _int(row.get("batch_Id") or row.get("batch_id")),
            row.get("batchName"),
            _int(row.get("class_Id") or row.get("class_id")),
            row.get("className"),
            _int(row.get("bundle_Id") or row.get("bundle_id")),
            row.get("bundleName"),
            _int(row.get("course_Id") or row.get("course_id")),
            row.get("courseName"),
            _int(row.get("attendance_id")),
            row.get("sessionName"),
            _int(row.get("teacher_Id") or row.get("teacher_id")),
            row.get("teacherName"),
            row.get("teacherEmail"),
            row.get("teacherClassSigninStatus"),
            row.get("studentAttendanceStatus"),
            row.get("classDate"),
            _parse_class_date(row.get("classDate")),
            row.get("startTime"),
            row.get("endTime"),
            row.get("classDuration"),
            _int(row.get("studentRating")),
            row.get("studentComments"),
            psycopg2.extras.Json(row),
        ))
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    cur.close()
    return inserted, staff_skip


# -- AGGREGATE TO SILVER -------------------------------------

def aggregate_to_silver(conn, pull_date, dry_run=False):
    """Group bronze rows by (batch_id, class_date_parsed) and upsert Silver summary.
    Counts P+L as present (Late counts as attended), A as absent.
    Excludes unmarked (-), On Leave (OL), Excused (E), Not Available (NA) from pct denominator.
    """
    if dry_run:
        return 0

    cur = conn.cursor()

    cur.execute("""
        INSERT INTO silver.class_attendance
            (batch_id, bundle_id, class_date, present_count, late_count,
             absent_count, total_enrolled, attendance_pct, pull_date)
        SELECT
            b.batch_id,
            b.bundle_id,
            b.class_date_parsed                                                AS class_date,
            COUNT(*) FILTER (WHERE b.attendance_status = 'P')                 AS present_count,
            COUNT(*) FILTER (WHERE b.attendance_status = 'L')                 AS late_count,
            COUNT(*) FILTER (WHERE b.attendance_status = 'A')                 AS absent_count,
            cb.admitted_students                                               AS total_enrolled,
            ROUND(
                COUNT(*) FILTER (WHERE b.attendance_status IN ('P','L'))
                * 100.0 / NULLIF(
                    COUNT(*) FILTER (WHERE b.attendance_status IN ('P','L','A')), 0
                ), 2
            )                                                                  AS attendance_pct,
            %(pull_date)s
        FROM bronze.attendance_raw b
        LEFT JOIN silver.course_batches cb ON b.batch_id = cb.batch_id
        WHERE b.pull_date = %(pull_date)s
          AND b.batch_id IS NOT NULL
          AND b.class_date_parsed IS NOT NULL
          AND b.attendance_status IN ('P', 'L', 'A')
        GROUP BY b.batch_id, b.bundle_id, b.class_date_parsed, cb.admitted_students
        ON CONFLICT (batch_id, class_date) DO UPDATE SET
            present_count  = EXCLUDED.present_count,
            late_count     = EXCLUDED.late_count,
            absent_count   = EXCLUDED.absent_count,
            attendance_pct = EXCLUDED.attendance_pct,
            pull_date      = EXCLUDED.pull_date
    """, {"pull_date": pull_date})

    silver_n = cur.rowcount

    # Recompute class_number for all affected batches
    cur.execute("""
        UPDATE silver.class_attendance ca
        SET class_number = sub.rn
        FROM (
            SELECT id,
                ROW_NUMBER() OVER (
                    PARTITION BY batch_id
                    ORDER BY class_date ASC
                ) AS rn
            FROM silver.class_attendance
            WHERE batch_id IN (
                SELECT DISTINCT batch_id FROM bronze.attendance_raw
                WHERE pull_date = %(pull_date)s
            )
        ) sub
        WHERE ca.id = sub.id
    """, {"pull_date": pull_date})

    conn.commit()
    cur.close()
    return silver_n


# -- MAIN ----------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Fetch report_type=55 attendance data")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date",  metavar="YYYY-MM-DD", help="Pull one specific date")
    g.add_argument("--start", metavar="YYYY-MM-DD", help="Backfill start date")
    p.add_argument("--end",   metavar="YYYY-MM-DD",
                   help="Backfill end date (default: yesterday)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print API response structure, no DB writes")
    return p.parse_args()


def date_range(start, end):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def main():
    args      = parse_args()
    dry_run   = args.dry_run
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    if args.date:
        dates = [datetime.date.fromisoformat(args.date)]
    elif args.start:
        start = datetime.date.fromisoformat(args.start)
        end   = datetime.date.fromisoformat(args.end) if args.end else yesterday
        dates = list(date_range(start, end))
    else:
        dates = [yesterday]

    print("=" * 50)
    print("ATTENDANCE PIPELINE  (report_type=55)")
    if dry_run:
        print("*** DRY RUN -- no DB writes ***")
    print("=" * 50)
    print(f"Dates to pull: {len(dates)}")
    if len(dates) <= 5:
        for d in dates:
            print(f"  {d}")
    else:
        print(f"  {dates[0]}  ..  {dates[-1]}")
    print()

    conn = None
    if not dry_run:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )

    total_bronze = 0
    total_silver = 0
    total_staff  = 0
    failed_dates = []
    call_count   = 0

    for i, d in enumerate(dates):
        date_str = d.isoformat()

        if call_count > 0 and call_count % RATE_LIMIT_CALLS == 0:
            print(f"  Rate limit: sleeping {RATE_LIMIT_SLEEP}s after {call_count} calls ...")
            time.sleep(RATE_LIMIT_SLEEP)

        rows = fetch_attendance_for_date(date_str, dry_run=dry_run)
        call_count += 1

        if rows is None:
            failed_dates.append(date_str)
            continue

        if not dry_run:
            bronze_n, staff_n = save_to_bronze(conn, rows, d)
            silver_n          = aggregate_to_silver(conn, d)
        else:
            bronze_n = len(rows)
            staff_n  = sum(1 for r in rows
                           if r.get("studentEmail") and "@vyoma" in r["studentEmail"].lower())
            silver_n = 0

        total_bronze += bronze_n
        total_silver += silver_n
        total_staff  += staff_n

        if dry_run or (i + 1) % 10 == 0 or len(dates) <= 5:
            print(f"  [{date_str}] API rows={len(rows)} | "
                  f"Bronze +{bronze_n} | Silver +{silver_n} | staff={staff_n}")

    if conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT batch_id), COUNT(DISTINCT bundle_id) "
                    "FROM silver.class_attendance")
        row = cur.fetchone()
        distinct_batches = row[0]
        distinct_bundles = row[1]
        cur.close()
        conn.close()
    else:
        distinct_batches = 0
        distinct_bundles = 0

    print()
    print("  ATTENDANCE PULL COMPLETE")
    print("  " + "-" * 44)
    print(f"  Dates pulled      : {len(dates) - len(failed_dates)}")
    if failed_dates:
        print(f"  Dates failed      : {len(failed_dates)}")
        for fd in failed_dates[:20]:
            print(f"    {fd}")
    else:
        print(f"  Dates failed      : 0")
    print(f"  Bronze rows added : {total_bronze}")
    print(f"  Silver rows added : {total_silver}")
    print(f"  Batches covered   : {distinct_batches}")
    print(f"  Bundles covered   : {distinct_bundles}")
    if dates:
        print(f"  Date range        : {dates[0]} -> {dates[-1]}")
    print(f"  Staff rows skipped: {total_staff} (@vyoma emails)")
    print("  " + "-" * 44)


if __name__ == "__main__":
    main()
