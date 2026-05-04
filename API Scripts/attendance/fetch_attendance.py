# ============================================================
# 01 — FETCH DAILY ATTENDANCE
# ============================================================
# What it does: Downloads student attendance data from the
#               Edmingle API for one or more dates, saves the
#               raw data to Bronze, and builds a summary in
#               Silver (one row per batch per class day).
#
# Why we need it: We cannot see attendance in Edmingle without
#                 pulling it from the API. This script automates
#                 the daily download so Power BI stays current.
#
# How to run:
#   Pull yesterday:
#     python 01_fetch_attendance/fetch_attendance.py
#
#   Pull a specific date:
#     python 01_fetch_attendance/fetch_attendance.py --date 2026-03-16
#
#   Backfill a range:
#     python 01_fetch_attendance/fetch_attendance.py --start 2024-01-01 --end 2024-12-31
#
#   Preview only (no database writes):
#     python 01_fetch_attendance/fetch_attendance.py --date 2026-03-16 --dry-run
#
# The API (report_type=55) returns student attendance for one date.
# P = Present, A = Absent, L = Late, - = Not yet marked
# Rate limit: maximum 25 API calls per minute.
# ============================================================

import argparse
import datetime
import time

import psycopg2
import psycopg2.extras
import requests

# ── DATABASE AND API SETTINGS ─────────────────────────────────
# Update these only if credentials or URLs change.
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
API_KEY     = "590605228a847624e065a76e986803fa"
ORG_ID      = 683
INST_ID     = 483
API_BASE    = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# ─────────────────────────────────────────────────────────────

# The full URL for the attendance report endpoint
REPORT_URL = API_BASE + "/report/csv"

# Headers sent with every API request — apikey authenticates us
API_HEADERS = {"apikey": API_KEY, "ORGID": str(ORG_ID)}

# Stop and wait after this many calls per minute (rate limit)
RATE_LIMIT_CALLS = 25

# How many seconds to pause when we reach the rate limit
RATE_LIMIT_SLEEP = 60

# India Standard Time offset: UTC + 5 hours 30 minutes
IST_OFFSET = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


# ── SQL CONSTANTS ─────────────────────────────────────────────
# SQL is defined here (not inside functions) so functions stay short.

SQL_INSERT_BRONZE = """
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
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (student_id, class_id) DO UPDATE SET
        pull_date                   = EXCLUDED.pull_date,
        attendance_status           = EXCLUDED.attendance_status,
        teacher_class_signin_status = EXCLUDED.teacher_class_signin_status,
        student_rating              = EXCLUDED.student_rating,
        student_comments            = EXCLUDED.student_comments,
        loaded_at                   = NOW()
"""

SQL_UPSERT_SILVER = """
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
"""

SQL_UPDATE_CLASS_NUMBERS = """
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
"""


# ── TYPE CONVERSION HELPERS ───────────────────────────────────

def convert_to_int(value):
    # Return None if there is nothing to convert
    if value is None:
        return None
    # Try to convert the text value to an integer number
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        # Conversion failed (e.g. empty string or invalid text) — return None
        return None


def parse_class_date(value):
    # Return None if there is no date value provided
    if not value:
        return None
    date_text = str(value).strip()
    # Try each date format that the API might return
    for date_format in ("%d %b %Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(date_text, date_format).date()
        except ValueError:
            # This format did not match — try the next one
            continue
    # None of the date formats matched
    return None


# ── API CALL HELPERS ──────────────────────────────────────────

def build_api_params(date_string):
    # Convert the date string "YYYY-MM-DD" into a datetime with IST timezone
    day = datetime.datetime.strptime(date_string, "%Y-%m-%d").replace(tzinfo=IST_OFFSET)
    # The API needs Unix timestamps (seconds since 1970-01-01)
    # We want the whole day: midnight (00:00:00) to end of day (23:59:59)
    start_unix = int(day.replace(hour=0, minute=0, second=0).timestamp())
    end_unix = int(day.replace(hour=23, minute=59, second=59).timestamp())
    # Build the dictionary of URL query parameters
    return {
        "report_type":     55,       # 55 = student attendance report type
        "organization_id": ORG_ID,
        "start_time":      start_unix,
        "end_time":        end_unix,
        "response_type":   1,        # 1 = return JSON format (not CSV)
    }


def extract_rows_from_response(api_response):
    # Parse the JSON from the response body
    response_data = api_response.json()
    # The API may put rows under different key names — try each one
    rows = response_data.get("data")
    if rows is None:
        rows = response_data.get("rows")
    if rows is None:
        rows = response_data.get("report")
    if rows is None:
        # No rows found under any key — return empty list
        rows = []
    return rows


def fetch_attendance_for_date(date_string, dry_run=False):
    # Build the API parameters for this date
    api_params = build_api_params(date_string)
    last_error = None

    # Try up to 3 times in case of temporary network errors
    for attempt_number in range(3):
        try:
            # Call the API with a 60-second timeout
            response = requests.get(REPORT_URL, headers=API_HEADERS, params=api_params, timeout=60)

            # 401 means our API key has expired — no point retrying
            if response.status_code == 401:
                print("  [" + date_string + "] 401 Unauthorized — API key may have expired")
                return []

            # A non-200 response means the server had an error
            if response.status_code != 200:
                last_error = "HTTP " + str(response.status_code)
                if attempt_number < 2:
                    # Wait 10 seconds before the next attempt
                    time.sleep(10)
                continue

            # Success — extract the attendance rows from the response
            rows = extract_rows_from_response(response)
            last_error = None
            break

        except Exception as error:
            last_error = error
            if attempt_number < 2:
                time.sleep(10)
    else:
        # All 3 attempts failed — report failure and return None
        print("  [" + date_string + "] All retries failed: " + str(last_error))
        return None

    # In dry-run mode, just print a preview without saving anything
    if dry_run:
        print("  [" + date_string + "] Rows from API: " + str(len(rows)))
        if rows:
            first_row = rows[0]
            print("    First student: " + str(first_row.get("studentName")) +
                  "  status=" + str(first_row.get("studentAttendanceStatus")))
    return rows


# ── BRONZE HELPERS ────────────────────────────────────────────

def is_staff_member(email):
    # Staff members have @vyoma in their email address — we skip them
    if email is None:
        return False
    return "@vyoma" in str(email).lower()


def get_email_from_row(row):
    # The API uses different field names in different contexts
    email = row.get("studentEmail")
    if email is None:
        email = row.get("student_email")
    return email


def build_bronze_params(row, pull_date, email):
    # Extract all the ID fields, trying both camelCase and snake_case names
    student_id    = convert_to_int(row.get("student_Id") or row.get("student_id"))
    batch_id      = convert_to_int(row.get("batch_Id") or row.get("batch_id"))
    class_id      = convert_to_int(row.get("class_Id") or row.get("class_id"))
    bundle_id     = convert_to_int(row.get("bundle_Id") or row.get("bundle_id"))
    course_id     = convert_to_int(row.get("course_Id") or row.get("course_id"))
    teacher_id    = convert_to_int(row.get("teacher_Id") or row.get("teacher_id"))
    attendance_id = convert_to_int(row.get("attendance_id"))
    student_rating = convert_to_int(row.get("studentRating"))
    # Parse the class date text (e.g. "16 Mar 2026") into a Python date object
    class_date_parsed = parse_class_date(row.get("classDate"))
    # Return values in the exact order the SQL INSERT expects them
    return (
        pull_date,
        student_id, row.get("studentName"), row.get("regNo"),
        email, row.get("studentContact"), row.get("studentBatchStatus"),
        batch_id, row.get("batchName"),
        class_id, row.get("className"),
        bundle_id, row.get("bundleName"),
        course_id, row.get("courseName"),
        attendance_id, row.get("sessionName"),
        teacher_id, row.get("teacherName"), row.get("teacherEmail"),
        row.get("teacherClassSigninStatus"),
        row.get("studentAttendanceStatus"),
        row.get("classDate"), class_date_parsed,
        row.get("startTime"), row.get("endTime"), row.get("classDuration"),
        student_rating, row.get("studentComments"),
        psycopg2.extras.Json(row),
    )


def save_to_bronze(conn, rows, pull_date, dry_run=False):
    # Skip database work completely in dry-run mode
    if not rows or dry_run:
        return 0, 0
    inserted_count = 0
    staff_skipped = 0
    cursor = conn.cursor()

    # Process each attendance row from the API one at a time
    for row in rows:
        email = get_email_from_row(row)

        # Skip staff members — they are not students
        if is_staff_member(email):
            staff_skipped = staff_skipped + 1
            continue

        # Build the parameter tuple and save this row to Bronze
        params = build_bronze_params(row, pull_date, email)
        cursor.execute(SQL_INSERT_BRONZE, params)
        if cursor.rowcount > 0:
            inserted_count = inserted_count + 1

    # Save all the inserts to the database at once
    conn.commit()
    cursor.close()
    return inserted_count, staff_skipped


# ── SILVER AGGREGATE ─────────────────────────────────────────

def aggregate_to_silver(conn, pull_date, dry_run=False):
    # Skip in dry-run mode — no database changes
    if dry_run:
        return 0
    cursor = conn.cursor()

    # Run the SQL that groups Bronze rows into Silver daily summaries
    # This counts Present/Late/Absent per batch per day
    cursor.execute(SQL_UPSERT_SILVER, {"pull_date": pull_date})
    silver_rows_updated = cursor.rowcount

    # Renumber all classes in order (1st class, 2nd class, 3rd...)
    # so Power BI can show "Class 1 attendance", "Class 2 attendance", etc.
    cursor.execute(SQL_UPDATE_CLASS_NUMBERS, {"pull_date": pull_date})

    # Commit both the Silver insert and the class numbering update
    conn.commit()
    cursor.close()
    return silver_rows_updated


# ── DATE RANGE HELPER ─────────────────────────────────────────

def build_date_list(start_date, end_date):
    # Build a list of every date from start_date to end_date (inclusive)
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        # Move to the next day
        current_date = current_date + datetime.timedelta(days=1)
    return date_list


# ── ARGUMENT PARSING ─────────────────────────────────────────

def parse_args():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Fetch student attendance from the Edmingle API"
    )
    # --date and --start are mutually exclusive (you can only use one)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date",  metavar="YYYY-MM-DD", help="Pull one specific date")
    group.add_argument("--start", metavar="YYYY-MM-DD", help="Backfill start date")
    parser.add_argument("--end",  metavar="YYYY-MM-DD", help="Backfill end date (default: yesterday)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only — no database writes")
    return parser.parse_args()


def decide_dates_to_pull(args):
    # Yesterday is the default when no date is specified
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    # A single specific date was requested
    if args.date:
        return [datetime.date.fromisoformat(args.date)]
    # A date range was requested
    if args.start:
        start_date = datetime.date.fromisoformat(args.start)
        if args.end:
            end_date = datetime.date.fromisoformat(args.end)
        else:
            end_date = yesterday
        return build_date_list(start_date, end_date)
    # Default: just yesterday
    return [yesterday]


# ── SUMMARY PRINTER ───────────────────────────────────────────

def print_final_summary(dates, total_bronze, total_silver, total_staff, failed_dates, conn):
    # Look up the final distinct batch and bundle counts in Silver
    distinct_batches = 0
    distinct_bundles = 0
    if conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(DISTINCT batch_id), COUNT(DISTINCT bundle_id) FROM silver.class_attendance"
        )
        result = cursor.fetchone()
        distinct_batches = result[0]
        distinct_bundles = result[1]
        cursor.close()
    # Print the results
    print("")
    print("  ATTENDANCE PULL COMPLETE")
    print("  " + "-" * 44)
    print("  Dates pulled      : " + str(len(dates) - len(failed_dates)))
    if failed_dates:
        print("  Dates failed      : " + str(len(failed_dates)))
        for failed_date in failed_dates:
            print("    " + failed_date)
    else:
        print("  Dates failed      : 0")
    print("  Bronze rows added : " + str(total_bronze))
    print("  Silver rows added : " + str(total_silver))
    print("  Batches covered   : " + str(distinct_batches))
    print("  Bundles covered   : " + str(distinct_bundles))
    if dates:
        print("  Date range        : " + str(dates[0]) + " -> " + str(dates[-1]))
    print("  Staff rows skipped: " + str(total_staff) + " (@vyoma emails)")
    print("  " + "-" * 44)


# ── MAIN ─────────────────────────────────────────────────────

def main():
    args    = parse_args()
    dry_run = args.dry_run
    dates   = decide_dates_to_pull(args)

    print("=" * 50)
    print("ATTENDANCE PIPELINE  (report_type=55)")
    if dry_run:
        print("*** DRY RUN — no database writes ***")
    print("=" * 50)
    print("Dates to pull: " + str(len(dates)))
    if len(dates) <= 5:
        for d in dates:
            print("  " + str(d))
    else:
        print("  " + str(dates[0]) + "  ..  " + str(dates[-1]))
    print("")

    # Connect to the database (skip in dry-run mode)
    conn = None
    if not dry_run:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
            )
        except Exception as error:
            print("ERROR: Could not connect to database: " + str(error))
            return

    total_bronze   = 0
    total_silver   = 0
    total_staff    = 0
    failed_dates   = []
    api_call_count = 0

    # Process each date one by one
    for i, current_date in enumerate(dates):
        date_string = current_date.isoformat()

        # Pause if we have made too many API calls (rate limiting)
        if api_call_count > 0 and api_call_count % RATE_LIMIT_CALLS == 0:
            print("  Rate limit: sleeping " + str(RATE_LIMIT_SLEEP) + "s ...")
            time.sleep(RATE_LIMIT_SLEEP)

        # Call the API to get attendance rows for this date
        rows = fetch_attendance_for_date(date_string, dry_run=dry_run)
        api_call_count = api_call_count + 1

        # If the API returned None, all retries failed
        if rows is None:
            failed_dates.append(date_string)
            continue

        # Save the data and compute Silver aggregates
        if not dry_run:
            bronze_added, staff_count = save_to_bronze(conn, rows, current_date)
            silver_added = aggregate_to_silver(conn, current_date)
        else:
            bronze_added = len(rows)
            staff_count  = 0
            for row in rows:
                row_email = row.get("studentEmail", "")
                if row_email and "@vyoma" in str(row_email).lower():
                    staff_count = staff_count + 1
            silver_added = 0

        total_bronze = total_bronze + bronze_added
        total_silver = total_silver + silver_added
        total_staff  = total_staff + staff_count

        # Print progress every 10 dates or for small pulls
        if dry_run or (i + 1) % 10 == 0 or len(dates) <= 5:
            print("  [" + date_string + "] API rows=" + str(len(rows)) +
                  " | Bronze +" + str(bronze_added) +
                  " | Silver +" + str(silver_added))

    # Print the final summary
    print_final_summary(dates, total_bronze, total_silver, total_staff, failed_dates, conn)

    # Close the database connection
    if conn:
        conn.close()


if __name__ == "__main__":
    main()
