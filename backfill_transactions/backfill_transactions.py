# ============================================================
# 11 — BACKFILL TRANSACTIONS
# ============================================================
# What it does: Reads the two Bronze CSV tables that were loaded
#               by script 08 and creates Silver records from them.
#
#   Step 1: bronze.student_courses_enrolled_raw
#           → silver.transactions
#           (one enrollment row per student per course batch)
#
#   Step 2: bronze.studentexport_raw + email lookup
#           → silver.users
#           (one user row per unique student)
#           Students whose email is not found in the enrollment
#           table go to: bronze.unresolved_students_raw
#
# Why we need it: Before the live webhook server existed, all
#                 student and enrollment data was exported to CSV.
#                 This script back-fills that historical data into
#                 Silver so Power BI has a complete picture.
#                 Run ONCE after 08_load_students_csv.
#
# How to run:
#   python 11_backfill_transactions/backfill_transactions.py
#
# What to check after:
#   - silver.transactions should have rows with source = 'csv'
#   - silver.users should have rows with event_type = 'csv.import'
#   - bronze.unresolved_students_raw shows students with no user_id
#   - Run 12_check_db_counts to verify
# ============================================================

import sys
import datetime
import psycopg2
import psycopg2.extras


# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# ── SQL CONSTANTS ─────────────────────────────────────────────

SQL_UPSERT_TRANSACTION = """
    INSERT INTO silver.transactions (
        event_id, event_type,
        user_id, email, full_name,
        bundle_id, course_name,
        master_batch_id, master_batch_name, institution_bundle_id,
        start_date_ist, end_date_ist,
        source
    ) VALUES (
        %s, 'csv.import',
        %s, %s, %s,
        %s, %s,
        %s, %s, %s,
        unix_to_ist(%s), unix_to_ist(%s),
        'csv'
    )
    ON CONFLICT (user_id, bundle_id, master_batch_id) DO UPDATE SET
        email                 = COALESCE(silver.transactions.email,                 EXCLUDED.email),
        full_name             = COALESCE(silver.transactions.full_name,             EXCLUDED.full_name),
        course_name           = COALESCE(silver.transactions.course_name,           EXCLUDED.course_name),
        master_batch_name     = COALESCE(silver.transactions.master_batch_name,     EXCLUDED.master_batch_name),
        institution_bundle_id = COALESCE(silver.transactions.institution_bundle_id, EXCLUDED.institution_bundle_id),
        start_date_ist        = COALESCE(silver.transactions.start_date_ist,        EXCLUDED.start_date_ist),
        end_date_ist          = COALESCE(silver.transactions.end_date_ist,          EXCLUDED.end_date_ist)
"""

SQL_UPSERT_USER = """
    INSERT INTO silver.users (
        event_id, event_type, user_id,
        email, full_name, user_name, contact_number,
        city, state, address,
        parent_name, parent_email, parent_contact,
        created_at_ist, received_at
    ) VALUES (
        %s, 'csv.import', %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s, NOW() AT TIME ZONE 'Asia/Kolkata'
    )
    ON CONFLICT (user_id) DO UPDATE SET
        email          = COALESCE(silver.users.email,          EXCLUDED.email),
        full_name      = COALESCE(silver.users.full_name,      EXCLUDED.full_name),
        user_name      = COALESCE(silver.users.user_name,      EXCLUDED.user_name),
        contact_number = COALESCE(silver.users.contact_number, EXCLUDED.contact_number),
        city           = COALESCE(silver.users.city,           EXCLUDED.city),
        state          = COALESCE(silver.users.state,          EXCLUDED.state),
        address        = COALESCE(silver.users.address,        EXCLUDED.address),
        parent_name    = COALESCE(silver.users.parent_name,    EXCLUDED.parent_name),
        parent_email   = COALESCE(silver.users.parent_email,   EXCLUDED.parent_email),
        parent_contact = COALESCE(silver.users.parent_contact, EXCLUDED.parent_contact),
        created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist)
"""

SQL_INSERT_UNRESOLVED = """
    INSERT INTO bronze.unresolved_students_raw (source_row, email, raw_row)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING
"""


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def connect_to_database():
    # Open a connection to the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
        return conn
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        sys.exit(1)


def unix_or_null(value):
    # Convert a raw CSV value to an integer Unix timestamp, or None if missing
    # The enrollment CSVs use '0' to mean "no date set", so we treat it as NULL
    if value is None or str(value).strip() in ("", "0", "None"):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_date_created(value):
    # Parse "M/D/YYYY HH:MM" date strings from studentexport.csv
    # These are stored in IST (India Standard Time = UTC+5:30)
    if not value or str(value).strip() in ("", "None"):
        return None
    try:
        # Parse the naive datetime string
        naive_datetime = datetime.datetime.strptime(value.strip(), "%m/%d/%Y %H:%M")
        # Attach the IST timezone offset (+5:30)
        ist_timezone = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        return naive_datetime.replace(tzinfo=ist_timezone)
    except (ValueError, AttributeError):
        return None


def count_rows_in_table(conn, table_name):
    # Return the current number of rows in a table
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# ── STEP 1: BACKFILL TRANSACTIONS ────────────────────────────

def backfill_transactions(conn):
    # Read Bronze enrollment rows and write one Silver transaction per row
    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Only process rows that have a user_id — we need it for the UPSERT key
    read_cursor.execute("""
        SELECT
            source_row,
            user_id, name, email,
            bundle_id, class_name, master_batch_id, master_batch_name,
            institution_bundle_id,
            classusers_start_date, classusers_end_date
        FROM bronze.student_courses_enrolled_raw
        WHERE user_id IS NOT NULL
    """)
    all_rows = read_cursor.fetchall()
    read_cursor.close()

    inserted_count = 0
    skipped_count  = 0
    error_count    = 0

    write_cursor = conn.cursor()

    for row in all_rows:
        source_row = row["source_row"]

        # Build a stable event_id from the source row number so re-runs are idempotent
        event_id = "csv-enrollment-" + str(source_row)

        # user_id must be a valid integer for the transaction table
        try:
            user_id = int(row["user_id"])
        except (TypeError, ValueError):
            error_count = error_count + 1
            continue

        # Convert IDs and timestamps — CSV stores them as text
        bundle_id             = unix_or_null(row["bundle_id"])
        master_batch_id       = unix_or_null(row["master_batch_id"])
        institution_bundle_id = unix_or_null(row["institution_bundle_id"])
        start_date_unix       = unix_or_null(row["classusers_start_date"])
        end_date_unix         = unix_or_null(row["classusers_end_date"])

        write_cursor.execute(SQL_UPSERT_TRANSACTION, (
            event_id,
            user_id,
            row["email"],
            row["name"],
            bundle_id,
            row["class_name"],
            master_batch_id,
            row["master_batch_name"],
            institution_bundle_id,
            start_date_unix,
            end_date_unix,
        ))

        # rowcount 1 means a new row; 0 means an existing row was updated (conflict)
        if write_cursor.rowcount == 1:
            inserted_count = inserted_count + 1
        else:
            skipped_count = skipped_count + 1

    conn.commit()
    write_cursor.close()
    return inserted_count, skipped_count, error_count


# ── STEP 2: BACKFILL USERS ────────────────────────────────────

def build_email_to_user_id_lookup(conn):
    # Build a dictionary: email_lowercase → user_id
    # We get user_ids from the enrollment table because studentexport.csv has no user_id column
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute("""
        SELECT DISTINCT ON (LOWER(TRIM(email)))
            LOWER(TRIM(email)) AS email_key,
            user_id::bigint    AS user_id
        FROM bronze.student_courses_enrolled_raw
        WHERE email IS NOT NULL
          AND user_id IS NOT NULL
        ORDER BY LOWER(TRIM(email)), source_row
    """)
    all_rows = cursor.fetchall()
    cursor.close()

    # Convert the list of rows into a plain dictionary for fast lookups
    email_lookup = {}
    for row in all_rows:
        email_lookup[row["email_key"]] = row["user_id"]

    return email_lookup


def backfill_users(conn):
    # Read Bronze student export rows and write Silver users
    # Students whose email cannot be matched to a user_id go to unresolved
    email_to_user_id = build_email_to_user_id_lookup(conn)

    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    read_cursor.execute("""
        SELECT source_row, name, email, contact_number_dial_code, contact_number,
               username, city, state, address,
               parent_name, parent_email, parent_contact,
               date_created
        FROM bronze.studentexport_raw
    """)
    all_rows = read_cursor.fetchall()
    read_cursor.close()

    inserted_count   = 0
    skipped_count    = 0
    unresolved_count = 0

    write_cursor = conn.cursor()

    for row in all_rows:
        source_row = row["source_row"]
        raw_email  = row["email"]

        # Convert email to lowercase for the lookup — CSV may have mixed case
        if raw_email:
            email_key = raw_email.lower().strip()
        else:
            email_key = None

        user_id = email_to_user_id.get(email_key) if email_key else None

        # If we cannot find a user_id for this student, save them to the unresolved table
        if user_id is None:
            write_cursor.execute(SQL_INSERT_UNRESOLVED, (
                source_row,
                raw_email,
                psycopg2.extras.Json(dict(row)),
            ))
            unresolved_count = unresolved_count + 1
            continue

        # Build the full phone number from dial code + number (e.g. "91" + "9876543210")
        dial_code    = (row["contact_number_dial_code"] or "").strip()
        phone_number = (row["contact_number"]           or "").strip()
        if dial_code and phone_number:
            # Format: +919876543210 (no space between country code and number)
            contact_number = "+" + dial_code.lstrip("+") + phone_number
        else:
            contact_number = phone_number or None

        # Parse the IST-based date_created string into a proper datetime
        created_at_ist = parse_date_created(row["date_created"])

        write_cursor.execute(SQL_UPSERT_USER, (
            "csv-student-" + str(source_row),
            user_id,
            raw_email,
            row["name"],
            row["username"],
            contact_number,
            row["city"],
            row["state"],
            row["address"],
            row["parent_name"],
            row["parent_email"],
            row["parent_contact"],
            created_at_ist,
        ))

        if write_cursor.rowcount == 1:
            inserted_count = inserted_count + 1
        else:
            skipped_count = skipped_count + 1

    conn.commit()
    write_cursor.close()
    return inserted_count, skipped_count, unresolved_count


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== backfill_transactions.py ===")
    print("")

    conn = connect_to_database()

    # Show input row counts so we know what we are working with
    enroll_count  = count_rows_in_table(conn, "bronze.student_courses_enrolled_raw")
    student_count = count_rows_in_table(conn, "bronze.studentexport_raw")
    print("Bronze input:")
    print("  bronze.student_courses_enrolled_raw : " + str(enroll_count) + " rows")
    print("  bronze.studentexport_raw            : " + str(student_count) + " rows")
    print("")

    # Step 1: build Silver transactions from enrollment CSV
    print("Step 1: Backfilling silver.transactions ...")
    t_inserted, t_skipped, t_errors = backfill_transactions(conn)
    print("  Inserted (new)       : " + str(t_inserted))
    print("  Skipped (conflict)   : " + str(t_skipped))
    if t_errors > 0:
        print("  Errors (bad user_id) : " + str(t_errors))
    print("")

    # Step 2: build Silver users from student export CSV
    print("Step 2: Backfilling silver.users ...")
    u_inserted, u_skipped, u_unresolved = backfill_users(conn)
    print("  Inserted (new)                 : " + str(u_inserted))
    print("  Skipped (conflict)             : " + str(u_skipped))
    print("  Unresolved (no user_id match)  : " + str(u_unresolved))
    print("")

    conn.close()

    # Re-open a fresh connection to get final Silver counts
    conn2 = connect_to_database()
    final_txn   = count_rows_in_table(conn2, "silver.transactions")
    final_users = count_rows_in_table(conn2, "silver.users")
    final_unres = count_rows_in_table(conn2, "bronze.unresolved_students_raw")
    conn2.close()

    print("  BACKFILL COMPLETE")
    print("  " + "-" * 41)
    print("  silver.transactions                : " + str(final_txn))
    print("  silver.users                       : " + str(final_users))
    print("  bronze.unresolved_students_raw     : " + str(final_unres))
    print("")


if __name__ == "__main__":
    main()
