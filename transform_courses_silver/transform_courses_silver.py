# ============================================================
# 10 — TRANSFORM COURSES SILVER
# ============================================================
# What it does: Reads the three Bronze course tables and
#               transforms each one into its Silver table.
#
#   bronze.course_catalogue_raw  → silver.course_metadata
#   bronze.course_lifecycle_raw  → silver.course_lifecycle
#   bronze.course_batches_raw    → silver.course_batches
#
# Why we need it: Bronze tables hold raw CSV data as text.
#                 Silver tables have proper data types (integers,
#                 dates, floats) and are what Power BI queries.
#                 This script performs the type conversion and
#                 cleaning step.
#
# How to run:
#   python 10_transform_courses_silver/transform_courses_silver.py
#
#   Run AFTER 09_load_courses_csv has finished.
#
# What to check after:
#   - All three Silver tables should have rows
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

SQL_UPSERT_COURSE_METADATA = """
    INSERT INTO silver.course_metadata (
        bundle_id, course_name, subject, course_type, status,
        term_of_course, position_in_funnel, adhyayanam_category,
        sss_category, viniyoga, course_division, division,
        level, language, num_students, cost, imported_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s, NOW()
    )
    ON CONFLICT (bundle_id) DO UPDATE SET
        course_name         = EXCLUDED.course_name,
        subject             = EXCLUDED.subject,
        course_type         = EXCLUDED.course_type,
        status              = EXCLUDED.status,
        term_of_course      = EXCLUDED.term_of_course,
        position_in_funnel  = EXCLUDED.position_in_funnel,
        adhyayanam_category = EXCLUDED.adhyayanam_category,
        sss_category        = EXCLUDED.sss_category,
        viniyoga            = EXCLUDED.viniyoga,
        course_division     = EXCLUDED.course_division,
        division            = EXCLUDED.division,
        level               = EXCLUDED.level,
        language            = EXCLUDED.language,
        num_students        = EXCLUDED.num_students,
        cost                = EXCLUDED.cost,
        imported_at         = EXCLUDED.imported_at
"""

SQL_UPSERT_COURSE_LIFECYCLE = """
    INSERT INTO silver.course_lifecycle (
        course_id, course_name, batch_name, type_of_launch, status,
        subject, position_in_funnel, learning_model, term_of_course,
        sss_category, persona,
        first_class_date, last_class_date,
        enrollments_on_fc, enrollments_on_lc,
        avg_attendance, total_classes_held, total_certified,
        pass_percentage, overall_rating, batch_id, imported_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s, %s,
        %s, %s, %s, NOW()
    )
    ON CONFLICT (course_id, batch_name) DO UPDATE SET
        course_name        = EXCLUDED.course_name,
        type_of_launch     = EXCLUDED.type_of_launch,
        status             = EXCLUDED.status,
        subject            = EXCLUDED.subject,
        position_in_funnel = EXCLUDED.position_in_funnel,
        learning_model     = EXCLUDED.learning_model,
        term_of_course     = EXCLUDED.term_of_course,
        sss_category       = EXCLUDED.sss_category,
        persona            = EXCLUDED.persona,
        first_class_date   = EXCLUDED.first_class_date,
        last_class_date    = EXCLUDED.last_class_date,
        enrollments_on_fc  = EXCLUDED.enrollments_on_fc,
        enrollments_on_lc  = EXCLUDED.enrollments_on_lc,
        avg_attendance     = EXCLUDED.avg_attendance,
        total_classes_held = EXCLUDED.total_classes_held,
        total_certified    = EXCLUDED.total_certified,
        pass_percentage    = EXCLUDED.pass_percentage,
        overall_rating     = EXCLUDED.overall_rating,
        batch_id           = EXCLUDED.batch_id,
        imported_at        = EXCLUDED.imported_at
"""

SQL_UPSERT_COURSE_BATCHES = """
    INSERT INTO silver.course_batches (
        bundle_id, bundle_name, batch_id, batch_name, batch_status,
        start_date_ist, end_date_ist, tutor_name, admitted_students,
        imported_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        NOW()
    )
    ON CONFLICT (bundle_id, batch_id) DO UPDATE SET
        bundle_name       = EXCLUDED.bundle_name,
        batch_name        = EXCLUDED.batch_name,
        batch_status      = EXCLUDED.batch_status,
        start_date_ist    = EXCLUDED.start_date_ist,
        end_date_ist      = EXCLUDED.end_date_ist,
        tutor_name        = EXCLUDED.tutor_name,
        admitted_students = EXCLUDED.admitted_students,
        imported_at       = EXCLUDED.imported_at
"""


# ── TYPE CONVERSION HELPERS ───────────────────────────────────

# These are used throughout all three transforms.
# They all return None (which becomes NULL) for any value that cannot be converted.

def to_text(value):
    # Clean a value to a text string, returning None for empty/missing values
    if value is None:
        return None
    cleaned = str(value).strip()
    if cleaned in ("", "nan", "None", "NaN"):
        return None
    return cleaned


def to_int(value):
    # Convert value to integer, returning None if not possible
    if value is None or str(value).strip() in ("", "nan", "None", "NaN"):
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def to_float(value):
    # Convert value to float, returning None for blanks or common bad values
    if value is None or str(value).strip() in ("", "nan", "None", "NaN",
                                                "#VALUE!", "Unavailable", "#DIV/0!"):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def to_bigint(value):
    # Convert value to large integer (handles decimal strings like "12477.0")
    if value is None or str(value).strip() in ("", "nan", "None", "NaN"):
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def to_date(value):
    # Parse a date string in several possible formats, returning a date object
    # Returns None if the value is empty or cannot be parsed
    if value is None or str(value).strip() in ("", "nan", "None", "NaN",
                                                "Unavailable", "#VALUE!"):
        return None
    date_string = str(value).strip()
    # Try each possible date format in order
    for date_format in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.datetime.strptime(date_string, date_format).date()
        except ValueError:
            continue
    # None of the formats worked — return None
    return None


def to_unix_timestamp(value):
    # Convert a Unix timestamp number to a timezone-aware UTC datetime
    if value is None or str(value).strip() in ("", "nan", "None", "NaN"):
        return None
    try:
        unix_float = float(str(value).strip())
        return datetime.datetime.fromtimestamp(unix_float, tz=datetime.timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


# ── TRANSFORM 1: bronze.course_catalogue_raw → silver.course_metadata ──────────

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


def transform_course_metadata(conn):
    # Read bronze.course_catalogue_raw and write cleaned rows to silver.course_metadata
    read_cursor  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    write_cursor = conn.cursor()

    # Load all Bronze rows sorted by their original order
    read_cursor.execute("SELECT * FROM bronze.course_catalogue_raw ORDER BY source_row")
    bronze_rows = read_cursor.fetchall()
    read_cursor.close()

    upserted_count = 0

    for row in bronze_rows:
        # bundle_id is the unique identifier — skip rows without one
        bundle_id = to_bigint(row.get("bundle_id"))
        if not bundle_id:
            continue

        write_cursor.execute(SQL_UPSERT_COURSE_METADATA, (
            bundle_id,
            to_text(row.get("course_name")),
            to_text(row.get("subject")),
            to_text(row.get("type")),
            to_text(row.get("status")),
            to_text(row.get("term_of_course")),
            to_text(row.get("position_in_funnel")),
            to_text(row.get("adhyayanam_category")),
            to_text(row.get("sss_category")),
            to_text(row.get("viniyoga")),
            to_text(row.get("course_division")),
            to_text(row.get("division")),
            to_text(row.get("level")),
            to_text(row.get("language")),
            to_int(row.get("num_students")),
            to_float(row.get("cost")),
        ))
        upserted_count = upserted_count + 1

    conn.commit()
    write_cursor.close()
    return upserted_count


# ── TRANSFORM 2: bronze.course_lifecycle_raw → silver.course_lifecycle ──────────

def transform_course_lifecycle(conn):
    # Read bronze.course_lifecycle_raw and write to silver.course_lifecycle
    read_cursor  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    write_cursor = conn.cursor()

    read_cursor.execute("SELECT * FROM bronze.course_lifecycle_raw ORDER BY source_row")
    bronze_rows = read_cursor.fetchall()
    read_cursor.close()

    upserted_count = 0

    for row in bronze_rows:
        # Both course_id and batch_name are needed for the composite primary key
        course_id  = to_bigint(row.get("course_id"))
        batch_name = to_text(row.get("batch_name"))
        if not course_id or not batch_name:
            continue

        write_cursor.execute(SQL_UPSERT_COURSE_LIFECYCLE, (
            course_id,
            to_text(row.get("course_name")),
            batch_name,
            to_text(row.get("type_of_launch")),
            to_text(row.get("status")),
            to_text(row.get("subject")),
            to_text(row.get("position_in_funnel")),
            to_text(row.get("samskritadhyayana_model")),
            to_text(row.get("term_of_course")),
            to_text(row.get("sss_category")),
            to_text(row.get("persona")),
            to_date(row.get("first_class_date")),
            to_date(row.get("last_class_and_valedictory_date")),
            to_int(row.get("enrolments_on_the_day_of_first_class")),
            to_int(row.get("enrolments_on_last_day")),
            to_float(row.get("average_attendance_of_all_classes")),
            to_int(row.get("total_no_of_classes_held")),
            to_int(row.get("total_students_certified")),
            to_float(row.get("pass_percentage_total_certified_vs_total_assessment_attendees")),
            to_float(row.get("overall_course_rating")),
            to_bigint(row.get("batch_id")),
        ))
        upserted_count = upserted_count + 1

    conn.commit()
    write_cursor.close()
    return upserted_count


# ── TRANSFORM 3: bronze.course_batches_raw → silver.course_batches ──────────────

def transform_course_batches(conn):
    # Read bronze.course_batches_raw and write to silver.course_batches
    read_cursor  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    write_cursor = conn.cursor()

    read_cursor.execute("SELECT * FROM bronze.course_batches_raw ORDER BY source_row")
    bronze_rows = read_cursor.fetchall()
    read_cursor.close()

    upserted_count = 0

    for row in bronze_rows:
        # Both bundle_id and batch_id are needed for the composite primary key
        bundle_id = to_bigint(row.get("bundle_id"))
        batch_id  = to_bigint(row.get("batch_id"))
        if bundle_id is None or batch_id is None:
            continue

        write_cursor.execute(SQL_UPSERT_COURSE_BATCHES, (
            bundle_id,
            to_text(row.get("bundle_name")),
            batch_id,
            to_text(row.get("batch_name")),
            to_text(row.get("batch_status")),
            # start_date and end_date are stored as Unix timestamps in the CSV
            to_unix_timestamp(row.get("start_date")),
            to_unix_timestamp(row.get("end_date")),
            to_text(row.get("tutor_name")),
            to_int(row.get("admitted_students")),
        ))
        upserted_count = upserted_count + 1

    conn.commit()
    write_cursor.close()
    return upserted_count


def count_rows_in_table(conn, table_name):
    # Return the current number of rows in a Silver table
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== transform_courses_silver.py ===")
    print("")

    conn = connect_to_database()

    # Transform 1: course catalogue
    print("Step 1: bronze.course_catalogue_raw → silver.course_metadata ...")
    count1 = transform_course_metadata(conn)
    final1 = count_rows_in_table(conn, "silver.course_metadata")
    print("  Rows processed : " + str(count1))
    print("  Table total    : " + str(final1))
    print("")

    # Transform 2: course lifecycle
    print("Step 2: bronze.course_lifecycle_raw → silver.course_lifecycle ...")
    count2 = transform_course_lifecycle(conn)
    final2 = count_rows_in_table(conn, "silver.course_lifecycle")
    print("  Rows processed : " + str(count2))
    print("  Table total    : " + str(final2))
    print("")

    # Transform 3: course batches
    print("Step 3: bronze.course_batches_raw → silver.course_batches ...")
    count3 = transform_course_batches(conn)
    final3 = count_rows_in_table(conn, "silver.course_batches")
    print("  Rows processed : " + str(count3))
    print("  Table total    : " + str(final3))
    print("")

    conn.close()

    print("  TRANSFORM COMPLETE")
    print("  " + "-" * 41)
    print("  silver.course_metadata  : " + str(final1) + " rows")
    print("  silver.course_lifecycle : " + str(final2) + " rows")
    print("  silver.course_batches   : " + str(final3) + " rows")
    print("")


if __name__ == "__main__":
    main()
