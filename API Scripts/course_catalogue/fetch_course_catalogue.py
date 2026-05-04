# ============================================================
# 02 — FETCH COURSE CATALOGUE
# ============================================================
# What it does: Downloads the list of all courses (bundles)
#               from the Edmingle API and saves it to the
#               database in two steps:
#               1. Raw data → Bronze (exact copy from API)
#               2. Cleaned data → Silver (typed, deduplicated)
#
# Why we need it: The course catalogue tells Power BI what
#                 courses exist, their names, subjects, and
#                 status. Run this weekly (courses change slowly).
#
# How to run:
#   python 02_fetch_course_catalogue/fetch_course_catalogue.py
#
# What to check after:
#   - bronze.course_catalogue_raw should have rows
#   - silver.course_metadata should have rows
#   - Run 12_check_db_counts to confirm
# ============================================================

import sys
import time

import psycopg2
import psycopg2.extras
import requests

# ── DATABASE AND API SETTINGS ─────────────────────────────────
DB_HOST       = "localhost"
DB_NAME       = "edmingle_analytics"
DB_USER       = "postgres"
DB_PASSWORD   = "Svyoma"
DB_PORT       = 5432
API_KEY       = "590605228a847624e065a76e986803fa"
ORG_ID        = "683"
INST_ID       = "483"
# ─────────────────────────────────────────────────────────────

# Build the full API URL for the course catalogue endpoint
CATALOGUE_URL = (
    "https://vyoma-api.edmingle.com/nuSource/api/v1"
    + "/institute/" + INST_ID + "/courses/catalogue"
)

# Headers sent with every API request
API_HEADERS = {"apikey": API_KEY, "ORGID": ORG_ID}

# Only these statuses are valid for the Silver layer
VALID_STATUSES = {"Completed", "Ongoing", "Upcoming"}


# ── SQL CONSTANTS ─────────────────────────────────────────────

SQL_UPSERT_BRONZE = """
    INSERT INTO bronze.course_catalogue_raw (
        source_row,
        bundle_id, course_name, course_description, cost,
        num_students, tutors, course_url, subject, level,
        language, type, course_division, status,
        sss_category, viniyoga, adhyayanam_category,
        term_of_course, position_in_funnel, division,
        raw_json
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s, %s,
        %s
    )
    ON CONFLICT (source_row) DO UPDATE SET
        bundle_id           = EXCLUDED.bundle_id,
        course_name         = EXCLUDED.course_name,
        course_description  = EXCLUDED.course_description,
        cost                = EXCLUDED.cost,
        num_students        = EXCLUDED.num_students,
        tutors              = EXCLUDED.tutors,
        course_url          = EXCLUDED.course_url,
        subject             = EXCLUDED.subject,
        level               = EXCLUDED.level,
        language            = EXCLUDED.language,
        type                = EXCLUDED.type,
        course_division     = EXCLUDED.course_division,
        status              = EXCLUDED.status,
        sss_category        = EXCLUDED.sss_category,
        viniyoga            = EXCLUDED.viniyoga,
        adhyayanam_category = EXCLUDED.adhyayanam_category,
        term_of_course      = EXCLUDED.term_of_course,
        position_in_funnel  = EXCLUDED.position_in_funnel,
        division            = EXCLUDED.division,
        raw_json            = EXCLUDED.raw_json,
        loaded_at           = NOW()
"""

SQL_UPSERT_SILVER = """
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
        imported_at         = NOW()
"""


# ── TYPE CONVERSION HELPERS ───────────────────────────────────

def clean_text(value):
    # Convert value to a clean string, returning None for empty/null values
    if value is None:
        return None
    cleaned = str(value).strip()
    if cleaned in ("", "nan", "None", "NaN"):
        return None
    return cleaned


def convert_to_int(value):
    # Convert value to integer, returning None if not possible
    if value is None:
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def convert_to_float(value):
    # Convert value to float (decimal number), returning None if not possible
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ── STEP 1: CALL THE API ──────────────────────────────────────

def call_catalogue_api():
    # Try up to 3 times in case of network errors
    last_error = None
    for attempt_number in range(3):
        try:
            # Call the API with a 60-second timeout to avoid hanging
            response = requests.get(CATALOGUE_URL, headers=API_HEADERS, timeout=60)
            if response.status_code != 200:
                print("  API error: HTTP " + str(response.status_code))
                sys.exit(1)
            return response.json()
        except requests.exceptions.ConnectionError as error:
            last_error = error
            print("  Connection error (attempt " + str(attempt_number + 1) + "/3): " + str(error))
            if attempt_number < 2:
                time.sleep(5)
    # All retries failed
    print("  All retries exhausted: " + str(last_error))
    sys.exit(1)


def extract_course_list(api_data):
    # The API can return courses in different formats — handle all of them
    if isinstance(api_data, list):
        # Response is already a list of courses
        return api_data
    if isinstance(api_data, dict):
        # Response is a dictionary — find the list under a known key
        for key in ("response", "data", "courses", "bundles", "result", "items"):
            if key in api_data and isinstance(api_data[key], list):
                return api_data[key]
        # No list found — try using the dict values
        return list(api_data.values()) if api_data else []
    # Unexpected format
    print("  Unexpected API response type: " + str(type(api_data)))
    sys.exit(1)


def fetch_catalogue():
    # Call the API and extract the list of courses
    print("Step 1: Calling catalogue API ...")
    raw_data = call_catalogue_api()
    courses = extract_course_list(raw_data)
    if not courses:
        print("  WARNING: API returned 0 courses — nothing to load")
        sys.exit(0)
    print("  API responded with " + str(len(courses)) + " courses")
    return courses


# ── STEP 2: SAVE TO BRONZE ────────────────────────────────────

def get_field(course, *field_names):
    # Try each field name and return the first non-None value found
    # This handles both old and new API field names gracefully
    for field_name in field_names:
        value = course.get(field_name)
        if value is not None:
            return value
    return None


def ensure_raw_json_column_exists(conn):
    # Check if the raw_json column already exists in the Bronze table
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
          AND table_name   = 'course_catalogue_raw'
          AND column_name  = 'raw_json'
    """)
    if not cursor.fetchone():
        # Column does not exist yet — add it now
        cursor.execute("ALTER TABLE bronze.course_catalogue_raw ADD COLUMN raw_json JSONB")
        conn.commit()
    cursor.close()


def upsert_bronze(conn, courses):
    # Make sure the raw_json column exists before we try to write to it
    ensure_raw_json_column_exists(conn)
    cursor = conn.cursor()
    upserted_count = 0

    for course in courses:
        # Get the bundle ID — try multiple field name variants
        bundle_id_raw = get_field(course, "bundle_id", "Bundle id", "id")
        if not bundle_id_raw:
            # Skip courses without an ID — we cannot identify them
            continue
        # Run the SQL INSERT/UPDATE for this course
        cursor.execute(SQL_UPSERT_BRONZE, (
            int(bundle_id_raw),
            str(bundle_id_raw),
            clean_text(get_field(course, "Course Name", "course_name", "name")),
            clean_text(get_field(course, "Course Description", "course_description", "description")),
            clean_text(get_field(course, "Cost", "cost", "price")),
            clean_text(get_field(course, "Num Students", "num_students", "student_count")),
            clean_text(get_field(course, "Tutors", "tutors", "tutor_name")),
            clean_text(get_field(course, "Course URL", "course_url", "url")),
            clean_text(get_field(course, "Subject", "subject")),
            clean_text(get_field(course, "Level", "level")),
            clean_text(get_field(course, "Language", "language")),
            clean_text(get_field(course, "Type", "type", "course_type")),
            clean_text(get_field(course, "Course Division", "course_division")),
            clean_text(get_field(course, "Status", "status")),
            clean_text(get_field(course, "SSS Category", "sss_category")),
            clean_text(get_field(course, "Viniyoga", "viniyoga")),
            clean_text(get_field(course, "Adhyayanam Category", "adhyayanam_category")),
            clean_text(get_field(course, "Term of Course", "term_of_course")),
            clean_text(get_field(course, "Position in Funnel", "position_in_funnel")),
            clean_text(get_field(course, "Division", "division")),
            psycopg2.extras.Json(course),
        ))
        if cursor.rowcount > 0:
            upserted_count = upserted_count + 1

    conn.commit()
    cursor.close()
    return upserted_count


# ── STEP 3: TRANSFORM TO SILVER ───────────────────────────────

def get_bundle_id_int(raw_bundle_id):
    # Convert the bundle_id text to an integer
    # bundle_id of 0 is not valid, so we return None for it
    try:
        bundle_id = int(float(str(raw_bundle_id).strip()))
        if bundle_id == 0:
            return None
        return bundle_id
    except (ValueError, TypeError):
        return None


def transform_to_silver(conn):
    # Read all rows from Bronze
    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    read_cursor.execute("SELECT * FROM bronze.course_catalogue_raw ORDER BY source_row")
    bronze_rows = read_cursor.fetchall()
    read_cursor.close()

    write_cursor = conn.cursor()
    upserted_count = 0
    skip_division = 0
    skip_bundle   = 0
    # Track bundle IDs we have already processed (for deduplication)
    seen_bundle_ids = {}

    for row in bronze_rows:
        # Skip rows with invalid bundle IDs
        bundle_id = get_bundle_id_int(row.get("bundle_id"))
        if bundle_id is None:
            skip_bundle = skip_bundle + 1
            continue

        # Rule: only include courses with course_division = 'Course'
        course_division = clean_text(row.get("course_division"))
        if course_division and course_division.lower() != "course":
            skip_division = skip_division + 1
            continue

        # Normalize the status — only keep known valid statuses
        raw_status = clean_text(row.get("status"))
        if raw_status in VALID_STATUSES:
            status = raw_status
        else:
            status = None

        # Deduplication: keep the row with a known status over a NULL status
        if bundle_id in seen_bundle_ids:
            previous_status = seen_bundle_ids[bundle_id]
            if previous_status is not None:
                # We already have a better row for this bundle — skip this one
                continue
        seen_bundle_ids[bundle_id] = status

        # Save this course to Silver
        write_cursor.execute(SQL_UPSERT_SILVER, (
            bundle_id,
            clean_text(row.get("course_name")),
            clean_text(row.get("subject")),
            clean_text(row.get("type")),
            status,
            clean_text(row.get("term_of_course")),
            clean_text(row.get("position_in_funnel")),
            clean_text(row.get("adhyayanam_category")),
            clean_text(row.get("sss_category")),
            clean_text(row.get("viniyoga")),
            course_division,
            clean_text(row.get("division")),
            clean_text(row.get("level")),
            clean_text(row.get("language")),
            convert_to_int(row.get("num_students")),
            convert_to_float(row.get("cost")),
        ))
        upserted_count = upserted_count + 1

    conn.commit()
    write_cursor.close()
    return upserted_count, skip_division, skip_bundle


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== fetch_course_catalogue.py ===")
    print("")

    # Step 1: Fetch from the API
    courses = fetch_catalogue()

    # Connect to the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        sys.exit(1)

    # Step 2: Save raw data to Bronze
    print("Step 2: Saving raw data to Bronze ...")
    bronze_count = upsert_bronze(conn, courses)
    print("  Bronze updated — " + str(bronze_count) + " rows upserted")

    # Step 3: Transform Bronze rows to Silver
    print("Step 3: Transforming to Silver ...")
    silver_count, skip_div, skip_bid = transform_to_silver(conn)
    print("  Silver updated — " + str(silver_count) + " rows upserted")

    conn.close()

    print("")
    print("  API call                        : SUCCESS")
    print("  Courses received                : " + str(len(courses)))
    print("  Bronze upserted                 : " + str(bronze_count))
    print("  Silver upserted                 : " + str(silver_count))
    print("  Skipped (not Course division)   : " + str(skip_div))
    print("  Skipped (invalid bundle_id)     : " + str(skip_bid))

    # Return values so run_course_pipeline.py can use them
    return True, len(courses), silver_count


if __name__ == "__main__":
    main()
