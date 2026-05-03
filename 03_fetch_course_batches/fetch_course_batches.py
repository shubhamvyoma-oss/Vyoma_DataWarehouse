# ============================================================
# 03 — FETCH COURSE BATCHES
# ============================================================
# What it does: Downloads all batch data from the Edmingle API
#               and saves it in three steps:
#               1. Raw data → Bronze (exact copy from API)
#               2. Cleaned data → Silver (course_batches)
#               3. Rebuild master table → Silver (course_master)
#
# Why we need it: Each course can have multiple batches (e.g.
#                 "Batch 2024-A", "Batch 2024-B"). This data
#                 changes daily and must be kept current.
#                 Power BI uses silver.course_master for reports.
#
# How to run:
#   python 03_fetch_course_batches/fetch_course_batches.py
#
# What to check after:
#   - silver.course_batches should have rows
#   - silver.course_master should have rows
#   - Run 12_check_db_counts to confirm
# ============================================================

import datetime
import sys
import time

import psycopg2
import psycopg2.extras
import requests

# ── DATABASE AND API SETTINGS ─────────────────────────────────
DB_HOST      = "localhost"
DB_NAME      = "edmingle_analytics"
DB_USER      = "postgres"
DB_PASSWORD  = "Svyoma"
DB_PORT      = 5432
API_KEY      = "590605228a847624e065a76e986803fa"
ORG_ID       = "683"
INST_ID      = "483"
BATCHES_URL  = "https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch"
# ─────────────────────────────────────────────────────────────

# Headers sent with every API request
API_HEADERS = {"apikey": API_KEY, "ORGID": ORG_ID}


# ── SQL CONSTANTS ─────────────────────────────────────────────

SQL_UPSERT_BRONZE = """
    INSERT INTO bronze.course_batches_raw (
        source_row, bundle_id, bundle_name,
        batch_id, batch_name, batch_status,
        start_date, start_date_converted,
        end_date, end_date_converted,
        tutor_id, tutor_name, admitted_students
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s, %s
    )
    ON CONFLICT (source_row) DO UPDATE SET
        bundle_id         = EXCLUDED.bundle_id,
        bundle_name       = EXCLUDED.bundle_name,
        batch_id          = EXCLUDED.batch_id,
        batch_name        = EXCLUDED.batch_name,
        batch_status      = EXCLUDED.batch_status,
        start_date        = EXCLUDED.start_date,
        end_date          = EXCLUDED.end_date,
        tutor_id          = EXCLUDED.tutor_id,
        tutor_name        = EXCLUDED.tutor_name,
        admitted_students = EXCLUDED.admitted_students,
        loaded_at         = NOW()
"""

SQL_UPSERT_SILVER = """
    INSERT INTO silver.course_batches (
        bundle_id, bundle_name, batch_id, batch_name, batch_status,
        start_date_ist, end_date_ist, tutor_name, admitted_students,
        imported_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, NOW()
    )
    ON CONFLICT (bundle_id, batch_id) DO UPDATE SET
        bundle_name       = EXCLUDED.bundle_name,
        batch_name        = EXCLUDED.batch_name,
        batch_status      = EXCLUDED.batch_status,
        start_date_ist    = EXCLUDED.start_date_ist,
        end_date_ist      = EXCLUDED.end_date_ist,
        tutor_name        = EXCLUDED.tutor_name,
        admitted_students = EXCLUDED.admitted_students,
        imported_at       = NOW()
"""

# The master table query: joins course_batches + course_metadata
# to create one wide reporting table with all course and batch info
SQL_REBUILD_MASTER = """
    INSERT INTO silver.course_master (
        bundle_id, bundle_name, batch_id, batch_name, batch_status,
        start_date, end_date, tutor_name, admitted_students,
        course_name, subject, course_type, term_of_course,
        position_in_funnel, adhyayanam_category, sss_category,
        viniyoga, division,
        catalogue_status, final_status,
        is_latest_batch, include_in_course_count,
        status_adjustment_reason, has_batch, built_at
    )
    WITH latest_batch AS (
        SELECT
            bundle_id,
            batch_id,
            ROW_NUMBER() OVER (
                PARTITION BY bundle_id
                ORDER BY end_date_ist DESC NULLS LAST
            ) AS rn
        FROM silver.course_batches
    ),
    has_batch_flag AS (
        SELECT DISTINCT bundle_id, 1 AS has_b
        FROM silver.course_batches
    )
    SELECT
        cb.bundle_id, cb.bundle_name, cb.batch_id, cb.batch_name, cb.batch_status,
        cb.start_date_ist::DATE, cb.end_date_ist::DATE,
        cb.tutor_name, cb.admitted_students,
        cm.course_name, cm.subject, cm.course_type, cm.term_of_course,
        cm.position_in_funnel, cm.adhyayanam_category, cm.sss_category,
        cm.viniyoga, cm.division,
        cm.status AS catalogue_status,
        cm.status AS final_status,
        CASE WHEN lb.rn = 1 THEN 1 ELSE 0 END AS is_latest_batch,
        CASE
            WHEN cm.course_division = 'Course'
             AND cb.bundle_id IS NOT NULL
             AND cm.status IN ('Completed', 'Ongoing', 'Upcoming')
            THEN 1 ELSE 0
        END AS include_in_course_count,
        '' AS status_adjustment_reason,
        COALESCE(hb.has_b, 0) AS has_batch,
        NOW()
    FROM silver.course_batches cb
    LEFT JOIN silver.course_metadata cm  ON cb.bundle_id = cm.bundle_id
    LEFT JOIN latest_batch lb
        ON cb.bundle_id = lb.bundle_id AND cb.batch_id = lb.batch_id
    LEFT JOIN has_batch_flag hb ON cb.bundle_id = hb.bundle_id

    UNION ALL

    SELECT
        cm.bundle_id, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        cm.course_name, cm.subject, cm.course_type, cm.term_of_course,
        cm.position_in_funnel, cm.adhyayanam_category, cm.sss_category,
        cm.viniyoga, cm.division,
        cm.status, cm.status,
        0, 0, '', 0, NOW()
    FROM silver.course_metadata cm
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.course_batches cb WHERE cb.bundle_id = cm.bundle_id
    )
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


def convert_to_bigint(value):
    # Convert value to a large integer (handles values like "12345.0")
    if value is None:
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def convert_to_int(value):
    # Convert value to a regular integer, stripping commas first
    if value is None:
        return None
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def parse_timestamp(value):
    # Convert a Unix timestamp (seconds since 1970) to a Python datetime
    if value is None or str(value).strip() in ("", "nan", "None", "NaN"):
        return None
    cleaned = str(value).strip()
    # Try parsing as a Unix timestamp number first
    try:
        unix_time = float(cleaned)
        return datetime.datetime.fromtimestamp(unix_time, tz=datetime.timezone.utc)
    except (ValueError, OSError):
        pass
    # Try common date string formats as fallback
    for date_format in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                        "%d/%m/%Y", "%m/%d/%Y"):
        try:
            date_part = cleaned.split("+")[0].strip()
            parsed = datetime.datetime.strptime(date_part, date_format)
            return parsed.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
    return None


# ── STEP 1: CALL THE API ──────────────────────────────────────

def call_batches_api_page(page_number):
    # Request one page of batch data from the API
    params = {"page": page_number, "org_id": ORG_ID}
    last_error = None

    for attempt_number in range(3):
        try:
            response = requests.get(BATCHES_URL, headers=API_HEADERS, params=params, timeout=60)
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


def flatten_bundles_to_batches(bundles):
    # The API returns bundles, each with a nested list of batches.
    # We flatten this so we have one row per batch.
    flat_batches = []
    for bundle in bundles:
        bundle_id   = bundle.get("bundle_id")
        bundle_name = bundle.get("bundle_name")
        # Get the list of batches inside this bundle
        batch_list = bundle.get("batch") or []
        # Sometimes the batch list comes as a JSON string — parse it if so
        if isinstance(batch_list, str):
            import json as _json
            try:
                batch_list = _json.loads(batch_list)
            except Exception:
                batch_list = []
        # Add the bundle_id and bundle_name onto each batch row
        for batch in batch_list:
            batch["bundle_id"]   = bundle_id
            batch["bundle_name"] = bundle_name
            flat_batches.append(batch)
    return flat_batches


def fetch_batches():
    # Download all pages of batch data and return a flat list of batches
    all_batches = []
    page_number = 1

    while True:
        data = call_batches_api_page(page_number)

        # Handle the different response formats the API can return
        if isinstance(data, list):
            bundles  = data
            has_more = False
        elif isinstance(data, dict):
            bundles  = data.get("courses") or []
            page_ctx = data.get("page_context") or {}
            has_more = bool(page_ctx.get("has_more_page"))
        else:
            bundles  = []
            has_more = False

        # Flatten bundles → individual batch rows
        page_batches = flatten_bundles_to_batches(bundles)
        all_batches  = all_batches + page_batches

        # Stop if there are no more pages
        if not has_more or not bundles:
            break
        page_number = page_number + 1

    print("  API responded with " + str(len(all_batches)) + " batches")
    return all_batches


# ── STEP 2: SAVE TO BRONZE ────────────────────────────────────

def get_next_source_row_offset(conn):
    # Find the next available source_row number in Bronze
    cursor = conn.cursor()
    cursor.execute("SELECT COALESCE(MAX(source_row) + 1, 0) FROM bronze.course_batches_raw")
    next_row = cursor.fetchone()[0]
    cursor.close()
    return next_row


def upsert_bronze(conn, batches):
    # Get the starting row number offset for these new batch rows
    source_row_offset = get_next_source_row_offset(conn)
    cursor = conn.cursor()
    upserted_count = 0

    for i, batch in enumerate(batches):
        bundle_id = batch.get("bundle_id")
        batch_id  = batch.get("class_id")
        # Skip batches without the required IDs
        if not bundle_id or not batch_id:
            continue

        # Determine batch status: mb_archived=1 means the batch is Archived
        archived = clean_text(batch.get("mb_archived"))
        if archived == "1":
            batch_status = "Archived"
        else:
            batch_status = "Active"

        # Assign a unique source_row number to this batch
        source_row = source_row_offset + i

        cursor.execute(SQL_UPSERT_BRONZE, (
            source_row,
            clean_text(bundle_id), clean_text(batch.get("bundle_name")),
            clean_text(batch_id), clean_text(batch.get("class_name")),
            batch_status,
            clean_text(batch.get("start_date")), None,
            clean_text(batch.get("end_date")), None,
            clean_text(batch.get("tutor_id")), clean_text(batch.get("tutor_name")),
            clean_text(batch.get("admitted_students") or batch.get("registered_students")),
        ))
        if cursor.rowcount > 0:
            upserted_count = upserted_count + 1

    conn.commit()
    cursor.close()
    return upserted_count


# ── STEP 3: TRANSFORM TO SILVER ───────────────────────────────

def transform_to_silver(conn):
    # Read Bronze rows, skipping any that look like test batches
    read_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    read_cursor.execute("""
        SELECT * FROM bronze.course_batches_raw
        WHERE (batch_name IS NULL
               OR (batch_name NOT ILIKE '%test batch%'
                   AND batch_name NOT ILIKE '%test%batch%'))
        ORDER BY source_row
    """)
    bronze_rows = read_cursor.fetchall()
    read_cursor.close()

    write_cursor = conn.cursor()
    upserted_count = 0

    for row in bronze_rows:
        bundle_id = convert_to_bigint(row.get("bundle_id"))
        batch_id  = convert_to_bigint(row.get("batch_id"))
        # Skip rows where we cannot determine the IDs
        if bundle_id is None or batch_id is None:
            continue

        write_cursor.execute(SQL_UPSERT_SILVER, (
            bundle_id, clean_text(row.get("bundle_name")),
            batch_id, clean_text(row.get("batch_name")),
            clean_text(row.get("batch_status")),
            parse_timestamp(row.get("start_date")),
            parse_timestamp(row.get("end_date")),
            clean_text(row.get("tutor_name")),
            convert_to_int(row.get("admitted_students")),
        ))
        upserted_count = upserted_count + 1

    conn.commit()
    write_cursor.close()
    return upserted_count


# ── STEP 4: REBUILD MASTER TABLE ─────────────────────────────

def rebuild_master(conn):
    cursor = conn.cursor()
    # Delete all existing rows — we rebuild the whole table fresh each time
    cursor.execute("TRUNCATE TABLE silver.course_master")
    # Insert new rows by joining course_batches + course_metadata
    cursor.execute(SQL_REBUILD_MASTER)
    total_rows = cursor.rowcount
    conn.commit()
    cursor.close()
    return total_rows


def count_latest_batch_flags(conn):
    # Count how many bundles have their "latest batch" flag set to 1
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM silver.course_master WHERE is_latest_batch = 1")
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def count_include_in_course_count(conn):
    # Count how many rows are included in the main course count for dashboards
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM silver.course_master WHERE include_in_course_count = 1")
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== fetch_course_batches.py ===")
    print("")

    # Step 1: Call the API and get all batches
    print("Step 1: Calling batches API ...")
    batches = fetch_batches()

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
    bronze_count = upsert_bronze(conn, batches)
    print("  Bronze updated — " + str(bronze_count) + " rows upserted")

    # Step 3: Transform Bronze to Silver
    print("Step 3: Transforming to Silver ...")
    silver_count = transform_to_silver(conn)
    print("  Silver updated — " + str(silver_count) + " rows upserted")

    # Step 4: Rebuild the master table that Power BI reads
    print("Step 4: Rebuilding master table ...")
    master_count  = rebuild_master(conn)
    latest_count  = count_latest_batch_flags(conn)
    include_count = count_include_in_course_count(conn)
    print("  Master table rebuilt — " + str(master_count) + " rows")

    conn.close()

    print("")
    print("  API call              : SUCCESS")
    print("  Batches received      : " + str(len(batches)))
    print("  Bronze upserted       : " + str(bronze_count))
    print("  Silver upserted       : " + str(silver_count))
    print("  Master table rebuilt  : " + str(master_count) + " rows")
    print("  Latest batch flags    : " + str(latest_count) + " bundles marked")
    print("  Include in count      : " + str(include_count) + " rows = 1")

    # Return values so run_course_pipeline.py can use them
    return True, len(batches), silver_count, master_count


if __name__ == "__main__":
    main()
