# SCHEDULED API SCRIPT — Course Catalogue
# Calls Edmingle catalogue API -> Bronze -> Silver
# Replaces manual download of course_catalogue_data.csv
# Schedule: run weekly (course catalogue changes slowly)
# API key rotates every 30 days -- update API_KEY when renewed

import sys
import time
import psycopg2
import psycopg2.extras
import requests

# ── CONFIG ──────────────────────────────────────
DB_HOST       = "localhost"
DB_NAME       = "edmingle_analytics"
DB_USER       = "postgres"
DB_PASSWORD   = "Svyoma"
DB_PORT       = 5432
API_KEY       = "590605228a847624e065a76e986803fa"
ORG_ID        = "683"
INST_ID       = "483"
CATALOGUE_URL = f"https://vyoma-api.edmingle.com/nuSource/api/v1/institute/{INST_ID}/courses/catalogue"
# ─────────────────────────────────────────────────

HEADERS = {'apikey': API_KEY, 'ORGID': ORG_ID}

VALID_STATUSES = {'Completed', 'Ongoing', 'Upcoming'}


# ── STEP 1: CALL THE API ─────────────────────────────────────────────────────

def fetch_catalogue():
    last_exc = None
    for attempt in range(3):
        try:
            r = requests.get(CATALOGUE_URL, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                print(f"  API error: HTTP {r.status_code} — {r.text[:200]}")
                sys.exit(1)
            data = r.json()
            break
        except requests.exceptions.ConnectionError as e:
            last_exc = e
            print(f"  Connection error (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
    else:
        print(f"  All retries exhausted: {last_exc}")
        sys.exit(1)

    # Unwrap response envelope
    if isinstance(data, list):
        courses = data
    elif isinstance(data, dict):
        for key in ('response', 'data', 'courses', 'bundles', 'result', 'items'):
            if key in data and isinstance(data[key], list):
                courses = data[key]
                break
        else:
            # Use the whole dict values if it's a dict of course objects
            courses = list(data.values()) if data else []
    else:
        print(f"  Unexpected response type: {type(data)}")
        sys.exit(1)

    if not courses:
        print("  WARNING: API returned 0 courses — nothing to load")
        sys.exit(0)

    print(f"  API responded with {len(courses)} courses")
    return courses


# ── STEP 2: SAVE RAW TO BRONZE ───────────────────────────────────────────────

def _ensure_raw_json_column(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'bronze'
          AND table_name   = 'course_catalogue_raw'
          AND column_name  = 'raw_json'
    """)
    if not cur.fetchone():
        cur.execute("ALTER TABLE bronze.course_catalogue_raw ADD COLUMN raw_json JSONB")
        conn.commit()
    cur.close()


def upsert_bronze(conn, courses):
    _ensure_raw_json_column(conn)
    cur = conn.cursor()
    upserted = 0

    for course in courses:
        bid = course.get('bundle_id') or course.get('Bundle id') or course.get('id')
        if not bid:
            continue

        cur.execute("""
            INSERT INTO bronze.course_catalogue_raw (
                source_row,
                bundle_id, course_name, course_description, cost,
                num_students, tutors, course_url, subject, level,
                language, type, course_division, status,
                sss_category, viniyoga, adhyayanam_category,
                term_of_course, position_in_funnel, division,
                raw_json
            ) VALUES (
                %s,
                %s, %s, %s, %s,
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
        """, (
            int(bid),
            str(bid),
            _s(course.get('Course Name') or course.get('course_name') or course.get('name')),
            _s(course.get('Course Description') or course.get('course_description') or course.get('description')),
            _s(course.get('Cost') or course.get('cost') or course.get('price')),
            _s(course.get('Num Students') or course.get('num_students') or course.get('student_count')),
            _s(course.get('Tutors') or course.get('tutors') or course.get('tutor_name')),
            _s(course.get('Course URL') or course.get('course_url') or course.get('url')),
            _s(course.get('Subject') or course.get('subject')),
            _s(course.get('Level') or course.get('level')),
            _s(course.get('Language') or course.get('language')),
            _s(course.get('Type') or course.get('type') or course.get('course_type')),
            _s(course.get('Course Division') or course.get('course_division')),
            _s(course.get('Status') or course.get('status')),
            _s(course.get('SSS Category') or course.get('sss_category')),
            _s(course.get('Viniyoga') or course.get('viniyoga')),
            _s(course.get('Adhyayanam Category') or course.get('adhyayanam_category')),
            _s(course.get('Term of Course') or course.get('term_of_course')),
            _s(course.get('Position in Funnel') or course.get('position_in_funnel')),
            _s(course.get('Division') or course.get('division')),
            psycopg2.extras.Json(course),
        ))
        if cur.rowcount > 0:
            upserted += 1

    conn.commit()
    cur.close()
    return upserted


def _s(val):
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ('', 'nan', 'None', 'NaN') else s


# ── STEP 3: TRANSFORM TO SILVER ──────────────────────────────────────────────

def transform_to_silver(conn):
    rcur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wcur = conn.cursor()

    rcur.execute("SELECT * FROM bronze.course_catalogue_raw ORDER BY source_row")
    rows = rcur.fetchall()
    rcur.close()

    upserted = 0
    skip_division = 0
    skip_bundle = 0
    seen_bundle_ids = {}

    for row in rows:
        # Rule 3 — valid bundle_id
        try:
            bid = int(float(str(row['bundle_id']).strip()))
            if bid == 0:
                raise ValueError
        except (ValueError, TypeError):
            skip_bundle += 1
            continue

        # Rule 1 — Course Division must be "Course"
        div = _s(row.get('course_division'))
        if div and div.lower() != 'course':
            skip_division += 1
            continue

        # Rule 2 — Normalize status
        raw_status = _s(row.get('status'))
        status = raw_status if raw_status in VALID_STATUSES else None

        # Rule 4 — Deduplicate: keep row with non-null status
        if bid in seen_bundle_ids:
            prev_status = seen_bundle_ids[bid]
            if prev_status is not None:
                continue
        seen_bundle_ids[bid] = status

        wcur.execute("""
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
        """, (
            bid,
            _s(row.get('course_name')),
            _s(row.get('subject')),
            _s(row.get('type')),
            status,
            _s(row.get('term_of_course')),
            _s(row.get('position_in_funnel')),
            _s(row.get('adhyayanam_category')),
            _s(row.get('sss_category')),
            _s(row.get('viniyoga')),
            div,
            _s(row.get('division')),
            _s(row.get('level')),
            _s(row.get('language')),
            _int(row.get('num_students')),
            _float(row.get('cost')),
        ))
        upserted += 1

    conn.commit()
    wcur.close()
    return upserted, skip_division, skip_bundle


def _int(val):
    if val is None:
        return None
    try:
        return int(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def _float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=== fetch_course_catalogue.py ===")
    print()

    print("Step 1: Calling catalogue API ...")
    courses = fetch_catalogue()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )

    print("Step 2: Saving raw data to Bronze ...")
    bronze_n = upsert_bronze(conn, courses)
    print(f"  Bronze updated -- {bronze_n} rows upserted")

    print("Step 3: Transforming to Silver ...")
    silver_n, skip_div, skip_bid = transform_to_silver(conn)
    print(f"  Silver updated -- {silver_n} rows upserted")

    conn.close()

    print()
    print("  API call                      : SUCCESS")
    print(f"  Courses received              : {len(courses)}")
    print(f"  Bronze upserted               : {bronze_n}")
    print(f"  Silver upserted               : {silver_n}")
    print(f"  Skipped (not Course division) : {skip_div}")
    print(f"  Skipped (invalid bundle_id)   : {skip_bid}")

    return True, len(courses), silver_n


if __name__ == '__main__':
    main()
