# SCHEDULED API SCRIPT — Course Batches + Master Table
# Calls Edmingle batches API -> Bronze -> Silver -> Master
# Replaces manual download of batches_data.csv
# Schedule: run daily (batches change frequently)
# After this runs, Power BI master table is up to date
# API key rotates every 30 days -- update API_KEY when renewed

import datetime
import sys
import time
import psycopg2
import psycopg2.extras
import requests

# ── CONFIG ──────────────────────────────────────
DB_HOST      = "localhost"
DB_NAME      = "edmingle_analytics"
DB_USER      = "postgres"
DB_PASSWORD  = "Svyoma"
DB_PORT      = 5432
API_KEY      = "590605228a847624e065a76e986803fa"
ORG_ID       = "683"
INST_ID      = "483"
BATCHES_URL  = "https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch"
# ─────────────────────────────────────────────────

HEADERS = {'apikey': API_KEY, 'ORGID': ORG_ID}


# ── STEP 1: CALL THE API ─────────────────────────────────────────────────────

def fetch_batches():
    """Fetches and flattens all batches.

    API returns bundles, each with a nested 'batch' list.
    We flatten to one row per (bundle, batch) pair and promote
    bundle_id/bundle_name onto each row.
    """
    all_batches = []
    page = 1
    last_exc = None

    while True:
        params = {'page': page, 'org_id': ORG_ID}
        for attempt in range(3):
            try:
                r = requests.get(BATCHES_URL, headers=HEADERS, params=params, timeout=60)
                if r.status_code != 200:
                    print(f"  API error: HTTP {r.status_code} — {r.text[:200]}")
                    sys.exit(1)
                data = r.json()
                last_exc = None
                break
            except requests.exceptions.ConnectionError as e:
                last_exc = e
                print(f"  Connection error (attempt {attempt + 1}/3, page {page}): {e}")
                if attempt < 2:
                    time.sleep(5)
        else:
            print(f"  All retries exhausted on page {page}: {last_exc}")
            sys.exit(1)

        # Unwrap bundle list — API uses 'courses' key
        if isinstance(data, list):
            bundles = data
            has_more = False
        elif isinstance(data, dict):
            bundles = data.get('courses') or []
            pc = data.get('page_context') or {}
            has_more = bool(pc.get('has_more_page'))
        else:
            bundles = []
            has_more = False

        # Flatten: promote bundle fields onto each nested batch row
        for bundle in bundles:
            bid   = bundle.get('bundle_id')
            bname = bundle.get('bundle_name')
            batch_list = bundle.get('batch') or []
            if isinstance(batch_list, str):
                import json as _json
                try:
                    batch_list = _json.loads(batch_list)
                except Exception:
                    batch_list = []
            for b in batch_list:
                b['bundle_id']   = bid
                b['bundle_name'] = bname
                all_batches.append(b)

        if not has_more or not bundles:
            break
        page += 1

    print(f"  API responded with {len(all_batches)} batches")
    return all_batches


# ── STEP 2: SAVE RAW TO BRONZE ───────────────────────────────────────────────

def upsert_bronze(conn, batches):
    cur = conn.cursor()
    upserted = 0
    source_row_offset = _next_source_row(conn)

    for i, b in enumerate(batches):
        bid     = b.get('bundle_id')
        batchid = b.get('class_id')
        if not bid or not batchid:
            continue

        # Derive status: mb_archived=1 means Archived, else Active
        archived = _s(b.get('mb_archived'))
        status   = 'Archived' if archived == '1' else 'Active'

        source_row = source_row_offset + i
        cur.execute("""
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
                bundle_id    = EXCLUDED.bundle_id,
                bundle_name  = EXCLUDED.bundle_name,
                batch_id     = EXCLUDED.batch_id,
                batch_name   = EXCLUDED.batch_name,
                batch_status = EXCLUDED.batch_status,
                start_date   = EXCLUDED.start_date,
                end_date     = EXCLUDED.end_date,
                tutor_id     = EXCLUDED.tutor_id,
                tutor_name   = EXCLUDED.tutor_name,
                admitted_students = EXCLUDED.admitted_students,
                loaded_at    = NOW()
        """, (
            source_row,
            _s(bid),
            _s(b.get('bundle_name')),
            _s(batchid),
            _s(b.get('class_name')),
            status,
            _s(b.get('start_date')),
            None,
            _s(b.get('end_date')),
            None,
            _s(b.get('tutor_id')),
            _s(b.get('tutor_name')),
            _s(b.get('admitted_students') or b.get('registered_students')),
        ))
        if cur.rowcount > 0:
            upserted += 1

    conn.commit()
    cur.close()
    return upserted


def _next_source_row(conn):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(source_row) + 1, 0) FROM bronze.course_batches_raw")
    n = cur.fetchone()[0]
    cur.close()
    return n


def _s(val):
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ('', 'nan', 'None', 'NaN') else s


# ── STEP 3: TRANSFORM TO SILVER ──────────────────────────────────────────────

def transform_to_silver(conn):
    rcur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wcur = conn.cursor()

    rcur.execute("""
        SELECT * FROM bronze.course_batches_raw
        WHERE (batch_name IS NULL
               OR (batch_name NOT ILIKE '%%test batch%%'
                   AND batch_name NOT ILIKE '%%test%%batch%%'))
        ORDER BY source_row
    """)
    rows = rcur.fetchall()
    rcur.close()

    upserted = 0
    skipped_test = 0

    rcur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    rcur2.execute("SELECT COUNT(*) AS n FROM bronze.course_batches_raw")
    total_bronze = rcur2.fetchone()['n']
    skipped_test = total_bronze - len(rows)
    rcur2.close()

    for row in rows:
        bundle_id = _bigint(row.get('bundle_id'))
        batch_id  = _bigint(row.get('batch_id'))
        if bundle_id is None or batch_id is None:
            continue

        wcur.execute("""
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
        """, (
            bundle_id,
            _s(row.get('bundle_name')),
            batch_id,
            _s(row.get('batch_name')),
            _s(row.get('batch_status')),
            _parse_ts(row.get('start_date')),
            _parse_ts(row.get('end_date')),
            _s(row.get('tutor_name')),
            _int(row.get('admitted_students')),
        ))
        upserted += 1

    conn.commit()
    wcur.close()
    return upserted, skipped_test


def _bigint(val):
    if val is None:
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _int(val):
    if val is None:
        return None
    try:
        return int(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def _parse_ts(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'):
        return None
    s = str(val).strip()
    # Unix timestamp (integer or float)
    try:
        ts = float(s)
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    except (ValueError, OSError):
        pass
    # ISO / other string formats
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d',
                '%d-%m-%Y %I:%M %p IST', '%d/%m/%Y', '%m/%d/%Y'):
        try:
            dt = datetime.datetime.strptime(s.split('+')[0].strip(), fmt)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            continue
    return None


# ── STEP 4: BUILD MASTER TABLE ───────────────────────────────────────────────

def rebuild_master(conn):
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE silver.course_master")

    cur.execute("""
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
            cb.bundle_id,
            cb.bundle_name,
            cb.batch_id,
            cb.batch_name,
            cb.batch_status,
            cb.start_date_ist::DATE,
            cb.end_date_ist::DATE,
            cb.tutor_name,
            cb.admitted_students,
            cm.course_name,
            cm.subject,
            cm.course_type,
            cm.term_of_course,
            cm.position_in_funnel,
            cm.adhyayanam_category,
            cm.sss_category,
            cm.viniyoga,
            cm.division,
            cm.status                                 AS catalogue_status,
            cm.status                                 AS final_status,
            CASE WHEN lb.rn = 1 THEN 1 ELSE 0 END    AS is_latest_batch,
            CASE
                WHEN cm.course_division = 'Course'
                 AND cb.bundle_id IS NOT NULL
                 AND cm.status IN ('Completed', 'Ongoing', 'Upcoming')
                THEN 1 ELSE 0
            END                                       AS include_in_course_count,
            ''                                        AS status_adjustment_reason,
            COALESCE(hb.has_b, 0)                     AS has_batch,
            NOW()
        FROM silver.course_batches cb
        LEFT JOIN silver.course_metadata cm  ON cb.bundle_id = cm.bundle_id
        LEFT JOIN latest_batch lb
            ON cb.bundle_id = lb.bundle_id AND cb.batch_id = lb.batch_id
        LEFT JOIN has_batch_flag hb ON cb.bundle_id = hb.bundle_id

        UNION ALL

        -- Courses in catalogue with no batches at all
        SELECT
            cm.bundle_id,
            NULL, NULL, NULL, NULL,
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
    """)

    total = cur.rowcount
    conn.commit()
    cur.close()
    return total


def count_silver_batches(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.course_batches")
    n = cur.fetchone()[0]
    cur.close()
    return n


def count_latest_flags(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.course_master WHERE is_latest_batch = 1")
    n = cur.fetchone()[0]
    cur.close()
    return n


def count_include_flags(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.course_master WHERE include_in_course_count = 1")
    n = cur.fetchone()[0]
    cur.close()
    return n


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("=== fetch_course_batches.py ===")
    print()

    print("Step 1: Calling batches API ...")
    batches = fetch_batches()

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )

    print("Step 2: Saving raw data to Bronze ...")
    bronze_n = upsert_bronze(conn, batches)
    print(f"  Bronze updated -- {bronze_n} rows upserted")

    print("Step 3: Transforming to Silver ...")
    silver_n, skipped_test = transform_to_silver(conn)
    print(f"  Silver updated -- {silver_n} rows upserted")

    print("Step 4: Rebuilding master table ...")
    master_n   = rebuild_master(conn)
    latest_n   = count_latest_flags(conn)
    include_n  = count_include_flags(conn)
    print(f"  Master table rebuilt -- {master_n} rows")

    conn.close()

    print()
    print(f"  API call              : SUCCESS")
    print(f"  Batches received      : {len(batches)}")
    print(f"  Test batches skipped  : {skipped_test}")
    print(f"  Bronze upserted       : {bronze_n}")
    print(f"  Silver upserted       : {silver_n}")
    print(f"  Master table rebuilt  : {master_n} rows")
    print(f"  Latest batch flags    : {latest_n} bundles marked")
    print(f"  Include in count      : {include_n} rows = 1")

    return True, len(batches), silver_n, master_n


if __name__ == '__main__':
    main()
