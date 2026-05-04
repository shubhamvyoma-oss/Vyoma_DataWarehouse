# ONE-TIME MIGRATION — Course metadata Silver transform
# Reads from Bronze, cleans and types, writes to Silver

import datetime
import os
import psycopg2
import psycopg2.extras

# ── CONFIG ──────────────────────────────────────
DB_HOST      = "localhost"
DB_NAME      = "edmingle_analytics"
DB_USER      = "postgres"
DB_PASSWORD  = "Svyoma"
DB_PORT      = 5432
# ─────────────────────────────────────────────────


def _int(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'):
        return None
    try:
        return int(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def _float(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN', '#VALUE!',
                                            'Unavailable', '#DIV/0!'):
        return None
    try:
        return float(str(val).replace(',', '').strip())
    except (ValueError, TypeError):
        return None


def _bigint(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'):
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None


def _text(val):
    if val is None:
        return None
    s = str(val).strip()
    return None if s in ('', 'nan', 'None', 'NaN') else s


def _date(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN',
                                            'Unavailable', '#VALUE!'):
        return None
    s = str(val).strip()
    for fmt in ('%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%m-%d-%Y'):
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _unix_ts(val):
    if val is None or str(val).strip() in ('', 'nan', 'None', 'NaN'):
        return None
    try:
        ts = float(str(val).strip())
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


# ── silver.course_metadata ────────────────────────────────────────────────────

def transform_course_metadata(conn):
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wcur = conn.cursor()

    cur.execute("SELECT * FROM bronze.course_catalogue_raw ORDER BY source_row")
    rows = cur.fetchall()
    cur.close()

    upserted = 0
    for row in rows:
        bundle_id = _bigint(row.get('bundle_id'))
        if not bundle_id:
            continue

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
                imported_at         = EXCLUDED.imported_at
        """, (
            bundle_id,
            _text(row.get('course_name')),
            _text(row.get('subject')),
            _text(row.get('type')),
            _text(row.get('status')),
            _text(row.get('term_of_course')),
            _text(row.get('position_in_funnel')),
            _text(row.get('adhyayanam_category')),
            _text(row.get('sss_category')),
            _text(row.get('viniyoga')),
            _text(row.get('course_division')),
            _text(row.get('division')),
            _text(row.get('level')),
            _text(row.get('language')),
            _int(row.get('num_students')),
            _float(row.get('cost')),
        ))
        upserted += 1

    conn.commit()
    wcur.close()
    return upserted


# ── silver.course_lifecycle ───────────────────────────────────────────────────

def transform_course_lifecycle(conn):
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wcur = conn.cursor()

    cur.execute("SELECT * FROM bronze.course_lifecycle_raw ORDER BY source_row")
    rows = cur.fetchall()
    cur.close()

    upserted = 0
    for row in rows:
        course_id  = _bigint(row.get('course_id'))
        batch_name = _text(row.get('batch_name'))
        if not course_id or not batch_name:
            continue

        wcur.execute("""
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
        """, (
            course_id,
            _text(row.get('course_name')),
            batch_name,
            _text(row.get('type_of_launch')),
            _text(row.get('status')),
            _text(row.get('subject')),
            _text(row.get('position_in_funnel')),
            _text(row.get('samskritadhyayana_model')),
            _text(row.get('term_of_course')),
            _text(row.get('sss_category')),
            _text(row.get('persona')),
            _date(row.get('first_class_date')),
            _date(row.get('last_class_and_valedictory_date')),
            _int(row.get('enrolments_on_the_day_of_first_class')),
            _int(row.get('enrolments_on_last_day')),
            _float(row.get('average_attendance_of_all_classes')),
            _int(row.get('total_no_of_classes_held')),
            _int(row.get('total_students_certified')),
            _float(row.get('pass_percentage_total_certified_vs_total_assessment_attendees')),
            _float(row.get('overall_course_rating')),
            _bigint(row.get('batch_id')),
        ))
        upserted += 1

    conn.commit()
    wcur.close()
    return upserted


# ── silver.course_batches ─────────────────────────────────────────────────────

def transform_course_batches(conn):
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    wcur = conn.cursor()

    cur.execute("SELECT * FROM bronze.course_batches_raw ORDER BY source_row")
    rows = cur.fetchall()
    cur.close()

    upserted = 0
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
        """, (
            bundle_id,
            _text(row.get('bundle_name')),
            batch_id,
            _text(row.get('batch_name')),
            _text(row.get('batch_status')),
            _unix_ts(row.get('start_date')),
            _unix_ts(row.get('end_date')),
            _text(row.get('tutor_name')),
            _int(row.get('admitted_students')),
        ))
        upserted += 1

    conn.commit()
    wcur.close()
    return upserted


def count_rows(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    n = cur.fetchone()[0]
    cur.close()
    return n


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )

    print("Transforming bronze.course_catalogue_raw -> silver.course_metadata ...", flush=True)
    n1 = transform_course_metadata(conn)
    print(f"  {n1} rows processed", flush=True)

    print("Transforming bronze.course_lifecycle_raw -> silver.course_lifecycle ...", flush=True)
    n2 = transform_course_lifecycle(conn)
    print(f"  {n2} rows processed", flush=True)

    print("Transforming bronze.course_batches_raw -> silver.course_batches ...", flush=True)
    n3 = transform_course_batches(conn)
    print(f"  {n3} rows processed", flush=True)

    conn.close()

    print()
    for tbl in ('silver.course_metadata', 'silver.course_lifecycle', 'silver.course_batches'):
        conn2 = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                                  user=DB_USER, password=DB_PASSWORD)
        n = count_rows(conn2, tbl)
        conn2.close()
        print(f"  {tbl:<45} : {n:,} rows")


if __name__ == '__main__':
    main()
