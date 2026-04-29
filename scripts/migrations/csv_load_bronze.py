# =============================================================================
# FILE    : scripts/migrations/csv_load_bronze.py
# PROJECT : Edmingle Webhook Data Pipeline — Vyoma Samskrta Pathasala
# PURPOSE : One-off script. Loads the two Edmingle CSV exports into Bronze.
#           Run ONCE before csv_backfill_transactions.py.
#           Safe to re-run: ON CONFLICT (source_row) DO NOTHING.
# DATE    : 2026-04-29
#
# USAGE (from project root):
#   python scripts/migrations/csv_load_bronze.py
#
# PREREQUISITES:
#   1. Run database/setup.sql in pgAdmin to create the Bronze CSV tables.
#   2. Ensure CSV files exist at:
#        CSV files/studentexport.csv
#        CSV files/studentCoursesEnrolled.csv
# =============================================================================

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'), override=False)

import pandas as pd
import psycopg2
import psycopg2.extras

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', 5432))
DB_NAME = os.getenv('DB_NAME', 'edmingle_analytics')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', '')

CSV_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'CSV files')

# ---------------------------------------------------------------------------
# Column name mapping: CSV header → SQL column name in bronze.studentexport_raw
# Preserves the original CSV order. source_row (pandas index) is added by code.
# ---------------------------------------------------------------------------
STUDENT_EXPORT_COLS = [
    ('#',                                                  'row_number'),
    ('Name',                                               'name'),
    ('Email',                                              'email'),
    ('Registration Number',                                'registration_number'),
    ('Contact Number Dial Code',                           'contact_number_dial_code'),
    ('Contact Number',                                     'contact_number'),
    ('Alternate Contact Number Dial Code',                 'alternate_contact_number_dial_code'),
    ('Alternate Contact Number',                           'alternate_contact_number'),
    ('Date Of Birth',                                      'date_of_birth'),
    ('Parent Name',                                        'parent_name'),
    ('Parent Contact',                                     'parent_contact'),
    ('Parent Email',                                       'parent_email'),
    ('Address',                                            'address'),
    ('city',                                               'city'),
    ('State',                                              'state'),
    ('Standard',                                           'standard'),
    ('Date Created',                                       'date_created'),
    ('Username',                                           'username'),
    ('Gender',                                             'gender'),
    ('Status',                                             'status'),
    ('Username.1',                                         'username_1'),
    ('Why do you want to study Sanskrit?',                 'why_study_sanskrit'),
    ('User Nice Name',                                     'user_nice_name'),
    ('User Last Name',                                     'user_last_name'),
    ('Would you like to teach Sanskrit through our portal?', 'would_like_to_teach'),
    ('Teaching Experience, if any',                        'teaching_experience'),
    ('Is Sanskrit your mainstream education',              'is_mainstream_education'),
    ('Objective',                                          'objective'),
    ('User Age',                                           'user_age'),
    ('Persona',                                            'persona'),
    ('Objective Package',                                  'objective_package'),
    ('Time per week (In Hours)',                           'time_per_week_hours'),
    ('Age_',                                               'age_'),
    ('Facebook profile URL',                               'facebook_profile_url'),
    ('Instagram profile URL',                              'instagram_profile_url'),
    ('Pinterest profile URL',                              'pinterest_profile_url'),
    ('SoundCloud profile URL',                             'soundcloud_profile_url'),
    ('Tumblr profile URL',                                 'tumblr_profile_url'),
    ('YouTube profile URL',                                'youtube_profile_url'),
    ('Wikipedia page about you (if one exists)',           'wikipedia_url'),
    ('Twitter username (without @)',                       'twitter_username'),
    ('GST Number',                                         'gst_number'),
    ('MySpace profile URL',                                'myspace_profile_url'),
    ('International Phone Number',                         'international_phone_number'),
    ('Website',                                            'website'),
    ('Educational Qualification',                          'educational_qualification'),
    ('LinkedIn profile URL',                               'linkedin_profile_url'),
    ('Age ',                                               'age_v2'),
    ('Gender_',                                            'gender_'),
    ('Sanskrit Qualification',                             'sanskrit_qualification'),
    ('Areas of Interest in Sanskrit',                      'areas_of_interest'),
    ('Studying Sanskrit Currently? If yes, give details',  'studying_sanskrit_currently'),
    ('What is your current education status?',             'current_education_status'),
    ('Country Name',                                       'country_name'),
]

# studentCoursesEnrolled.csv columns already match the SQL table column names
ENROLLMENT_COLS = [
    'user_id', 'name', 'email', 'class_id', 'class_name', 'tutor_name',
    'total_classes', 'present', 'absent', 'late', 'excused',
    'start_date', 'end_date', 'master_batch_id', 'master_batch_name',
    'classusers_start_date', 'classusers_end_date', 'batch_status',
    'cu_status', 'cu_state', 'institution_bundle_id', 'archived_at', 'bundle_id',
]


def _none_if_nan(val):
    if val is None:
        return None
    try:
        import math
        if isinstance(val, float) and math.isnan(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def load_student_export(conn):
    path = os.path.join(CSV_DIR, 'studentexport.csv')
    # skiprows=1 skips the decorative title row that appears before the header
    df = pd.read_csv(path, skiprows=1, dtype=str)

    csv_cols  = [c for c, _ in STUDENT_EXPORT_COLS]
    sql_cols  = [s for _, s in STUDENT_EXPORT_COLS]

    sql_col_list = ', '.join(['source_row'] + sql_cols)
    placeholders = ', '.join(['%s'] * (1 + len(sql_cols)))

    rows = []
    for idx, row in df.iterrows():
        vals = [idx]  # source_row = 0-based pandas index
        for csv_col in csv_cols:
            vals.append(_none_if_nan(row.get(csv_col)))
        rows.append(tuple(vals))

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bronze.studentexport_raw")
    before = cur.fetchone()[0]
    psycopg2.extras.execute_values(
        cur,
        f"""
        INSERT INTO bronze.studentexport_raw ({sql_col_list})
        VALUES %s
        ON CONFLICT (source_row) DO NOTHING
        """,
        rows,
        page_size=500,
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM bronze.studentexport_raw")
    after = cur.fetchone()[0]
    cur.close()

    total    = len(rows)
    inserted = after - before
    skipped  = total - inserted
    print(f"bronze.studentexport_raw:              {total:,} rows in CSV")
    print(f"  inserted: {inserted:,}   skipped (already loaded): {skipped:,}")
    return total


def load_enrollments(conn):
    path = os.path.join(CSV_DIR, 'studentCoursesEnrolled.csv')
    df = pd.read_csv(path, dtype=str)

    sql_col_list = ', '.join(['source_row'] + ENROLLMENT_COLS)
    placeholders = ', '.join(['%s'] * (1 + len(ENROLLMENT_COLS)))

    rows = []
    for idx, row in df.iterrows():
        vals = [idx]
        for col in ENROLLMENT_COLS:
            vals.append(_none_if_nan(row.get(col)))
        rows.append(tuple(vals))

    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM bronze.student_courses_enrolled_raw")
    before = cur.fetchone()[0]
    psycopg2.extras.execute_values(
        cur,
        f"""
        INSERT INTO bronze.student_courses_enrolled_raw ({sql_col_list})
        VALUES %s
        ON CONFLICT (source_row) DO NOTHING
        """,
        rows,
        page_size=500,
    )
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM bronze.student_courses_enrolled_raw")
    after = cur.fetchone()[0]
    cur.close()

    total    = len(rows)
    inserted = after - before
    skipped  = total - inserted
    print(f"bronze.student_courses_enrolled_raw:   {total:,} rows in CSV")
    print(f"  inserted: {inserted:,}   skipped (already loaded): {skipped:,}")
    return total


def main():
    print("=" * 60)
    print("CSV Bronze Load — 2026-04-29")
    print("=" * 60)

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
    )
    try:
        load_student_export(conn)
        print()
        load_enrollments(conn)
    finally:
        conn.close()

    print()
    print("Done. Run csv_backfill_transactions.py next.")


if __name__ == '__main__':
    main()
