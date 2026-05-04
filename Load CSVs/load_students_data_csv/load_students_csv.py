# ============================================================
# 08 — LOAD STUDENTS CSV
# ============================================================
# What it does: Reads two CSV files from the "CSV files" folder
#               and loads them into Bronze tables in the database.
#               File 1: studentexport.csv     → bronze.studentexport_raw
#               File 2: studentCoursesEnrolled.csv → bronze.student_courses_enrolled_raw
#
# Why we need it: These CSVs are a historical one-time export from
#                 Edmingle. We load them first so that script 11
#                 (backfill_transactions) can build Silver users
#                 and transactions from them.
#                 Run this ONCE before script 11.
#
# How to run:
#   python 08_load_students_csv/load_students_csv.py
#
# What to check after:
#   - bronze.studentexport_raw should have rows
#   - bronze.student_courses_enrolled_raw should have rows
#   - Run 12_check_db_counts to verify
# ============================================================

import os
import sys
import math
import psycopg2
import psycopg2.extras

# pandas is used just for reading the CSV files
import pandas as pd


# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# The CSV files folder is two levels up from this script's folder
# This script lives at: 08_load_students_csv/load_students_csv.py
# CSV files live at:    CSV files/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CSV_DIR = os.path.join(PROJECT_DIR, "CSV files")

# Column mapping for studentexport.csv
# Format: (CSV header name, SQL column name in the database)
# This handles renaming so messy header names become clean column names
STUDENT_EXPORT_COLUMNS = [
    ("#",                                                  "row_number"),
    ("Name",                                               "name"),
    ("Email",                                              "email"),
    ("Registration Number",                                "registration_number"),
    ("Contact Number Dial Code",                           "contact_number_dial_code"),
    ("Contact Number",                                     "contact_number"),
    ("Alternate Contact Number Dial Code",                 "alternate_contact_number_dial_code"),
    ("Alternate Contact Number",                           "alternate_contact_number"),
    ("Date Of Birth",                                      "date_of_birth"),
    ("Parent Name",                                        "parent_name"),
    ("Parent Contact",                                     "parent_contact"),
    ("Parent Email",                                       "parent_email"),
    ("Address",                                            "address"),
    ("city",                                               "city"),
    ("State",                                              "state"),
    ("Standard",                                           "standard"),
    ("Date Created",                                       "date_created"),
    ("Username",                                           "username"),
    ("Gender",                                             "gender"),
    ("Status",                                             "status"),
    ("Username.1",                                         "username_1"),
    ("Why do you want to study Sanskrit?",                 "why_study_sanskrit"),
    ("User Nice Name",                                     "user_nice_name"),
    ("User Last Name",                                     "user_last_name"),
    ("Would you like to teach Sanskrit through our portal?", "would_like_to_teach"),
    ("Teaching Experience, if any",                        "teaching_experience"),
    ("Is Sanskrit your mainstream education",              "is_mainstream_education"),
    ("Objective",                                          "objective"),
    ("User Age",                                           "user_age"),
    ("Persona",                                            "persona"),
    ("Objective Package",                                  "objective_package"),
    ("Time per week (In Hours)",                           "time_per_week_hours"),
    ("Age_",                                               "age_"),
    ("Facebook profile URL",                               "facebook_profile_url"),
    ("Instagram profile URL",                              "instagram_profile_url"),
    ("Pinterest profile URL",                              "pinterest_profile_url"),
    ("SoundCloud profile URL",                             "soundcloud_profile_url"),
    ("Tumblr profile URL",                                 "tumblr_profile_url"),
    ("YouTube profile URL",                                "youtube_profile_url"),
    ("Wikipedia page about you (if one exists)",           "wikipedia_url"),
    ("Twitter username (without @)",                       "twitter_username"),
    ("GST Number",                                         "gst_number"),
    ("MySpace profile URL",                                "myspace_profile_url"),
    ("International Phone Number",                         "international_phone_number"),
    ("Website",                                            "website"),
    ("Educational Qualification",                          "educational_qualification"),
    ("LinkedIn profile URL",                               "linkedin_profile_url"),
    ("Age ",                                               "age_v2"),
    ("Gender_",                                            "gender_"),
    ("Sanskrit Qualification",                             "sanskrit_qualification"),
    ("Areas of Interest in Sanskrit",                      "areas_of_interest"),
    ("Studying Sanskrit Currently? If yes, give details",  "studying_sanskrit_currently"),
    ("What is your current education status?",             "current_education_status"),
    ("Country Name",                                       "country_name"),
]

# These column names in studentCoursesEnrolled.csv already match the DB column names
ENROLLMENT_COLUMNS = [
    "user_id", "name", "email", "class_id", "class_name", "tutor_name",
    "total_classes", "present", "absent", "late", "excused",
    "start_date", "end_date", "master_batch_id", "master_batch_name",
    "classusers_start_date", "classusers_end_date", "batch_status",
    "cu_status", "cu_state", "institution_bundle_id", "archived_at", "bundle_id",
]


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def none_if_nan(value):
    # Convert NaN (pandas missing value) to Python None so the DB gets NULL
    if value is None:
        return None
    try:
        if isinstance(value, float) and math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def connect_to_database():
    # Open a single connection to the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        sys.exit(1)


def count_rows_in_table(conn, table_name):
    # Return the number of rows currently in a given table
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# ── STEP 1: LOAD studentexport.csv → bronze.studentexport_raw ─

def build_student_export_rows(file_path):
    # Read the CSV and build a list of tuples ready to INSERT
    # skiprows=1 skips the decorative title row above the real header row
    dataframe = pd.read_csv(file_path, skiprows=1, dtype=str)

    # Separate the CSV column names from the SQL column names
    csv_column_names = [csv_col for csv_col, sql_col in STUDENT_EXPORT_COLUMNS]

    all_rows = []
    for row_index, row_data in dataframe.iterrows():
        # Start each row with the row index (used as source_row primary key)
        row_values = [row_index]
        # Add each column value in the order defined by STUDENT_EXPORT_COLUMNS
        for csv_col in csv_column_names:
            row_values.append(none_if_nan(row_data.get(csv_col)))
        all_rows.append(tuple(row_values))

    return all_rows


def load_student_export(conn):
    # Load studentexport.csv into bronze.studentexport_raw
    file_path = os.path.join(CSV_DIR, "studentexport.csv")
    print("Step 1: Loading " + file_path + " ...")

    all_rows = build_student_export_rows(file_path)

    # Build the SQL column list from the mapping (all SQL column names)
    sql_column_names = [sql_col for csv_col, sql_col in STUDENT_EXPORT_COLUMNS]
    sql_column_list = "source_row, " + ", ".join(sql_column_names)

    # Record how many rows are already in the table before we insert
    count_before = count_rows_in_table(conn, "bronze.studentexport_raw")

    cursor = conn.cursor()

    # execute_values is faster than looping over cursor.execute() one row at a time
    # ON CONFLICT DO NOTHING means we skip rows that are already loaded (safe to re-run)
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO bronze.studentexport_raw (" + sql_column_list + ") "
        "VALUES %s "
        "ON CONFLICT (source_row) DO NOTHING",
        all_rows,
        page_size=500,
    )
    conn.commit()
    cursor.close()

    count_after = count_rows_in_table(conn, "bronze.studentexport_raw")
    total_in_csv = len(all_rows)
    inserted_count = count_after - count_before
    skipped_count = total_in_csv - inserted_count

    print("  Rows in CSV          : " + str(total_in_csv))
    print("  Inserted             : " + str(inserted_count))
    print("  Skipped (duplicates) : " + str(skipped_count))
    return total_in_csv


# ── STEP 2: LOAD studentCoursesEnrolled.csv → bronze.student_courses_enrolled_raw ─

def build_enrollment_rows(file_path):
    # Read the enrollment CSV and build a list of tuples ready to INSERT
    dataframe = pd.read_csv(file_path, dtype=str)

    all_rows = []
    for row_index, row_data in dataframe.iterrows():
        # Start each row with the row index (primary key for ON CONFLICT)
        row_values = [row_index]
        for col_name in ENROLLMENT_COLUMNS:
            row_values.append(none_if_nan(row_data.get(col_name)))
        all_rows.append(tuple(row_values))

    return all_rows


def load_enrollments(conn):
    # Load studentCoursesEnrolled.csv into bronze.student_courses_enrolled_raw
    file_path = os.path.join(CSV_DIR, "studentCoursesEnrolled.csv")
    print("Step 2: Loading " + file_path + " ...")

    all_rows = build_enrollment_rows(file_path)

    sql_column_list = "source_row, " + ", ".join(ENROLLMENT_COLUMNS)

    count_before = count_rows_in_table(conn, "bronze.student_courses_enrolled_raw")

    cursor = conn.cursor()
    psycopg2.extras.execute_values(
        cursor,
        "INSERT INTO bronze.student_courses_enrolled_raw (" + sql_column_list + ") "
        "VALUES %s "
        "ON CONFLICT (source_row) DO NOTHING",
        all_rows,
        page_size=500,
    )
    conn.commit()
    cursor.close()

    count_after = count_rows_in_table(conn, "bronze.student_courses_enrolled_raw")
    total_in_csv = len(all_rows)
    inserted_count = count_after - count_before
    skipped_count = total_in_csv - inserted_count

    print("  Rows in CSV          : " + str(total_in_csv))
    print("  Inserted             : " + str(inserted_count))
    print("  Skipped (duplicates) : " + str(skipped_count))
    return total_in_csv


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== load_students_csv.py ===")
    print("")

    # Confirm that the CSV folder actually exists before trying to read files
    if not os.path.isdir(CSV_DIR):
        print("ERROR: CSV folder not found at: " + CSV_DIR)
        print("  Make sure the 'CSV files' folder is in the project root.")
        sys.exit(1)

    conn = connect_to_database()

    # Load both CSV files one at a time
    total_students   = load_student_export(conn)
    print("")
    total_enrollments = load_enrollments(conn)

    conn.close()

    print("")
    print("  LOAD COMPLETE")
    print("  " + "-" * 41)
    print("  studentexport.csv rows          : " + str(total_students))
    print("  studentCoursesEnrolled.csv rows : " + str(total_enrollments))
    print("")
    print("  Next step: run 11_backfill_transactions/backfill_transactions.py")
    print("")


if __name__ == "__main__":
    main()
