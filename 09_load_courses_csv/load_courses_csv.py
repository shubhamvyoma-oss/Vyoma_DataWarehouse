# ============================================================
# 09 — LOAD COURSES CSV
# ============================================================
# What it does: Reads three course-related CSV files from the
#               "CSV files" folder and loads each one into its
#               Bronze table in the database.
#
#   File 1: course_catalogue_data.csv
#           → bronze.course_catalogue_raw
#
#   File 2: Elearning MIS Merged Tracker - Course Lifecycle (1).csv
#           → bronze.course_lifecycle_raw
#
#   File 3: batches_data.csv
#           → bronze.course_batches_raw
#
# Why we need it: These CSVs are a historical one-time export.
#                 They give us course names, lifecycle stats, and
#                 batch details that pre-date the live API feed.
#                 Run this ONCE before script 10 (transform_courses_silver).
#
# How to run:
#   python 09_load_courses_csv/load_courses_csv.py
#
# What to check after:
#   - All three Bronze tables should have rows
#   - Run 12_check_db_counts to verify
# ============================================================

import os
import re
import sys
import psycopg2

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
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
CSV_DIR     = os.path.join(PROJECT_DIR, "CSV files")

# List of CSV files to load — each entry has:
#   label    : short name shown in progress messages
#   table    : target Bronze table in the database
#   filename : the CSV file name inside the CSV files folder
CSV_SOURCES = [
    {
        "label":    "catalogue",
        "table":    "bronze.course_catalogue_raw",
        "filename": "course_catalogue_data.csv",
    },
    {
        "label":    "lifecycle",
        "table":    "bronze.course_lifecycle_raw",
        "filename": "Elearning MIS Merged Tracker - Course Lifecycle (1).csv",
    },
    {
        "label":    "batches",
        "table":    "bronze.course_batches_raw",
        "filename": "batches_data.csv",
    },
]


# ── HELPER FUNCTIONS ─────────────────────────────────────────

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


def sanitize_column_name(column_header):
    # Convert a messy CSV header into a safe PostgreSQL column name
    # Example: "Course Name (Official)" → "course_name_official"
    cleaned = column_header.lower().strip()
    # Replace any character that is not a letter, digit, or underscore with _
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    # Remove leading or trailing underscores
    cleaned = cleaned.strip("_")
    # Collapse runs of multiple underscores into one
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned


def build_unique_column_names(raw_column_headers):
    # Sanitize all column headers, and make duplicates unique by adding a number suffix
    # Example: if two columns both sanitize to "name", second becomes "name_1"
    seen_names = {}
    unique_names = []

    for header in raw_column_headers:
        sanitized = sanitize_column_name(header)
        if sanitized in seen_names:
            # We have seen this name before — add a number to make it unique
            seen_names[sanitized] = seen_names[sanitized] + 1
            sanitized = sanitized + "_" + str(seen_names[sanitized])
        else:
            seen_names[sanitized] = 0
        unique_names.append(sanitized)

    return unique_names


def count_rows_in_table(conn, table_name):
    # Return the current number of rows in a table
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    count = cursor.fetchone()[0]
    cursor.close()
    return count


# ── LOAD ONE CSV FILE ─────────────────────────────────────────

def load_one_csv(conn, table_name, file_path):
    # Read the CSV, sanitize column names, then INSERT into the Bronze table
    # Returns: (inserted_count, total_rows_in_csv)

    # Read as all strings to avoid type conversion surprises
    # Replace NaN with None so the database gets NULL instead of the string "nan"
    dataframe = pd.read_csv(file_path, encoding="utf-8", encoding_errors="replace", dtype=str)
    dataframe = dataframe.where(pd.notna(dataframe), None)

    # Build unique sanitized column names from the CSV headers
    sanitized_names = build_unique_column_names(list(dataframe.columns))
    # Replace the dataframe column names with the sanitized versions
    dataframe.columns = sanitized_names

    # Build the SQL INSERT statement dynamically using the sanitized column names
    column_list = ", ".join(sanitized_names)
    sql_insert = (
        "INSERT INTO " + table_name + " (source_row, " + column_list + ") "
        "VALUES (%s, " + ", ".join(["%s"] * len(sanitized_names)) + ") "
        "ON CONFLICT (source_row) DO NOTHING"
    )

    cursor = conn.cursor()
    inserted_count = 0

    for row_index, row_data in dataframe.iterrows():
        # Build the list of values: row index first, then all column values
        row_values = [int(row_index)]
        for col_name in sanitized_names:
            row_values.append(row_data[col_name])

        cursor.execute(sql_insert, row_values)

        # rowcount == 1 means a new row was inserted; 0 means it was a duplicate
        if cursor.rowcount > 0:
            inserted_count = inserted_count + 1

    conn.commit()
    cursor.close()

    return inserted_count, len(dataframe)


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=== load_courses_csv.py ===")
    print("")

    # Confirm that the CSV folder exists before trying to read files
    if not os.path.isdir(CSV_DIR):
        print("ERROR: CSV folder not found at: " + CSV_DIR)
        print("  Make sure the 'CSV files' folder is in the project root.")
        sys.exit(1)

    conn = connect_to_database()

    summary = []

    # Process each CSV source one at a time
    for source in CSV_SOURCES:
        file_path = os.path.join(CSV_DIR, source["filename"])
        print("Loading " + source["label"] + " ...")
        print("  File  : " + source["filename"])
        print("  Table : " + source["table"])

        # Check that this particular file exists before trying to read it
        if not os.path.isfile(file_path):
            print("  SKIPPED — file not found")
            summary.append((source["table"], 0, 0))
            print("")
            continue

        inserted, total = load_one_csv(conn, source["table"], file_path)
        final_count = count_rows_in_table(conn, source["table"])

        print("  Rows in CSV  : " + str(total))
        print("  Inserted     : " + str(inserted))
        print("  Table total  : " + str(final_count))
        print("")

        summary.append((source["table"], inserted, final_count))

    conn.close()

    print("  LOAD COMPLETE")
    print("  " + "-" * 41)
    for table_name, inserted, total in summary:
        print("  " + table_name + ": " + str(total) + " rows (" + str(inserted) + " new)")
    print("")
    print("  Next step: run 10_transform_courses_silver/transform_courses_silver.py")
    print("")


if __name__ == "__main__":
    main()
