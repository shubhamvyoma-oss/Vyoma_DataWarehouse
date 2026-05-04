# ============================================================
# 12 — CHECK DATABASE ROW COUNTS
# ============================================================
# What it does: Connects to the PostgreSQL database and prints
#               how many rows are in each important table.
#
# Why we need it: Quick sanity check — tells you if data is
#                 flowing into Bronze and Silver tables.
#
# How to run:
#   python 12_check_db_counts/check_db_counts.py
#
# What to check after:
#   - Bronze tables should have numbers > 0 if imports have run
#   - Silver tables should have numbers > 0 if pipeline has run
#   - If all zeros — the import scripts haven't run yet
# ============================================================

import psycopg2

# ── DATABASE SETTINGS ─────────────────────────────────────────
# Update these if your database host or credentials change.
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# The list of every table we want to count rows in.
# Bronze = raw data exactly as received from APIs or CSV files.
# Silver = cleaned and transformed data used for reporting.
TABLES_TO_CHECK = [
    "bronze.webhook_events",
    "bronze.failed_events",
    "bronze.attendance_raw",
    "bronze.course_catalogue_raw",
    "bronze.course_batches_raw",
    "bronze.course_lifecycle_raw",
    "bronze.studentexport_raw",
    "bronze.student_courses_enrolled_raw",
    "bronze.unresolved_students_raw",
    "silver.users",
    "silver.transactions",
    "silver.sessions",
    "silver.assessments",
    "silver.courses",
    "silver.announcements",
    "silver.certificates",
    "silver.course_metadata",
    "silver.course_batches",
    "silver.class_attendance",
]


def connect_to_database():
    # Open a connection to the PostgreSQL database using the settings above
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    return connection


def count_rows_in_table(cursor, table_name):
    # Run SELECT COUNT(*) on the given table to get the number of rows
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    # fetchone() returns a tuple like (12345,) — we take the first element [0]
    result_row = cursor.fetchone()
    return result_row[0]


def print_all_counts(cursor):
    # Print a header line so the output is easy to read
    print("")
    print("{:<52} {:>12}".format("Table Name", "Row Count"))
    print("-" * 67)

    # Go through each table name in our list
    for table_name in TABLES_TO_CHECK:
        # Get the row count for this table
        row_count = count_rows_in_table(cursor, table_name)
        # Print the table name and count with commas for readability (e.g. 1,234,567)
        print("{:<52} {:>12,}".format(table_name, row_count))

    print("")


def main():
    # Step 1: Try to connect to the database
    print("Connecting to database " + DB_NAME + " ...")
    try:
        connection = connect_to_database()
    except Exception as error:
        print("ERROR: Could not connect to database.")
        print("Error message: " + str(error))
        print("Check that PostgreSQL is running and the settings above are correct.")
        return

    # Step 2: Create a cursor (like a pointer into the database)
    cursor = connection.cursor()

    # Step 3: Print the row counts for all tables
    print_all_counts(cursor)

    # Step 4: Close the cursor and connection to free resources
    cursor.close()
    connection.close()
    print("Done.")


# This block only runs when you execute this file directly
# (not when it is imported by another script)
if __name__ == "__main__":
    main()
