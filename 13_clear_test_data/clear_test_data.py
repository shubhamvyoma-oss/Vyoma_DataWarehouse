# ============================================================
# 13 — CLEAR TEST DATA
# ============================================================
# What it does: Finds and deletes rows that were inserted by
#               test scripts so the database stays clean.
#
# Why we need it: After running tests, fake records (test@...
#                 emails, user_ids >= 99990000) must be removed
#                 so they don't pollute real analytics.
#
# How to run:
#   python 13_clear_test_data/clear_test_data.py
#
# WARNING: This script DELETES data. It asks you to confirm
#          before deleting. Type 'y' and press Enter to proceed.
#
# What to check after:
#   - Run 12_check_db_counts to verify counts dropped
#   - Check that real production data was not affected
# ============================================================

import psycopg2

# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# Each entry in this list is: (table_name, WHERE_condition)
# The WHERE condition identifies rows that are test data.
# We identify test data by email patterns or high user_id numbers.
TABLES_AND_CONDITIONS = [
    (
        "silver.users",
        "email LIKE '%test%' OR email LIKE '%load%' OR user_id >= 99990000"
    ),
    (
        "silver.transactions",
        "email LIKE '%test%' OR email LIKE '%load%' OR user_id >= 99990000"
    ),
    (
        "silver.assessments",
        "user_id >= 99990000"
    ),
    (
        "silver.courses",
        "user_id >= 99990000"
    ),
    (
        "silver.certificates",
        "user_id >= 99990000"
    ),
    (
        "bronze.webhook_events",
        # These prefixes are used in test event IDs (e.g. "e2e-user-001")
        "event_id ~ '^(e2e|dupe|resilience|conc|dbtest|constraint|manual|load)-'"
    ),
]


def connect_to_database():
    # Open a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    return connection


def count_test_rows(cursor, table_name, where_condition):
    # Count how many rows match the test-data condition in this table
    sql = "SELECT COUNT(*) FROM " + table_name + " WHERE " + where_condition
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0]


def show_preview(cursor):
    # Show the user how many rows will be deleted in each table
    print("")
    print("Test data found (rows that will be deleted):")
    print("")
    total_rows = 0

    for table_name, where_condition in TABLES_AND_CONDITIONS:
        row_count = count_test_rows(cursor, table_name, where_condition)
        print("  {:<45} {:>8,} rows".format(table_name, row_count))
        total_rows = total_rows + row_count

    print("")
    print("  TOTAL: {:,} rows would be deleted".format(total_rows))
    print("")
    return total_rows


def delete_test_rows(cursor, table_name, where_condition):
    # Delete the rows that match the test-data condition
    sql = "DELETE FROM " + table_name + " WHERE " + where_condition
    cursor.execute(sql)
    # rowcount tells us how many rows were actually deleted
    return cursor.rowcount


def main():
    # Step 1: Connect to the database
    print("Connecting to database " + DB_NAME + " ...")
    try:
        connection = connect_to_database()
    except Exception as error:
        print("ERROR: Could not connect to database.")
        print("Error message: " + str(error))
        return

    # Step 2: Use autocommit=False so we can rollback if something goes wrong
    connection.autocommit = False
    cursor = connection.cursor()

    # Step 3: Show how many rows will be deleted
    total_rows = show_preview(cursor)

    # Step 4: If nothing to delete, exit early
    if total_rows == 0:
        print("Nothing to delete. Database is already clean.")
        cursor.close()
        connection.close()
        return

    # Step 5: Ask the user to confirm before deleting anything
    print("WARNING: This will permanently delete " + str(total_rows) + " rows.")
    user_input = input("Type 'y' and press Enter to confirm, or anything else to cancel: ")

    if user_input.strip().lower() != "y":
        print("Cancelled. No data was deleted.")
        cursor.close()
        connection.close()
        return

    # Step 6: Delete the test data from each table
    print("")
    for table_name, where_condition in TABLES_AND_CONDITIONS:
        deleted_count = delete_test_rows(cursor, table_name, where_condition)
        print("  Deleted {:,} rows from {}".format(deleted_count, table_name))

    # Step 7: Commit (save) the deletions permanently
    connection.commit()
    print("")
    print("Done. Test data has been removed.")

    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
