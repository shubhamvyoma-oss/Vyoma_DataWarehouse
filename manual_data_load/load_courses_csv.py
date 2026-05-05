# This script loads course data from CSV files into our "Bronze" database tables.
# We use this to bring historical course information into our analytics system.

import os
import re
import csv
import psycopg2

# These variables store our database connection settings
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# Path to the folder containing our CSV files
CURRENT_FILE_PATH = os.path.abspath(__file__)
PROJECT_HOME = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE_PATH)))
CSV_FILES_DIR = os.path.join(PROJECT_HOME, 'CSV files')

# This list tells us which CSV file goes into which table
SOURCE_FILES = [
    {
        "table": "bronze.course_catalogue_raw",
        "filename": "course_catalogue_data.csv",
    },
    {
        "table": "bronze.course_lifecycle_raw",
        "filename": "Elearning MIS Merged Tracker - Course Lifecycle (1).csv",
    },
    {
        "table": "bronze.course_batches_raw",
        "filename": "batches_data.csv",
    },
]

# This function makes a column name safe for the database
def clean_column_header(header_text):
    # Make everything lowercase and remove outer spaces
    header_clean = header_text.lower().strip()
    # Replace any special character with an underscore
    header_clean = re.sub(r"[^a-z0-9]+", "_", header_clean)
    # Remove underscores from the beginning or end
    header_clean = header_clean.strip("_")
    # Change multiple underscores into a single one
    header_clean = re.sub(r"_+", "_", header_clean)
    return header_clean

# This function ensures all column names are unique
def build_unique_columns(raw_headers):
    already_seen = {}
    unique_headers = []
    for h in raw_headers:
        name = clean_column_header(h)
        # If we have seen this name before, we add a number
        if name in already_seen:
            already_seen[name] = already_seen[name] + 1
            name = name + "_" + str(already_seen[name])
        else:
            already_seen[name] = 0
        unique_headers.append(name)
    return unique_headers

# This function builds the SQL command to insert rows
def get_insert_sql_command(table, column_list):
    # Combine the column names with commas
    columns_string = ", ".join(column_list)
    # Create the %s placeholders for the values
    placeholders = ["%s"]
    for i in range(len(column_list)):
        placeholders.append("%s")
    placeholders_string = ", ".join(placeholders)
    # Build the final SQL string
    sql = "INSERT INTO " + table + " (source_row, " + columns_string + ") "
    sql = sql + "VALUES (" + placeholders_string + ") "
    sql = sql + "ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function saves a single row from the CSV to the database
def save_row_to_db(cursor, sql, row_data, headers, index):
    # Start the values with the row number
    data_values = [index]
    # Add each column value
    for h in headers:
        val = row_data.get(h)
        # Use None if the value is empty
        if val == "":
            val = None
        data_values.append(val)
    # Execute the SQL
    cursor.execute(sql, data_values)
    return cursor.rowcount

# This function handles the process for one CSV file
def process_csv_file(connection, table_name, file_path):
    cursor = connection.cursor()
    new_rows = 0
    # Open the file and read it
    with open(file_path, mode='r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        raw_headers = reader.fieldnames
        # Create safe column names
        safe_headers = build_unique_columns(raw_headers)
        # Build the SQL command
        sql = get_insert_sql_command(table_name, safe_headers)
        idx = 0
        for row in reader:
            # Save each row
            new_rows = new_rows + save_row_to_db(cursor, sql, row, raw_headers, idx)
            idx = idx + 1
    # Save changes and close
    connection.commit()
    cursor.close()
    return new_rows

# Main entry point for the script
def main():
    print("Starting course CSV load...")
    try:
        # Connect to the database
        db_connection = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Load each file
        for item in SOURCE_FILES:
            path = os.path.join(CSV_FILES_DIR, item["filename"])
            print("Loading " + item["filename"] + "...")
            added = process_csv_file(db_connection, item["table"], path)
            print("  Rows added: " + str(added))
        db_connection.close()
    except Exception as error:
        print("An error occurred:")
        print(error)

if __name__ == "__main__":
    main()
