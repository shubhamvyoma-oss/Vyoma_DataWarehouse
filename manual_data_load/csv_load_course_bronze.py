# This script loads course-related CSV files into the database's "Bronze" layer.
# We do this so we can have a raw copy of our course data to work with.

import os
import re
import csv
import psycopg2

# Database connection details
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# Path to the CSV files
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
CSV_DATA_DIR = os.path.join(PROJECT_DIR, 'CSV files')

# List of files to load and where they should go
DATA_SOURCES = [
    {
        'table': 'bronze.course_catalogue_raw',
        'filename': 'course_catalogue_data.csv',
    },
    {
        'table': 'bronze.course_lifecycle_raw',
        'filename': 'Elearning MIS Merged Tracker - Course Lifecycle (1).csv',
    },
    {
        'table': 'bronze.course_batches_raw',
        'filename': 'batches_data.csv',
    },
]

# This function makes column names safe for the database
def make_column_name_safe(name):
    # Change to lowercase and remove outer spaces
    clean_name = name.lower().strip()
    # Replace anything not a letter or number with an underscore
    clean_name = re.sub(r'[^a-z0-9]+', '_', clean_name)
    # Remove underscores from the ends
    clean_name = clean_name.strip('_')
    # Make sure we don't have double underscores
    clean_name = re.sub(r'_+', '_', clean_name)
    return clean_name

# This function builds a list of safe and unique column names
def get_safe_columns(raw_columns):
    column_counts = {}
    safe_columns_list = []
    # Look at every raw column name from the CSV
    for col in raw_columns:
        safe_name = make_column_name_safe(col)
        # If we have seen this name before, add a number to make it unique
        if safe_name in column_counts:
            column_counts[safe_name] = column_counts[safe_name] + 1
            safe_name = safe_name + "_" + str(column_counts[safe_name])
        else:
            column_counts[safe_name] = 0
        safe_columns_list.append(safe_name)
    return safe_columns_list

# This function creates the SQL insert command
def create_insert_sql(table_name, columns):
    # Join column names with a comma
    col_names_string = ", ".join(columns)
    # Build the %s placeholders
    placeholders = ["%s"]
    for i in range(len(columns)):
        placeholders.append("%s")
    placeholders_string = ", ".join(placeholders)
    # Create the final SQL query
    sql = "INSERT INTO " + table_name + " (source_row, " + col_names_string + ") "
    sql = sql + "VALUES (" + placeholders_string + ") "
    sql = sql + "ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function saves one row of data to the database
def save_data_row(cursor, sql_query, row_dict, raw_headers, row_index):
    # Start the values with the row number
    data_values = [row_index]
    # Add each value from the CSV row
    for header in raw_headers:
        val = row_dict.get(header)
        # If the value is empty, use None
        if val == "":
            val = None
        data_values.append(val)
    # Run the insert command
    cursor.execute(sql_query, data_values)
    return cursor.rowcount

# This function reads the CSV and coordinates the loading process
def load_csv_to_database(connection, table_name, file_path):
    cursor = connection.cursor()
    new_rows_count = 0
    with open(file_path, mode='r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        # Get the original headers and make them safe for the database
        raw_headers = reader.fieldnames
        safe_headers = get_safe_columns(raw_headers)
        # Create the SQL command
        sql_query = create_insert_sql(table_name, safe_headers)
        row_idx = 0
        # Loop through every row in the CSV
        for row in reader:
            added = save_data_row(cursor, sql_query, row, raw_headers, row_idx)
            new_rows_count = new_rows_count + added
            row_idx = row_idx + 1
    # Save work and return the count
    connection.commit()
    cursor.close()
    return new_rows_count

# Main part of the script
def main():
    try:
        # Connect to database
        db_conn = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Process each data source
        for source in DATA_SOURCES:
            full_path = os.path.join(CSV_DATA_DIR, source['filename'])
            print("Loading " + source['table'] + "...")
            rows_added = load_csv_to_database(db_conn, source['table'], full_path)
            print("  Added " + str(rows_added) + " new rows.")
        # Close connection
        db_conn.close()
    except Exception as err:
        print("An error occurred:")
        print(err)

if __name__ == '__main__':
    main()
