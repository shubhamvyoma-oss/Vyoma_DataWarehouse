# This script cleans up course data and moves it from "Bronze" to "Silver" tables.
# It makes sure that numbers and text are in the right format for our database.

import sys
import datetime
import psycopg2

# These variables store our database connection info
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# This function cleans up text data
def clean_text_value(value):
    # Check if the value is missing
    if value is None:
        return None
    # Remove extra spaces from the beginning and end
    cleaned_string = str(value).strip()
    # Check if the text is empty or just says "nan"
    if cleaned_string == "" or cleaned_string == "nan" or cleaned_string == "None":
        return None
    return cleaned_string

# This function turns text into a whole number (integer)
def clean_integer_value(value):
    if value is None:
        return None
    text_val = str(value).strip()
    if text_val == "" or text_val == "nan":
        return None
    try:
        # Remove commas like in "1,234"
        no_commas = text_val.replace(",", "")
        return int(no_commas)
    except:
        return None

# This function turns text into a decimal number (float)
def clean_float_value(value):
    if value is None:
        return None
    text_val = str(value).strip()
    # Some spreadsheets have error marks like "#VALUE!"
    if text_val == "" or text_val == "nan" or text_val == "#VALUE!":
        return None
    try:
        no_commas = text_val.replace(",", "")
        return float(no_commas)
    except:
        return None

# This function turns a database row into a dictionary
def convert_row_to_dict(row_data, column_info):
    row_dictionary = {}
    for i in range(len(row_data)):
        # Get the name of the column
        column_name = column_info[i][0]
        # Store the data using the column name
        row_dictionary[column_name] = row_data[i]
    return row_dictionary

# This function saves a course metadata record to the silver table
def save_course_catalogue(cursor, course_data):
    # Clean the ID and other fields
    bundle_id = clean_integer_value(course_data.get("bundle_id"))
    # If there is no ID, we skip this row
    if bundle_id is None:
        return 0
    name = clean_text_value(course_data.get("course_name"))
    cost = clean_float_value(course_data.get("cost"))
    # SQL command to insert the data
    sql = "INSERT INTO silver.course_catalogue (bundle_id, course_name, cost, imported_at) "
    sql = sql + "VALUES (%s, %s, %s, NOW()) ON CONFLICT (bundle_id) DO NOTHING"
    # Execute the command
    cursor.execute(sql, (bundle_id, name, cost))
    return cursor.rowcount

# This function handles all the metadata rows
def process_all_course_catalogue(connection):
    # Read from the bronze table
    read_cursor = connection.cursor()
    read_cursor.execute("SELECT * FROM bronze.course_catalogue_raw")
    all_rows = read_cursor.fetchall()
    columns = read_cursor.description
    # Write to the silver table
    write_cursor = connection.cursor()
    total_added = 0
    for row in all_rows:
        # Convert row to dictionary for easier use
        data_dict = convert_row_to_dict(row, columns)
        # Save the record and update the count
        rows_affected = save_course_catalogue(write_cursor, data_dict)
        total_added = total_added + rows_affected
    # Save the changes
    connection.commit()
    write_cursor.close()
    read_cursor.close()
    return total_added

# Main function that runs the script
def main():
    print("Starting Silver course transformation...")
    try:
        # Connect to the database
        db_connection = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Run the process
        print("Processing metadata...")
        added_count = process_all_course_catalogue(db_connection)
        print("Course metadata rows added: " + str(added_count))
        # Close the connection
        db_connection.close()
        print("Transformation finished.")
    except Exception as error:
        print("Something went wrong:")
        print(error)

if __name__ == "__main__":
    main()
