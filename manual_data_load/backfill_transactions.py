# This script moves historical data from the "bronze" layer to the "silver" layer.
# We need this because we have old data in CSV files that isn't coming through the live system.

import os
import sys
import datetime
import psycopg2
import json

# These variables store how to connect to our database
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# This function creates a connection to our PostgreSQL database
def get_database_connection():
    try:
        # We use the psycopg2 library to talk to the database
        connection = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # If it works, we return the connection object
        return connection
    except Exception as error:
        # If there is an error, we print it so we know what went wrong
        print("Could not connect to the database:")
        print(error)
        # We return None to show that the connection failed
        return None

# This function turns a string into a whole number (integer)
def convert_to_integer(value):
    # Check if the value is empty or not there
    if value is None:
        return None
    # Turn the value into a string and remove extra spaces
    value_string = str(value).strip()
    # Check for common ways people write "nothing"
    if value_string == '':
        return None
    if value_string == '0':
        return None
    if value_string == 'None':
        return None
    try:
        # Try to turn it into a real number
        return int(value_string)
    except:
        # If it's not a number (like "hello"), return None
        return None

# This function turns a database row into a dictionary for easier use
def create_row_dictionary(row_data, column_descriptions):
    # We start with an empty dictionary
    row_as_dictionary = {}
    # We loop through every column in the row
    for i in range(len(row_data)):
        # Get the name of the column from the descriptions
        column_name = column_descriptions[i][0]
        # Put the data into our dictionary using that name
        row_as_dictionary[column_name] = row_data[i]
    # Return the dictionary we made
    return row_as_dictionary

# This function builds the SQL query and values for a transaction
def insert_transaction_data(cursor, row_data):
    # We make a unique ID using the row number from the CSV
    row_number = row_data['source_row']
    unique_id = "csv-enrollment-" + str(row_number)
    
    # Get the user ID and make sure it is a number
    user_id = convert_to_integer(row_data['user_id'])
    # If there is no user ID, we cannot save this transaction
    if user_id is None:
        return 0
        
    # This is the SQL command to save the data into the silver table
    sql_command = """
        INSERT INTO silver.transactions (
            event_id, event_type, user_id, email, full_name,
            bundle_id, course_name, master_batch_id, master_batch_name,
            institution_bundle_id, start_date_ist, end_date_ist, source
        ) VALUES (
            %s, 'csv.import', %s, %s, %s,
            %s, %s, %s, %s, %s,
            unix_to_ist(%s), unix_to_ist(%s), 'csv'
        )
        ON CONFLICT (user_id, bundle_id, master_batch_id) DO NOTHING
    """
    # These are the actual values we want to save
    data_values = (
        unique_id, user_id, row_data['email'], row_data['name'],
        convert_to_integer(row_data['bundle_id']), row_data['class_name'],
        convert_to_integer(row_data['master_batch_id']), row_data['master_batch_name'],
        convert_to_integer(row_data['institution_bundle_id']),
        convert_to_integer(row_data['classusers_start_date']),
        convert_to_integer(row_data['classusers_end_date'])
    )
    # We tell the database to run the command
    cursor.execute(sql_command, data_values)
    # Return 1 if a row was added, or 0 if it was a duplicate
    return cursor.rowcount

# This function goes through all the rows in the bronze table
def process_all_rows(connection):
    # Create a "cursor" to read from the database
    read_cursor = connection.cursor()
    # Find all rows that have a user ID
    query = "SELECT * FROM bronze.student_courses_enrolled_raw WHERE user_id IS NOT NULL"
    read_cursor.execute(query)
    # Get all the rows and the column names
    all_rows = read_cursor.fetchall()
    column_info = read_cursor.description
    
    # Create another cursor to write to the database
    write_cursor = connection.cursor()
    total_added = 0
    # Loop through every row we found
    for row in all_rows:
        # Convert the row into a dictionary so it's easier to read
        row_dict = create_row_dictionary(row, column_info)
        # Try to save this row into the silver table
        rows_affected = insert_transaction_data(write_cursor, row_dict)
        total_added = total_added + rows_affected
        
    # Save the changes to the database forever
    connection.commit()
    # Close the cursors to clean up
    write_cursor.close()
    read_cursor.close()
    # Return the total number of new records we added
    return total_added

# This is the main part of the script that runs first
def main():
    print("Starting historical data backfill...")
    # Try to connect to our database
    database_connection = get_database_connection()
    
    # If the connection failed, we stop here
    if database_connection is None:
        print("Failed to start because database could not be reached.")
        return

    print("Processing transactions from enrollments...")
    # Run the backfill process
    new_records_count = process_all_rows(database_connection)
    print("New transactions added: " + str(new_records_count))
    
    # Close the database connection when we are finished
    database_connection.close()
    print("Backfill process finished.")

# This tells Python to run the main function
if __name__ == "__main__":
    main()
