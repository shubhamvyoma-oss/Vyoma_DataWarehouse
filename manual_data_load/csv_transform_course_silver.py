# This script cleans up course data and moves it from "Bronze" to "Silver" tables.
# It handles converting text to numbers and making dates look correct.

import datetime
import os
import psycopg2

# These variables store our database connection information
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# This function turns text into a whole number (integer)
def turn_into_integer(value):
    # Check if the value is missing
    if value is None:
        return None
    # Turn into text and remove spaces
    text_value = str(value).strip()
    # Check for "nothing" values
    if text_value == '' or text_value == 'nan' or text_value == 'None':
        return None
    try:
        # Remove commas like in "1,000"
        clean_text = text_value.replace(',', '')
        return int(clean_text)
    except:
        return None

# This function turns text into a standard date
def turn_into_date(value):
    if value is None:
        return None
    text_value = str(value).strip()
    if text_value == '' or text_value == 'nan':
        return None
    # We try these different ways a date might be written
    date_formats = ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d']
    for fmt in date_formats:
        try:
            # Try to turn the text into a date using the format
            parsed_date = datetime.datetime.strptime(text_value, fmt)
            return parsed_date.date()
        except:
            # If it fails, try the next format in the list
            continue
    return None

# This function turns a database row into a dictionary
def row_to_dictionary(row_data, column_info):
    new_dictionary = {}
    for i in range(len(row_data)):
        # Get the name of the column
        name = column_info[i][0]
        # Save the data in the dictionary using the column name
        new_dictionary[name] = row_data[i]
    return new_dictionary

# This function saves one course catalogue record
def save_catalogue_row(cursor, data):
    # Get the ID and make sure it is a number
    bundle_id = turn_into_integer(data.get('bundle_id'))
    if bundle_id is None:
        return 0
    # SQL command to save the data
    sql = """
        INSERT INTO silver.course_metadata (
            bundle_id, course_name, subject, status, num_students, imported_at
        ) VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bundle_id) DO UPDATE SET
            course_name = EXCLUDED.course_name,
            status = EXCLUDED.status,
            num_students = EXCLUDED.num_students
    """
    # Values for the SQL command
    values = (bundle_id, str(data.get('course_name')).strip(), 
              str(data.get('subject')).strip(), str(data.get('status')).strip(), 
              turn_into_integer(data.get('num_students')))
    cursor.execute(sql, values)
    return 1

# This function saves one course lifecycle record
def save_lifecycle_row(cursor, data):
    course_id = turn_into_integer(data.get('course_id'))
    batch_name = str(data.get('batch_name')).strip()
    if course_id is None or batch_name == 'None' or batch_name == '':
        return 0
    # SQL command to save the data
    sql = """
        INSERT INTO silver.course_lifecycle (
            course_id, course_name, batch_name, status,
            first_class_date, last_class_date, imported_at
        ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (course_id, batch_name) DO UPDATE SET
            status = EXCLUDED.status
    """
    # Get cleaned values
    c_name = str(data.get('course_name')).strip()
    status = str(data.get('status')).strip()
    start_d = turn_into_date(data.get('first_class_date'))
    end_d = turn_into_date(data.get('last_class_and_valedictory_date'))
    # Run command
    cursor.execute(sql, (course_id, c_name, batch_name, status, start_d, end_d))
    return 1

# This function handles all catalogue rows
def process_catalogue_data(connection):
    # Read from bronze
    read_cursor = connection.cursor()
    read_cursor.execute("SELECT * FROM bronze.course_catalogue_raw")
    all_rows = read_cursor.fetchall()
    columns = read_cursor.description
    # Write to silver
    write_cursor = connection.cursor()
    total = 0
    for row in all_rows:
        row_dict = row_to_dictionary(row, columns)
        total = total + save_catalogue_row(write_cursor, row_dict)
    # Finish up
    connection.commit()
    write_cursor.close()
    read_cursor.close()
    return total

# This function handles all lifecycle rows
def process_lifecycle_data(connection):
    # Read from bronze
    read_cursor = connection.cursor()
    read_cursor.execute("SELECT * FROM bronze.course_lifecycle_raw")
    all_rows = read_cursor.fetchall()
    columns = read_cursor.description
    # Write to silver
    write_cursor = connection.cursor()
    total = 0
    for row in all_rows:
        row_dict = row_to_dictionary(row, columns)
        total = total + save_lifecycle_row(write_cursor, row_dict)
    # Finish up
    connection.commit()
    write_cursor.close()
    read_cursor.close()
    return total

# Main part of the script
def main():
    print("Starting Course Silver Transformation...")
    try:
        # Connect to database
        db_conn = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Run transformations
        c_count = process_catalogue_data(db_conn)
        print("Catalogue rows processed: " + str(c_count))
        l_count = process_lifecycle_data(db_conn)
        print("Lifecycle rows processed: " + str(l_count))
        # Close connection
        db_conn.close()
    except Exception as error:
        print("An error occurred:")
        print(error)

if __name__ == '__main__':
    main()
