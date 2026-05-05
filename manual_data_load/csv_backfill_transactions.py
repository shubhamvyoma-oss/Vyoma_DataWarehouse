# This script helps move data from the "Bronze" layer to the "Silver" layer.
# It also connects students to their transactions by looking at their email.

import os
import sys
import datetime
import psycopg2

# These variables tell us how to connect to the database
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# This function gets a connection to our database
def get_database_connection():
    try:
        # Connect using the variables we set above
        connection = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        return connection
    except Exception as error:
        # If it fails, show the error
        print("Could not connect to the database:")
        print(error)
        return None

# This function handles empty or zero date numbers
def clean_date_number(value):
    # Check if value is missing
    if value is None:
        return None
    # Turn it into text and remove spaces
    value_string = str(value).strip()
    # Check if it represents "nothing"
    if value_string == '' or value_string == '0' or value_string == 'None':
        return None
    try:
        # Try to turn it into a whole number
        return int(value)
    except:
        return None

# This function turns a text date into a Python date object
def parse_date_text(date_value):
    # If the date is missing, return None
    if date_value is None:
        return None
    # Clean the text
    date_string = str(date_value).strip()
    if date_string == '' or date_string == 'None':
        return None
    try:
        # The CSV has dates like "M/D/YYYY HH:MM"
        naive_date = datetime.datetime.strptime(date_string, '%m/%d/%Y %H:%M')
        # We need to set the time zone to India (IST)
        ist_offset = datetime.timedelta(hours=5, minutes=30)
        ist_timezone = datetime.timezone(ist_offset)
        return naive_date.replace(tzinfo=ist_timezone)
    except:
        return None

# This function builds a list of emails and their user IDs
def build_email_map(connection):
    cursor = connection.cursor()
    # Get all emails and IDs that are not empty
    query = "SELECT DISTINCT email, user_id FROM bronze.student_courses_enrolled_raw "
    query = query + "WHERE email IS NOT NULL AND user_id IS NOT NULL"
    cursor.execute(query)
    # We store the mapping in a dictionary
    email_to_id_map = {}
    for row in cursor.fetchall():
        email = row[0]
        user_id = row[1]
        # Clean the email so it's all lowercase
        clean_email = email.lower().strip()
        email_to_id_map[clean_email] = user_id
    cursor.close()
    return email_to_id_map

# This function saves a single transaction record
def save_transaction(cursor, row_data):
    # Get all the pieces of data from the row
    row_num, user_id, name, email = row_data[0], row_data[1], row_data[2], row_data[3]
    bundle_id = clean_date_number(row_data[4])
    course_name = row_data[5]
    batch_id = clean_date_number(row_data[6])
    batch_name = row_data[7]
    inst_bundle = clean_date_number(row_data[8])
    start_time = clean_date_number(row_data[9])
    end_time = clean_date_number(row_data[10])
    record_id = "csv-enrollment-" + str(row_num)
    sql = """
        INSERT INTO silver.transactions (
            event_id, event_type, user_id, email, full_name, bundle_id, course_name, 
            master_batch_id, master_batch_name, institution_bundle_id, 
            start_date_ist, end_date_ist, source
        ) VALUES ( %s, 'csv.import', %s, %s, %s, %s, %s, %s, %s, %s, 
                   unix_to_ist(%s), unix_to_ist(%s), 'csv' )
        ON CONFLICT (user_id, bundle_id, master_batch_id) DO UPDATE SET
            email = COALESCE(silver.transactions.email, EXCLUDED.email),
            full_name = COALESCE(silver.transactions.full_name, EXCLUDED.full_name)
    """
    values = (record_id, user_id, email, name, bundle_id, course_name, 
              batch_id, batch_name, inst_bundle, start_time, end_time)
    cursor.execute(sql, values)
    return cursor.rowcount

# This function finds enrollment rows in the database
def find_enrollment_rows(connection):
    read_cursor = connection.cursor()
    query = "SELECT source_row, user_id, name, email, bundle_id, class_name, "
    query = query + "master_batch_id, master_batch_name, institution_bundle_id, "
    query = query + "classusers_start_date, classusers_end_date "
    query = query + "FROM bronze.student_courses_enrolled_raw WHERE user_id IS NOT NULL"
    read_cursor.execute(query)
    rows = read_cursor.fetchall()
    read_cursor.close()
    return rows

# This function runs the transaction backfill loop
def run_transaction_backfill(connection):
    rows = find_enrollment_rows(connection)
    write_cursor = connection.cursor()
    success_count = 0
    for row in rows:
        try:
            # Try to save the transaction
            rows_added = save_transaction(write_cursor, row)
            if rows_added > 0:
                success_count = success_count + 1
        except:
            print("Failed on row: " + str(row[0]))
    connection.commit()
    write_cursor.close()
    return success_count

# This function saves a single user record
def save_user(cursor, student_data, user_id):
    row_num, full_name, email = student_data[0], student_data[1], student_data[2]
    user_name, city, state, address = student_data[5], student_data[6], student_data[7], student_data[8]
    created_date = parse_date_text(student_data[12])
    record_id = "csv-student-" + str(row_num)
    sql = """
        INSERT INTO silver.users (
            event_id, event_type, user_id, email, full_name, user_name,
            city, state, address, created_at_ist, received_at
        ) VALUES ( %s, 'csv.import', %s, %s, %s, %s, %s, %s, %s, %s, 
                   NOW() AT TIME ZONE 'Asia/Kolkata' )
        ON CONFLICT (user_id) DO UPDATE SET
            full_name = COALESCE(silver.users.full_name, EXCLUDED.full_name)
    """
    values = (record_id, user_id, email, full_name, user_name, city, state, address, created_date)
    cursor.execute(sql, values)
    return cursor.rowcount

# This function gets all student rows from the database
def get_all_student_rows(connection):
    cursor = connection.cursor()
    query = "SELECT source_row, name, email, contact_number_dial_code, contact_number, "
    query = query + "username, city, state, address, parent_name, parent_email, "
    query = query + "parent_contact, date_created FROM bronze.studentexport_raw"
    cursor.execute(query)
    all_student_rows = cursor.fetchall()
    cursor.close()
    return all_student_rows

# This function processes all users and returns the counts
def run_user_backfill(connection, email_map):
    all_students = get_all_student_rows(connection)
    write_cursor = connection.cursor()
    processed_count, skipped_count = 0, 0
    for row in all_students:
        raw_email = row[2]
        if raw_email is None:
            continue
        email_key = raw_email.lower().strip()
        user_id = email_map.get(email_key)
        if user_id is not None:
            save_user(write_cursor, row, user_id)
            processed_count = processed_count + 1
        else:
            skipped_count = skipped_count + 1
    connection.commit()
    write_cursor.close()
    return processed_count, skipped_count

# Main function to run the whole process
def main():
    print("Starting CSV Silver Backfill...")
    db_connection = get_database_connection()
    if db_connection is None:
        return
    try:
        print("Processing transactions...")
        txn_total = run_transaction_backfill(db_connection)
        print("  Transactions finished: " + str(txn_total))
        print("Building email map...")
        email_lookup = build_email_map(db_connection)
        print("Processing users...")
        user_total, skip_total = run_user_backfill(db_connection, email_lookup)
        print("  Users finished: " + str(user_total))
        print("  Students skipped: " + str(skip_total))
        db_connection.close()
        print("All done!")
    except Exception as error:
        print("Something went wrong:")
        print(error)

if __name__ == '__main__':
    main()
