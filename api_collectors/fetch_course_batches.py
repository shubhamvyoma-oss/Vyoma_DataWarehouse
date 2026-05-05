# Import the tool for handling dates and times
import datetime
# Import system tools for exiting the program and working with paths
import sys
# Import the time tool to allow the script to wait or sleep
import time
# Import the os tool for working with file and folder paths
import os
# Import the tool for handling command line arguments
import argparse
# Import the psycopg2 tool to connect to the Postgres database
import psycopg2
# Import extra database tools for handling JSON data in SQL
import psycopg2.extras
# Import the requests tool to make web calls to the API
import requests
# Import the json tool to work with JSON data
import json

# Try to import our custom local storage helper function
try:
    # Attempt to import the saving function from our helper file
    from local_storage_helper import save_data_to_local_file
# If the import fails, it might be because of the folder structure
except ImportError:
    # Get the absolute path to the folder where this script is located
    script_directory = os.path.dirname(os.path.abspath(__file__))
    # Add this folder to the system path so Python can find our helper file
    sys.path.append(script_directory)
    # Try to import the saving function again
    from local_storage_helper import save_data_to_local_file

# --- CONFIGURATION SETTINGS ---

# The address where the database is located
DATABASE_HOST = "localhost"
# The name of the database we want to connect to
DATABASE_NAME = "edmingle_analytics"
# The username used to log into the database
DATABASE_USER = "postgres"
# The password used to log into the database
DATABASE_PASSWORD = "Svyoma"
# The port number for the database connection
DATABASE_PORT = 5432

# The secret key needed to talk to the Edmingle API
API_SECRET_KEY = "590605228a847624e065a76e986803fa"
# The unique ID for the organization in the API
ORGANIZATION_ID = "683"
# The unique ID for the institution in the API
INSTITUTION_ID = "483"

# The web address for fetching batch information
BATCHES_API_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch"

# The headers we must send with every API request for security
API_REQUEST_HEADERS = {
    'apikey': API_SECRET_KEY, 
    'ORGID': ORGANIZATION_ID
}

# --- HELPER FUNCTIONS ---

# This function cleans text by removing extra spaces and handling empty values
def clean_text_data(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None if there is no value
        return None
    # Convert the value to text and remove extra spaces from both ends
    cleaned_text = str(input_value).strip()
    # Check if the text is empty or contains words like 'nan' or 'None'
    if cleaned_text == '' or cleaned_text == 'nan' or cleaned_text == 'None' or cleaned_text == 'NaN':
        # Return None if it is essentially empty
        return None
    # Return the clean text
    return cleaned_text

# This function safely converts a value to a whole number (integer)
def convert_to_safe_integer(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None if no value
        return None
    # Try to convert the value
    try:
        # Remove commas and spaces, then convert to integer
        text_without_commas = str(input_value).replace(',', '').strip()
        return int(text_without_commas)
    # If conversion fails, catch the error
    except Exception:
        # Return None if it could not be converted
        return None

# This function converts large numbers or numbers with decimals to big integers
def convert_to_safe_bigint(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None
        return None
    # Try to convert
    try:
        # Remove spaces
        clean_text = str(input_value).strip()
        # Convert to a float first, then to an integer
        return int(float(clean_text))
    # Catch errors
    except Exception:
        # Return None if failed
        return None

# This function tries to turn various date formats into a date object
def parse_date_text_to_object(input_date_text):
    # Check if the text is empty
    if input_date_text is None:
        return None
    # Clean the text first
    clean_date_text = clean_text_data(input_date_text)
    if clean_date_text is None:
        return None
    # Try to parse as a Unix timestamp (a long number)
    try:
        unix_timestamp = float(clean_date_text)
        return datetime.datetime.fromtimestamp(unix_timestamp, tz=datetime.timezone.utc)
    except Exception:
        # If it's not a number, we will try other formats below
        pass
    # List of different date formats to try
    formats_to_try = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y %I:%M %p IST', '%d/%m/%Y', '%m/%d/%Y']
    # Try each format
    for date_format in formats_to_try:
        try:
            # Only use the part before the '+' if there is a time zone
            part_before_timezone = clean_date_text.split('+')[0].strip()
            return datetime.datetime.strptime(part_before_timezone, date_format).replace(tzinfo=datetime.timezone.utc)
        except Exception:
            # Continue to the next format if this one fails
            continue
    # Return None if nothing worked
    return None

# This function asks the API for one specific page of data
def fetch_one_page_from_api(page_number):
    # Prepare the settings for this specific page
    query_settings = {'page': page_number, 'org_id': ORGANIZATION_ID}
    # Try the request up to 3 times
    for attempt_count in range(3):
        try:
            # Make the web call
            api_response = requests.get(BATCHES_API_URL, headers=API_REQUEST_HEADERS, params=query_settings, timeout=60)
            # If successful (code 200)
            if api_response.status_code == 200:
                # Return the data as a Python dictionary
                return api_response.json()
            # If not successful, print a message
            print("API Error on page " + str(page_number) + ": " + str(api_response.status_code))
        except Exception as error:
            # If a network error happens, print it and wait
            print("Attempt " + str(attempt_count + 1) + " failed: " + str(error))
            time.sleep(5)
    # If all 3 tries failed, stop the program
    sys.exit(1)

# This function extracts bundles and checks if there are more pages to read
def extract_bundles_and_pagination(json_api_data):
    # Default variables
    list_of_bundles = []
    is_there_more = False
    # Check if the data is a list
    if isinstance(json_api_data, list):
        list_of_bundles = json_api_data
    # Check if the data is a dictionary
    elif isinstance(json_api_data, dict):
        # Look for the 'courses' key
        list_of_bundles = json_api_data.get('courses')
        if list_of_bundles is None:
            list_of_bundles = []
        # Check if the API says there is another page
        pagination_info = json_api_data.get('page_context')
        if pagination_info is not None:
            is_there_more = bool(pagination_info.get('has_more_page'))
    # Return the results
    return list_of_bundles, is_there_more

# This function extracts batches from a bundle and adds them to a main list
def extract_batches_from_bundle(bundle_object, target_main_list):
    # Get the ID and name of the bundle
    bundle_id = bundle_object.get('bundle_id')
    bundle_name = bundle_object.get('bundle_name')
    # Get the list of batches inside this bundle
    batch_data = bundle_object.get('batch')
    if batch_data is None:
        batch_data = []
    # Sometimes batch data is a string that looks like JSON
    if isinstance(batch_data, str):
        try:
            batch_data = json.loads(batch_data)
        except Exception:
            batch_data = []
    # Loop through each batch found
    for individual_batch in batch_data:
        # Store the bundle information inside the batch object
        individual_batch['bundle_id'] = bundle_id
        individual_batch['bundle_name'] = bundle_name
        # Add this batch to our big list
        target_main_list.append(individual_batch)

# This function fetches all batch data from the API by looping through pages
def get_all_batches_from_api_loop(should_save_backup):
    # Start with an empty list and begin at page 1
    all_found_batches = []
    current_page_number = 1
    has_more_pages_to_read = True
    # Keep looping as long as there are more pages
    while has_more_pages_to_read == True:
        # Get the data for the current page
        page_json = fetch_one_page_from_api(current_page_number)
        # Extract the bundles and check if there's a next page
        bundles_list, has_more_pages_to_read = extract_bundles_and_pagination(page_json)
        # Process each bundle to get its batches
        for single_bundle in bundles_list:
            extract_batches_from_bundle(single_bundle, all_found_batches)
        # If no bundles were found on this page, stop
        if len(bundles_list) == 0:
            has_more_pages_to_read = False
        # Move to the next page number
        current_page_number = current_page_number + 1
    # Save a backup on the computer if requested
    if should_save_backup == True:
        if len(all_found_batches) > 0:
            save_data_to_local_file(all_found_batches, 'batches', 'course_batches_raw', 'json')
    # Return the final list of all batches
    return all_found_batches

# This function finds the next available row number for the database
def get_starting_row_number(db_connection):
    # Create a cursor to talk to the database
    db_cursor = db_connection.cursor()
    # Ask the database for the highest row number currently used
    db_cursor.execute("SELECT COALESCE(MAX(source_row) + 1, 0) FROM bronze.course_batches_raw")
    # Get the answer
    query_result = db_cursor.fetchone()
    starting_number = query_result[0]
    # Close the cursor
    db_cursor.close()
    # Return the number
    return starting_number

# This function inserts one batch record into the bronze database table
def insert_batch_into_bronze(db_cursor, batch_dict, row_index):
    # Get the bundle ID and the batch ID
    b_id = batch_dict.get('bundle_id')
    c_id = batch_dict.get('class_id')
    # If IDs are missing, skip this record
    if b_id is None or c_id is None:
        return False
    # Work out if the batch is archived or active
    archived_status = clean_text_data(batch_dict.get('mb_archived'))
    status_label = 'Active'
    if archived_status == '1':
        status_label = 'Archived'
    # SQL command to insert or update the record
    sql_command = """
        INSERT INTO bronze.course_batches_raw (
            source_row, bundle_id, bundle_name, batch_id, batch_name, batch_status,
            start_date, end_date, tutor_id, tutor_name, admitted_students
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_row) DO UPDATE SET
            bundle_id = EXCLUDED.bundle_id, bundle_name = EXCLUDED.bundle_name,
            batch_id = EXCLUDED.batch_id, batch_name = EXCLUDED.batch_name,
            batch_status = EXCLUDED.batch_status, start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date, tutor_id = EXCLUDED.tutor_id,
            tutor_name = EXCLUDED.tutor_name, admitted_students = EXCLUDED.admitted_students,
            loaded_at = NOW()
    """
    # Prepare the values for the SQL command
    data_values = (
        row_index, clean_text_data(b_id), clean_text_data(batch_dict.get('bundle_name')),
        clean_text_data(c_id), clean_text_data(batch_dict.get('class_name')),
        status_label, clean_text_data(batch_dict.get('start_date')),
        clean_text_data(batch_dict.get('end_date')), clean_text_data(batch_dict.get('tutor_id')),
        clean_text_data(batch_dict.get('tutor_name')),
        clean_text_data(batch_dict.get('admitted_students') or batch_dict.get('registered_students'))
    )
    # Execute the command
    db_cursor.execute(sql_command, data_values)
    return True

# This function saves the entire list of batches into the bronze table
def save_all_batches_to_bronze(db_connection, batch_list):
    # Create a cursor
    db_cursor = db_connection.cursor()
    # Get the next available row number
    current_row_number = get_starting_row_number(db_connection)
    # Success counter
    success_count = 0
    # Loop through all batches in the list
    for i in range(len(batch_list)):
        # Get the batch at the current position
        current_batch = batch_list[i]
        # Insert it and increment counter if successful
        if insert_batch_into_bronze(db_cursor, current_batch, current_row_number + i) == True:
            success_count = success_count + 1
    # Save all changes to the database
    db_connection.commit()
    # Close the cursor
    db_cursor.close()
    # Return the count of records saved
    return success_count

# This function gets rows from the bronze table that are ready for silver transformation
def get_eligible_bronze_rows(db_connection):
    # Create a special cursor that returns data as dictionaries
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Query to get rows, excluding any that contain the word 'test' in their name
    sql_query = "SELECT * FROM bronze.course_batches_raw WHERE (batch_name IS NULL OR (batch_name NOT ILIKE '%test batch%' AND batch_name NOT ILIKE '%test%batch%')) ORDER BY source_row"
    # Execute and fetch all results
    db_cursor.execute(sql_query)
    found_rows = db_cursor.fetchall()
    # Close the cursor
    db_cursor.close()
    # Return the rows
    return found_rows

# This function inserts one record into the silver database table
def insert_batch_into_silver(db_cursor, row_dict):
    # Convert IDs to large integers
    bundle_id_val = convert_to_safe_bigint(row_dict.get('bundle_id'))
    batch_id_val = convert_to_safe_bigint(row_dict.get('batch_id'))
    # Skip if important IDs are missing
    if bundle_id_val is None or batch_id_val is None:
        return False
    # SQL command to insert or update the record
    sql_command = """
        INSERT INTO silver.course_batches (
            bundle_id, bundle_name, batch_id, batch_name, batch_status,
            start_date_ist, end_date_ist, tutor_name, admitted_students, imported_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (bundle_id, batch_id) DO UPDATE SET
            bundle_name = EXCLUDED.bundle_name, batch_name = EXCLUDED.batch_name,
            batch_status = EXCLUDED.batch_status, start_date_ist = EXCLUDED.start_date_ist,
            end_date_ist = EXCLUDED.end_date_ist, tutor_name = EXCLUDED.tutor_name,
            admitted_students = EXCLUDED.admitted_students, imported_at = NOW()
    """
    # Prepare values
    data_values = (
        bundle_id_val, clean_text_data(row_dict.get('bundle_name')), batch_id_val,
        clean_text_data(row_dict.get('batch_name')), clean_text_data(row_dict.get('batch_status')),
        parse_date_text_to_object(row_dict.get('start_date')), parse_date_text_to_object(row_dict.get('end_date')),
        clean_text_data(row_dict.get('tutor_name')), convert_to_safe_integer(row_dict.get('admitted_students'))
    )
    # Execute
    db_cursor.execute(sql_command, data_values)
    return True

# This function runs the transformation from bronze to silver
def run_silver_transformation(db_connection):
    # Get the rows from bronze
    rows_to_process = get_eligible_bronze_rows(db_connection)
    # Create a cursor
    db_cursor = db_connection.cursor()
    # Count how many total records are in bronze
    db_cursor.execute("SELECT COUNT(*) FROM bronze.course_batches_raw")
    total_bronze_count = db_cursor.fetchone()[0]
    # Calculate how many 'test' records we skipped
    skipped_count = total_bronze_count - len(rows_to_process)
    # Success counter
    updated_count = 0
    # Process each row
    for single_row in rows_to_process:
        if insert_batch_into_silver(db_cursor, single_row) == True:
            updated_count = updated_count + 1
    # Save changes
    db_connection.commit()
    # Close cursor
    db_cursor.close()
    # Return the counts
    return updated_count, skipped_count

# This function rebuilds the final master course table
def rebuild_final_master_table(db_connection):
    # Create a cursor
    db_cursor = db_connection.cursor()
    # Clear the existing table data
    db_cursor.execute("TRUNCATE TABLE silver.course_master")
    # Long SQL command to join batch data with metadata
    sql_command = """
        INSERT INTO silver.course_master (
            bundle_id, bundle_name, batch_id, batch_name, batch_status,
            start_date, end_date, tutor_name, admitted_students,
            course_name, subject, course_type, term_of_course,
            position_in_funnel, adhyayanam_category, sss_category,
            viniyoga, division, catalogue_status, final_status,
            is_latest_batch, include_in_course_count,
            status_adjustment_reason, has_batch, built_at
        )
        WITH latest_batch AS (
            SELECT bundle_id, batch_id,
            ROW_NUMBER() OVER (PARTITION BY bundle_id ORDER BY end_date_ist DESC NULLS LAST) AS rn
            FROM silver.course_batches
        ),
        has_batch_flag AS (
            SELECT DISTINCT bundle_id, 1 AS has_b FROM silver.course_batches
        )
        SELECT
            cb.bundle_id, cb.bundle_name, cb.batch_id, cb.batch_name, cb.batch_status,
            cb.start_date_ist::DATE, cb.end_date_ist::DATE, cb.tutor_name, cb.admitted_students,
            cm.course_name, cm.subject, cm.course_type, cm.term_of_course,
            cm.position_in_funnel, cm.adhyayanam_category, cm.sss_category,
            cm.viniyoga, cm.division, cm.status, cm.status,
            CASE WHEN lb.rn = 1 THEN 1 ELSE 0 END,
            CASE WHEN cm.course_division = 'Course' AND cb.bundle_id IS NOT NULL AND cm.status IN ('Completed', 'Ongoing', 'Upcoming') THEN 1 ELSE 0 END,
            '', COALESCE(hb.has_b, 0), NOW()
        FROM silver.course_batches cb
        LEFT JOIN silver.course_catalogue cm ON cb.bundle_id = cm.bundle_id
        LEFT JOIN latest_batch lb ON cb.bundle_id = lb.bundle_id AND cb.batch_id = lb.batch_id
        LEFT JOIN has_batch_flag hb ON cb.bundle_id = hb.bundle_id
        UNION ALL
        SELECT cm.bundle_id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            cm.course_name, cm.subject, cm.course_type, cm.term_of_course,
            cm.position_in_funnel, cm.adhyayanam_category, cm.sss_category,
            cm.viniyoga, cm.division, cm.status, cm.status,
            0, 0, '', 0, NOW()
        FROM silver.course_catalogue cm
        WHERE NOT EXISTS (SELECT 1 FROM silver.course_batches cb WHERE cb.bundle_id = cm.bundle_id)
    """
    # Execute the command
    db_cursor.execute(sql_command)
    # Get the count of records inserted
    total_inserted = db_cursor.rowcount
    # Save changes
    db_connection.commit()
    # Close cursor
    db_cursor.close()
    # Return count
    return total_inserted

# This is the main function that runs all the steps in order
def main():
    # Setup the argument parser
    arg_parser = argparse.ArgumentParser()
    # Add an option to save a backup locally
    arg_parser.add_argument("--save-local", action="store_true", help="Save backup files on computer")
    # Parse the user arguments
    parsed_args = arg_parser.parse_args()
    # Print start message
    print("=== STARTING COURSE BATCHES PIPELINE ===")
    # Step 1: Fetch all batch data from the API
    all_batches_data = get_all_batches_from_api_loop(parsed_args.save_local)
    # Connect to the database
    db_conn = psycopg2.connect(host=DATABASE_HOST, dbname=DATABASE_NAME, user=DATABASE_USER, password=DATABASE_PASSWORD, port=DATABASE_PORT)
    # Step 2: Save the data into the bronze table
    bronze_total = save_all_batches_to_bronze(db_conn, all_batches_data)
    print("Bronze Table Updated: " + str(bronze_total) + " records")
    # Step 3: Transform and save to the silver table
    silver_total, tests_skipped = run_silver_transformation(db_conn)
    print("Silver Table Updated: " + str(silver_total) + " records")
    # Step 4: Rebuild the master course table
    master_total = rebuild_final_master_table(db_conn)
    print("Master Table Rebuilt: " + str(master_total) + " records")
    # Close the connection
    db_conn.close()
    # Final success message
    print("SUCCESS: Pipeline completed for " + str(len(all_batches_data)) + " batches.")

# If this script is run directly
if __name__ == '__main__':
    # Start the program
    main()
