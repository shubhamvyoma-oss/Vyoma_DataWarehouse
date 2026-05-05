# Import system tools for exiting the program and working with folders
import sys
# Import time tools to allow the script to wait or sleep
import time
# Import the os tool for working with file and folder paths
import os
# Import the tool for handling command line arguments
import argparse
# Import the psycopg2 tool to connect to the Postgres database
import psycopg2
# Import extra database tools for handling JSON data and dictionaries
import psycopg2.extras
# Import the requests tool to make web calls to the API
import requests

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
EDMINGLE_API_KEY = "590605228a847624e065a76e986803fa"
# The unique ID for the organization in the API
EDMINGLE_ORG_ID = "683"
# The unique ID for the institution in the API
EDMINGLE_INST_ID = "483"

# The web address for fetching the course catalogue
CATALOGUE_API_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue"

# The headers we must send with every API request for security
API_HEADERS = {
    'apikey': EDMINGLE_API_KEY, 
    'ORGID': EDMINGLE_ORG_ID
}

# The list of course statuses we want to keep
VALID_COURSE_STATUSES = ['Completed', 'Ongoing', 'Upcoming']

# --- HELPER FUNCTIONS ---

# This function cleans text data by removing spaces and handling empty values
def clean_text_data(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None
        return None
    # Convert to text and remove extra spaces from both ends
    cleaned_text = str(input_value).strip()
    # Check if the text is essentially empty
    if cleaned_text == '' or cleaned_text == 'nan' or cleaned_text == 'None' or cleaned_text == 'NaN':
        return None
    # Return the clean text
    return cleaned_text

# This function safely converts a value to a whole number (integer)
def convert_to_safe_integer(input_value):
    # Check if empty
    if input_value is None:
        return None
    # Try to convert
    try:
        # Remove commas and spaces, then convert
        clean_text = str(input_value).replace(',', '').strip()
        return int(clean_text)
    # Catch errors
    except Exception:
        return None

# This function safely converts a value to a number with decimals (float)
def convert_to_safe_float(input_value):
    # Check if empty
    if input_value is None:
        return None
    # Try to convert
    try:
        # Remove commas and spaces, then convert
        clean_text = str(input_value).replace(',', '').strip()
        return float(clean_text)
    # Catch errors
    except Exception:
        return None

# This function asks the API for the course catalogue data
def fetch_catalogue_data_from_api():
    # Try the request up to 3 times
    for attempt_count in range(3):
        try:
            # Make the web call
            api_response = requests.get(CATALOGUE_API_URL, headers=API_HEADERS, timeout=60)
            # If successful (code 200)
            if api_response.status_code == 200:
                # Return the data as a Python dictionary
                return api_response.json()
            # If not successful, print a message
            print("API Error: status code " + str(api_response.status_code))
        except Exception as error:
            # Print the error and wait before trying again
            print("Connection failed on attempt " + str(attempt_count + 1) + ": " + str(error))
            time.sleep(5)
    # Stop the program if all tries failed
    sys.exit(1)

# This function extracts the list of courses from the API response
def extract_course_list_from_data(json_data):
    # Check if the data is already a list
    if isinstance(json_data, list):
        return json_data
    # Check if the data is a dictionary
    if isinstance(json_data, dict):
        # List of keys where the courses might be stored
        key_names = ['response', 'data', 'courses', 'bundles', 'result', 'items']
        # Look for each key in the dictionary
        for current_key in key_names:
            # If the key exists and holds a list
            if current_key in json_data:
                if isinstance(json_data[current_key], list):
                    # Return that list
                    return json_data[current_key]
        # If no common keys were found, just return all values as a list
        return list(json_data.values())
    # Return an empty list if nothing was found
    return []

# This function fetches all courses and handles local backups
def get_all_courses_with_backup(should_save_locally):
    # Get raw data from the API
    raw_data = fetch_catalogue_data_from_api()
    # Pull out the course list
    all_courses = extract_course_list_from_data(raw_data)
    # If no courses found, stop
    if len(all_courses) == 0:
        print("No courses were found in the API response.")
        sys.exit(0)
    # Print progress
    print("Found " + str(len(all_courses)) + " courses in the API.")
    # Save a backup file if requested
    if should_save_locally == True:
        save_data_to_local_file(all_courses, 'catalogue', 'course_catalogue_raw', 'json')
    # Return the courses
    return all_courses

# This function ensures the raw_json column exists in the bronze table
def update_bronze_table_schema(db_connection):
    # Create a cursor
    db_cursor = db_connection.cursor()
    # Check if the column 'raw_json' exists
    sql_check = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = 'course_catalogue_raw' AND column_name = 'raw_json'"
    db_cursor.execute(sql_check)
    # Get the result
    found_column = db_cursor.fetchone()
    # If the column was not found
    if found_column is None:
        # Add the column to the table
        db_cursor.execute("ALTER TABLE bronze.course_catalogue_raw ADD COLUMN raw_json JSONB")
        db_connection.commit()
    # Close cursor
    db_cursor.close()

# This function finds the unique ID for a course
def extract_unique_course_id(course_dict):
    # Try different possible ID field names
    course_id = course_dict.get('bundle_id')
    if course_id is None:
        course_id = course_dict.get('Bundle id')
    if course_id is None:
        course_id = course_dict.get('id')
    # Return the ID
    return course_id

# This function inserts one course into the bronze database table
def insert_single_course_to_bronze(db_cursor, course_item):
    # Get the course ID
    unique_id = extract_unique_course_id(course_item)
    # Skip if there is no ID
    if unique_id is None:
        return False
    # SQL command to save the data
    sql_command = """
        INSERT INTO bronze.course_catalogue_raw (
            source_row, bundle_id, course_name, course_description, cost,
            num_students, tutors, course_url, subject, level,
            language, type, course_division, status,
            sss_category, viniyoga, adhyayanam_category,
            term_of_course, position_in_funnel, division, raw_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_row) DO UPDATE SET
            bundle_id = EXCLUDED.bundle_id, course_name = EXCLUDED.course_name,
            course_description = EXCLUDED.course_description, cost = EXCLUDED.cost,
            num_students = EXCLUDED.num_students, tutors = EXCLUDED.tutors,
            course_url = EXCLUDED.course_url, subject = EXCLUDED.subject,
            level = EXCLUDED.level, language = EXCLUDED.language,
            type = EXCLUDED.type, course_division = EXCLUDED.course_division,
            status = EXCLUDED.status, sss_category = EXCLUDED.sss_category,
            viniyoga = EXCLUDED.viniyoga, adhyayanam_category = EXCLUDED.adhyayanam_category,
            term_of_course = EXCLUDED.term_of_course, position_in_funnel = EXCLUDED.position_in_funnel,
            division = EXCLUDED.division, raw_json = EXCLUDED.raw_json, loaded_at = NOW()
    """
    # Create a list of values for the command
    values_list = (
        int(unique_id), str(unique_id),
        clean_text_data(course_item.get('Course Name') or course_item.get('course_name') or course_item.get('name')),
        clean_text_data(course_item.get('Course Description') or course_item.get('course_description')),
        clean_text_data(course_item.get('Cost') or course_item.get('cost')),
        clean_text_data(course_item.get('Num Students') or course_item.get('num_students')),
        clean_text_data(course_item.get('Tutors') or course_item.get('tutors')),
        clean_text_data(course_item.get('Course URL') or course_item.get('course_url')),
        clean_text_data(course_item.get('Subject') or course_item.get('subject')),
        clean_text_data(course_item.get('Level') or course_item.get('level')),
        clean_text_data(course_item.get('Language') or course_item.get('language')),
        clean_text_data(course_item.get('Type') or course_item.get('type')),
        clean_text_data(course_item.get('Course Division') or course_item.get('course_division')),
        clean_text_data(course_item.get('Status') or course_item.get('status')),
        clean_text_data(course_item.get('SSS Category') or course_item.get('sss_category')),
        clean_text_data(course_item.get('Viniyoga') or course_item.get('viniyoga')),
        clean_text_data(course_item.get('Adhyayanam Category') or course_item.get('adhyayanam_category')),
        clean_text_data(course_item.get('Term of Course') or course_item.get('term_of_course')),
        clean_text_data(course_item.get('Position in Funnel') or course_item.get('position_in_funnel')),
        clean_text_data(course_item.get('Division') or course_item.get('division')),
        psycopg2.extras.Json(course_item)
    )
    # Run the SQL command
    db_cursor.execute(sql_command, values_list)
    return True

# This function saves the list of courses into the bronze database table
def save_all_courses_to_bronze_table(db_connection, courses_list):
    # Ensure the table structure is correct
    update_bronze_table_schema(db_connection)
    # Create a cursor
    db_cursor = db_connection.cursor()
    # Success counter
    saved_count = 0
    # Loop through each course
    for single_course in courses_list:
        # Save the course
        if insert_single_course_to_bronze(db_cursor, single_course) == True:
            saved_count = saved_count + 1
    # Save changes to the database
    db_connection.commit()
    # Close cursor
    db_cursor.close()
    # Return count
    return saved_count

# This function gets all rows from the bronze table for transformation
def get_rows_from_bronze_table(db_connection):
    # Create a special cursor for dictionaries
    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Get all rows sorted by ID
    db_cursor.execute("SELECT * FROM bronze.course_catalogue_raw ORDER BY source_row")
    rows = db_cursor.fetchall()
    # Close cursor
    db_cursor.close()
    # Return results
    return rows

# This function checks if a record is valid for the silver table
def is_record_valid_for_silver(data_row):
    # Try to convert the bundle ID to a number
    try:
        raw_id = str(data_row['bundle_id']).strip()
        bundle_id_numeric = int(float(raw_id))
        # ID cannot be zero
        if bundle_id_numeric == 0:
            return None, "invalid_id"
    except Exception:
        # If conversion fails, the ID is invalid
        return None, "invalid_id"
    # The course division must be 'Course'
    division_name = clean_text_data(data_row.get('course_division'))
    if division_name is None or division_name.lower() != 'course':
        return None, "not_a_course"
    # Return the ID if all tests pass
    return bundle_id_numeric, "ok"

# This function inserts one record into the silver database table
def insert_single_record_to_silver(db_cursor, row_data, b_id, status_text):
    # SQL command to insert or update silver metadata
    sql_command = """
        INSERT INTO silver.course_catalogue (
            bundle_id, course_name, subject, level, language, texts,
            type, course_type, course_division, division, viniyoga, certificate, course_sponsor, status,
            number_of_lectures, duration, personas, sss_category,
            adhyayanam_category, term_of_course, position_in_funnel,
            num_students, imported_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (bundle_id) DO UPDATE SET
            course_name = EXCLUDED.course_name, subject = EXCLUDED.subject,
            level = EXCLUDED.level, language = EXCLUDED.language,
            texts = EXCLUDED.texts, type = EXCLUDED.type,
            course_type = EXCLUDED.course_type,
            course_division = EXCLUDED.course_division, division = EXCLUDED.division,
            viniyoga = EXCLUDED.viniyoga,
            certificate = EXCLUDED.certificate,
            course_sponsor = EXCLUDED.course_sponsor, status = EXCLUDED.status,
            number_of_lectures = EXCLUDED.number_of_lectures, duration = EXCLUDED.duration,
            personas = EXCLUDED.personas, sss_category = EXCLUDED.sss_category,
            adhyayanam_category = EXCLUDED.adhyayanam_category,
            term_of_course = EXCLUDED.term_of_course,
            position_in_funnel = EXCLUDED.position_in_funnel,
            num_students = EXCLUDED.num_students,
            imported_at = NOW()
    """
    # Create values list
    data_values = (
        b_id, clean_text_data(row_data.get('course_name')),
        clean_text_data(row_data.get('subject')), clean_text_data(row_data.get('level')),
        clean_text_data(row_data.get('language')), clean_text_data(row_data.get('texts')),
        clean_text_data(row_data.get('type')), clean_text_data(row_data.get('type')),
        clean_text_data(row_data.get('course_division')), clean_text_data(row_data.get('division')),
        clean_text_data(row_data.get('viniyoga')),
        clean_text_data(row_data.get('certificate')), clean_text_data(row_data.get('course_sponsor')),
        status_text, clean_text_data(row_data.get('number_of_lectures')),
        clean_text_data(row_data.get('duration')), clean_text_data(row_data.get('personas')),
        clean_text_data(row_data.get('sss_category')), clean_text_data(row_data.get('adhyayanam_category')),
        clean_text_data(row_data.get('term_of_course')), clean_text_data(row_data.get('position_in_funnel')),
        convert_to_safe_integer(row_data.get('num_students'))
    )
    # Execute
    db_cursor.execute(sql_command, data_values)

# This function performs the silver transformation by processing bronze rows
def run_silver_transformation_process(db_connection):
    # Fetch rows from bronze
    bronze_rows = get_rows_from_bronze_table(db_connection)
    # Create writing cursor
    db_cursor = db_connection.cursor()
    # Tracking counters
    upsert_count = 0
    # Keep track of IDs we have already processed
    processed_ids = {}
    # Loop through each bronze row
    for row in bronze_rows:
        # Check if row is valid for silver
        numeric_id, validation_result = is_record_valid_for_silver(row)
        if validation_result != "ok":
            continue
        # Work out the final status
        row_status = clean_text_data(row.get('status'))
        final_status_label = None
        if row_status in VALID_COURSE_STATUSES:
            final_status_label = row_status
        # Skip if we already saved this ID
        if numeric_id in processed_ids:
            continue
        # Mark as processed
        processed_ids[numeric_id] = True
        # Save to silver table
        insert_single_record_to_silver(db_cursor, row, numeric_id, final_status_label)
        upsert_count = upsert_count + 1
    # Save changes
    db_connection.commit()
    # Close cursor
    db_cursor.close()
    # Return count
    return upsert_count

# This is the main function that runs all the pipeline steps
def main():
    # Setup the argument parser
    arg_parser = argparse.ArgumentParser()
    # Option to save a local backup
    arg_parser.add_argument("--save-local", action="store_true", help="Save a local copy of API data")
    # Parse the arguments
    parsed_args = arg_parser.parse_args()
    # Print start message
    print("=== STARTING COURSE CATALOGUE PIPELINE ===")
    # Step 1: Get data from the API
    found_courses = get_all_courses_with_backup(parsed_args.save_local)
    # Connect to the database
    db_conn = psycopg2.connect(host=DATABASE_HOST, port=DATABASE_PORT, dbname=DATABASE_NAME, user=DATABASE_USER, password=DATABASE_PASSWORD)
    # Step 2: Save to the bronze table
    bronze_saved = save_all_courses_to_bronze_table(db_conn, found_courses)
    print("Bronze Table Updated: " + str(bronze_saved) + " records saved.")
    # Step 3: Transform to the silver table
    silver_saved = run_silver_transformation_process(db_conn)
    print("Silver Table Updated: " + str(silver_saved) + " records saved.")
    # Close connection
    db_conn.close()
    # Final message
    print("SUCCESS: Course catalogue pipeline completed.")

# If this script is run directly
if __name__ == '__main__':
    # Start the program
    main()
