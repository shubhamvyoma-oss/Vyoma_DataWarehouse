# Import the tool for handling command line arguments
import argparse
# Import the date and time tool for working with dates
import datetime
# Import the time tool to allow the script to wait or sleep
import time
# Import the os tool for working with file and folder paths
import os
# Import the psycopg2 tool to connect to the Postgres database
import psycopg2
# Import extra database tools for handling JSON data in SQL
import psycopg2.extras
# Import the requests tool to make web calls to the API
import requests

# Try to import our custom local storage helper function
try:
    # Attempt to import the saving function from our helper file
    from local_storage_helper import save_data_to_local_file
# If the import fails, it might be because of the folder structure
except ImportError:
    # Import the system tool to modify the search path
    import sys
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
ORGANIZATION_ID = 683
# The unique ID for the institution in the API
INSTITUTION_ID = 483

# The main web address for the Edmingle API
API_BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# The specific web address for downloading reports
REPORT_API_URL = API_BASE_URL + "/report/csv"

# The headers we must send with every API request for security
API_HEADERS = {
    "apikey": API_SECRET_KEY, 
    "ORGID": str(ORGANIZATION_ID)
}

# How many API calls to make before taking a break
MAX_API_CALLS_BEFORE_PAUSE = 25
# How many seconds to wait during the break
PAUSE_DURATION_SECONDS = 60
# The time zone for India (GMT +5:30)
INDIA_TIME_ZONE = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# --- SQL QUERIES ---

# SQL command to insert raw attendance data into the bronze table
# We use ON CONFLICT to update the record if it already exists
SQL_INSERT_ATTENDANCE_BRONZE = """
    INSERT INTO bronze.attendance_raw (
        pull_date, student_id, student_name, reg_no,
        student_email, student_contact, student_batch_status,
        batch_id, batch_name, class_id, class_name,
        bundle_id, bundle_name, course_id, course_name,
        attendance_id, session_name, teacher_id, teacher_name, 
        teacher_email, teacher_class_signin_status,
        attendance_status, class_date, class_date_parsed,
        start_time, end_time, class_duration,
        student_rating, student_comments, raw_payload
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (student_id, class_id) DO UPDATE SET
        pull_date = EXCLUDED.pull_date, 
        attendance_status = EXCLUDED.attendance_status,
        teacher_class_signin_status = EXCLUDED.teacher_class_signin_status,
        student_rating = EXCLUDED.student_rating, 
        student_comments = EXCLUDED.student_comments,
        loaded_at = NOW()
"""

# SQL command to summarize attendance data and save it in the silver table
SQL_UPSERT_ATTENDANCE_SILVER = """
    INSERT INTO silver.class_attendance
        (batch_id, batch_name, class_id, bundle_id, bundle_name, 
         class_date, present_count, late_count, absent_count, 
         total_records, attendance_pct, attendance_id, 
         teacher_id, teacher_name, class_duration, pull_date)
    SELECT
        b.batch_id, MIN(b.batch_name), b.class_id, b.bundle_id, MIN(b.bundle_name),
        MIN(b.class_date_parsed), COUNT(*) FILTER (WHERE b.attendance_status = 'P'),
        COUNT(*) FILTER (WHERE b.attendance_status = 'L'), COUNT(*) FILTER (WHERE b.attendance_status = 'A'),
        COUNT(*) FILTER (WHERE b.attendance_status IN ('P','L','A')),
        ROUND(COUNT(*) FILTER (WHERE b.attendance_status = 'P') * 100.0 / NULLIF(COUNT(*) FILTER (WHERE b.attendance_status IN ('P','L','A')), 0), 2),
        MIN(b.attendance_id), MIN(b.teacher_id), MIN(b.teacher_name), MIN(b.class_duration), %(pull_date)s
    FROM bronze.attendance_raw b
    WHERE b.pull_date = %(pull_date)s AND b.batch_id IS NOT NULL AND b.class_id IS NOT NULL AND b.attendance_status IN ('P', 'L', 'A')
    GROUP BY b.batch_id, b.class_id, b.bundle_id
    ON CONFLICT (batch_id, class_id) DO UPDATE SET
        batch_name = EXCLUDED.batch_name, bundle_name = EXCLUDED.bundle_name,
        class_date = EXCLUDED.class_date, present_count = EXCLUDED.present_count,
        late_count = EXCLUDED.late_count, absent_count = EXCLUDED.absent_count,
        total_records = EXCLUDED.total_records, attendance_pct = EXCLUDED.attendance_pct,
        attendance_id = EXCLUDED.attendance_id, teacher_id = EXCLUDED.teacher_id,
        teacher_name = EXCLUDED.teacher_name, class_duration = EXCLUDED.class_duration,
        pull_date = EXCLUDED.pull_date
"""

# SQL command to update the class sequence numbers for each batch
SQL_UPDATE_CLASS_NUMBERS = """
    UPDATE silver.class_attendance ca SET class_number = sub.rn
    FROM (
        SELECT id, ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY class_date ASC) AS rn
        FROM silver.class_attendance
        WHERE batch_id IN (SELECT DISTINCT batch_id FROM bronze.attendance_raw WHERE pull_date = %(pull_date)s)
    ) sub WHERE ca.id = sub.id
"""

# --- HELPER FUNCTIONS ---

# This function safely converts a value to an integer
def convert_to_safe_integer(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None if there is no value
        return None
    # Try to convert the value
    try:
        # Convert to string, remove extra spaces, then to integer
        return int(str(input_value).strip())
    # If conversion fails, catch the error
    except Exception:
        # Return None if it could not be converted
        return None

# This function tries to turn a date string into a date object
def parse_date_string_to_object(date_text):
    # Check if the string is empty
    if not date_text:
        # Return None if empty
        return None
    # Remove any extra spaces from the start or end
    clean_date_text = str(date_text).strip()
    # A list of different ways dates might be written
    date_formats = ["%d %b %Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y"]
    # Try each format one by one
    for date_format in date_formats:
        # Use try to see if the format matches
        try:
            # Try to parse the string using the current format
            return datetime.datetime.strptime(clean_date_text, date_format).date()
        # If it doesn't match, move to the next format
        except Exception:
            # Continue to the next format in the list
            continue
    # Return None if no formats worked
    return None

# This function prepares the parameters for the API request
def create_api_parameters(date_string):
    # Turn the date string into a datetime object
    date_object = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    # Tell the object to use the India time zone
    date_with_timezone = date_object.replace(tzinfo=INDIA_TIME_ZONE)
    # Get the time stamp for the very beginning of the day (00:00:00)
    start_timestamp = int(date_with_timezone.replace(hour=0, minute=0, second=0).timestamp())
    # Get the time stamp for the very end of the day (23:59:59)
    end_timestamp = int(date_with_timezone.replace(hour=23, minute=59, second=59).timestamp())
    # Return a dictionary containing all the settings for the API
    return {
        "report_type": 55, 
        "organization_id": ORGANIZATION_ID,
        "start_time": start_timestamp, 
        "end_time": end_timestamp, 
        "response_type": 1
    }

# This function extracts the rows of data from the API's JSON response
def extract_rows_from_json(json_response_data):
    # Try to find the data under the 'data' key
    rows_list = json_response_data.get("data")
    # If not found, try the 'rows' key
    if rows_list is None:
        rows_list = json_response_data.get("rows")
    # If still not found, try the 'report' key
    if rows_list is None:
        rows_list = json_response_data.get("report")
    # If nothing was found, use an empty list
    if rows_list is None:
        rows_list = []
    # Return the list of rows
    return rows_list

# This function makes the actual call to the Edmingle API
def call_edmingle_api(parameters_dict, date_for_logging):
    # Try the API call up to 3 times in case of temporary network issues
    for attempt_count in range(3):
        # Use try to catch network errors
        try:
            # Send a GET request to the API web address
            response = requests.get(REPORT_API_URL, headers=API_HEADERS, params=parameters_dict, timeout=60)
            # Check if we were denied access (401 error)
            if response.status_code == 401:
                # Print a message and stop trying
                print("[" + date_for_logging + "] API Error: Unauthorized access")
                return None
            # Check if the call was successful (200 OK)
            if response.status_code == 200:
                # Return the successful response
                return response
            # If it wasn't successful or denied, wait 10 seconds before trying again
            time.sleep(10)
        # Catch any other errors like no internet
        except Exception as error:
            # Print the error and wait before retrying
            print("Network Error: " + str(error))
            time.sleep(10)
    # Return None if all 3 attempts failed
    return None

# This function fetches all attendance records for one specific day
def fetch_attendance_for_one_day(date_to_fetch, save_to_local_flag=False):
    # Prepare the settings for the API call
    api_params = create_api_parameters(date_to_fetch)
    # Ask the API for the data
    api_response = call_edmingle_api(api_params, date_to_fetch)
    # If the API call failed, stop here
    if api_response is None:
        return None
    # Get the list of rows from the API's answer
    all_data_rows = extract_rows_from_json(api_response.json())
    # Check if we should save a copy of this data on our computer
    if save_to_local_flag == True:
        # Check if we actually got any data to save
        if len(all_data_rows) > 0:
            # Use our helper to save the data as a CSV file
            save_data_to_local_file(all_data_rows, 'attendance', 'attendance_raw_' + date_to_fetch, 'csv')
    # Return the data we found
    return all_data_rows

# This function checks if an email address belongs to a staff member
def is_staff_member(email_address_text):
    # Check if the email is missing
    if email_address_text is None:
        # If no email, assume they are not staff
        return False
    # Check if the word '@vyoma' is inside the email address
    # We convert to lowercase to make sure we don't miss '@Vyoma' or '@VYOMA'
    if "@vyoma" in str(email_address_text).lower():
        # It belongs to Vyoma staff
        return True
    # If the word was not found, they are not staff
    return False

# This function extracts the student's email from a data row
def get_student_email_from_row(data_row_dict):
    # Look for the email under the name 'studentEmail'
    student_email = data_row_dict.get("studentEmail")
    # If not found, look under the name 'student_email'
    if student_email is None:
        student_email = data_row_dict.get("student_email")
    # Return whatever we found (might be None)
    return student_email

# This function extracts all the important IDs from a data row
def get_all_ids_from_data_row(row_dict):
    # Create an empty dictionary to store our extracted IDs
    extracted_ids = {}
    # Extract the student ID safely
    extracted_ids["student_id"] = convert_to_safe_integer(row_dict.get("student_Id") or row_dict.get("student_id"))
    # Extract the batch ID safely
    extracted_ids["batch_id"] = convert_to_safe_integer(row_dict.get("batch_Id") or row_dict.get("batch_id"))
    # Extract the class ID safely
    extracted_ids["class_id"] = convert_to_safe_integer(row_dict.get("class_Id") or row_dict.get("class_id"))
    # Extract the bundle ID safely
    extracted_ids["bundle_id"] = convert_to_safe_integer(row_dict.get("bundle_Id") or row_dict.get("bundle_id"))
    # Extract the course ID safely
    extracted_ids["course_id"] = convert_to_safe_integer(row_dict.get("course_Id") or row_dict.get("course_id"))
    # Extract the teacher ID safely
    extracted_ids["teacher_id"] = convert_to_safe_integer(row_dict.get("teacher_Id") or row_dict.get("teacher_id"))
    # Extract the attendance record ID safely
    extracted_ids["attendance_id"] = convert_to_safe_integer(row_dict.get("attendance_id"))
    # Extract the student rating safely
    extracted_ids["student_rating"] = convert_to_safe_integer(row_dict.get("studentRating"))
    # Extract and parse the class date safely
    extracted_ids["parsed_date"] = parse_date_string_to_object(row_dict.get("classDate"))
    # Return the dictionary of IDs
    return extracted_ids

# This function organizes one row of data into a format ready for the database
def prepare_data_for_bronze_insertion(row_dict, pull_date_object, email_address):
    # Get all the numeric IDs and dates from the row
    id_dictionary = get_all_ids_from_data_row(row_dict)
    # Create a list (tuple) of values in the exact order the database expects
    return (
        pull_date_object, id_dictionary["student_id"], row_dict.get("studentName"), 
        row_dict.get("regNo"), email_address, row_dict.get("studentContact"), 
        row_dict.get("studentBatchStatus"), id_dictionary["batch_id"], 
        row_dict.get("batchName"), id_dictionary["class_id"], row_dict.get("className"),
        id_dictionary["bundle_id"], row_dict.get("bundleName"), 
        id_dictionary["course_id"], row_dict.get("courseName"),
        id_dictionary["attendance_id"], row_dict.get("sessionName"), 
        id_dictionary["teacher_id"], row_dict.get("teacherName"),
        row_dict.get("teacherEmail"), row_dict.get("teacherClassSigninStatus"),
        row_dict.get("studentAttendanceStatus"), row_dict.get("classDate"), 
        id_dictionary["parsed_date"], row_dict.get("startTime"), 
        row_dict.get("endTime"), row_dict.get("classDuration"),
        id_dictionary["student_rating"], row_dict.get("studentComments"), 
        psycopg2.extras.Json(row_dict)
    )

# This function saves a list of rows into the bronze database table
def save_data_to_bronze_table(db_connection, data_rows_list, pull_date):
    # Keep track of how many records we successfully saved
    saved_records_count = 0
    # Keep track of how many staff members we skipped
    skipped_staff_count = 0
    # Create a cursor to talk to the database
    database_cursor = db_connection.cursor()
    # Loop through every row of data we got from the API
    for current_item in data_rows_list:
        # Get the email address for this student
        student_email = get_student_email_from_row(current_item)
        # Check if this person is a staff member
        if is_staff_member(student_email) == True:
            # Increment staff count and move to the next item
            skipped_staff_count = skipped_staff_count + 1
            continue
        # Prepare the data in the right format for SQL
        values_to_insert = prepare_data_for_bronze_insertion(current_item, pull_date, student_email)
        # Tell the database to run the insert command
        database_cursor.execute(SQL_INSERT_ATTENDANCE_BRONZE, values_to_insert)
        # Check if the database updated or inserted a row
        if database_cursor.rowcount > 0:
            # Increment the success count
            saved_records_count = saved_records_count + 1
    # Save all the changes to the database permanently
    db_connection.commit()
    # Close the database tool
    database_cursor.close()
    # Return the counts so we can print them later
    return saved_records_count, skipped_staff_count

# This function runs the logic to summarize data into the silver table
def update_silver_attendance_summary(db_connection, pull_date_object):
    # Create a cursor to talk to the database
    database_cursor = db_connection.cursor()
    # Run the SQL that aggregates raw data into summaries
    database_cursor.execute(SQL_UPSERT_ATTENDANCE_SILVER, {"pull_date": pull_date_object})
    # Get the number of summary rows created or updated
    silver_rows_count = database_cursor.rowcount
    # Run the SQL that calculates the class sequence numbers (Class 1, Class 2, etc.)
    database_cursor.execute(SQL_UPDATE_CLASS_NUMBERS, {"pull_date": pull_date_object})
    # Save the changes to the database
    db_connection.commit()
    # Close the cursor
    database_cursor.close()
    # Return how many records were processed
    return silver_rows_count

# This function creates a list of dates between a start and end date
def create_list_of_dates(start_date, end_date):
    # Start with an empty list
    date_list = []
    # Begin at the starting date
    current_date = start_date
    # Loop while we haven't reached the end date yet
    while current_date <= end_date:
        # Add the current date to our list
        date_list.append(current_date)
        # Move to the next day by adding one day of time
        current_date = current_date + datetime.timedelta(days=1)
    # Return the full list of dates
    return date_list

# This function decides which dates we need to pull data for
def determine_target_dates(parsed_arguments):
    # Calculate yesterday's date as the default
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    # If the user provided a single specific date
    if parsed_arguments.date:
        # Return that one date in a list
        return [datetime.date.fromisoformat(parsed_arguments.date)]
    # If the user provided a starting date
    if parsed_arguments.start:
        # Convert the start date string to a date object
        start_day = datetime.date.fromisoformat(parsed_arguments.start)
        # If the user also provided an end date
        if parsed_arguments.end:
            # Convert the end date string
            end_day = datetime.date.fromisoformat(parsed_arguments.end)
        else:
            # Otherwise, use yesterday as the end date
            end_day = yesterday
        # Create and return the list of dates in between
        return create_list_of_dates(start_day, end_day)
    # If no dates were provided, just return yesterday in a list
    return [yesterday]

# This function processes all the tasks for one single date
def process_data_for_specific_date(db_connection, date_object, is_dry_run, should_save_local):
    # Convert the date object to a simple string like '2023-10-27'
    date_string = date_object.isoformat()
    # Fetch the attendance rows from the Edmingle API
    found_rows = fetch_attendance_for_one_day(date_string, should_save_local)
    # If we couldn't get any data, stop here
    if found_rows is None:
        # Return empty counts to signify failure
        return None, None, None
    # If this is just a dry run (test), don't save to the database
    if is_dry_run == True:
        # Return the number of rows found, but 0 for database counts
        return len(found_rows), 0, 0
    # Save the raw data into the bronze database table
    bronze_count, staff_count = save_data_to_bronze_table(db_connection, found_rows, date_object)
    # Create the summary records in the silver database table
    silver_count = update_silver_attendance_summary(db_connection, date_object)
    # Return the final counts for this date
    return bronze_count, silver_count, staff_count

# This is the main loop that goes through each date in our list
def run_main_attendance_pipeline(db_connection, target_dates_list, is_dry_run, should_save_local):
    # Total counters for all dates combined
    grand_total_bronze = 0
    grand_total_silver = 0
    grand_total_staff = 0
    # Loop through each date in our list using an index 'i'
    for i in range(len(target_dates_list)):
        # Get the date at the current position
        current_date = target_dates_list[i]
        # To avoid overwhelming the API, take a break every few calls
        if i > 0:
            if i % MAX_API_CALLS_BEFORE_PAUSE == 0:
                # Wait for a minute before continuing
                time.sleep(PAUSE_DURATION_SECONDS)
        # Process the data for this specific date
        b_count, sil_count, s_count = process_data_for_specific_date(db_connection, current_date, is_dry_run, should_save_local)
        # If the processing was successful
        if b_count is not None:
            # Add the counts to our grand totals
            grand_total_bronze = grand_total_bronze + b_count
            grand_total_silver = grand_total_silver + sil_count
            grand_total_staff = grand_total_staff + s_count
            # Print a progress message for this date
            print("[" + current_date.isoformat() + "] Saved to Bronze: " + str(b_count) + ", Saved to Silver: " + str(sil_count))
    # Return the final grand totals
    return grand_total_bronze, grand_total_silver, grand_total_staff

# This is the entry point function that starts the whole script
def main():
    # Setup the tool to read command line arguments
    argument_parser = argparse.ArgumentParser()
    # Add options for specific dates or date ranges
    argument_parser.add_argument("--date", help="One specific date in YYYY-MM-DD format")
    argument_parser.add_argument("--start", help="Start date in YYYY-MM-DD format")
    argument_parser.add_argument("--end", help="End date in YYYY-MM-DD format")
    # Add an option to run without saving to the database
    argument_parser.add_argument("--dry-run", action="store_true", help="Run without saving to DB")
    # Add an option to save a backup copy on the local computer
    argument_parser.add_argument("--save-local", action="store_true", help="Save a backup file locally")
    # Parse the arguments the user typed
    args = argument_parser.parse_args()
    # Work out which dates we need to fetch
    dates_to_process = determine_target_dates(args)
    # Print a starting message
    print("ATTENDANCE PIPELINE STARTED. Number of dates: " + str(len(dates_to_process)))
    # Create a variable for the database connection
    db_conn = None
    # If this is not a dry run, connect to the database
    if args.dry_run == False:
        # Open the connection using our settings
        db_conn = psycopg2.connect(host=DATABASE_HOST, dbname=DATABASE_NAME, user=DATABASE_USER, password=DATABASE_PASSWORD, port=DATABASE_PORT)
    # Run the pipeline for all target dates
    total_bronze, total_silver, total_staff = run_main_attendance_pipeline(db_conn, dates_to_process, args.dry_run, args.save_local)
    # Print a final summary message
    print("PIPELINE FINISHED. Total Bronze: " + str(total_bronze) + ", Total Silver: " + str(total_silver) + ", Staff records skipped: " + str(total_staff))
    # If the database was connected, close it now
    if db_conn is not None:
        db_conn.close()

# If this file is run directly, call the main function
if __name__ == "__main__":
    # Start the program
    main()
