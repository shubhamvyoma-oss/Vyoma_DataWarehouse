# Import the tool for handling command line arguments
import argparse
# Import the date and time tool for working with dates
import datetime
# Import the time tool to allow the script to wait or sleep
import time
# Import the requests tool to make web calls to the API
import requests
# Import the csv tool to handle reading and writing CSV files
import csv
# Import the os tool for working with file and folder paths
import os

# ================== CONFIGURATION SETTINGS ==================

# The folder on the computer where the final CSV file will be saved
OUTPUT_DATA_FOLDER_PATH = r"C:\Users\asust\E-Learning-Vyoma-DataWarehouse\Local_no_db"

# The secret key needed to talk to the Edmingle API
API_SECRET_KEY_VALUE = "590605228a847624e065a76e986803fa"

# Our unique organization ID number
ORGANIZATION_ID_NUMBER = 683

# The main web address for the Edmingle API
BASE_API_URL_TEXT = "https://vyoma-api.edmingle.com/nuSource/api/v1"

# The specific web address used to download reports
FULL_REPORT_API_URL = BASE_API_URL_TEXT + "/report/csv"

# The headers we must send with every API request for security
API_AUTHENTICATION_HEADERS = {
    "apikey": API_SECRET_KEY_VALUE, 
    "ORGID": str(ORGANIZATION_ID_NUMBER)
}

# Settings to control how fast we talk to the API
MAX_API_CALLS_PER_MINUTE = 25
REST_PERIOD_SECONDS = 60

# The time zone for India (GMT +5:30)
INDIA_TIME_ZONE_SETTING = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# ==========================================================

# --- HELPER FUNCTIONS ---

# This function safely converts any value into a whole number (integer)
def convert_value_to_safe_integer(input_value):
    # Check if the input is empty
    if input_value is None:
        # Return None if no value
        return None
    # Try to convert the value
    try:
        # Convert to text, remove extra spaces, and turn into integer
        return int(str(input_value).strip())
    # If it fails, catch the error
    except Exception:
        # Return None if it could not be converted
        return None

# This function prepares the settings (parameters) for an API request
def create_api_query_parameters(date_text_string):
    # Turn the date text into a date object
    date_object = datetime.datetime.strptime(date_text_string, "%Y-%m-%d")
    # Tell the object to use the India time zone
    date_with_timezone = date_object.replace(tzinfo=INDIA_TIME_ZONE_SETTING)
    # Get the timestamp for the start of the day (00:00:00)
    start_of_day_unix = int(date_with_timezone.replace(hour=0, minute=0, second=0).timestamp())
    # Get the timestamp for the end of the day (23:59:59)
    end_of_day_unix = int(date_with_timezone.replace(hour=23, minute=59, second=59).timestamp())
    # Return a dictionary of settings for the API
    return {
        "report_type": 55, 
        "organization_id": ORGANIZATION_ID_NUMBER,
        "start_time": start_of_day_unix, 
        "end_time": end_of_day_unix, 
        "response_type": 1
    }

# This function makes the actual call to the API and handles retries
def call_api_with_retries(query_settings_dict):
    # Try the call up to 3 times in case of temporary issues
    for current_attempt in range(3):
        # Use try to catch network errors
        try:
            # Send the request to the API
            api_resp = requests.get(FULL_REPORT_API_URL, headers=API_AUTHENTICATION_HEADERS, params=query_settings_dict, timeout=60)
            # Check if access was denied (401 error)
            if api_resp.status_code == 401:
                return None
            # Check if the call was successful (200 OK)
            if api_resp.status_code == 200:
                return api_resp
            # If unsuccessful, wait 10 seconds before trying again
            time.sleep(10)
        # Catch network or connection errors
        except Exception:
            # Wait 10 seconds before the next try
            time.sleep(10)
    # Return None if all 3 tries failed
    return None

# This function gets all data rows for one specific day from the API
def fetch_all_rows_for_specific_day(date_iso_string):
    # Prepare the settings for the call
    api_query_params = create_api_query_parameters(date_iso_string)
    # Ask the API for the data
    api_result_object = call_api_with_retries(api_query_params)
    # If the API call failed completely
    if api_result_object is None:
        # Return an empty list
        return []
    # Convert the API answer into a JSON dictionary
    answer_json_data = api_result_object.json()
    # Find the rows of data under common names like 'data' or 'rows'
    data_rows_list = answer_json_data.get("data")
    if data_rows_list is None:
        data_rows_list = answer_json_data.get("rows")
    if data_rows_list is None:
        data_rows_list = answer_json_data.get("report")
    if data_rows_list is None:
        data_rows_list = []
    # Return the list of rows
    return data_rows_list

# This function creates a starting record for a batch and class summary
def create_new_summary_record(row_data_dict, batch_id_num, class_id_num, pull_date_obj):
    # Return a dictionary with starting values (zeros)
    return {
        "batch_id": batch_id_num, 
        "batch_name": row_data_dict.get("batchName"),
        "class_id": class_id_num, 
        "bundle_id": convert_value_to_safe_integer(row_data_dict.get("bundle_Id") or row_data_dict.get("bundle_id")),
        "bundle_name": row_data_dict.get("bundleName"), 
        "class_date": str(pull_date_obj),
        "present_count": 0, "late_count": 0, "absent_count": 0, "total_records": 0,
        "attendance_id": convert_value_to_safe_integer(row_data_dict.get("attendance_id")),
        "teacher_id": convert_value_to_safe_integer(row_data_dict.get("teacher_Id") or row_data_dict.get("teacher_id")),
        "teacher_name": row_data_dict.get("teacherName"), 
        "class_duration": row_data_dict.get("classDuration"),
        "pull_date": str(pull_date_obj), 
        "loaded_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "attendance_pct": 0.0
    }

# This function updates the counts in a summary record based on a student's status
def update_attendance_counts(summary_record, status_code_text):
    # Clean up the status text (P, L, or A)
    clean_status = str(status_code_text).strip().upper()
    # Check if the student was present (P)
    if clean_status == "P":
        summary_record["present_count"] = summary_record["present_count"] + 1
    # Check if the student was late (L)
    elif clean_status == "L":
        summary_record["late_count"] = summary_record["late_count"] + 1
    # Check if the student was absent (A)
    elif clean_status == "A":
        summary_record["absent_count"] = summary_record["absent_count"] + 1
    # Always increment the total records count
    summary_record["total_records"] = summary_record["total_records"] + 1

# This function summarizes many data rows into a list of summary records
def calculate_daily_summary(rows_of_data, pull_date_val):
    # Create a dictionary to group records by batch and class
    summary_group_map = {}
    # Loop through each row of data
    for individual_row in rows_of_data:
        # Get the IDs for the batch and the class
        b_id = convert_value_to_safe_integer(individual_row.get("batch_Id") or individual_row.get("batch_id"))
        c_id = convert_value_to_safe_integer(individual_row.get("class_Id") or individual_row.get("class_id"))
        # Skip this row if IDs are missing
        if b_id is None or c_id is None:
            continue
        # Create a unique text key for this batch and class
        grouping_key = str(b_id) + "_" + str(c_id)
        # If we haven't seen this batch and class before, create a new record
        if grouping_key not in summary_group_map:
            summary_group_map[grouping_key] = create_new_summary_record(individual_row, b_id, c_id, pull_date_val)
        # Update the counts for this record
        update_attendance_counts(summary_group_map[grouping_key], individual_row.get("studentAttendanceStatus", ""))
    # Calculate the attendance percentage for each summary record
    for record_item in summary_group_map.values():
        total = record_item["total_records"]
        present = record_item["present_count"]
        # Only calculate if there is at least one record
        if total > 0:
            record_item["attendance_pct"] = round((present * 100.0) / total, 2)
    # Return all the summary records as a list
    return list(summary_group_map.values())

# This function defines the column names for the final CSV file
def define_csv_header_names():
    # Return a list of header strings
    return [
        "batch_id", "batch_name", "class_id", "bundle_id", "bundle_name",
        "class_date", "present_count", "absent_count", "total_records",
        "attendance_pct", "attendance_id", "teacher_id", "teacher_name",
        "class_duration", "pull_date", "loaded_at"
    ]

# This function saves the final summary list into a CSV file
def save_summary_to_csv_file(summary_data_list, start_date_val, end_date_val):
    # Stop if there is no data to save
    if len(summary_data_list) == 0:
        return
    # Check if the output folder exists, create it if not
    if os.path.exists(OUTPUT_DATA_FOLDER_PATH) == False:
        os.makedirs(OUTPUT_DATA_FOLDER_PATH)
    # Build a filename using the date range
    csv_file_name = "attendance_summary_" + str(start_date_val) + "_to_" + str(end_date_val) + ".csv"
    # Create the complete path to the file
    full_output_path = os.path.join(OUTPUT_DATA_FOLDER_PATH, csv_file_name)
    # Try to open and write to the file
    try:
        # Open the file for writing
        with open(full_output_path, "w", newline="", encoding="utf-8") as csv_output_file:
            # Create a CSV writer that uses dictionaries
            csv_writer = csv.DictWriter(csv_output_file, fieldnames=define_csv_header_names())
            # Write the top header row
            csv_writer.writeheader()
            # Loop through the list and write each record as a row
            for summary_row in summary_data_list:
                csv_writer.writerow(summary_row)
        # Print a success message
        print("Success! Summary saved to: " + full_output_path)
    # Catch any errors during saving
    except Exception as save_error:
        print("Error saving the CSV file: " + str(save_error))

# This is the main function that runs the whole research process
def main():
    # Setup the argument parser for the command line
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--date", help="Specific date YYYY-MM-DD")
    arg_parser.add_argument("--start", help="Start date YYYY-MM-DD")
    arg_parser.add_argument("--end", help="End date YYYY-MM-DD")
    # Read the arguments from the user
    user_args = arg_parser.parse_args()
    
    # Work out which dates we need to process
    dates_to_pull = []
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    # Case 1: User provided a single date
    if user_args.date:
        dates_to_pull = [datetime.date.fromisoformat(user_args.date)]
    # Case 2: User provided a start date
    elif user_args.start:
        current_day = datetime.date.fromisoformat(user_args.start)
        # Use end date or default to yesterday
        if user_args.end:
            stop_day = datetime.date.fromisoformat(user_args.end)
        else:
            stop_day = yesterday_date
        # Create a list of all dates in the range
        while current_day <= stop_day:
            dates_to_pull.append(current_day)
            current_day = current_day + datetime.timedelta(days=1)
    # Case 3: No dates provided, use yesterday
    else:
        dates_to_pull = [yesterday_date]
    
    # Fetch and summarize data for each date
    final_compiled_records = []
    for i in range(len(dates_to_pull)):
        # To avoid hitting the API limit, take a break every few calls
        if i > 0:
            if i % MAX_API_CALLS_PER_MINUTE == 0:
                time.sleep(REST_PERIOD_SECONDS)
        # Get the date string for this loop
        current_date_iso = dates_to_pull[i].isoformat()
        # Fetch the rows for this day
        daily_rows = fetch_all_rows_for_specific_day(current_date_iso)
        # If we got data, summarize it and add to our big list
        if len(daily_rows) > 0:
            daily_summary_list = calculate_daily_summary(daily_rows, dates_to_pull[i])
            final_compiled_records.extend(daily_summary_list)
    
    # Save all the compiled results into one CSV file
    save_summary_to_csv_file(final_compiled_records, dates_to_pull[0], dates_to_pull[-1])

# If this file is run directly by the user
if __name__ == "__main__":
    # Run the main process
    main()
