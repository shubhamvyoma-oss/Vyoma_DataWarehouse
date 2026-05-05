import requests
import psycopg2
import json

# This is the address where the webhook receiver program is running
# We need this to send our test data to the pipeline
WEBHOOK_URL = "http://localhost:5000/webhook"

# These are the settings to connect to our database
# We need these to check if our data arrived safely in the tables
DATABASE_HOST = "localhost"
DATABASE_PORT = 5432
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"

# This is a list of test events that look like real messages from Edmingle
# We use these to test if our system can handle different types of data
TEST_EVENTS = [
    {
        "id": "txn-initiated-001",
        "event_name": "transaction.user_purchase_initiated",
        "event_timestamp": 1709853000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "email": "test.student@example.com",
            "full_name": "Test Student",
            "bundle_id": 12477,
            "course_name": "Sanskrit Foundation Course",
            "institution_bundle_id": 363,
            "master_batch_id": 1281,
            "master_batch_name": "Batch A 2024",
            "original_price": 5000.00,
            "discount": 500.00,
            "final_price": 4500.00,
            "currency": "INR",
            "credits_applied": 0.00,
            "start_date": 1709856600,
            "end_date": 1741392600,
            "created_at": 1709853000,
        },
    },
    {
        "id": "txn-failed-001",
        "event_name": "transaction.user_purchase_failed",
        "event_timestamp": 1709854000,
        "is_live_mode": False,
        "data": {
            "user_id": 99002,
            "email": "another.student@example.com",
            "full_name": "Another Student",
            "bundle_id": 12477,
            "course_name": "Sanskrit Foundation Course",
            "final_price": 4500.00,
            "currency": "INR",
            "payment_method": "razorpay",
        },
    },
    {
        "id": "session-created-001",
        "event_name": "session.session_created",
        "event_timestamp": 1709856600,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar - Lesson 1",
            "class_type_formatted": "Live Class",
            "gmt_start_time": 1709870400,
            "gmt_end_time": 1709874000,
            "duration_minutes": 60,
            "taken_by": 15,
            "taken_by_name": "Prof. Ramesh Sharma",
            "taken_by_email": "ramesh@example.com",
            "master_batches": [
                {
                    "master_batch_id": 1281,
                    "master_batch_name": "Batch A 2024",
                    "bundle_id": 12477,
                    "bundle_name": "Sanskrit Foundation Course",
                }
            ],
            "schedule_id": 5001,
            "is_recurring": True,
            "status": 0,
            "virtual_class_type_formatted": "Zoom",
            "zoom_meeting_id": "123456789",
        },
    },
    {
        "id": "session-update-001",
        "event_name": "session.session_update",
        "event_timestamp": 1709857000,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar - Lesson 1",
            "gmt_start_time": 1709874000,
            "gmt_end_time": 1709877600,
        },
    },
    {
        "id": "session-cancel-001",
        "event_name": "session.session_cancel",
        "event_timestamp": 1709858000,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "cancellation_reason": "Teacher unavailable",
            "cancelled_by": 15,
        },
    },
    {
        "id": "session-started-001",
        "event_name": "session.session_started",
        "event_timestamp": 1709874300,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "class_id": 121333,
            "class_name": "Sanskrit Grammar - Lesson 1",
            "gmt_start_time": 1709874000,
            "actual_start_time": 1709874300,
            "taken_at": 1709874300,
            "status": 1,
            "is_late_signin": True,
            "delay_minutes": 5,
        },
    },
    {
        "id": "session-reminder-001",
        "event_name": "session.session_reminders",
        "event_timestamp": 1709870400,
        "is_live_mode": False,
        "data": {
            "attendance_id": 8914546,
            "reminder_type": "1h_before",
        },
    },
    {
        "id": "assess-test-sub-001",
        "event_name": "assessments.test_submitted",
        "event_timestamp": 1709860000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55001,
            "mark": 0,
            "is_evaluated": 0,
            "submitted_at": 1709860000,
        },
    },
    {
        "id": "assess-test-eval-001",
        "event_name": "assessments.test_evaluated",
        "event_timestamp": 1709861000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55001,
            "mark": 85.5,
            "is_evaluated": 1,
            "faculty_comments": "Good work. Review chapter 3.",
            "submitted_at": 1709860000,
        },
    },
    {
        "id": "assess-ex-sub-001",
        "event_name": "assessments.exercise_submitted",
        "event_timestamp": 1709862000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55002,
            "exercise_id": 7001,
            "mark": 0,
            "is_evaluated": 0,
            "submitted_at": 1709862000,
        },
    },
    {
        "id": "assess-ex-eval-001",
        "event_name": "assessments.exercise_evaluated",
        "event_timestamp": 1709863000,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "attempt_id": 55002,
            "exercise_id": 7001,
            "mark": 90.0,
            "is_evaluated": 1,
            "faculty_comments": "Excellent exercise work.",
            "submitted_at": 1709862000,
        },
    },
    {
        "id": "course-completed-001",
        "event_name": "course.user_course_completed",
        "event_timestamp": 1741392600,
        "is_live_mode": False,
        "data": {
            "user_id": 99001,
            "bundle_id": 12477,
            "completed_at": 1741392600,
        },
    },
    {
        "id": "announce-001",
        "event_name": "announcement.announcement_created",
        "event_timestamp": 1709857000,
        "is_live_mode": False,
        "data": {
            "announcement_id": 301,
            "title": "System Maintenance Notice",
            "message": "Platform will be down for maintenance on Sunday.",
        },
    },
    {
        "id": "cert-001",
        "event_name": "certificate.certificate_issued",
        "event_timestamp": 1741392700,
        "is_live_mode": False,
        "data": {
            "certificate_id": "CERT-2024-99001-12477",
            "user_id": 99001,
            "issued_at": 1741392700,
        },
    },
]

# This function sends one test event to the webhook receiver
def send_one_test_event(event_dictionary):
    # We get the event name for printing to the screen
    event_name = event_dictionary["event_name"]
    try:
        # We use the requests library to send a POST message
        # We include the event as JSON data and wait up to 5 seconds
        response = requests.post(WEBHOOK_URL, json=event_dictionary, timeout=5)
        # We check if the server responded with a success code (200)
        if response.status_code == 200:
            # We print that it worked
            print("  OK    " + event_name)
            return True
        else:
            # We print that it failed and show the status code
            print("  FAIL  " + event_name + " (Status: " + str(response.status_code) + ")")
            return False
    except Exception as error_message:
        # If the server is not running, we catch the error here
        print("  FAIL  " + event_name + " (Error: " + str(error_message) + ")")
        return False

# This function tries to connect to our analytics database
def get_database_connection():
    try:
        # We start the connection with our settings
        connection = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # We give back the successful connection
        return connection
    except Exception as error_message:
        # If we cannot connect, we show the reason
        print("Could not connect to the database.")
        print(error_message)
        # We give back nothing to show failure
        return None

# This function checks if a specific table has any rows in it
def check_if_table_has_data(database_cursor, table_name):
    try:
        # We write a SQL command to count everything in the table
        sql_command = "SELECT COUNT(*) FROM " + table_name
        # We tell the database to run the count command
        database_cursor.execute(sql_command)
        # We get the result from the database
        result_row = database_cursor.fetchone()
        # We take the number of rows from the result
        row_count = result_row[0]
        # We check if the count is more than zero
        if row_count > 0:
            # We print that the table is not empty
            print("  OK   " + table_name + ": " + str(row_count) + " rows")
            return True
        else:
            # We print that the table is empty
            print("  EMPTY " + table_name)
            return False
    except Exception as error_message:
        # If the table does not exist, we catch the error here
        print("  ERROR " + table_name + " (" + str(error_message) + ")")
        return False

# This function runs all our tests in order
def run_all_the_tests():
    # We start Step 1: Sending data
    print("Step 1: Sending all test events to the server...")
    # We keep track of how many worked
    success_count = 0
    # We loop through every event in our test list
    for single_event in TEST_EVENTS:
        # We call our sender function
        if send_one_test_event(single_event):
            # If it worked, we add one to our counter
            success_count = success_count + 1
    
    # We print a summary of how many messages were sent
    print("\nSummary: " + str(success_count) + " out of " + str(len(TEST_EVENTS)) + " events sent.")

    # We start Step 2: Checking the database
    print("\nStep 2: Checking if database tables are populated...")
    # We make a list of the tables we expect to see data in
    tables_to_verify = [
        "silver.users", "silver.transactions", "silver.sessions",
        "silver.assessments", "silver.course_completion", "silver.announcements",
        "silver.certificates"
    ]
    
    # We try to connect to the database
    database_connection = get_database_connection()
    # If it failed, we stop here
    if database_connection is None:
        return

    # We create a cursor for running our check commands
    database_cursor = database_connection.cursor()
    # We loop through each table name in our verification list
    for table_name in tables_to_verify:
        # We call our check function for each table
        check_if_table_has_data(database_cursor, table_name)
    
    # We close the database tools
    database_cursor.close()
    database_connection.close()

# We start the script here
if __name__ == "__main__":
    # We call the main test function
    run_all_the_tests()
