import requests
import time
import psycopg2

# This is the address where the webhook receiver is running
WEBHOOK_URL = "http://localhost:5000/webhook"

# These are the settings to connect to the database
DATABASE_HOST = "localhost"
DATABASE_PORT = 5432
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"

# This function sends a single test event to the webhook
def send_the_test_event(unique_id):
    # We create a simple event with a unique ID
    test_event = {
        "id": unique_id,
        "event_name": "db_test.unavailability",
        "data": {"message": "testing database downtime"}
    }
    try:
        # We send the event using a POST request
        response = requests.post(WEBHOOK_URL, json=test_event, timeout=5)
        # We give back the response from the server
        return response
    except Exception as error_message:
        # If the server is down, we print the error
        print("Could not send event: " + str(error_message))
        # We give back nothing to show failure
        return None

# This function checks if an event exists in the failed_events table
def check_if_event_failed(event_id):
    try:
        # We connect to the database
        connection = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # We create a cursor to run our search
        cursor = connection.cursor()
        # We write a SQL command to find the specific event ID
        sql_command = "SELECT COUNT(*) FROM bronze.failed_events WHERE event_id = '" + event_id + "'"
        # We tell the database to run the command
        cursor.execute(sql_command)
        # We get the count from the results
        result_row = cursor.fetchone()
        # We close the database tools
        cursor.close()
        connection.close()
        # We return True if the event was found in the failed list
        return result_row[0] > 0
    except Exception:
        # If the database is still down, we cannot check
        return False

# This function guides the user through the test steps
def run_unavailability_test():
    # We explain what the user needs to do
    print("--- DATABASE UNAVAILABILITY TEST ---")
    print("1. Please STOP the PostgreSQL database service now.")
    # We wait for the user to press Enter
    input("Press Enter once the database is stopped...")

    # We send a test event while the database is down
    event_id = "dbtest-down-" + str(int(time.time()))
    print("Sending event: " + event_id)
    response = send_the_test_event(event_id)
    
    # We check if the server responded at all
    if response is not None:
        # We print the response code (it should be 200 even if DB is down)
        print("Server responded with: " + str(response.status_code))
    
    # We tell the user to turn the database back on
    print("\n2. Please START the PostgreSQL database service now.")
    # We wait for the user to press Enter
    input("Press Enter once the database is started...")

    # We wait a few seconds for the database to wake up
    print("Waiting 5 seconds for database to initialize...")
    time.sleep(5)

    # We check if the event was recorded in the failed_events table
    print("Checking bronze.failed_events for the record...")
    is_in_failed = check_if_event_failed(event_id)
    
    # We show the final result of the test
    if is_in_failed:
        print("SUCCESS: Event was correctly saved to failed_events table.")
    else:
        print("FAILURE: Event was NOT found in failed_events table.")

# We start the script here
if __name__ == "__main__":
    # We call the test function
    run_unavailability_test()
