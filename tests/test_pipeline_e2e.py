import requests
import time
import psycopg2

# This is the address where the webhook receiver program is running
WEBHOOK_URL = "http://localhost:5000/webhook"

# These are the settings to connect to our database
DATABASE_HOST = "localhost"
DATABASE_PORT = 5432
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"

# This function sends a test transaction to the pipeline
def send_test_purchase(student_email, student_id):
    # We create a unique ID for this specific test event
    unique_event_id = "e2e-txn-" + str(int(time.time()))
    # We build the event data structure
    purchase_event = {
        "id": unique_event_id,
        "event_name": "transaction.user_purchase_initiated",
        "data": {
            "user_id": student_id,
            "email": student_email,
            "full_name": "E2E Test Student",
            "course_name": "End-to-End Test Course",
            "final_price": 100.00,
            "currency": "INR"
        }
    }
    # We send the data to the webhook
    requests.post(WEBHOOK_URL, json=purchase_event, timeout=5)
    # We give back the event ID so we can search for it later
    return unique_event_id

# This function checks if the transaction reached the silver table
def check_silver_table(student_email):
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
        # We write a SQL command to find the student's email
        sql_command = "SELECT COUNT(*) FROM silver.transactions WHERE email = '" + student_email + "'"
        # We tell the database to run the command
        cursor.execute(sql_command)
        # We get the count result
        result_row = cursor.fetchone()
        # We close the database tools
        cursor.close()
        connection.close()
        # We return True if the record was found
        return result_row[0] > 0
    except Exception as error_message:
        # If the check fails, we show the error
        print("Database check failed: " + str(error_message))
        return False

# This function runs the full end-to-end test
def run_the_pipeline_test():
    # We define a unique email for this test
    test_email = "e2e." + str(int(time.time())) + "@example.com"
    # We define a high user ID to avoid collisions
    test_user_id = 99991001
    
    # We start Step 1: Sending data
    print("Step 1: Sending test purchase for " + test_email)
    event_id = send_test_purchase(test_email, test_user_id)
    print("Event sent with ID: " + event_id)

    # We wait for the pipeline to process the data
    print("Step 2: Waiting 5 seconds for processing...")
    time.sleep(5)

    # We start Step 3: Checking the results
    print("Step 3: Checking silver.transactions table...")
    is_found = check_silver_table(test_email)
    
    # We show the final result of the test
    if is_found:
        print("SUCCESS: Data successfully reached the silver table!")
    else:
        print("FAILURE: Data did NOT reach the silver table.")

# We start the program here
if __name__ == "__main__":
    # We call the main test function
    run_the_pipeline_test()
