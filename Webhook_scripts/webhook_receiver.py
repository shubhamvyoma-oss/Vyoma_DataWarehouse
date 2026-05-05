# We import the flask module to create our web server
from flask import Flask, request, jsonify
# We import the json module to work with JSON data
import json
# We import the time module to work with time
import time
# We import the psycopg2 module to talk to the database
import psycopg2
# We import extras to handle JSON data in database commands
import psycopg2.extras

# We set the database host address
DB_HOST = "localhost"
# We set the database name
DB_NAME = "edmingle_analytics"
# We set the database user name
DB_USER = "postgres"
# We set the database password
DB_PASSWORD = "Svyoma"
# We set the database port number
DB_PORT = 5432

# This function creates a new connection to the database
# We need this to send information to our tables
def get_database_connection():
    # We try to connect using our settings
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # We return the connection if it worked
        return connection
    # If it fails, we catch the error
    except Exception as error:
        # We print a message to show what went wrong
        print("Database connection failed: " + str(error))
        # We return None to show there is no connection
        return None

# This function saves a message to a file if the database is down
# This ensures we don't lose any data from Edmingle
def save_to_backup_file(reason, body, content_type):
    # We prepare the data we want to save
    backup_data = {
        'reason': reason,
        'body': body,
        'type': content_type
    }
    # We convert the data to a JSON string
    json_line = json.dumps(backup_data)
    # We try to open the file and add the new line
    try:
        # We use 'a' to add to the end of the file
        with open("webhook_backup.jsonl", "a") as my_file:
            # We write the line and a new line character
            my_file.write(json_line + "\n")
    # If saving to file also fails
    except Exception as error:
        # We print a message to the console
        print("Backup file save failed: " + str(error))

# This function saves information about a message that failed
# It puts the error details into the failed_events table
def save_failed_message(reason, body, content_type):
    # We get a connection to the database
    database_connection = get_database_connection()
    # If the database is not working
    if database_connection is None:
        # We save to our backup file instead
        save_to_backup_file(reason, body, content_type)
        # We stop here
        return
    # We try to save the failure details to the database
    try:
        # We create a cursor to run the command
        database_cursor = database_connection.cursor()
        # We write the SQL command
        sql_query = "INSERT INTO bronze.failed_events (failure_reason, raw_body, content_type) VALUES (%s, %s, %s)"
        # We run the command with the details
        database_cursor.execute(sql_query, (reason, body, content_type))
        # We save the changes
        database_connection.commit()
        # We close the cursor
        database_cursor.close()
    # If the database command fails
    except Exception:
        # We save to the backup file
        save_to_backup_file(reason, body, content_type)
    # We always close the connection
    database_connection.close()

# This function saves the raw message into the bronze table
# This is the first step in our data pipeline
def save_to_bronze(connection, event_id, event_type, payload, is_live_mode):
    # We create a cursor for the database
    database_cursor = connection.cursor()
    # We write the SQL for inserting the raw event
    sql_query = """
        INSERT INTO bronze.webhook_events (event_id, event_type, raw_payload, is_live_mode)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """
    # We convert the payload to a JSON-ready object
    json_payload = psycopg2.extras.Json(payload)
    # We run the command
    database_cursor.execute(sql_query, (event_id, event_type, json_payload, is_live_mode))
    # We close the cursor
    database_cursor.close()

# This function handles the health check request
# It simply says the server is running
def health_check():
    # We return a simple OK message
    return jsonify({"status": "ok"}), 200

# This function gets the event details from the JSON data
# It handles different formats of messages from Edmingle
def get_event_details(payload):
    # If 'event' is inside the message
    if 'event' in payload:
        # If 'id' is NOT at the top level
        if 'id' not in payload:
            # We get the event object
            event_object = payload.get('event', {})
            # We get the type and time
            event_type = event_object.get('event')
            event_timestamp = event_object.get('event_ts', '')
            # We make a unique ID
            event_id = str(event_type) + "-" + str(event_timestamp)
            # We get the live mode status
            is_live_mode = event_object.get('livemode', True)
            # We get the actual data
            inner_data = payload.get('payload', {})
            # We return all four values
            return event_id, event_type, is_live_mode, inner_data
    # For other message formats
    event_id = payload.get('id')
    event_type = payload.get('event_name')
    is_live_mode = payload.get('is_live_mode', True)
    inner_data = payload.get('data', {})
    # We return the values
    return event_id, event_type, is_live_mode, inner_data

# This function handles incoming webhook messages
# It parses the data and saves it to the database
def receive_webhook():
    # We get the raw text of the message
    raw_body = request.get_data(as_text=True)
    # We try to parse the JSON data
    payload = request.get_json(silent=True)
    # If the message is not valid JSON
    if payload is None:
        # We save it as a failed message
        save_failed_message("Not JSON", raw_body, request.content_type)
        # We still tell Edmingle we received it
        return jsonify({"status": "received"}), 200
    # We extract the event details
    event_id, event_type, is_live_mode, inner_data = get_event_details(payload)
    # If we don't have an ID
    if event_id is None:
        # We save it as a failure
        save_failed_message("No ID", raw_body, request.content_type)
        return jsonify({"status": "received"}), 200
    # We get a database connection
    database_connection = get_database_connection()
    # If the database is down
    if database_connection is None:
        # We save to our backup file
        save_failed_message("DB down", raw_body, request.content_type)
        return jsonify({"status": "received"}), 200
    # We try to save to the bronze table
    try:
        # We run the save function
        save_to_bronze(database_connection, event_id, event_type, payload, is_live_mode)
        # We save the change
        database_connection.commit()
    # If saving to bronze fails
    except Exception as error:
        # We save as a failure
        save_failed_message("Bronze fail: " + str(error), raw_body, request.content_type)
    # We always close the connection
    database_connection.close()
    # We return a success message
    return jsonify({"status": "received"}), 200

# We create the Flask application
app = Flask(__name__)

# We add the rule for the health check route
app.add_url_rule('/health', view_func=health_check, methods=['GET'])

# We add the rule for the webhook receiver route
app.add_url_rule('/webhook', view_func=receive_webhook, methods=['POST'])

# If this file is run directly
if __name__ == '__main__':
    # We start the web server on port 5000
    # We listen on all addresses (0.0.0.0)
    app.run(host='0.0.0.0', port=5000)
