# We import the system module to handle system-level tasks
import sys
# We import the datetime module to work with dates and times
import datetime
# We import the json module to read and write JSON data
import json
# We import the psycopg2 module to connect to the PostgreSQL database
import psycopg2
# We import extras from psycopg2 to handle special data types like JSON
import psycopg2.extras

# We set the database host name
DB_HOST = "localhost"
# We set the name of the database we want to connect to
DB_NAME = "edmingle_analytics"
# We set the username for the database
DB_USER = "postgres"
# We set the password for the database
DB_PASSWORD = "Svyoma"
# We set the port number for the database
DB_PORT = 5432

# This function creates a new connection to the database
# We need this to talk to the database and run our commands
def get_new_db_connection():
    # We try to connect using the details we defined above
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # We return the successful connection
        return connection
    # If the connection fails, we catch the error
    except Exception as error:
        # We print the error so the user knows why it failed
        print("Could not connect to the database: " + str(error))
        # We return None to show there is no connection
        return None

# This function marks an event as processed in the bronze table
# This helps us avoid processing the same event many times
def mark_event_as_done(connection, event_id):
    # We create a cursor object to run SQL commands
    database_cursor = connection.cursor()
    # We write the SQL command to update the event status
    sql_query = "UPDATE bronze.webhook_events SET routed_to_silver = true WHERE event_id = %s"
    # We run the command with the event ID
    database_cursor.execute(sql_query, (event_id,))
    # We close the cursor to save computer memory
    database_cursor.close()

# This function looks for a field in a list of items
# Edmingle sends some data in a list format that we need to search
def find_field_in_list(field_list, names_to_find):
    # i is the item we are currently looking at in the list
    for i in field_list:
        # We check if the item is a dictionary (a set of key-value pairs)
        if isinstance(i, dict):
            # We get the display name of the field
            display_name = i.get('field_display_name')
            # We get the internal name of the field
            internal_name = i.get('field_name')
            # We check if the display name matches what we want
            if display_name is not None:
                if display_name.lower().strip() in names_to_find:
                    return i.get('field_value')
            # We check if the internal name matches what we want
            if internal_name is not None:
                if internal_name.lower().strip() in names_to_find:
                    return i.get('field_value')
    # If we find nothing, we return None
    return None

# This function gets a value from system fields data
# It handles cases where fields are either in a dictionary or a list
def get_field_value(fields_data, names_to_search):
    # We create an empty list to store lower-case names
    clean_names = []
    # We loop through the names we are searching for
    for name in names_to_search:
        # We make the name lower-case and remove extra spaces
        clean_names.append(name.lower().strip())
    # If the data is a dictionary, we look up values directly
    if isinstance(fields_data, dict):
        for name in names_to_search:
            val = fields_data.get(name)
            if val is not None:
                return val
    # If the data is a list, we use our search function
    if isinstance(fields_data, list):
        return find_field_in_list(fields_data, clean_names)
    # If we don't know the format, we return None
    return None

# This function pulls out the basic info for a user
# It gets the ID, email, and name from the data object
def get_user_basics(data_object):
    # We get the user's ID
    user_id = data_object.get('user_id')
    # We get the user's email
    email = data_object.get('email')
    # We try to get the name from 'name' field
    full_name = data_object.get('name')
    # If name is missing, we try 'full_name' field
    if full_name is None:
        full_name = data_object.get('full_name')
    # We return the values we found
    return user_id, email, full_name

# This function runs the SQL to save a new user
# It uses the data we extracted to fill in the database table
def save_user_to_db(cursor, user_data_tuple):
    # We write the SQL query with placeholders for the data
    sql = """
        INSERT INTO silver.users (
            event_id, event_type, user_id, email, full_name,
            created_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, %s, unix_to_ist(%s), NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (user_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            event_type = EXCLUDED.event_type,
            email = COALESCE(EXCLUDED.email, silver.users.email),
            full_name = COALESCE(EXCLUDED.full_name, silver.users.full_name),
            received_at = EXCLUDED.received_at
    """
    # We run the query using the cursor and the data tuple
    cursor.execute(sql, user_data_tuple)

# This function routes "user created" events
# It pulls user info and saves it to the silver users table
def route_user_created(conn, event_id, event_type, data, event_timestamp):
    # We extract the basic user details
    user_id, email, name = get_user_basics(data)
    # We get the creation time, or use the event time if missing
    created_at = data.get('created_at')
    if created_at is None:
        created_at = event_timestamp
    # We create a cursor to run the database command
    cursor = conn.cursor()
    # We prepare the data to be inserted
    data_to_save = (event_id, event_type, user_id, email, name, created_at)
    # We try to save the user data
    try:
        save_user_to_db(cursor, data_to_save)
    except Exception as error:
        print("Error saving user: " + str(error))
    # We close the cursor
    cursor.close()

# This function gets location data for a user
# it looks in the user object and the system fields
def get_location_info(user_object, system_fields):
    # We try to get the city
    city = user_object.get('city')
    if city is None:
        city = get_field_value(system_fields, ['city', 'City'])
    # We try to get the state
    state = user_object.get('state')
    if state is None:
        state = get_field_value(system_fields, ['state', 'State'])
    # We try to get the address
    address = user_object.get('address')
    if address is None:
        address = get_field_value(system_fields, ['address', 'Address'])
    # We try to get the pincode
    pincode = user_object.get('pincode')
    if pincode is None:
        pincode = get_field_value(system_fields, ['pincode', 'Pincode', 'pin code'])
    # We return all four values
    return city, state, address, pincode

# This function gets parent info for a user
# These details are usually in the system fields
def get_parent_info(system_fields):
    # We search for the parent's name
    parent_name = get_field_value(system_fields, ['parent_name', 'Parent Name'])
    # We search for the parent's email
    parent_email = get_field_value(system_fields, ['parent_email', 'Parent Email'])
    # We search for the parent's phone number
    parent_phone = get_field_value(system_fields, ['parent_contact', 'Parent Phone'])
    # We return the three values
    return parent_name, parent_email, parent_phone

# This function runs the SQL to update a user's profile
# It handles many fields like address and parent information
def update_user_in_db(cursor, update_tuple):
    # We write the complex SQL query for updating a user
    sql = """
        INSERT INTO silver.users (
            event_id, event_type, user_id, email, full_name, contact_number,
            city, state, address, pincode, parent_name, parent_email, parent_contact,
            updated_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            unix_to_ist(%s), NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (user_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            city = COALESCE(EXCLUDED.city, silver.users.city),
            state = COALESCE(EXCLUDED.state, silver.users.state),
            address = COALESCE(EXCLUDED.address, silver.users.address),
            pincode = COALESCE(EXCLUDED.pincode, silver.users.pincode),
            parent_name = COALESCE(EXCLUDED.parent_name, silver.users.parent_name),
            parent_email = COALESCE(EXCLUDED.parent_email, silver.users.parent_email),
            parent_contact = COALESCE(EXCLUDED.parent_contact, silver.users.parent_contact),
            updated_at_ist = EXCLUDED.updated_at_ist,
            received_at = EXCLUDED.received_at
    """
    # We execute the query with the provided data
    cursor.execute(sql, update_tuple)

# This function routes "user updated" events
# It collects all profile changes and saves them
def route_user_updated(conn, event_id, event_type, data, event_timestamp):
    # We find the user object in the data
    user_object = data.get('user')
    if user_object is None:
        user_object = data
    # We extract IDs and names
    user_id = user_object.get('user_id')
    if user_id is None:
        user_id = user_object.get('id')
    email = user_object.get('email')
    full_name = user_object.get('name')
    if full_name is None:
        full_name = user_object.get('full_name')
    # We get contact and time info
    phone = user_object.get('phone')
    if phone is None:
        phone = user_object.get('contact_number')
    updated_at = user_object.get('updated_at')
    if updated_at is None:
        updated_at = event_timestamp
    # We get extra details from system fields
    system_fields = data.get('system_fields')
    city, state, address, pincode = get_location_info(user_object, system_fields)
    parent_name, parent_email, parent_phone = get_parent_info(system_fields)
    # We prepare the cursor and data for the database
    cursor = conn.cursor()
    update_data = (event_id, event_type, user_id, email, full_name, phone, city, state, address, pincode, parent_name, parent_email, parent_phone, updated_at)
    # We try to perform the update
    try:
        update_user_in_db(cursor, update_data)
    except Exception as error:
        print("Error updating user: " + str(error))
    # We close the cursor
    cursor.close()

# This function extracts pricing details for a purchase
# It gets prices, discounts, and currency
def get_price_info(data):
    original = data.get('original_price')
    discount = data.get('discount')
    final = data.get('final_price')
    currency = data.get('currency')
    return original, discount, final, currency

# This function extracts batch and bundle IDs for a transaction
# It helps us know what was bought and which batch it belongs to
def get_id_info(data):
    bundle_id = data.get('bundle_id')
    course_name = data.get('course_name')
    batch_id = data.get('master_batch_id')
    batch_name = data.get('master_batch_name')
    return bundle_id, course_name, batch_id, batch_name

# This function runs the SQL to save a transaction
# It records the sale and who bought what
def save_trans_to_db(cursor, trans_tuple):
    # We write the SQL for inserting a transaction
    sql = """
        INSERT INTO silver.transactions (
            event_id, event_type, event_timestamp_ist, user_id, email, full_name,
            bundle_id, course_name, master_batch_id, master_batch_name,
            original_price, discount, final_price, currency, source
        ) VALUES (
            %s, %s, unix_to_ist(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'webhook'
        )
        ON CONFLICT (user_id, bundle_id, master_batch_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            final_price = EXCLUDED.final_price,
            source = 'webhook'
    """
    # We execute the query
    cursor.execute(sql, trans_tuple)

# This function routes transaction events
# It collects purchase details and saves them to silver
def route_transaction(conn, event_id, event_type, data, event_timestamp):
    # We extract user identifiers
    user_id = data.get('user_id')
    email = data.get('email')
    full_name = data.get('name')
    if full_name is None:
        full_name = data.get('full_name')
    # We extract product and price details
    bid, cn, mid, mn = get_id_info(data)
    op, ds, fp, cu = get_price_info(data)
    # We prepare the cursor and the data for insertion
    cursor = conn.cursor()
    transaction_data = (event_id, event_type, event_timestamp, user_id, email, full_name, bid, cn, mid, mn, op, ds, fp, cu)
    # We try to save the transaction
    try:
        save_trans_to_db(cursor, transaction_data)
    except Exception as error:
        print("Error saving transaction: " + str(error))
    # We close the cursor
    cursor.close()

# This function extracts class info for a session
# It gets the class ID, name and type
def get_class_info(data):
    class_id = data.get('class_id')
    class_name = data.get('class_name')
    class_type = data.get('class_type_formatted')
    return class_id, class_name, class_type

# This function runs the SQL to save a session event
# It records things like when a class started or ended
def save_session_to_db(cursor, session_tuple):
    # We write the SQL for inserting a session
    sql = """
        INSERT INTO silver.sessions (
            event_id, event_type, attendance_id, class_id, class_name,
            class_type_formatted, scheduled_start_ist, actual_start_ist, status, received_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, unix_to_ist(%s), unix_to_ist(%s), %s, NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (attendance_id) DO UPDATE SET
            event_id = EXCLUDED.event_id,
            status = EXCLUDED.status,
            received_at = EXCLUDED.received_at
    """
    # We execute the query
    cursor.execute(sql, session_tuple)

# This function routes session events
# It gathers class details and timing info for the database
def route_session(conn, event_id, event_type, data, event_timestamp):
    # We get the unique attendance ID
    attendance_id = data.get('attendance_id')
    # We get class information
    class_id, class_name, class_type = get_class_info(data)
    # We get the start times
    scheduled_start = data.get('gmt_start_time')
    actual_start = data.get('actual_start_time')
    if actual_start is None:
        actual_start = data.get('taken_at')
    # We get the session status
    session_status = data.get('status')
    # We prepare the cursor and data
    cursor = conn.cursor()
    session_data = (event_id, event_type, attendance_id, class_id, class_name, class_type, scheduled_start, actual_start, session_status)
    # We try to save the session
    try:
        save_session_to_db(cursor, session_data)
    except Exception as error:
        print("Error saving session: " + str(error))
    # We close the cursor
    cursor.close()

# This function routes assessment events
# It records test marks and submission times
def route_assessment(conn, event_id, event_type, data, event_timestamp):
    # We get IDs for user and test attempt
    user_id = data.get('user_id')
    attempt_id = data.get('attempt_id')
    exercise_id = data.get('exercise_id')
    # We get the mark and status
    mark = data.get('mark')
    is_evaluated = data.get('is_evaluated')
    # We get the submission time
    submitted_at = data.get('submitted_at')
    if submitted_at is None:
        submitted_at = data.get('test_date')
    # We prepare and run the SQL insert
    cursor = conn.cursor()
    sql = """
        INSERT INTO silver.assessments (
            event_id, event_type, user_id, attempt_id, exercise_id,
            mark, is_evaluated, submitted_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, unix_to_ist(%s), NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (event_id) DO UPDATE SET
            mark = EXCLUDED.mark,
            received_at = EXCLUDED.received_at
    """
    try:
        cursor.execute(sql, (event_id, event_type, user_id, attempt_id, exercise_id, mark, is_evaluated, submitted_at))
    except Exception as error:
        print("Error saving assessment: " + str(error))
    cursor.close()

# This function routes course completion events
# It tracks when a user finishes a course bundle
def route_course(conn, event_id, event_type, data, event_timestamp):
    # We get user and bundle IDs
    user_id = data.get('user_id')
    bundle_id = data.get('bundle_id')
    # We get completion time
    completed_at = data.get('completed_at')
    if completed_at is None:
        completed_at = event_timestamp
    # We prepare and run the SQL insert
    cursor = conn.cursor()
    sql = """
        INSERT INTO silver.course_completion (
            event_id, event_type, user_id, bundle_id, completed_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, unix_to_ist(%s), NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (event_id) DO NOTHING
    """
    try:
        cursor.execute(sql, (event_id, event_type, user_id, bundle_id, completed_at))
    except Exception as error:
        print("Error saving course: " + str(error))
    cursor.close()

# This function routes announcement events
# It saves the whole announcement as JSON for later use
def route_announcement(conn, event_id, event_type, data, event_timestamp):
    # We convert the data into a JSON object for the database
    json_data = None
    if data is not None:
        json_data = psycopg2.extras.Json(data)
    # We prepare and run the SQL insert
    cursor = conn.cursor()
    sql = """
        INSERT INTO silver.announcements (event_id, event_type, raw_data, received_at)
        VALUES (%s, %s, %s, NOW() AT TIME ZONE 'Asia/Kolkata')
        ON CONFLICT (event_id) DO NOTHING
    """
    try:
        cursor.execute(sql, (event_id, event_type, json_data))
    except Exception as error:
        print("Error saving announcement: " + str(error))
    cursor.close()

# This function routes certificate events
# It records when a certificate is given to a user
def route_certificate(conn, event_id, event_type, data, event_timestamp):
    # We get IDs for certificate and user
    certificate_id = data.get('certificate_id')
    user_id = data.get('user_id')
    issued_at = data.get('issued_at')
    # We prepare and run the SQL insert
    cursor = conn.cursor()
    sql = """
        INSERT INTO silver.certificates (
            event_id, event_type, certificate_id, user_id, issued_at_ist, received_at
        ) VALUES (
            %s, %s, %s, %s, unix_to_ist(%s), NOW() AT TIME ZONE 'Asia/Kolkata'
        )
        ON CONFLICT (event_id) DO NOTHING
    """
    try:
        cursor.execute(sql, (event_id, event_type, certificate_id, user_id, issued_at))
    except Exception as error:
        print("Error saving certificate: " + str(error))
    cursor.close()

# This function chooses which routing function to use based on the event name
# It acts like a traffic controller for our data
def run_event_router(conn, event_id, event_type, data, event_timestamp):
    # If the event is about a new user
    if event_type == 'user.user_created':
        route_user_created(conn, event_id, event_type, data, event_timestamp)
    # If the event is about updating a user
    elif event_type == 'user.user_updated':
        route_user_updated(conn, event_id, event_type, data, event_timestamp)
    # If the event is about a purchase
    elif event_type.startswith('transaction.user_purchase_'):
        route_transaction(conn, event_id, event_type, data, event_timestamp)
    # If the event is about a class session
    elif event_type.startswith('session.session_'):
        route_session(conn, event_id, event_type, data, event_timestamp)
    # If the event is about an assessment or test
    elif event_type.startswith('assessments.'):
        route_assessment(conn, event_id, event_type, data, event_timestamp)
    # If the event is about finishing a course
    elif event_type == 'course.user_course_completed':
        route_course(conn, event_id, event_type, data, event_timestamp)
    # If the event is about an announcement
    elif event_type == 'announcement.announcement_created':
        route_announcement(conn, event_id, event_type, data, event_timestamp)
    # If the event is about a certificate
    elif event_type == 'certificate.certificate_issued':
        route_certificate(conn, event_id, event_type, data, event_timestamp)

# This function tries to find the time when the event happened
# It looks in different places in the JSON data
def parse_time(payload):
    # We look for the 'event' part of the data
    event_part = payload.get('event')
    # If it is a dictionary, we look inside it
    if isinstance(event_part, dict):
        # We try to get the time string
        timestamp_string = event_part.get('event_ts', '')
        # We try to convert the string to a real timestamp
        try:
            date_object = datetime.datetime.fromisoformat(timestamp_string)
            return int(date_object.timestamp())
        except Exception:
            return None
    # If not found, we look at the top level
    return payload.get('event_timestamp')

# This function gets the main data out of the JSON message
# Different messages put the data in different places
def extract_data(payload):
    # If there is an 'event' key, it is the nested format
    if 'event' in payload:
        # If 'id' is NOT at the top, it is definitely the nested format
        if 'id' not in payload:
            return payload.get('payload', {})
    # Otherwise, we use the 'data' key
    return payload.get('data', {})

# This function handles the processing of one single event
# It connects, routes, marks as done, and commits the change
def process_one_event(row):
    # We pull details from the database row
    event_id = row.get('event_id')
    event_type = row.get('event_type')
    raw_payload = row.get('raw_payload')
    # We extract the inner data and the timestamp
    inner_data = extract_data(raw_payload)
    event_timestamp = parse_time(raw_payload)
    # We get a new connection to the database
    connection = get_new_db_connection()
    # If we could not connect, we stop here
    if connection is None:
        return False
    # We try to process the event
    try:
        # We run the router to save data to silver tables
        run_event_router(connection, event_id, event_type, inner_data, event_timestamp)
        # We mark the event as finished in the bronze table
        mark_event_as_done(connection, event_id)
        # We save the changes permanently
        connection.commit()
        # We close the connection
        connection.close()
        return True
    # If anything fails, we undo our changes
    except Exception as error:
        connection.rollback()
        connection.close()
        print("Failed to process " + str(event_id) + ": " + str(error))
        return False

# This is the main function that starts the whole script
# It reads all unfinished events and processes them one by one
def main():
    # We get a connection to read the events
    connection = get_new_db_connection()
    # If connection fails, we exit
    if connection is None:
        return
    # We create a special cursor that returns results as dictionaries
    cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # We write the SQL to find events that need processing
    sql_query = """
        SELECT event_id, event_type, raw_payload
        FROM bronze.webhook_events
        WHERE routed_to_silver = false
        AND event_type != 'url.validate'
        ORDER BY id ASC
    """
    # We run the query and get all the rows
    cursor.execute(sql_query)
    all_rows = cursor.fetchall()
    # We close the reader cursor and the connection
    cursor.close()
    connection.close()
    # We tell the user how many events we found
    print("Found " + str(len(all_rows)) + " events to process.")
    # We keep track of how many we succeed with
    success_count = 0
    # We loop through every row we found
    for row in all_rows:
        # We process the row and check if it worked
        if process_one_event(row) == True:
            success_count = success_count + 1
    # We print the final summary
    print("Finished! We processed " + str(success_count) + " out of " + str(len(all_rows)) + ".")

# This line tells Python to run the main function when the script starts
if __name__ == '__main__':
    main()
