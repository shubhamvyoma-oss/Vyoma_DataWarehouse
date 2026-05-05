import psycopg2

# This is the address of the computer where the database is running
# We need this to know where to send our data requests
DATABASE_HOST = "localhost"

# This is the name of the database that we want to look at
# We need this because one database server can have many different databases
DATABASE_NAME = "edmingle_analytics"

# This is the username we use to log in to the database
# We need this to prove to the database that we have permission to see it
DATABASE_USER = "postgres"

# This is the password we use to log in to the database
# We need this to keep our data safe so only we can see it
DATABASE_PASSWORD = "Svyoma"

# This is the port number that the database is listening on
# We need this because computers use ports to talk to different programs
DATABASE_PORT = 5432

# This is a list of all the tables we want to count the rows for
# We put them in a list so we can use a loop to check them all easily
TABLES_TO_CHECK = [
    'bronze.webhook_events',
    'bronze.failed_events',
    'bronze.studentexport_raw',
    'bronze.student_courses_enrolled_raw',
    'bronze.unresolved_students_raw',
    'silver.users',
    'silver.transactions',
    'silver.sessions',
    'silver.assessments',
    'silver.course_completion',
    'silver.announcements',
    'silver.certificates',
]

# This function tries to start a connection with our database
# We use a function because we might need to connect many times
def connect_to_database():
    try:
        # We call the connect function from the psycopg2 library
        # This opens a communication line between Python and the database
        connection = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # We give back the connection object so we can use it to run commands
        return connection
    except Exception as error_message:
        # If the connection fails, we print a simple message
        # This helps us know that the database might be turned off
        print("I could not connect to the database.")
        # We also print the specific error message to help us fix the problem
        print(error_message)
        # We give back nothing to show that the connection failed
        return None

# This function asks the database for the number of rows in one table
# We pass it a cursor and the name of the table we are interested in
def get_row_count(database_cursor, table_name):
    try:
        # We create a string that contains a SQL command
        # SELECT COUNT(*) is the standard way to ask a database for row totals
        sql_command = "SELECT COUNT(*) FROM " + table_name
        # We tell the cursor to send this command to the database
        database_cursor.execute(sql_command)
        # We fetch the result that the database sent back to us
        result_row = database_cursor.fetchone()
        # The result comes as a list, so we take the first number in the list
        number_of_rows = result_row[0]
        # We give back the number we found
        return number_of_rows
    except Exception as error_message:
        # If something goes wrong, like a missing table, we print a warning
        print("Could not get count for table: " + table_name)
        # We print the error so we can see why it failed
        print(error_message)
        # We give back zero as a safe default value
        return 0

# This function prints the titles at the top of our report
# This makes it easier for people to understand what the numbers mean
def print_report_titles():
    # We print the names of the columns we are showing
    print("\nTable Name                                     Row Count")
    # We print a line of dashes to separate the titles from the data
    print("--------------------------------------------- ----------")

# This function goes through all our tables and prints their row counts
# It is the main organizer for the work this script does
def run_the_counting_report():
    # We try to get a connection to the database first
    database_connection = connect_to_database()
    
    # We check if the connection actually worked before we try to use it
    if database_connection is None:
        # If it failed, we stop the function early
        return

    # We create a cursor which is like a pointer for sending commands
    database_cursor = database_connection.cursor()
    
    # We print the titles at the top of our report
    print_report_titles()

    # We use a loop to look at every table name in our list
    for table_name in TABLES_TO_CHECK:
        # We call our other function to find out how many rows are in the table
        count_result = get_row_count(database_cursor, table_name)
        # We figure out how many spaces we need for nice alignment
        number_of_spaces = 45 - len(table_name)
        # We print the table name, then spaces, then the number
        print(table_name + " " * number_of_spaces + str(count_result))

    # We print an empty line to make the end of the report look tidy
    print("")

    # We close the cursor to tell the database we are done sending commands
    database_cursor.close()
    # We close the connection to let the database free up its memory
    database_connection.close()

# This part makes sure our script only runs when we click 'play'
# It prevents the script from running if another script just wants to borrow its functions
if __name__ == '__main__':
    # We start the report process
    run_the_counting_report()
