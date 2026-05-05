import os
import psycopg2
from datetime import datetime

# Database connection settings
# These variables store the information needed to connect to our database
# We use constants (ALL_CAPS) for settings that do not change
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "edmingle_analytics"
DB_USER = "postgres"
DB_PASSWORD = "Svyoma"

# The list of folders containing SQL files in the order they must run
# We must set up schemas before creating tables and views to avoid errors
FOLDERS_TO_PROCESS = ["setup", "bronze", "silver", "gold", "verification"]

# This function creates a connection to our PostgreSQL database
# It returns a connection object that we can use to talk to the database
def connect_to_database():
    # We use a try block to catch any connection errors
    # This prevents the script from crashing if the database is not running
    try:
        # We use the psycopg2 library to create a bridge to our database
        # This bridge allows us to send commands and receive data
        database_connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        # We return the successful connection to the main program
        # This allows other functions to use this connection later
        return database_connection
    except Exception as connection_error:
        # We print a message so the user knows the connection failed
        print("Could not connect to the database.")
        # We print the specific error message to help with fixing the problem
        print(connection_error)
        # We return None to signal that we could not connect
        return None

# This function reads the text inside a SQL file
# It takes the full path to a file and returns its content as a string
def read_sql_file(full_path_to_file):
    # We use a try block in case the file is missing or cannot be opened
    try:
        # We open the file in 'r' (read) mode with UTF-8 encoding
        # The 'with' keyword ensures the file is closed automatically
        with open(full_path_to_file, "r", encoding="utf-8") as file_handle:
            # We read everything inside the file into a variable
            sql_script_content = file_handle.read()
            # We return the text so we can run it on the database
            return sql_script_content
    except Exception as read_error:
        # We tell the user which file we could not read
        print("Could not read the file: " + full_path_to_file)
        # We return an empty string so the script doesn't try to run nothing
        return ""

# This function runs a SQL command on the database
# It takes the database connection, the SQL text, and the file name
def execute_sql_command(db_connection, sql_text, sql_file_name):
    # We check if there is any actual SQL code to run
    if sql_text.strip() == "":
        # We stop early if the file was empty
        return
    # We use a try block to catch any errors in the SQL code itself
    try:
        # We create a cursor which is like a pointer for the database
        db_cursor = db_connection.cursor()
        # We tell the cursor to run the SQL commands we read from the file
        db_cursor.execute(sql_text)
        # We close the cursor because we are finished with this file
        db_cursor.close()
        # We commit the changes to make them permanent in the database
        db_connection.commit()
        # We print a success message so the user knows it worked
        print("  SUCCESS: " + sql_file_name)
    except Exception as sql_error:
        # If there is a mistake in the SQL, we print an error message
        print("  ERROR in " + sql_file_name)
        # We show the exact error from the database to help fix it
        print(sql_error)
        # We roll back any changes to keep the database safe and clean
        db_connection.rollback()

# This function looks for all SQL files in a specific folder
# It returns a list of file names sorted alphabetically
def get_list_of_sql_files(directory_path):
    # We create an empty list to store the names of our SQL files
    list_of_sql_files = []
    # We get a list of all items (files and folders) in the directory
    all_items = os.listdir(directory_path)
    # We look at each item one by one
    for item_name in all_items:
        # We check if the item is a SQL file by looking at its end
        if item_name.endswith(".sql"):
            # We add the SQL file name to our list
            list_of_sql_files.append(item_name)
    # We sort the list so files run in the correct order (like 01, 02)
    list_of_sql_files.sort()
    # We return the final sorted list of files
    return list_of_sql_files

# This function processes all SQL files inside a single folder
# It organizes the reading and running of each script in that folder
def run_all_scripts_in_folder(db_connection, folder_path, folder_name):
    # We print a header for the current phase (like BRONZE or SILVER)
    print("\n Phase: " + folder_name.upper())
    # We print a decorative line to make the output easier to read
    print("-" * 30)
    # We get the list of all SQL files in this specific folder
    files_to_process = get_list_of_sql_files(folder_path)
    # We loop through each file in our list
    for current_file_name in files_to_process:
        # We build the full path so Python can find the file on disk
        full_file_path = os.path.join(folder_path, current_file_name)
        # We read the SQL code from the file
        sql_content = read_sql_file(full_file_path)
        # We send the SQL code to the database to be executed
        execute_sql_command(db_connection, sql_content, current_file_name)

# This function prints a nice welcome message when the script starts
def print_welcome_message():
    # We print a line of equal signs for decoration
    print("=" * 60)
    # We print the name of our system
    print("VYOMA DATA WAREHOUSE - MASTER INITIALIZATION")
    # We print another line for decoration
    print("=" * 60)

# This function prints a final message when everything is done
# It also shows how many seconds the script took to finish
def print_completion_message(start_time):
    # We record the time when the script finished
    end_time = datetime.now()
    # We calculate how much time passed since the start
    duration = end_time - start_time
    # We convert the duration into total seconds
    total_seconds = duration.total_seconds()
    # We print a decorative line
    print("\n" + "=" * 60)
    # We tell the user that the process is finished
    print("INITIALIZATION COMPLETE")
    # We show the time taken rounded to two decimal places
    print("Time taken: " + str(round(total_seconds, 2)) + " seconds")
    # We print a final decorative line
    print("=" * 60)

# This is the main part of our script where everything begins
def main():
    # We show the welcome message to the user
    print_welcome_message()
    # We record the exact time we started the script
    script_start_time = datetime.now()
    # We try to connect to our database
    connection = connect_to_database()
    # If we could not connect, we stop the script immediately
    if connection == None:
        return
    # We find the folder where this script is currently saved
    current_script_directory = os.path.dirname(os.path.abspath(__file__))
    # We loop through each folder name in our predefined list
    for folder_name in FOLDERS_TO_PROCESS:
        # We build the full path to the SQL folder
        full_folder_path = os.path.join(current_script_directory, folder_name)
        # We check if the folder actually exists on the computer
        if os.path.exists(full_folder_path):
            # We run all the SQL scripts found in that folder
            run_all_scripts_in_folder(connection, full_folder_path, folder_name)
    # We close the database connection to be tidy and safe
    connection.close()
    # We show the final message and the time it took to run
    print_completion_message(script_start_time)

# This line tells Python to run our 'main' function
# This only happens if we run this file directly
if __name__ == "__main__":
    # We call the main function to start the database setup
    main()
