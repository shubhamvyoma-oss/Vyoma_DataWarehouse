import psycopg2

# This is the computer address where the database lives
DATABASE_HOST = "localhost"

# This is the specific database name we want to clean up
DATABASE_NAME = "edmingle_analytics"

# This is the name we use to talk to the database
DATABASE_USER = "postgres"

# This is the secret password for the database
DATABASE_PASSWORD = "Svyoma"

# This is the port number for the database connection
DATABASE_PORT = 5432

# This is a list of tables and the rules to find test data in them
# We use these rules so we do not delete real student data by mistake
CLEAR_TARGETS = [
    ("silver.users",
     "email LIKE '%test%' OR email LIKE '%load%' OR user_id >= 99990000"),
    ("silver.transactions",
     "email LIKE '%test%' OR email LIKE '%load%' OR user_id >= 99990000"),
    ("silver.assessments",
     "user_id >= 99990000"),
    ("silver.courses",
     "user_id >= 99990000"),
    ("silver.certificates",
     "user_id >= 99990000"),
    ("bronze.webhook_events",
     "event_id ~ '^(e2e|dupe|resilience|conc|dbtest|constraint|manual|load)-'"),
]

# This function creates a connection to our database
def connect_to_database():
    try:
        # We start the connection using our settings
        connection = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            dbname=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # We tell the connection not to save changes automatically
        # This lets us decide when to permanently save our deletions
        connection.autocommit = False
        # We return the successful connection
        return connection
    except Exception as error_message:
        # If we cannot connect, we show a message
        print("I could not connect to the database.")
        # We show the error to help with troubleshooting
        print(error_message)
        # We return nothing to show the failure
        return None

# This function counts how many rows match our test data rule
def count_the_test_rows(database_cursor, table_name, condition):
    try:
        # We build a SQL command to count matching rows
        sql_command = "SELECT COUNT(*) FROM " + table_name + " WHERE " + condition
        # We tell the database to run the count command
        database_cursor.execute(sql_command)
        # We get the first row of results
        result_row = database_cursor.fetchone()
        # We take the count number from the result row
        number_of_rows = result_row[0]
        # We return the number of rows found
        return number_of_rows
    except Exception as error_message:
        # If counting fails, we show a warning
        print("Could not count rows in " + table_name)
        # We show the error details
        print(error_message)
        # We return zero if there was an error
        return 0

# This function removes the test rows from the database
def delete_the_test_rows(database_cursor, table_name, condition):
    try:
        # We build a SQL command to delete the matching rows
        sql_command = "DELETE FROM " + table_name + " WHERE " + condition
        # We tell the database to run the delete command
        database_cursor.execute(sql_command)
        # We get the number of rows that were actually removed
        number_deleted = database_cursor.rowcount
        # We return the count of deleted rows
        return number_deleted
    except Exception as error_message:
        # If deleting fails, we show a warning
        print("Could not delete from " + table_name)
        # We show the error details
        print(error_message)
        # We return zero if there was an error
        return 0

# This function shows a list of what will be deleted
def show_the_cleanup_summary(database_cursor):
    # We print a header for the summary
    print("\nTest data found that can be deleted:")
    # We start our total count at zero
    total_found_rows = 0
    # We loop through every table in our target list
    for target_pair in CLEAR_TARGETS:
        # We get the table name from the first part of the pair
        table_name = target_pair[0]
        # We get the rule from the second part of the pair
        rule_condition = target_pair[1]
        # We call our counting function
        found_count = count_the_test_rows(database_cursor, table_name, rule_condition)
        # We figure out how many spaces to use for alignment
        padding_spaces = " " * (45 - len(table_name))
        # We print the result for this specific table
        print("  " + table_name + padding_spaces + str(found_count))
        # We add this count to our overall total
        total_found_rows = total_found_rows + found_count
    # We print the final total count
    print("  TOTAL" + " " * 40 + str(total_found_rows))
    # We give back the total so the main part of the script can use it
    return total_found_rows

# This is the main function that runs the whole cleanup process
def run_the_data_cleanup():
    # We try to connect to the database
    database_connection = connect_to_database()
    # If the connection did not work, we stop
    if database_connection is None:
        return

    # We create a cursor to send commands to the database
    database_cursor = database_connection.cursor()
    
    # We find out how many rows are test data
    total_to_remove = show_the_cleanup_summary(database_cursor)

    # If there is nothing to remove, we finish early
    if total_to_remove == 0:
        # We tell the user there is no work to do
        print("\nThere is nothing to delete.")
        # We close our database tools
        database_cursor.close()
        database_connection.close()
        return

    # We ask the user if they are sure they want to delete data
    print("\nDo you want to delete these " + str(total_to_remove) + " rows?")
    # We wait for the user to type something
    user_decision = input("Type 'y' for yes or 'n' for no: ")
    
    # We check if the user said 'y'
    if user_decision != 'y':
        # If they did not say yes, we tell them we are stopping
        print("Deletion cancelled by user.")
        # We close our database tools
        database_cursor.close()
        database_connection.close()
        return

    # We loop through and delete from each table in our list
    for target_pair in CLEAR_TARGETS:
        # We get the table name
        table_name = target_pair[0]
        # We get the rule for finding test data
        rule_condition = target_pair[1]
        # We call our deletion function
        removed_count = delete_the_test_rows(database_cursor, table_name, rule_condition)
        # We print how many rows were removed from this table
        print("  Deleted " + str(removed_count) + " rows from " + table_name)

    # We permanently save all the deletions to the database
    database_connection.commit()
    # We tell the user we are finished
    print("\nCleanup is complete.")

    # We close the database tools to be tidy
    database_cursor.close()
    database_connection.close()

# We start the script here if it is the main program being run
if __name__ == '__main__':
    # We call the main function
    run_the_data_cleanup()
