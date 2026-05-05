import sys
import datetime
import psycopg2

# This is the computer address where the database lives
DATABASE_HOST = "localhost"
# This is the port number for the database connection
DATABASE_PORT = 5432
# This is the name of the database we want to analyze
DATABASE_NAME = "edmingle_analytics"
# This is the username for logging into the database
DATABASE_USER = "postgres"
# This is the secret password for the database
DATABASE_PASSWORD = "Svyoma"

# This is a rule to ignore emails that belong to the staff
# We use this so our report only shows real student data
STAFF_FILTER = "email NOT LIKE '%@vyoma%'"

# This function prints a big heading for each section of the report
def print_section_header(section_number, section_title):
    # We print an empty line to give some breathing room
    print("")
    # We print a long line of equals signs to make a border
    print("=" * 72)
    # We print the section number and the title
    print("  " + str(section_number) + ". " + section_title)
    # We print another border line at the bottom
    print("=" * 72)

# This function prints a smaller heading inside a section
def print_subsection_title(title_text):
    # We print an empty line for spacing
    print("")
    # We print the title with dashes to make it look like a list item
    print("  -- " + title_text)

# This function prints a single piece of data with a label
def print_metric_line(label_text, value_to_show):
    # We calculate how many spaces we need to make the labels line up
    padding_amount = 52 - len(label_text)
    # We create a string of spaces for padding
    padding_string = " " * padding_amount
    # We print the label, the padding, and the value
    print("    " + label_text + padding_string + " " + str(value_to_show))

# This function adds commas to big numbers to make them easier to read
def format_big_number(number_value):
    # If there is no number, we show dashes instead
    if number_value is None:
        return "--"
    try:
        # We turn the value into a whole number (integer)
        whole_number = int(number_value)
        # We use a special Python trick to add commas every three digits
        formatted_string = "{:,}".format(whole_number)
        # We give back the pretty number string
        return formatted_string
    except Exception:
        # If something goes wrong, we just show the original value
        return str(number_value)

# This function formats a number as a percentage
def format_as_percentage(decimal_value):
    # If there is no value, we show dashes
    if decimal_value is None:
        return "--"
    try:
        # We turn the value into a decimal number (float)
        number = float(decimal_value)
        # We format it to show one number after the decimal point
        formatted_string = "{:.1f}%".format(number)
        # We give back the percentage string
        return formatted_string
    except Exception:
        # If it fails, we just show the original value
        return str(decimal_value)

# This function runs a SQL command and gets back just one value
def get_single_value(database_cursor, sql_command):
    try:
        # We tell the database to run the command we wrote
        database_cursor.execute(sql_command)
        # We get the first row of the results
        result_row = database_cursor.fetchone()
        # If we got a row back, we return the first thing in it
        if result_row:
            return result_row[0]
        # If there were no results, we return nothing
        return None
    except Exception as error_message:
        # If the SQL command fails, we show the error
        print("SQL Error: " + str(error_message))
        # We return nothing to show the failure
        return None

# This function shows how many rows are in the bronze tables
def report_bronze_health(database_cursor):
    # We print the subsection title
    print_subsection_title("Bronze tables (Raw Data)")
    # We make a list of the tables we want to check
    bronze_tables = ["bronze.webhook_events", "bronze.failed_events", "bronze.attendance_raw"]
    # We loop through each table name in our list
    for table_name in bronze_tables:
        # We build a command to count the rows
        sql = "SELECT COUNT(*) FROM " + table_name
        # We get the count from the database
        count_result = get_single_value(database_cursor, sql)
        # We print the table name and the formatted count
        print_metric_line(table_name, format_big_number(count_result))

# This function shows how many rows are in the silver tables
def report_silver_health(database_cursor):
    # We print the subsection title
    print_subsection_title("Silver tables (Clean Data)")
    # We make a list of the silver tables
    silver_tables = ["silver.users", "silver.transactions", "silver.sessions", "silver.course_completion"]
    # We loop through each table
    for table_name in silver_tables:
        # We count the rows for this table
        sql = "SELECT COUNT(*) FROM " + table_name
        # We get the count result
        count_result = get_single_value(database_cursor, sql)
        # We print the metric
        print_metric_line(table_name, format_big_number(count_result))

# This function summarizes the course catalogue information
def report_course_catalogue(database_cursor):
    # We print the main section heading
    print_section_header(2, "COURSE CATALOGUE")
    # We count how many courses we have in total
    sql = "SELECT COUNT(*) FROM silver.course_metadata"
    total_courses = get_single_value(database_cursor, sql)
    # We show the total course count
    print_metric_line("Total courses in catalogue", format_big_number(total_courses))
    
    # We show a breakdown by subject
    print_subsection_title("By Subject")
    # We ask the database to count courses for each subject
    sql = "SELECT subject, COUNT(*) FROM silver.course_metadata GROUP BY 1 ORDER BY 2 DESC"
    database_cursor.execute(sql)
    # We loop through every row the database gives us
    for result_row in database_cursor.fetchall():
        # We print the subject name and its count
        print_metric_line(str(result_row[0]), format_big_number(result_row[1]))

# This function reports on active student batches
def report_batches(database_cursor):
    # We print the section heading
    print_section_header(3, "BATCHES")
    # We count every batch we have ever tracked
    sql = "SELECT COUNT(*) FROM silver.course_batches"
    total_batches = get_single_value(database_cursor, sql)
    # We show the total count
    print_metric_line("Total batches tracked", format_big_number(total_batches))
    
    # We count batches that have not ended yet
    sql = "SELECT COUNT(*) FROM silver.course_batches WHERE end_date_ist::DATE >= CURRENT_DATE"
    active_batches = get_single_value(database_cursor, sql)
    # We show the count of active batches
    print_metric_line("Ongoing or future batches", format_big_number(active_batches))

# This function reports on student enrollments
def report_enrollments(database_cursor):
    # We print the section heading
    print_section_header(4, "ENROLLMENTS")
    # We count enrollments while ignoring staff emails
    sql = "SELECT COUNT(*) FROM silver.transactions WHERE " + STAFF_FILTER
    total_enrollments = get_single_value(database_cursor, sql)
    # We show the total enrollment count
    print_metric_line("Total student enrollments", format_big_number(total_enrollments))
    
    # We count how many unique students have enrolled
    sql = "SELECT COUNT(DISTINCT user_id) FROM silver.transactions WHERE " + STAFF_FILTER
    unique_students = get_single_value(database_cursor, sql)
    # We show the unique student count
    print_metric_line("Unique students enrolled", format_big_number(unique_students))

# This function reports on registered students
def report_students(database_cursor):
    # We print the section heading
    print_section_header(5, "STUDENTS")
    # We count everyone in the users table
    sql = "SELECT COUNT(*) FROM silver.users"
    total_users = get_single_value(database_cursor, sql)
    # We show the total user count
    print_metric_line("Total registered users", format_big_number(total_users))
    
    # We count only those who are not staff
    sql = "SELECT COUNT(*) FROM silver.users WHERE " + STAFF_FILTER
    real_students = get_single_value(database_cursor, sql)
    # We show the real student count
    print_metric_line("Total students (non-staff)", format_big_number(real_students))

# This function reports on class attendance
def report_attendance(database_cursor):
    # We print the section heading
    print_section_header(6, "CLASS ATTENDANCE")
    # We count all attendance records
    sql = "SELECT COUNT(*) FROM silver.class_attendance"
    total_records = get_single_value(database_cursor, sql)
    # We show the total records count
    print_metric_line("Total attendance records", format_big_number(total_records))
    
    # We calculate the average attendance percentage
    sql = "SELECT AVG(attendance_pct) FROM silver.class_attendance"
    average_percent = get_single_value(database_cursor, sql)
    # We show the formatted average percentage
    print_metric_line("Average attendance percentage", format_as_percentage(average_percent))

# This function reports on assessments and certificates
def report_assessments(database_cursor):
    # We print the section heading
    print_section_header(7, "ASSESSMENTS & CERTIFICATIONS")
    # We count every submission made by students
    sql = "SELECT COUNT(*) FROM silver.assessments"
    total_submissions = get_single_value(database_cursor, sql)
    # We show the submission count
    print_metric_line("Total assessment submissions", format_big_number(total_submissions))
    
    # We count how many certificates have been issued
    sql = "SELECT COUNT(*) FROM silver.certificates"
    total_certificates = get_single_value(database_cursor, sql)
    # We show the certificate count
    print_metric_line("Total certificates issued", format_big_number(total_certificates))

# This function reports on money earned from courses
def report_revenue(database_cursor):
    # We print the section heading
    print_section_header(8, "REVENUE & PRICING")
    # We sum up the final price for all student transactions
    sql = "SELECT SUM(final_price) FROM silver.transactions WHERE " + STAFF_FILTER
    total_money = get_single_value(database_cursor, sql)
    # We show the total revenue
    print_metric_line("Total revenue (INR)", format_big_number(total_money))
    
    # We calculate the average price students paid
    sql = "SELECT AVG(final_price) FROM silver.transactions WHERE " + STAFF_FILTER + " AND final_price > 0"
    average_price = get_single_value(database_cursor, sql)
    # We show the average price
    print_metric_line("Average paid price", format_big_number(average_price))

# This function reports on live teaching sessions
def report_sessions(database_cursor):
    # We print the section heading
    print_section_header(9, "LIVE SESSIONS")
    # We count all session events
    sql = "SELECT COUNT(*) FROM silver.sessions"
    total_sessions = get_single_value(database_cursor, sql)
    # We show the total session count
    print_metric_line("Total session events", format_big_number(total_sessions))
    
    # We count how many sessions were cancelled
    sql = "SELECT COUNT(*) FROM silver.sessions WHERE cancellation_reason IS NOT NULL"
    cancelled_count = get_single_value(database_cursor, sql)
    # We show the cancelled session count
    print_metric_line("Cancelled sessions", format_big_number(cancelled_count))

# This function shows the most popular courses
def report_top_courses(database_cursor):
    # We print the section heading
    print_section_header(10, "TOP LISTS")
    # We print a subsection title
    print_subsection_title("Top 5 Courses by Enrollment")
    # We ask for the names and counts of the top 5 courses
    sql = "SELECT course_name, COUNT(*) FROM silver.transactions WHERE " + STAFF_FILTER + " GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
    database_cursor.execute(sql)
    # We loop through the top 5 results
    for result_row in database_cursor.fetchall():
        # We print each course name and how many students enrolled
        print_metric_line(str(result_row[0]), format_big_number(result_row[1]))

# This function runs all the individual report sections
def run_all_report_sections(cursor):
    # We run each part of the report one by one
    report_bronze_health(cursor)
    report_silver_health(cursor)
    report_course_catalogue(cursor)
    report_batches(cursor)
    report_enrollments(cursor)
    report_students(cursor)
    report_attendance(cursor)
    report_assessments(cursor)
    report_revenue(cursor)
    report_sessions(cursor)
    report_top_courses(cursor)

# This is the main function that runs everything in order
def run_the_full_report():
    # We print the title of the report and the current time
    print("\nVYOMA SAMSKRTA PATHASALA -- DATA PIPELINE ANALYSIS")
    print("Generated on: " + str(datetime.datetime.now()))
    try:
        # We try to connect to the database
        connection = psycopg2.connect(host=DATABASE_HOST, port=DATABASE_PORT, dbname=DATABASE_NAME, user=DATABASE_USER, password=DATABASE_PASSWORD)
        # We create a cursor for running our queries
        cursor = connection.cursor()
        # We print the first section header manually
        print_section_header(1, "DATA HEALTH -- ROW COUNTS & FRESHNESS")
        # We call the helper function to run all sections
        run_all_report_sections(cursor)
        # We close the cursor and the connection
        cursor.close()
        connection.close()
        print("\nAll sections complete.")
    except Exception as error_message:
        # If the whole report fails, we show why
        print("Report failed: " + str(error_message))

# We start the program here
if __name__ == "__main__":
    # We call the main function to start the work
    run_the_full_report()
