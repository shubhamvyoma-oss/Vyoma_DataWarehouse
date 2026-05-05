# This script looks at the data in our database and creates a report.
# It tells us how many courses, students, and enrollments we have.
# This helps the team understand how the e-learning platform is performing.

import psycopg2
from datetime import datetime

# These are the settings to connect to our database.
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "edmingle_analytics"
DB_USER = "postgres"
DB_PASSWORD = "Svyoma"

# This filter removes staff members from our student counts.
# We do this so our numbers only show real students.
STAFF_FILTER = "email NOT LIKE '%%@vyoma%%'"

# This function connects to the database.
def get_database_connection():
    # Connect using the details from above.
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    # Return the connection to be used by other parts of the script.
    return connection

# This function prints a big title for a new section of the report.
def print_section_header(section_number, section_title):
    # Print a long line of equal signs for visual separation.
    print("\n========================================================================")
    # Print the number and title.
    print("  " + str(section_number) + ". " + section_title)
    # Print another long line of equal signs.
    print("========================================================================")

# This function prints a smaller title for a sub-section.
def print_sub_header(sub_title):
    # Print a dashed line and the title.
    print("\n  -- " + sub_title)

# This function prints a label and a value in a neat way.
def print_metric_line(label_text, value_to_show):
    # Create a long string of spaces for padding.
    padding_spaces = "                                                    "
    # Calculate how many spaces we need to align the value.
    label_length = len(label_text)
    extra_spaces_needed = 50 - label_length
    # Make sure there is at least one space if the label is too long.
    if extra_spaces_needed < 1:
        extra_spaces_needed = 1
    # Take only the number of spaces we need.
    padding = padding_spaces[0:extra_spaces_needed]
    # Print the label, the padding, and then the value.
    print("    " + label_text + padding + str(value_to_show))

# This function gets a single piece of information from a database query.
def get_one_value_from_db(database_cursor, sql_query, query_params=None):
    # Execute the SQL command.
    database_cursor.execute(sql_query, query_params)
    # Fetch only the first row of the result.
    single_row = database_cursor.fetchone()
    # Check if we actually got a row.
    if single_row is not None:
        # Return the first column of that row.
        return single_row[0]
    # If no row was found, return nothing.
    return None

# This function adds commas to large numbers to make them easier to read.
def format_with_commas(number_value):
    # If there is no number, return a dash.
    if number_value is None:
        return "-"
    # Try to add commas using Python's formatting.
    try:
        # The colon and comma mean "add commas as thousands separators".
        return "{:,}".format(int(number_value))
    except:
        # If it's not a number, just return it as a string.
        return str(number_value)

# This function formats decimal numbers with 2 digits after the dot.
def format_as_decimal(decimal_value):
    # If there is no value, return a dash.
    if decimal_value is None:
        return "-"
    # Try to format it with two decimal places.
    try:
        # The .2f means "two numbers after the decimal point".
        return "{:,.2f}".format(float(decimal_value))
    except:
        # If it fails, just return it as a string.
        return str(decimal_value)

# This function tells us how many days ago something happened.
def calculate_days_ago(date_timestamp):
    # If there is no date, return a dash.
    if date_timestamp is None:
        return "-"
    # Get the current time and adjust for time zones.
    now_time = datetime.now().astimezone()
    # Subtract the date from the current time.
    time_difference = now_time - date_timestamp
    # Get the number of full days from the difference.
    days_passed = time_difference.days
    # If it was 0 days, it happened today.
    if days_passed == 0:
        return "today"
    # If it was 1 day, it happened yesterday.
    if days_passed == 1:
        return "yesterday"
    # Otherwise, return the number of days as a string.
    return str(days_passed) + " days ago"

# This function shows a list of items and their counts from the database.
def show_data_breakdown(database_cursor, sql_query, label_name, count_name):
    # Execute the query to get multiple rows.
    database_cursor.execute(sql_query)
    # Get all the rows from the query result.
    all_rows = database_cursor.fetchall()
    # Check if we got any data.
    if not all_rows:
        print("    (no data found)")
        return
    # Print the header for our list.
    print("    " + label_name + " | " + count_name)
    print("    -----------------------------")
    # Loop through each row and print the details.
    for current_row in all_rows:
        item_label = current_row[0]
        # If the label is empty, call it "not set".
        if item_label is None:
            item_label = "(not set)"
        item_count = current_row[1]
        # Format the count and print the line.
        formatted_count = format_with_commas(item_count)
        print("    " + str(item_label) + " | " + formatted_count)

# Section 1: Shows how much data we have in our tables.
def show_data_health(database_cursor):
    # Print the section title.
    print_section_header(1, "DATA HEALTH - ROW COUNTS")
    # Show counts for raw data tables.
    print_sub_header("Bronze Tables (Raw Data)")
    count1 = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM bronze.webhook_events")
    print_metric_line("webhook_events", format_with_commas(count1))
    count2 = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM bronze.course_catalogue_raw")
    print_metric_line("course_catalogue_raw", format_with_commas(count2))
    # Show counts for cleaned data tables.
    print_sub_header("Silver Tables (Cleaned Data)")
    count3 = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.users")
    print_metric_line("users", format_with_commas(count3))
    count4 = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.transactions")
    print_metric_line("transactions", format_with_commas(count4))
    # Show how fresh our data is.
    print_sub_header("Data Freshness")
    last_date = get_one_value_from_db(database_cursor, "SELECT MAX(received_at) FROM bronze.webhook_events")
    print_metric_line("Last webhook received", calculate_days_ago(last_date))

# Section 2: Shows information about our courses.
def show_course_info(database_cursor):
    # Print the section title.
    print_section_header(2, "COURSE CATALOGUE")
    # Count total unique course bundles.
    sql = "SELECT COUNT(*) FROM silver.course_metadata"
    total_courses = get_one_value_from_db(database_cursor, sql)
    print_metric_line("Total unique courses", format_with_commas(total_courses))
    # Show how many courses we have for each subject.
    print_sub_header("Courses by Subject")
    breakdown_sql = "SELECT subject, COUNT(*) FROM silver.course_metadata GROUP BY subject ORDER BY 2 DESC"
    show_data_breakdown(database_cursor, breakdown_sql, "Subject", "Count")

# Section 3: Shows information about course batches.
def show_batch_info(database_cursor):
    # Print the section title.
    print_section_header(3, "BATCHES")
    # Count total batches across all courses.
    total_batches = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.course_batches")
    print_metric_line("Total batches", format_with_commas(total_batches))
    # Show the status of our batches (Ongoing, Completed, etc).
    print_sub_header("Batch Status")
    sql = "SELECT batch_status, COUNT(*) FROM silver.course_batches GROUP BY batch_status ORDER BY 2 DESC"
    show_data_breakdown(database_cursor, sql, "Status", "Count")

# Section 4: Shows enrollment numbers.
def show_enrollment_info(database_cursor):
    # Print the section title.
    print_section_header(4, "ENROLLMENTS")
    # Count total student enrollments (not staff).
    sql = "SELECT COUNT(*) FROM silver.transactions WHERE " + STAFF_FILTER
    total_enrollments = get_one_value_from_db(database_cursor, sql)
    print_metric_line("Total student enrollments", format_with_commas(total_enrollments))
    # Show where our enrollments are coming from.
    print_sub_header("Enrollments by Source")
    source_sql = "SELECT source, COUNT(*) FROM silver.transactions WHERE " + STAFF_FILTER + " GROUP BY source ORDER BY 2 DESC"
    show_data_breakdown(database_cursor, source_sql, "Source", "Count")

# Section 5: Shows information about our students.
def show_student_info(database_cursor):
    # Print the section title.
    print_section_header(5, "STUDENTS")
    # Count total registered students (not staff).
    sql = "SELECT COUNT(*) FROM silver.users WHERE " + STAFF_FILTER
    total_students = get_one_value_from_db(database_cursor, sql)
    print_metric_line("Total students registered", format_with_commas(total_students))
    # Show which states have the most students.
    print_sub_header("Top 5 States")
    state_sql = "SELECT state, COUNT(*) FROM silver.users WHERE " + STAFF_FILTER + " GROUP BY state ORDER BY 2 DESC LIMIT 5"
    show_data_breakdown(database_cursor, state_sql, "State", "Students")

# Section 6: Shows attendance records.
def show_attendance_info(database_cursor):
    # Print the section title.
    print_section_header(6, "ATTENDANCE")
    # Count total attendance records in the system.
    total_records = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.class_attendance")
    print_metric_line("Total class records", format_with_commas(total_records))
    # Calculate the average attendance percentage.
    avg_sql = "SELECT AVG(attendance_pct) FROM silver.class_attendance"
    average_pct = get_one_value_from_db(database_cursor, avg_sql)
    print_metric_line("Average attendance %", format_as_decimal(average_pct) + "%")

# Section 7: Shows assessment and certificate data.
def show_assessment_info(database_cursor):
    # Print the section title.
    print_section_header(7, "ASSESSMENTS")
    # Count how many assessment events were recorded.
    total_events = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.assessments")
    print_metric_line("Total assessment events", format_with_commas(total_events))
    # Count how many certificates were issued to students.
    total_certs = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.certificates")
    print_metric_line("Total certificates issued", format_with_commas(total_certs))

# Section 8: Shows revenue information.
def show_revenue_info(database_cursor):
    # Print the section title.
    print_section_header(8, "REVENUE")
    # Calculate total revenue from all student transactions.
    rev_sql = "SELECT SUM(final_price) FROM silver.transactions WHERE " + STAFF_FILTER
    total_revenue = get_one_value_from_db(database_cursor, rev_sql)
    print_metric_line("Total revenue all time", format_as_decimal(total_revenue))
    # Show how much revenue we made each year.
    print_sub_header("Revenue by Year")
    year_sql = "SELECT EXTRACT(YEAR FROM created_at_ist), SUM(final_price) FROM silver.transactions WHERE " + STAFF_FILTER + " GROUP BY 1 ORDER BY 1"
    show_data_breakdown(database_cursor, year_sql, "Year", "Revenue")

# Section 9: Shows information about live sessions.
def show_session_info(database_cursor):
    # Print the section title.
    print_section_header(9, "LIVE SESSIONS")
    # Count total session events recorded in the system.
    total_sessions = get_one_value_from_db(database_cursor, "SELECT COUNT(*) FROM silver.sessions")
    print_metric_line("Total session events", format_with_commas(total_sessions))

# Section 10: Shows the most popular courses.
def show_top_courses(database_cursor):
    # Print the section title.
    print_section_header(10, "TOP COURSES")
    # Get the top 5 courses based on the number of students.
    top_sql = "SELECT cm.course_name, COUNT(*) FROM silver.transactions t JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id WHERE t." + STAFF_FILTER + " GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
    show_data_breakdown(database_cursor, top_sql, "Course Name", "Students")

# This function runs all 10 report sections.
def run_all_report_sections(database_cursor):
    # Run each section one by one.
    show_data_health(database_cursor)
    show_course_info(database_cursor)
    show_batch_info(database_cursor)
    show_enrollment_info(database_cursor)
    show_student_info(database_cursor)
    show_attendance_info(database_cursor)
    show_assessment_info(database_cursor)
    show_revenue_info(database_cursor)
    show_session_info(database_cursor)
    show_top_courses(database_cursor)

# This is the main function that runs the entire report.
def main():
    # Tell the user we are starting.
    print("VYOMA DATA ANALYSIS REPORT")
    # Try to connect to the database and run the report.
    try:
        # Step 1: Connect.
        db_connection = get_database_connection()
        # Step 2: Get cursor.
        db_cursor = db_connection.cursor()
        # Step 3: Run all sections.
        run_all_report_sections(db_cursor)
        # Step 4: Close.
        db_cursor.close()
        db_connection.close()
        # Success message.
        print("\nReport finished successfully.")
    except Exception as error_message:
        # Show error.
        print("ERROR: Report failed.")
        print(error_message)

# This line starts the script.
if __name__ == "__main__":
    main()
