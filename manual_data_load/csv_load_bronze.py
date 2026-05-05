# This script loads data from CSV files into our "Bronze" database tables.
# We need this to get historical data into our system for analysis.

import os
import sys
import csv
import psycopg2

# These variables store our database connection info
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# Find the path to the CSV folder
CURRENT_FILE_PATH = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE_PATH)))
CSV_FOLDER = os.path.join(PROJECT_ROOT, 'CSV files')

# This list matches CSV headers to database column names
STUDENT_MAPPING = [
    ('#', 'row_number'), ('Name', 'name'), ('Email', 'email'),
    ('Registration Number', 'registration_number'),
    ('Contact Number Dial Code', 'contact_number_dial_code'),
    ('Contact Number', 'contact_number'),
    ('Alternate Contact Number Dial Code', 'alternate_contact_number_dial_code'),
    ('Alternate Contact Number', 'alternate_contact_number'),
    ('Date Of Birth', 'date_of_birth'), ('Parent Name', 'parent_name'),
    ('Parent Contact', 'parent_contact'), ('Parent Email', 'parent_email'),
    ('Address', 'address'), ('city', 'city'), ('State', 'state'),
    ('Standard', 'standard'), ('Date Created', 'date_created'),
    ('Username', 'username'), ('Gender', 'gender'), ('Status', 'status'),
    ('Username.1', 'username_1'), ('Why do you want to study Sanskrit?', 'why_study_sanskrit'),
    ('User Nice Name', 'user_nice_name'), ('User Last Name', 'user_last_name'),
    ('Would you like to teach Sanskrit through our portal?', 'would_like_to_teach'),
    ('Teaching Experience, if any', 'teaching_experience'),
    ('Is Sanskrit your mainstream education', 'is_mainstream_education'),
    ('Objective', 'objective'), ('User Age', 'user_age'), ('Persona', 'persona'),
    ('Objective Package', 'objective_package'),
    ('Time per week (In Hours)', 'time_per_week_hours'), ('Age_', 'age_'),
    ('Facebook profile URL', 'facebook_profile_url'),
    ('Instagram profile URL', 'instagram_profile_url'),
    ('Pinterest profile URL', 'pinterest_profile_url'),
    ('SoundCloud profile URL', 'soundcloud_profile_url'),
    ('Tumblr profile URL', 'tumblr_profile_url'),
    ('YouTube profile URL', 'youtube_profile_url'),
    ('Wikipedia page about you (if one exists)', 'wikipedia_url'),
    ('Twitter username (without @)', 'twitter_username'), ('GST Number', 'gst_number'),
    ('MySpace profile URL', 'myspace_profile_url'),
    ('International Phone Number', 'international_phone_number'),
    ('Website', 'website'), ('Educational Qualification', 'educational_qualification'),
    ('LinkedIn profile URL', 'linkedin_profile_url'), ('Age ', 'age_v2'),
    ('Gender_', 'gender_'), ('Sanskrit Qualification', 'sanskrit_qualification'),
    ('Areas of Interest in Sanskrit', 'areas_of_interest'),
    ('Studying Sanskrit Currently? If yes, give details', 'studying_sanskrit_currently'),
    ('What is your current education status?', 'current_education_status'),
    ('Country Name', 'country_name'),
]

# Enrollment column names in the database
ENROLLMENT_DB_COLS = [
    'user_id', 'name', 'email', 'class_id', 'class_name', 'tutor_name',
    'total_classes', 'present', 'absent', 'late', 'excused',
    'start_date', 'end_date', 'master_batch_id', 'master_batch_name',
    'classusers_start_date', 'classusers_end_date', 'batch_status',
    'cu_status', 'cu_state', 'institution_bundle_id', 'archived_at', 'bundle_id'
]

# This function creates the SQL command for students
def build_student_insert_sql():
    # Build the list of column names
    column_names = "source_row"
    # Build the list of placeholders (%s)
    placeholders = "%s"
    for i in range(len(STUDENT_MAPPING)):
        # Get the database column name from the mapping
        column_names = column_names + ", " + STUDENT_MAPPING[i][1]
        placeholders = placeholders + ", %s"
    # Create the final SQL string
    sql = "INSERT INTO bronze.studentexport_raw (" + column_names + ") "
    sql = sql + "VALUES (" + placeholders + ") "
    sql = sql + "ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function creates the SQL command for enrollments
def build_enrollment_insert_sql():
    # Build the list of column names
    column_names = "source_row"
    # Build the list of placeholders (%s)
    placeholders = "%s"
    for i in range(len(ENROLLMENT_DB_COLS)):
        column_names = column_names + ", " + ENROLLMENT_DB_COLS[i]
        placeholders = placeholders + ", %s"
    # Create the final SQL string
    sql = "INSERT INTO bronze.student_courses_enrolled_raw (" + column_names + ") "
    sql = sql + "VALUES (" + placeholders + ") "
    sql = sql + "ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function processes a single student row from the CSV
def get_student_values(row_data, row_index):
    # Start the list with the row number
    values = [row_index]
    # Look for each field in the row data
    for i in range(len(STUDENT_MAPPING)):
        csv_header = STUDENT_MAPPING[i][0]
        # Get the value from the dictionary that the CSV reader made
        field_value = row_data.get(csv_header)
        # If the value is empty string, make it None
        if field_value == "":
            field_value = None
        values.append(field_value)
    return values

# This function reads the student CSV and saves it to the database
def load_students_from_csv(connection):
    file_path = os.path.join(CSV_FOLDER, 'studentexport.csv')
    sql_command = build_student_insert_sql()
    cursor = connection.cursor()
    # Open the file and use DictReader to read headers
    with open(file_path, mode='r', encoding='utf-8') as csv_file:
        # Skip the first line because it is a title line
        csv_file.readline()
        reader = csv.DictReader(csv_file)
        row_count = 0
        for row in reader:
            # Build the list of values for the SQL command
            row_values = get_student_values(row, row_count)
            cursor.execute(sql_command, row_values)
            row_count = row_count + 1
    # Save the work
    connection.commit()
    cursor.close()
    print("Students loaded: " + str(row_count))

# This function reads the enrollment CSV and saves it to the database
def load_enrollments_from_csv(connection):
    file_path = os.path.join(CSV_FOLDER, 'studentCoursesEnrolled.csv')
    sql_command = build_enrollment_insert_sql()
    cursor = connection.cursor()
    # Open the file and use DictReader
    with open(file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        row_count = 0
        for row in reader:
            # Build the list of values
            row_values = [row_count]
            for i in range(len(ENROLLMENT_DB_COLS)):
                val = row.get(ENROLLMENT_DB_COLS[i])
                if val == "":
                    val = None
                row_values.append(val)
            cursor.execute(sql_command, row_values)
            row_count = row_count + 1
    # Save the work
    connection.commit()
    cursor.close()
    print("Enrollments loaded: " + str(row_count))

# Main function to run the script
def main():
    print("Starting CSV Bronze Load...")
    try:
        # Connect to the database
        db_conn = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Load the data
        load_students_from_csv(db_conn)
        load_enrollments_from_csv(db_conn)
        # Close connection
        db_conn.close()
        print("Done. Run csv_backfill_transactions.py next.")
    except Exception as error:
        print("Something went wrong:")
        print(error)

if __name__ == '__main__':
    main()
