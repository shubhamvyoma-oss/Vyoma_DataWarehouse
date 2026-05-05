# This script loads student and enrollment data from CSV files into our database.
# It brings data into the "Bronze" layer so we can clean it later.

import os
import sys
import csv
import psycopg2

# These variables tell us how to connect to our database
DATABASE_HOST = "localhost"
DATABASE_NAME = "edmingle_analytics"
DATABASE_USER = "postgres"
DATABASE_PASSWORD = "Svyoma"
DATABASE_PORT = 5432

# Path to the folder where our CSV files are kept
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))
CSV_SOURCE_DIR = os.path.join(PROJECT_ROOT_DIR, 'CSV files')

# This mapping helps us rename messy CSV headers to clean database column names
STUDENT_COLUMN_MAPPING = [
    ("#", "row_number"), ("Name", "name"), ("Email", "email"),
    ("Registration Number", "registration_number"),
    ("Contact Number Dial Code", "contact_number_dial_code"),
    ("Contact Number", "contact_number"),
    ("Alternate Contact Number Dial Code", "alternate_contact_number_dial_code"),
    ("Alternate Contact Number", "alternate_contact_number"),
    ("Date Of Birth", "date_of_birth"), ("Parent Name", "parent_name"),
    ("Parent Contact", "parent_contact"), ("Parent Email", "parent_email"),
    ("Address", "address"), ("city", "city"), ("State", "state"),
    ("Standard", "standard"), ("Date Created", "date_created"),
    ("Username", "username"), ("Gender", "gender"), ("Status", "status"),
    ("Username.1", "username_1"), ("Why do you want to study Sanskrit?", "why_study_sanskrit"),
    ("User Nice Name", "user_nice_name"), ("User Last Name", "user_last_name"),
    ("Would you like to teach Sanskrit through our portal?", "would_like_to_teach"),
    ("Teaching Experience, if any", "teaching_experience"),
    ("Is Sanskrit your mainstream education", "is_mainstream_education"),
    ("Objective", "objective"), ("User Age", "user_age"), ("Persona", "persona"),
    ("Objective Package", "objective_package"), ("Time per week (In Hours)", "time_per_week_hours"),
    ("Age_", "age_"), ("Facebook profile URL", "facebook_profile_url"),
    ("Instagram profile URL", "instagram_profile_url"),
    ("Pinterest profile URL", "pinterest_profile_url"),
    ("SoundCloud profile URL", "soundcloud_profile_url"),
    ("Tumblr profile URL", "tumblr_profile_url"), ("YouTube profile URL", "youtube_profile_url"),
    ("Wikipedia page about you (if one exists)", "wikipedia_url"),
    ("Twitter username (without @)", "twitter_username"), ("GST Number", "gst_number"),
    ("MySpace profile URL", "myspace_profile_url"),
    ("International Phone Number", "international_phone_number"), ("Website", "website"),
    ("Educational Qualification", "educational_qualification"),
    ("LinkedIn profile URL", "linkedin_profile_url"), ("Age ", "age_v2"),
    ("Gender_", "gender_"), ("Sanskrit Qualification", "sanskrit_qualification"),
    ("Areas of Interest in Sanskrit", "areas_of_interest"),
    ("Studying Sanskrit Currently? If yes, give details", "studying_sanskrit_currently"),
    ("What is your current education status?", "current_education_status"),
    ("Country Name", "country_name"),
]

# Database column names for enrollment data
ENROLL_DB_COLUMNS = [
    "user_id", "name", "email", "class_id", "class_name", "tutor_name",
    "total_classes", "present", "absent", "late", "excused",
    "start_date", "end_date", "master_batch_id", "master_batch_name",
    "classusers_start_date", "classusers_end_date", "batch_status",
    "cu_status", "cu_state", "institution_bundle_id", "archived_at", "bundle_id",
]

# This function builds the SQL for inserting student records
def build_sql_for_students():
    col_names = "source_row"
    placeholders = "%s"
    for i in range(len(STUDENT_COLUMN_MAPPING)):
        col_names = col_names + ", " + STUDENT_COLUMN_MAPPING[i][1]
        placeholders = placeholders + ", %s"
    # Create the final SQL command
    sql = "INSERT INTO bronze.studentexport_raw (" + col_names + ") "
    sql = sql + "VALUES (" + placeholders + ") ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function builds the SQL for inserting enrollment records
def build_sql_for_enrollments():
    col_names = "source_row"
    placeholders = "%s"
    for i in range(len(ENROLL_DB_COLUMNS)):
        col_names = col_names + ", " + ENROLL_DB_COLUMNS[i]
        placeholders = placeholders + ", %s"
    # Create the final SQL command
    sql = "INSERT INTO bronze.student_courses_enrolled_raw (" + col_names + ") "
    sql = sql + "VALUES (" + placeholders + ") ON CONFLICT (source_row) DO NOTHING"
    return sql

# This function handles loading students from the CSV file
def run_student_load(connection):
    file_path = os.path.join(CSV_SOURCE_DIR, "studentexport.csv")
    if not os.path.exists(file_path):
        return 0
    sql_command = build_sql_for_students()
    cursor = connection.cursor()
    with open(file_path, mode='r', encoding='utf-8', errors='replace') as f:
        # Skip the first row because it is just a title
        f.readline()
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            vals = [idx]
            for i in range(len(STUDENT_COLUMN_MAPPING)):
                val = row.get(STUDENT_COLUMN_MAPPING[i][0])
                if val == "":
                    val = None
                vals.append(val)
            cursor.execute(sql_command, vals)
            idx = idx + 1
    connection.commit()
    cursor.close()
    return idx

# This function handles loading enrollments from the CSV file
def run_enrollment_load(connection):
    file_path = os.path.join(CSV_SOURCE_DIR, "studentCoursesEnrolled.csv")
    if not os.path.exists(file_path):
        return 0
    sql_command = build_sql_for_enrollments()
    cursor = connection.cursor()
    with open(file_path, mode='r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        idx = 0
        for row in reader:
            vals = [idx]
            for i in range(len(ENROLL_DB_COLUMNS)):
                val = row.get(ENROLL_DB_COLUMNS[i])
                if val == "":
                    val = None
                vals.append(val)
            cursor.execute(sql_command, vals)
            idx = idx + 1
    connection.commit()
    cursor.close()
    return idx

# Main execution function
def main():
    print("Starting student and enrollment load...")
    try:
        # Connect to the database
        db_conn = psycopg2.connect(
            host=DATABASE_HOST, port=DATABASE_PORT,
            dbname=DATABASE_NAME, user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )
        # Process student file
        s_count = run_student_load(db_conn)
        print("Students processed: " + str(s_count))
        # Process enrollment file
        e_count = run_enrollment_load(db_conn)
        print("Enrollments processed: " + str(e_count))
        # Close connection
        db_conn.close()
    except Exception as error:
        print("Something went wrong:")
        print(error)

if __name__ == "__main__":
    main()
