# This script helps us build a master report for our courses.
# It combines data from course lists and batch lists into one big table.
# This table is then used by tools like Power BI to show charts.

import psycopg2

# These are the settings to connect to our database.
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "edmingle_analytics"
DB_USER = "postgres"
DB_PASSWORD = "Svyoma"

# This is the SQL command to create our new table if it does not exist.
SQL_CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS silver.course_batch_merge (
        id                          SERIAL PRIMARY KEY,
        bundle_id                   BIGINT,
        bundle_name                 TEXT,
        batch_id                    BIGINT,
        batch_name                  TEXT,
        final_status                TEXT,
        catalogue_status            TEXT,
        is_latest_batch             INTEGER,
        include_in_course_count     INTEGER,
        status_adjustment_reason    TEXT,
        status                      TEXT,
        batch_status                TEXT,
        has_batch                   INTEGER,
        start_date                  DATE,
        end_date                    DATE,
        batch_enrollment_count      INTEGER,
        bundle_enrollment_count     INTEGER,
        tutor_name                  TEXT,
        tutor_id                    BIGINT,
        course_division             TEXT,
        type                        TEXT,
        subject                     TEXT,
        level                       TEXT,
        language                    TEXT,
        sss_category                TEXT,
        adhyayanam_category         TEXT,
        personas                    TEXT,
        position_in_funnel          TEXT,
        term_of_course              TEXT,
        texts                       TEXT,
        certificate                 TEXT,
        course_sponsor              TEXT,
        number_of_lectures          TEXT,
        duration                    TEXT,
        computer_based_assessment   TEXT,
        course_ids                  TEXT,
        built_at                    TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (bundle_id, batch_id)
    )
"""

# This is a very long SQL command that calculates the report data.
SQL_FILL_TABLE = """
    TRUNCATE TABLE silver.course_batch_merge;

    INSERT INTO silver.course_batch_merge (
        bundle_id, bundle_name, batch_id, batch_name,
        final_status, catalogue_status, is_latest_batch,
        include_in_course_count, status_adjustment_reason,
        status, batch_status, has_batch,
        start_date, end_date,
        batch_enrollment_count, bundle_enrollment_count,
        tutor_name, tutor_id,
        course_division, type, subject, level, language,
        sss_category, adhyayanam_category, personas,
        position_in_funnel, term_of_course,
        texts, certificate, course_sponsor,
        number_of_lectures, duration,
        computer_based_assessment, course_ids
    )
    WITH ranked_batches AS (
        SELECT 
            cb.bundle_id,
            cb.bundle_name,
            cb.batch_id,
            cb.batch_name,
            cb.batch_status,
            cb.start_date::DATE AS s_date,
            cb.end_date::DATE AS e_date,
            cb.batch_enrollment_count AS batch_count,
            cb.tutor_name AS t_name,
            cb.tutor_id AS t_id,
            cm.course_name,
            cm.status AS cat_status,
            cm.num_students AS bundle_count,
            cm.course_division AS c_div,
            cm.type AS c_type,
            cm.subject AS c_subj,
            cm.level AS c_lvl,
            cm.language AS c_lang,
            cm.sss_category AS c_sss,
            cm.adhyayanam_category AS c_adh,
            cm.personas AS c_pers,
            cm.position_in_funnel AS c_fun,
            cm.term_of_course AS c_term,
            cm.texts AS c_text,
            cm.certificate AS c_cert,
            cm.course_sponsor AS c_spon,
            cm.number_of_lectures AS c_lec,
            cm.duration AS c_dur,
            cm.course_ids AS c_ids,
            ROW_NUMBER() OVER (
                PARTITION BY cb.bundle_id 
                ORDER BY cb.start_date DESC NULLS LAST, cb.batch_id DESC
            ) AS rank_order
        FROM silver.course_batches AS cb
        LEFT JOIN silver.course_metadata AS cm ON cb.bundle_id = cm.bundle_id
        WHERE cb.batch_name NOT LIKE '%Test batch%'
    )
    SELECT 
        bundle_id, bundle_name, batch_id, batch_name,
        CASE WHEN rank_order = 1 THEN cat_status ELSE 'Completed' END,
        cat_status,
        CASE WHEN rank_order = 1 THEN 1 ELSE 0 END,
        CASE 
            WHEN rank_order = 1 AND cat_status IN ('Completed', 'Ongoing', 'Upcoming') 
            THEN 1 ELSE 0 
        END,
        '', 
        cat_status,
        batch_status,
        1, 
        s_date, e_date,
        batch_count, bundle_count,
        t_name, t_id,
        c_div, c_type, c_subj, c_lvl, c_lang,
        c_sss, c_adh, c_pers,
        c_fun, c_term,
        c_text, c_cert, c_spon,
        c_lec, c_dur,
        '', 
        c_ids
    FROM ranked_batches

    UNION ALL

    SELECT 
        cm.bundle_id, cm.course_name, NULL, NULL,
        cm.status, cm.status, 1, 0, '', cm.status, NULL,
        0, 
        NULL, NULL, 0, cm.num_students, NULL, NULL,
        cm.course_division, cm.type, cm.subject, cm.level, cm.language,
        cm.sss_category, cm.adhyayanam_category, cm.personas,
        cm.position_in_funnel, cm.term_of_course,
        cm.texts, cm.certificate, cm.course_sponsor,
        cm.number_of_lectures, cm.duration,
        '', cm.course_ids
    FROM silver.course_metadata AS cm
    WHERE NOT EXISTS (
        SELECT 1 FROM silver.course_batches AS cb 
        WHERE cb.bundle_id = cm.bundle_id 
        AND cb.batch_name NOT LIKE '%Test batch%'
    );
"""

# Connect to database.
def get_database_connection():
    # Return new connection.
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

# Create table and fill with data.
def build_report_data(database_cursor):
    # Print message.
    print("Ensuring table exists...")
    # Run create table SQL.
    database_cursor.execute(SQL_CREATE_TABLE)
    # Print message.
    print("Filling table with data...")
    # Run fill table SQL.
    database_cursor.execute(SQL_FILL_TABLE)

# Print final count of rows.
def show_row_count(database_cursor):
    # Count rows.
    database_cursor.execute("SELECT COUNT(*) FROM silver.course_batch_merge")
    # Get count.
    row_count = database_cursor.fetchone()[0]
    # Print count.
    print("Success! Table has " + str(row_count) + " rows.")

# Main function to run the script.
def main():
    # Try all steps.
    try:
        # Step 1: Connect.
        conn = get_database_connection()
        # Step 2: Get cursor.
        cursor = conn.cursor()
        # Step 3: Build.
        build_report_data(cursor)
        # Step 4: Save.
        conn.commit()
        # Step 5: Check.
        show_row_count(cursor)
        # Step 6: Close.
        cursor.close()
        conn.close()
    except Exception as error:
        # Print error.
        print("ERROR: Build failed.")
        print(error)

# Start script.
if __name__ == "__main__":
    main()
