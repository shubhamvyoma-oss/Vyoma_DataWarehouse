# ============================================================
# 05 — BUILD COURSE REPORTING TABLES
# ============================================================
# What it does: Joins data from multiple Silver tables and
#               creates two wide reporting tables:
#
#               Merge 1: silver.MasterCourseBatch
#                        (one row per bundle+batch pair)
#
#               Merge 2: silver.course_meta_data
#                        (adds lifecycle performance data)
#
# Why we need it: Power BI needs a single "wide" table with
#                 all course information in one place. This
#                 script builds that by joining:
#                 - course_batches (schedules)
#                 - course_metadata (catalogue info)
#                 - course_lifecycle (historical performance)
#                 - Bronze catalogue (extra descriptive fields)
#
# How to run:
#   python 05_build_courses/build_courses.py
#
# Run this AFTER scripts 02 and 03 have fetched fresh data.
#
# What to check after:
#   - silver.MasterCourseBatch should have rows
#   - silver.course_meta_data should have rows
# ============================================================

import psycopg2

# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
# ─────────────────────────────────────────────────────────────


# ── SQL: CREATE AND FILL MasterCourseBatch ────────────────────
# This SQL creates the table if it does not exist yet.
SQL_CREATE_MASTER_COURSE_BATCH = """
    CREATE TABLE IF NOT EXISTS silver."MasterCourseBatch" (
        id                      SERIAL PRIMARY KEY,
        bundle_id               BIGINT,
        batch_id                BIGINT,
        bundle_name             TEXT,
        course_name             TEXT,
        subject                 TEXT,
        course_type             TEXT,
        status                  TEXT,
        term_of_course          TEXT,
        position_in_funnel      TEXT,
        adhyayanam_category     TEXT,
        sss_category            TEXT,
        viniyoga                TEXT,
        course_division         TEXT,
        division                TEXT,
        level                   TEXT,
        language                TEXT,
        num_students            INTEGER,
        cost                    NUMERIC(10,2),
        tutors                  TEXT,
        tutor_ids               TEXT,
        course_ids              TEXT,
        texts                   TEXT,
        type                    TEXT,
        certificate             TEXT,
        course_sponsor          TEXT,
        number_of_lectures      TEXT,
        duration                TEXT,
        personas                TEXT,
        batch_name              TEXT,
        batch_status            TEXT,
        start_date              TIMESTAMPTZ,
        end_date                TIMESTAMPTZ,
        tutor_name              TEXT,
        admitted_students       INTEGER,
        is_latest_batch         SMALLINT DEFAULT 0,
        include_in_course_count SMALLINT DEFAULT 0,
        has_batch               SMALLINT DEFAULT 1,
        created_at              TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (bundle_id, batch_id)
    )
"""

# This SQL fills MasterCourseBatch by joining three tables.
# ROW_NUMBER() OVER (PARTITION BY bundle_id ...) ranks batches
# within each bundle by end_date so we can flag the latest one.
SQL_FILL_MASTER_COURSE_BATCH = """
    WITH ranked AS (
        SELECT
            cb.bundle_id, cb.batch_id,
            cb.bundle_name,
            cm.course_name, cm.subject, cm.course_type, cm.status,
            cm.term_of_course, cm.position_in_funnel, cm.adhyayanam_category,
            cm.sss_category, cm.viniyoga, cm.course_division, cm.division,
            cm.level, cm.language, cm.num_students, cm.cost,
            bc.tutors,
            bc.tutord_ids    AS tutor_ids,
            bc.course_ids, bc.texts, bc.type,
            bc.certificate, bc.course_sponsor, bc.number_of_lectures,
            bc.duration, bc.personas,
            cb.batch_name, cb.batch_status,
            cb.start_date_ist, cb.end_date_ist,
            cb.tutor_name, cb.admitted_students,
            ROW_NUMBER() OVER (
                PARTITION BY cb.bundle_id
                ORDER BY cb.end_date_ist DESC NULLS LAST, cb.batch_id DESC
            ) AS rn
        FROM silver.course_batches cb
        LEFT JOIN silver.course_metadata cm ON cm.bundle_id = cb.bundle_id
        LEFT JOIN bronze.course_catalogue_raw bc ON bc.bundle_id::BIGINT = cb.bundle_id
    )
    INSERT INTO silver."MasterCourseBatch" (
        bundle_id, batch_id,
        bundle_name, course_name, subject, course_type, status,
        term_of_course, position_in_funnel, adhyayanam_category,
        sss_category, viniyoga, course_division, division,
        level, language, num_students, cost,
        tutors, tutor_ids, course_ids, texts, type,
        certificate, course_sponsor, number_of_lectures, duration, personas,
        batch_name, batch_status, start_date, end_date, tutor_name, admitted_students,
        is_latest_batch, include_in_course_count, has_batch
    )
    SELECT
        bundle_id, batch_id,
        bundle_name, course_name, subject, course_type, status,
        term_of_course, position_in_funnel, adhyayanam_category,
        sss_category, viniyoga, course_division, division,
        level, language, num_students, cost,
        tutors, tutor_ids, course_ids, texts, type,
        certificate, course_sponsor, number_of_lectures, duration, personas,
        batch_name, batch_status, start_date_ist, end_date_ist, tutor_name, admitted_students,
        CASE WHEN rn = 1 THEN 1 ELSE 0 END AS is_latest_batch,
        CASE
            WHEN LOWER(COALESCE(batch_status, '')) = 'active' OR rn = 1
            THEN 1 ELSE 0
        END AS include_in_course_count,
        1 AS has_batch
    FROM ranked
    ON CONFLICT (bundle_id, batch_id) DO UPDATE SET
        bundle_name             = EXCLUDED.bundle_name,
        course_name             = EXCLUDED.course_name,
        subject                 = EXCLUDED.subject,
        course_type             = EXCLUDED.course_type,
        status                  = EXCLUDED.status,
        term_of_course          = EXCLUDED.term_of_course,
        position_in_funnel      = EXCLUDED.position_in_funnel,
        adhyayanam_category     = EXCLUDED.adhyayanam_category,
        sss_category            = EXCLUDED.sss_category,
        viniyoga                = EXCLUDED.viniyoga,
        course_division         = EXCLUDED.course_division,
        division                = EXCLUDED.division,
        level                   = EXCLUDED.level,
        language                = EXCLUDED.language,
        num_students            = EXCLUDED.num_students,
        cost                    = EXCLUDED.cost,
        tutors                  = EXCLUDED.tutors,
        tutor_ids               = EXCLUDED.tutor_ids,
        course_ids              = EXCLUDED.course_ids,
        texts                   = EXCLUDED.texts,
        type                    = EXCLUDED.type,
        certificate             = EXCLUDED.certificate,
        course_sponsor          = EXCLUDED.course_sponsor,
        number_of_lectures      = EXCLUDED.number_of_lectures,
        duration                = EXCLUDED.duration,
        personas                = EXCLUDED.personas,
        batch_name              = EXCLUDED.batch_name,
        batch_status            = EXCLUDED.batch_status,
        start_date              = EXCLUDED.start_date,
        end_date                = EXCLUDED.end_date,
        tutor_name              = EXCLUDED.tutor_name,
        admitted_students       = EXCLUDED.admitted_students,
        is_latest_batch         = EXCLUDED.is_latest_batch,
        include_in_course_count = EXCLUDED.include_in_course_count,
        has_batch               = EXCLUDED.has_batch
"""


# ── SQL: CREATE AND FILL course_meta_data ─────────────────────

SQL_CREATE_COURSE_META_DATA = """
    CREATE TABLE IF NOT EXISTS silver.course_meta_data (
        id                          SERIAL PRIMARY KEY,
        bundle_id                   BIGINT,
        batch_id                    BIGINT,
        bundle_name                 TEXT,
        course_name                 TEXT,
        subject                     TEXT,
        course_type                 TEXT,
        status                      TEXT,
        term_of_course              TEXT,
        position_in_funnel          TEXT,
        adhyayanam_category         TEXT,
        sss_category                TEXT,
        viniyoga                    TEXT,
        course_division             TEXT,
        division                    TEXT,
        level                       TEXT,
        language                    TEXT,
        num_students                INTEGER,
        cost                        NUMERIC(10,2),
        tutors                      TEXT,
        tutor_ids                   TEXT,
        course_ids                  TEXT,
        texts                       TEXT,
        type                        TEXT,
        certificate                 TEXT,
        course_sponsor              TEXT,
        number_of_lectures          TEXT,
        duration                    TEXT,
        personas                    TEXT,
        batch_name                  TEXT,
        batch_status                TEXT,
        start_date                  TIMESTAMPTZ,
        end_date                    TIMESTAMPTZ,
        tutor_name                  TEXT,
        admitted_students           INTEGER,
        is_latest_batch             SMALLINT DEFAULT 0,
        include_in_course_count     SMALLINT DEFAULT 0,
        has_batch                   SMALLINT DEFAULT 1,
        type_of_launch              TEXT,
        learning_model              TEXT,
        persona                     TEXT,
        first_class_date            DATE,
        last_class_date             DATE,
        enrollments_on_fc           INTEGER,
        enrollments_on_lc           INTEGER,
        avg_attendance_all_classes  NUMERIC(8,2),
        total_classes_held          INTEGER,
        total_students_certified    INTEGER,
        pass_percentage             NUMERIC(5,2),
        overall_course_rating       NUMERIC(3,2),
        primary_teacher_name        TEXT,
        ela                         TEXT,
        assessment_start_date       TEXT,
        assessment_end_date         TEXT,
        total_assessment_attendees  TEXT,
        created_at                  TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (bundle_id, batch_id)
    )
"""

SQL_FILL_COURSE_META_DATA = """
    INSERT INTO silver.course_meta_data (
        bundle_id, batch_id,
        bundle_name, course_name, subject, course_type, status,
        term_of_course, position_in_funnel, adhyayanam_category,
        sss_category, viniyoga, course_division, division,
        level, language, num_students, cost,
        tutors, tutor_ids, course_ids, texts, type,
        certificate, course_sponsor, number_of_lectures, duration, personas,
        batch_name, batch_status, start_date, end_date, tutor_name, admitted_students,
        is_latest_batch, include_in_course_count, has_batch,
        type_of_launch, learning_model, persona,
        first_class_date, last_class_date,
        enrollments_on_fc, enrollments_on_lc,
        avg_attendance_all_classes,
        total_classes_held, total_students_certified,
        pass_percentage, overall_course_rating,
        primary_teacher_name, ela,
        assessment_start_date, assessment_end_date, total_assessment_attendees
    )
    SELECT
        mcb.bundle_id, mcb.batch_id,
        mcb.bundle_name, mcb.course_name, mcb.subject, mcb.course_type, mcb.status,
        mcb.term_of_course, mcb.position_in_funnel, mcb.adhyayanam_category,
        mcb.sss_category, mcb.viniyoga, mcb.course_division, mcb.division,
        mcb.level, mcb.language, mcb.num_students, mcb.cost,
        mcb.tutors, mcb.tutor_ids, mcb.course_ids, mcb.texts, mcb.type,
        mcb.certificate, mcb.course_sponsor, mcb.number_of_lectures,
        mcb.duration, mcb.personas,
        mcb.batch_name, mcb.batch_status, mcb.start_date, mcb.end_date,
        mcb.tutor_name, mcb.admitted_students,
        mcb.is_latest_batch, mcb.include_in_course_count, mcb.has_batch,
        cl.type_of_launch, cl.learning_model, cl.persona,
        cl.first_class_date, cl.last_class_date,
        cl.enrollments_on_fc, cl.enrollments_on_lc,
        cl.avg_attendance          AS avg_attendance_all_classes,
        cl.total_classes_held,
        cl.total_certified         AS total_students_certified,
        cl.pass_percentage,
        cl.overall_rating          AS overall_course_rating,
        blc.primary_teacher_name_sfh_link AS primary_teacher_name,
        blc.ela,
        blc.assessment_start_date,
        blc.assessment_end_date,
        blc.total_assessment_attendees
    FROM silver."MasterCourseBatch" mcb
    LEFT JOIN silver.course_lifecycle cl
        ON  cl.course_id  = mcb.bundle_id
        AND cl.batch_name = mcb.batch_name
    LEFT JOIN bronze.course_lifecycle_raw blc
        ON  blc.course_id::BIGINT = mcb.bundle_id
        AND blc.batch_name        = mcb.batch_name
    ON CONFLICT (bundle_id, batch_id) DO UPDATE SET
        bundle_name                 = EXCLUDED.bundle_name,
        course_name                 = EXCLUDED.course_name,
        subject                     = EXCLUDED.subject,
        course_type                 = EXCLUDED.course_type,
        status                      = EXCLUDED.status,
        batch_name                  = EXCLUDED.batch_name,
        batch_status                = EXCLUDED.batch_status,
        start_date                  = EXCLUDED.start_date,
        end_date                    = EXCLUDED.end_date,
        tutor_name                  = EXCLUDED.tutor_name,
        admitted_students           = EXCLUDED.admitted_students,
        is_latest_batch             = EXCLUDED.is_latest_batch,
        include_in_course_count     = EXCLUDED.include_in_course_count,
        has_batch                   = EXCLUDED.has_batch,
        type_of_launch              = EXCLUDED.type_of_launch,
        learning_model              = EXCLUDED.learning_model,
        persona                     = EXCLUDED.persona,
        first_class_date            = EXCLUDED.first_class_date,
        last_class_date             = EXCLUDED.last_class_date,
        enrollments_on_fc           = EXCLUDED.enrollments_on_fc,
        enrollments_on_lc           = EXCLUDED.enrollments_on_lc,
        avg_attendance_all_classes  = EXCLUDED.avg_attendance_all_classes,
        total_classes_held          = EXCLUDED.total_classes_held,
        total_students_certified    = EXCLUDED.total_students_certified,
        pass_percentage             = EXCLUDED.pass_percentage,
        overall_course_rating       = EXCLUDED.overall_course_rating,
        primary_teacher_name        = EXCLUDED.primary_teacher_name,
        ela                         = EXCLUDED.ela,
        assessment_start_date       = EXCLUDED.assessment_start_date,
        assessment_end_date         = EXCLUDED.assessment_end_date,
        total_assessment_attendees  = EXCLUDED.total_assessment_attendees
"""


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def connect_to_database():
    # Open a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )
    return connection


def check_required_tables_exist(cursor):
    # Verify that all the source tables we need are present in the database
    required_tables = [
        ("silver", "course_metadata"),
        ("silver", "course_batches"),
        ("silver", "course_lifecycle"),
        ("bronze", "course_catalogue_raw"),
        ("bronze", "course_lifecycle_raw"),
    ]
    print("Checking that all required source tables exist ...")
    for schema_name, table_name in required_tables:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema_name, table_name)
        )
        if cursor.fetchone() is None:
            # A required table is missing — tell the user what to do
            raise RuntimeError(
                "Required table " + schema_name + "." + table_name + " not found. "
                "Run database/run_all.sql first, then run the CSV import scripts."
            )
    print("  All required tables found.")


def count_rows_in_table(cursor, table_name):
    # Count the rows in a table, quoted to handle mixed-case names
    cursor.execute("SELECT COUNT(*) FROM " + table_name)
    return cursor.fetchone()[0]


# ── MERGE 1: CREATE MasterCourseBatch ────────────────────────

def run_merge_1(cursor):
    # Create the MasterCourseBatch table if it doesn't exist yet
    cursor.execute(SQL_CREATE_MASTER_COURSE_BATCH)
    # Fill the table by joining course_batches + course_metadata + bronze catalogue
    cursor.execute(SQL_FILL_MASTER_COURSE_BATCH)


# ── MERGE 2: CREATE course_meta_data ─────────────────────────

def run_merge_2(cursor):
    # Create the course_meta_data table if it doesn't exist yet
    cursor.execute(SQL_CREATE_COURSE_META_DATA)
    # Fill it by joining MasterCourseBatch + course_lifecycle + bronze lifecycle
    cursor.execute(SQL_FILL_COURSE_META_DATA)


# ── MAIN ─────────────────────────────────────────────────────

def main():
    # Step 1: Connect to the database
    print("Connecting to database ...")
    try:
        connection = connect_to_database()
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        return
    cursor = connection.cursor()

    # Step 2: Check that all source tables exist
    try:
        check_required_tables_exist(cursor)
    except RuntimeError as error:
        print("ERROR: " + str(error))
        cursor.close()
        connection.close()
        return

    # Step 3: Run Merge 1 — create silver.MasterCourseBatch
    print("Running Merge 1 — creating silver.MasterCourseBatch ...")
    try:
        run_merge_1(cursor)
        connection.commit()
    except Exception as error:
        print("ERROR in Merge 1: " + str(error))
        connection.rollback()
        cursor.close()
        connection.close()
        return
    count1 = count_rows_in_table(cursor, 'silver."MasterCourseBatch"')
    print("  silver.MasterCourseBatch: " + str(count1) + " rows")

    # Step 4: Run Merge 2 — create silver.course_meta_data
    print("Running Merge 2 — creating silver.course_meta_data ...")
    try:
        run_merge_2(cursor)
        connection.commit()
    except Exception as error:
        print("ERROR in Merge 2: " + str(error))
        connection.rollback()
        cursor.close()
        connection.close()
        return
    count2 = count_rows_in_table(cursor, "silver.course_meta_data")
    print("  silver.course_meta_data: " + str(count2) + " rows")

    # Step 5: Close the database connection
    cursor.close()
    connection.close()
    print("Done. Both tables are ready for Power BI.")


if __name__ == "__main__":
    main()
