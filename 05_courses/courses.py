import psycopg2

# ─── CONFIG ────────────────────────────────────────────────────────────────────
# Update these values before running on your machine.
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "your_password"
# ───────────────────────────────────────────────────────────────────────────────


def get_connection():
    """Open a connection to the PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )


def check_source_tables(cur):
    """Confirm all source tables exist before running the merges."""
    required = [
        ("silver", "course_metadata"),
        ("silver", "course_batches"),
        ("silver", "course_lifecycle"),
        ("bronze", "course_catalogue_raw"),
        ("bronze", "course_lifecycle_raw"),
    ]
    for schema, table in required:
        cur.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table)
        )
        if cur.fetchone() is None:
            raise RuntimeError(
                f"Required table {schema}.{table} not found — "
                "run database/run_all.sql first, then run the CSV import scripts."
            )
    print("All source tables found.")


# ─── MERGE 1 ───────────────────────────────────────────────────────────────────
# Combines silver.course_metadata (course-level metadata) with
# silver.course_batches (one row per batch) and bronze.course_catalogue_raw
# (extra descriptive columns not promoted to Silver).
#
# Result: silver.MasterCourseBatch — one row per bundle-batch pair, with three
# computed flags:
#   is_latest_batch         — 1 for the most recent batch per bundle (by end_date)
#   include_in_course_count — 1 for active batches OR the latest batch per bundle
#   has_batch               — 1 for every row here (all rows come from course_batches)
# ───────────────────────────────────────────────────────────────────────────────

def create_master_course_batch(cur):
    # Create the target table the first time this script runs.
    # UNIQUE (bundle_id, batch_id) allows ON CONFLICT upserts on re-runs.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver."MasterCourseBatch" (
            id                      SERIAL PRIMARY KEY,

            -- Join keys
            bundle_id               BIGINT,
            batch_id                BIGINT,

            -- Course-level columns (from silver.course_metadata)
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

            -- Extra descriptive columns from bronze.course_catalogue_raw
            -- (not in Silver because they are not needed for webhook processing)
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

            -- Batch-level columns (from silver.course_batches)
            batch_name              TEXT,
            batch_status            TEXT,
            start_date              TIMESTAMPTZ,
            end_date                TIMESTAMPTZ,
            tutor_name              TEXT,
            admitted_students       INTEGER,

            -- Computed flags
            -- is_latest_batch: 1 for the batch with the highest end_date for this bundle
            is_latest_batch         SMALLINT DEFAULT 0,
            -- include_in_course_count: 1 if this batch is active OR is the latest batch
            include_in_course_count SMALLINT DEFAULT 0,
            -- has_batch: always 1 in this table because every row comes from course_batches
            has_batch               SMALLINT DEFAULT 1,

            created_at              TIMESTAMPTZ DEFAULT NOW(),

            UNIQUE (bundle_id, batch_id)
        )
    """)

    # We use a CTE (WITH clause) so that is_latest_batch can be computed once
    # via a window function, then referenced again when computing include_in_course_count.
    # Without a CTE you cannot reference a window-function alias in the same SELECT.
    cur.execute("""
        WITH ranked AS (
            SELECT
                cb.bundle_id,
                cb.batch_id,

                -- bundle_name lives in course_batches, not course_metadata
                cb.bundle_name,

                -- Course classification from silver.course_metadata
                cm.course_name,
                cm.subject,
                cm.course_type,
                cm.status,
                cm.term_of_course,
                cm.position_in_funnel,
                cm.adhyayanam_category,
                cm.sss_category,
                cm.viniyoga,
                cm.course_division,
                cm.division,
                cm.level,
                cm.language,
                cm.num_students,
                cm.cost,

                -- Extra fields from Bronze catalogue
                -- Note: the Bronze column is named "tutord_ids" (typo in source data)
                bc.tutors,
                bc.tutord_ids    AS tutor_ids,
                bc.course_ids,
                bc.texts,
                bc.type,
                bc.certificate,
                bc.course_sponsor,
                bc.number_of_lectures,
                bc.duration,
                bc.personas,

                -- Batch schedule from silver.course_batches
                cb.batch_name,
                cb.batch_status,
                cb.start_date_ist,
                cb.end_date_ist,
                cb.tutor_name,
                cb.admitted_students,

                -- Rank batches within each bundle by end_date, newest first.
                -- NULLS LAST means batches without an end_date rank behind dated batches.
                -- batch_id DESC breaks ties when two batches share the same end_date.
                ROW_NUMBER() OVER (
                    PARTITION BY cb.bundle_id
                    ORDER BY cb.end_date_ist DESC NULLS LAST, cb.batch_id DESC
                ) AS rn

            FROM silver.course_batches cb

            -- Join course-level metadata on bundle_id
            LEFT JOIN silver.course_metadata cm
                ON cm.bundle_id = cb.bundle_id

            -- Join Bronze catalogue for descriptive columns not in Silver
            -- bundle_id is stored as TEXT in Bronze, so cast to BIGINT for the join
            LEFT JOIN bronze.course_catalogue_raw bc
                ON bc.bundle_id::BIGINT = cb.bundle_id
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
            bundle_id,
            batch_id,
            bundle_name,
            course_name,
            subject,
            course_type,
            status,
            term_of_course,
            position_in_funnel,
            adhyayanam_category,
            sss_category,
            viniyoga,
            course_division,
            division,
            level,
            language,
            num_students,
            cost,
            tutors,
            tutor_ids,
            course_ids,
            texts,
            type,
            certificate,
            course_sponsor,
            number_of_lectures,
            duration,
            personas,
            batch_name,
            batch_status,
            start_date_ist,
            end_date_ist,
            tutor_name,
            admitted_students,

            -- is_latest_batch: the row ranked 1st (newest end_date) for this bundle gets 1
            CASE WHEN rn = 1 THEN 1 ELSE 0 END   AS is_latest_batch,

            -- include_in_course_count: 1 if active OR latest batch for this bundle
            CASE
                WHEN LOWER(COALESCE(batch_status, '')) = 'active' OR rn = 1
                THEN 1 ELSE 0
            END                                    AS include_in_course_count,

            -- has_batch: always 1 here — every row in this table has a batch by definition
            1                                      AS has_batch

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
    """)


# ─── MERGE 2 ───────────────────────────────────────────────────────────────────
# Adds lifecycle performance data to every row in MasterCourseBatch.
# Result: silver.course_meta_data — the final wide reporting table.
#
# Join key: silver.course_lifecycle.course_id = bundle_id
#           AND silver.course_lifecycle.batch_name = batch_name
#
# LEFT JOIN so every batch row is kept even if no lifecycle record exists.
# The three computed flags (is_latest_batch, include_in_course_count, has_batch)
# are copied directly from MasterCourseBatch — no need to recompute them.
# ───────────────────────────────────────────────────────────────────────────────

def create_course_meta_data(cur):
    # Create the final wide reporting table.
    # Table name is lowercase and unquoted (no mixed-case identifier issues).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.course_meta_data (
            id                      SERIAL PRIMARY KEY,

            -- ── from MasterCourseBatch ────────────────────────────────────────
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

            -- Computed flags (propagated from MasterCourseBatch)
            is_latest_batch         SMALLINT DEFAULT 0,
            include_in_course_count SMALLINT DEFAULT 0,
            has_batch               SMALLINT DEFAULT 1,

            -- ── from silver.course_lifecycle ──────────────────────────────────
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

            -- ── extra detail from bronze.course_lifecycle_raw ─────────────────
            -- These columns were not promoted to Silver because Silver only stores
            -- summary metrics. We pull them from Bronze to complete the picture.
            primary_teacher_name        TEXT,
            ela                         TEXT,
            assessment_start_date       TEXT,
            assessment_end_date         TEXT,
            total_assessment_attendees  TEXT,

            created_at                  TIMESTAMPTZ DEFAULT NOW(),

            UNIQUE (bundle_id, batch_id)
        )
    """)

    # LEFT JOIN course_lifecycle so every batch row appears even with no lifecycle data.
    # lifecycle columns will be NULL for batches that have no lifecycle record yet.
    cur.execute("""
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
            -- All columns from MasterCourseBatch, including the computed flags
            mcb.bundle_id,
            mcb.batch_id,
            mcb.bundle_name,
            mcb.course_name,
            mcb.subject,
            mcb.course_type,
            mcb.status,
            mcb.term_of_course,
            mcb.position_in_funnel,
            mcb.adhyayanam_category,
            mcb.sss_category,
            mcb.viniyoga,
            mcb.course_division,
            mcb.division,
            mcb.level,
            mcb.language,
            mcb.num_students,
            mcb.cost,
            mcb.tutors,
            mcb.tutor_ids,
            mcb.course_ids,
            mcb.texts,
            mcb.type,
            mcb.certificate,
            mcb.course_sponsor,
            mcb.number_of_lectures,
            mcb.duration,
            mcb.personas,
            mcb.batch_name,
            mcb.batch_status,
            mcb.start_date,
            mcb.end_date,
            mcb.tutor_name,
            mcb.admitted_students,

            -- Flags are already computed in MasterCourseBatch — copy them directly
            mcb.is_latest_batch,
            mcb.include_in_course_count,
            mcb.has_batch,

            -- Lifecycle summary columns from Silver
            -- Silver names them "avg_attendance" and "total_certified";
            -- we use clearer reporting names here
            cl.type_of_launch,
            cl.learning_model,
            cl.persona,
            cl.first_class_date,
            cl.last_class_date,
            cl.enrollments_on_fc,
            cl.enrollments_on_lc,
            cl.avg_attendance          AS avg_attendance_all_classes,
            cl.total_classes_held,
            cl.total_certified         AS total_students_certified,
            cl.pass_percentage,
            cl.overall_rating          AS overall_course_rating,

            -- Extra detail from Bronze lifecycle (column names are long in the source)
            blc.primary_teacher_name_sfh_link AS primary_teacher_name,
            blc.ela,
            blc.assessment_start_date,
            blc.assessment_end_date,
            blc.total_assessment_attendees

        FROM silver."MasterCourseBatch" mcb

        -- LEFT JOIN: every batch row appears; lifecycle columns are NULL when no match
        LEFT JOIN silver.course_lifecycle cl
            ON  cl.course_id  = mcb.bundle_id
            AND cl.batch_name = mcb.batch_name

        -- LEFT JOIN Bronze for extra detail not in Silver
        -- course_id in Bronze is stored as TEXT, so cast to BIGINT for the join
        LEFT JOIN bronze.course_lifecycle_raw blc
            ON  blc.course_id::BIGINT = mcb.bundle_id
            AND blc.batch_name        = mcb.batch_name

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
            has_batch               = EXCLUDED.has_batch,
            type_of_launch          = EXCLUDED.type_of_launch,
            learning_model          = EXCLUDED.learning_model,
            persona                 = EXCLUDED.persona,
            first_class_date        = EXCLUDED.first_class_date,
            last_class_date         = EXCLUDED.last_class_date,
            enrollments_on_fc       = EXCLUDED.enrollments_on_fc,
            enrollments_on_lc       = EXCLUDED.enrollments_on_lc,
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
    """)


def main():
    print("Connecting to database...")
    conn = get_connection()
    cur  = conn.cursor()

    # Step 1: confirm all source tables exist before doing any work
    check_source_tables(cur)

    # Step 2: run Merge 1 — create silver.MasterCourseBatch
    print("Running Merge 1 — creating silver.MasterCourseBatch...")
    create_master_course_batch(cur)
    conn.commit()

    cur.execute('SELECT COUNT(*) FROM silver."MasterCourseBatch"')
    count1 = cur.fetchone()[0]
    print(f"silver.MasterCourseBatch: {count1} rows")

    # Step 3: run Merge 2 — create silver.course_meta_data
    print("Running Merge 2 — creating silver.course_meta_data...")
    create_course_meta_data(cur)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM silver.course_meta_data")
    count2 = cur.fetchone()[0]
    print(f"silver.course_meta_data: {count2} rows")

    cur.close()
    conn.close()
    print("Done. Both tables are ready.")


if __name__ == "__main__":
    main()
