import os
import sys
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# ── CONFIG ──────────────────────────────────────
DB_HOST           = "localhost"
DB_NAME           = "edmingle_analytics"
DB_USER           = "postgres"
DB_PASSWORD       = "Svyoma"
DB_PORT           = 5432
WEBHOOK_SECRET    = "your_webhook_secret_here"
EDMINGLE_API_KEY  = "859b19531f4b149a605679c5ea21eeb8"
ORG_ID            = 683
INSTITUTION_ID    = 483
API_BASE_URL      = "https://vyoma-api.edmingle.com/nuSource/api/v1"
# ─────────────────────────────────────────────────

import psycopg2
import psycopg2.extras


def db_connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def show_transactions_schema(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT column_name, data_type, column_default, is_nullable
        FROM   information_schema.columns
        WHERE  table_schema = 'silver' AND table_name = 'transactions'
        ORDER  BY ordinal_position
    """)
    rows = cur.fetchall()
    cur.close()

    print("silver.transactions schema:")
    print(f"  {'Column':<25} {'Type':<22} {'Nullable':<10} {'Default'}")
    print(f"  {'-'*25} {'-'*22} {'-'*10} {'-'*30}")
    for r in rows:
        default = (r['column_default'] or '')[:30]
        print(f"  {r['column_name']:<25} {r['data_type']:<22} {r['is_nullable']:<10} {default}")
    print()


def _unix_or_null(val):
    # Treat '0' and '' as NULL — enrollment CSVs use zero for missing timestamps.
    if val is None or str(val).strip() in ('', '0', 'None'):
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_date_created(val):
    # studentexport.csv uses "M/D/YYYY HH:MM" in IST — attach IST offset before storing.
    if not val or str(val).strip() in ('', 'None'):
        return None
    try:
        naive = datetime.datetime.strptime(val.strip(), '%m/%d/%Y %H:%M')
        ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        return naive.replace(tzinfo=ist)
    except (ValueError, AttributeError):
        return None


# ── STEP 1: bronze.student_courses_enrolled_raw → silver.transactions ─────────

def backfill_transactions(conn):
    cur_read = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur_read.execute("""
        SELECT
            source_row,
            user_id, name, email,
            bundle_id, class_name, master_batch_id, master_batch_name,
            institution_bundle_id,
            classusers_start_date, classusers_end_date
        FROM bronze.student_courses_enrolled_raw
        WHERE user_id IS NOT NULL
    """)
    rows = cur_read.fetchall()
    cur_read.close()

    inserted = 0
    skipped  = 0
    errors   = 0

    cur_write = conn.cursor()
    for row in rows:
        source_row = row['source_row']
        event_id   = f"csv-enrollment-{source_row}"

        try:
            user_id = int(row['user_id'])
        except (TypeError, ValueError):
            errors += 1
            continue

        bundle_id             = _unix_or_null(row['bundle_id'])
        master_batch_id       = _unix_or_null(row['master_batch_id'])
        institution_bundle_id = _unix_or_null(row['institution_bundle_id'])
        start_date_unix       = _unix_or_null(row['classusers_start_date'])
        end_date_unix         = _unix_or_null(row['classusers_end_date'])

        cur_write.execute("""
            INSERT INTO silver.transactions (
                event_id, event_type,
                user_id, email, full_name,
                bundle_id, course_name,
                master_batch_id, master_batch_name, institution_bundle_id,
                start_date_ist, end_date_ist,
                source
            ) VALUES (
                %s, 'csv.import',
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                unix_to_ist(%s), unix_to_ist(%s),
                'csv'
            )
            ON CONFLICT (user_id, bundle_id, master_batch_id) DO UPDATE SET
                email             = COALESCE(silver.transactions.email,         EXCLUDED.email),
                full_name         = COALESCE(silver.transactions.full_name,     EXCLUDED.full_name),
                course_name       = COALESCE(silver.transactions.course_name,   EXCLUDED.course_name),
                master_batch_name = COALESCE(silver.transactions.master_batch_name, EXCLUDED.master_batch_name),
                institution_bundle_id = COALESCE(silver.transactions.institution_bundle_id, EXCLUDED.institution_bundle_id),
                start_date_ist    = COALESCE(silver.transactions.start_date_ist, EXCLUDED.start_date_ist),
                end_date_ist      = COALESCE(silver.transactions.end_date_ist,  EXCLUDED.end_date_ist)
        """, (
            event_id,
            user_id, row['email'], row['name'],
            bundle_id, row['class_name'],
            master_batch_id, row['master_batch_name'], institution_bundle_id,
            start_date_unix, end_date_unix,
        ))

        if cur_write.rowcount == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cur_write.close()
    return inserted, skipped, errors


# ── STEP 2: bronze.studentexport_raw → silver.users + bronze.unresolved_students_raw

def backfill_users(conn):
    # Build email → user_id lookup from enrollment table (one user_id per email).
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT ON (LOWER(TRIM(email)))
            LOWER(TRIM(email)) AS email_key,
            user_id::bigint    AS user_id
        FROM bronze.student_courses_enrolled_raw
        WHERE email IS NOT NULL
          AND user_id IS NOT NULL
        ORDER BY LOWER(TRIM(email)), source_row
    """)
    email_to_uid = {r['email_key']: r['user_id'] for r in cur.fetchall()}
    cur.close()

    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT source_row, name, email, contact_number_dial_code, contact_number,
               username, city, state, address,
               parent_name, parent_email, parent_contact,
               date_created
        FROM bronze.studentexport_raw
    """)
    student_rows = cur.fetchall()
    cur.close()

    inserted_users   = 0
    skipped_users    = 0
    unresolved_count = 0
    cur_u = conn.cursor()

    for row in student_rows:
        source_row = row['source_row']
        raw_email  = row['email']
        email_key  = raw_email.lower().strip() if raw_email else None
        user_id    = email_to_uid.get(email_key) if email_key else None

        if user_id is None:
            cur_u.execute("""
                INSERT INTO bronze.unresolved_students_raw (source_row, email, raw_row)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (source_row, raw_email, psycopg2.extras.Json(dict(row))))
            unresolved_count += 1
            continue

        dial  = (row['contact_number_dial_code'] or '').strip()
        phone = (row['contact_number']           or '').strip()
        if dial and phone:
            contact_number = f"+{dial.lstrip('+')}{phone}"
        else:
            contact_number = phone or None

        created_at_ist = _parse_date_created(row['date_created'])

        cur_u.execute("""
            INSERT INTO silver.users (
                event_id, event_type, user_id,
                email, full_name, user_name, contact_number,
                city, state, address,
                parent_name, parent_email, parent_contact,
                created_at_ist, received_at
            ) VALUES (
                %s, 'csv.import', %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, NOW() AT TIME ZONE 'Asia/Kolkata'
            )
            ON CONFLICT (user_id) DO UPDATE SET
                email          = COALESCE(silver.users.email,          EXCLUDED.email),
                full_name      = COALESCE(silver.users.full_name,      EXCLUDED.full_name),
                user_name      = COALESCE(silver.users.user_name,      EXCLUDED.user_name),
                contact_number = COALESCE(silver.users.contact_number, EXCLUDED.contact_number),
                city           = COALESCE(silver.users.city,           EXCLUDED.city),
                state          = COALESCE(silver.users.state,          EXCLUDED.state),
                address        = COALESCE(silver.users.address,        EXCLUDED.address),
                parent_name    = COALESCE(silver.users.parent_name,    EXCLUDED.parent_name),
                parent_email   = COALESCE(silver.users.parent_email,   EXCLUDED.parent_email),
                parent_contact = COALESCE(silver.users.parent_contact, EXCLUDED.parent_contact),
                created_at_ist = COALESCE(silver.users.created_at_ist, EXCLUDED.created_at_ist)
        """, (
            f"csv-student-{source_row}", user_id,
            raw_email, row['name'], row['username'], contact_number,
            row['city'], row['state'], row['address'],
            row['parent_name'], row['parent_email'], row['parent_contact'],
            created_at_ist,
        ))

        if cur_u.rowcount == 1:
            inserted_users += 1
        else:
            skipped_users += 1

    conn.commit()
    cur_u.close()
    return inserted_users, skipped_users, unresolved_count


def main():
    print("=" * 60)
    print("CSV Silver Backfill")
    print("=" * 60)
    print()

    conn = db_connect()
    try:
        show_transactions_schema(conn)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze.student_courses_enrolled_raw")
        n_enroll = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM bronze.studentexport_raw")
        n_students = cur.fetchone()[0]
        cur.close()

        print(f"bronze.student_courses_enrolled_raw:  {n_enroll} rows")
        print(f"bronze.studentexport_raw:             {n_students} rows")
        print()

        print("Loading silver.transactions from enrollment CSV ...")
        t_ins, t_skip, t_err = backfill_transactions(conn)
        print(f"  silver.transactions inserted:          {t_ins}")
        print(f"  silver.transactions skipped (conflict): {t_skip}")
        if t_err:
            print(f"  silver.transactions errors (bad user_id): {t_err}")
        print()

        print("Loading silver.users + unresolved from student export ...")
        u_ins, u_skip, u_unres = backfill_users(conn)
        print(f"  silver.users inserted:                 {u_ins}")
        print(f"  silver.users skipped (conflict):       {u_skip}")
        print(f"  bronze.unresolved_students_raw:        {u_unres} (no email match)")
        print()

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM silver.transactions")
        total_t = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM silver.users")
        total_u = cur.fetchone()[0]
        cur.close()

        print("Final row counts:")
        print(f"  silver.transactions:                  {total_t}")
        print(f"  silver.users:                         {total_u}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
