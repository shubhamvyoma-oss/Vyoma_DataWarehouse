import psycopg2
from datetime import datetime

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "your_password"
# ───────────────────────────────────────────────────────────────────────────────

# Staff emails are excluded from all student-facing metrics
STAFF_FILTER = "email NOT LIKE '%%@vyoma%%'"


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def connect():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def hdr(n, title):
    print(f"\n{'═' * 72}")
    print(f"  {n}. {title}")
    print(f"{'═' * 72}")

def sub(title):
    print(f"\n  ── {title}")

def m(label, val, pad=52):
    """Print a single key-value metric line."""
    print(f"    {label:<{pad}} {val}")

def q1(cur, sql, params=None):
    """Run a query and return the first value of the first row (or None)."""
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None

def qall(cur, sql, params=None):
    """Run a query and return all rows."""
    cur.execute(sql, params)
    return cur.fetchall()

def fmt(val, decimals=0):
    """Format a number with thousands commas. Returns '—' for None."""
    if val is None:
        return "—"
    try:
        if decimals:
            return f"{float(val):,.{decimals}f}"
        return f"{int(val):,}"
    except (TypeError, ValueError):
        return str(val)

def pct(val):
    if val is None:
        return "—"
    return f"{float(val):.1f}%"

def ago(ts):
    """Format a timestamp as a human-readable age string."""
    if ts is None:
        return "—"
    delta = datetime.now().astimezone() - ts
    days  = delta.days
    if days == 0:
        hours = delta.seconds // 3600
        return f"today ({hours}h ago)" if hours else "just now"
    if days == 1:
        return "yesterday"
    return f"{days} days ago"

def breakdown(cur, sql, col1_label="Value", col2_label="Count", indent=4):
    """Print a simple breakdown table from a (label, count) query."""
    rows = qall(cur, sql)
    if not rows:
        print(f"{'':>{indent}}(no data)")
        return
    max_lbl = max(len(str(r[0])) for r in rows)
    max_lbl = max(max_lbl, len(col1_label))
    header  = f"{'':>{indent}}{col1_label:<{max_lbl + 2}}  {col2_label}"
    print(header)
    print(f"{'':>{indent}}{'-' * (max_lbl + 2 + 2 + 10)}")
    for r in rows:
        label = str(r[0]) if r[0] is not None else "(not set)"
        val   = fmt(r[1]) if len(r) > 1 else ""
        extra = f"  {fmt(r[2])}" if len(r) > 2 else ""
        print(f"{'':>{indent}}{label:<{max_lbl + 2}}  {val}{extra}")

def toplist(cur, sql, headers, indent=4):
    """Print a numbered top-N table."""
    rows = qall(cur, sql)
    if not rows:
        print(f"{'':>{indent}}(no data)")
        return
    # Column widths
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
              for i, h in enumerate(headers)]
    hrow = "  ".join(f"{h:<{widths[i]}}" for i, h in enumerate(headers))
    print(f"{'':>{indent}}{'#':<4}{hrow}")
    print(f"{'':>{indent}}{'-' * (sum(widths) + 2 * len(widths) + 4)}")
    for rank, r in enumerate(rows, 1):
        cols = "  ".join(f"{str(v) if v is not None else '—':<{widths[i]}}" for i, v in enumerate(r))
        print(f"{'':>{indent}}{rank:<4}{cols}")


# ─── SECTION 1: ROW COUNTS & DATA FRESHNESS ───────────────────────────────────

def section_data_health(cur):
    hdr(1, "DATA HEALTH — ROW COUNTS & FRESHNESS")

    sub("Bronze tables")
    tables_bronze = [
        ("bronze.webhook_events",           "Main event store"),
        ("bronze.failed_events",            "Parse / DB failures"),
        ("bronze.course_catalogue_raw",     "Course catalogue CSV"),
        ("bronze.course_batches_raw",       "Batches CSV"),
        ("bronze.course_lifecycle_raw",     "MIS lifecycle tracker"),
        ("bronze.attendance_raw",           "Daily attendance pulls"),
        ("bronze.studentexport_raw",        "Student export CSV"),
        ("bronze.student_courses_enrolled_raw", "Enrollments CSV"),
        ("bronze.unresolved_students_raw",  "Unmatched CSV students"),
    ]
    for table, desc in tables_bronze:
        cnt = q1(cur, f"SELECT COUNT(*) FROM {table}")
        m(f"{table}  ({desc})", fmt(cnt))

    sub("Silver tables")
    tables_silver = [
        "silver.users",
        "silver.transactions",
        "silver.sessions",
        "silver.assessments",
        "silver.certificates",
        "silver.courses",
        "silver.announcements",
        "silver.course_metadata",
        "silver.course_batches",
        "silver.course_lifecycle",
        "silver.class_attendance",
        'silver."MasterCourseBatch"',
        "silver.course_meta_data",
    ]
    for table in tables_silver:
        cnt = q1(cur, f"SELECT COUNT(*) FROM {table}")
        m(table, fmt(cnt))

    sub("Data freshness")
    last_event = q1(cur, "SELECT MAX(received_at) FROM bronze.webhook_events")
    last_txn   = q1(cur, "SELECT MAX(inserted_at) FROM silver.transactions")
    last_att   = q1(cur, "SELECT MAX(pull_date)   FROM silver.class_attendance")
    last_user  = q1(cur, "SELECT MAX(received_at) FROM silver.users")
    m("Last webhook event received",         ago(last_event))
    m("Last transaction inserted",           ago(last_txn))
    m("Last attendance pull date",           str(last_att) if last_att else "—")
    m("Last user event received",            ago(last_user))

    failed = q1(cur, "SELECT COUNT(*) FROM bronze.failed_events")
    unrouted = q1(cur, "SELECT COUNT(*) FROM bronze.webhook_events WHERE routed_to_silver = false")
    m("Failed events in bronze.failed_events",   fmt(failed))
    m("Bronze events not yet routed to Silver",   fmt(unrouted))


# ─── SECTION 2: COURSE CATALOGUE ──────────────────────────────────────────────

def section_courses(cur):
    hdr(2, "COURSE CATALOGUE — BUNDLES")

    sub("Bundle counts")
    total_bundles = q1(cur, "SELECT COUNT(*) FROM silver.course_metadata")
    with_batch    = q1(cur, """
        SELECT COUNT(DISTINCT cm.bundle_id)
        FROM   silver.course_metadata cm
        JOIN   silver.course_batches  cb ON cb.bundle_id = cm.bundle_id
    """)
    no_batch = q1(cur, """
        SELECT COUNT(*) FROM silver.course_metadata cm
        WHERE  NOT EXISTS (
            SELECT 1 FROM silver.course_batches cb WHERE cb.bundle_id = cm.bundle_id
        )
    """)
    in_count = q1(cur, """
        SELECT COUNT(DISTINCT bundle_id)
        FROM   silver."MasterCourseBatch"
        WHERE  include_in_course_count = 1
    """)
    m("Total unique bundles (courses)",      fmt(total_bundles))
    m("Bundles with at least one batch",     fmt(with_batch))
    m("Bundles with no batch yet",           fmt(no_batch))
    m("Bundles counted in course count",     fmt(in_count))

    sub("By subject")
    breakdown(cur, """
        SELECT COALESCE(subject, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Subject", "Bundles")

    sub("By course type")
    breakdown(cur, """
        SELECT COALESCE(course_type, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Course Type", "Bundles")

    sub("By SSS category")
    breakdown(cur, """
        SELECT COALESCE(sss_category, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "SSS Category", "Bundles")

    sub("By adhyayanam category")
    breakdown(cur, """
        SELECT COALESCE(adhyayanam_category, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Adhyayanam Category", "Bundles")

    sub("By position in funnel")
    breakdown(cur, """
        SELECT COALESCE(position_in_funnel, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Funnel Position", "Bundles")

    sub("By division")
    breakdown(cur, """
        SELECT COALESCE(division, '(not set)'), COUNT(*)
        FROM   silver.course_metadata
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Division", "Bundles")

    sub("Data completeness")
    null_name    = q1(cur, "SELECT COUNT(*) FROM silver.course_metadata WHERE course_name IS NULL")
    null_subject = q1(cur, "SELECT COUNT(*) FROM silver.course_metadata WHERE subject    IS NULL")
    null_type    = q1(cur, "SELECT COUNT(*) FROM silver.course_metadata WHERE course_type IS NULL")
    m("Bundles with no course name",  fmt(null_name))
    m("Bundles with no subject",      fmt(null_subject))
    m("Bundles with no course type",  fmt(null_type))


# ─── SECTION 3: BATCHES ───────────────────────────────────────────────────────

def section_batches(cur):
    hdr(3, "BATCHES")

    sub("Batch counts")
    total_batches = q1(cur, "SELECT COUNT(*) FROM silver.course_batches")
    latest_count  = q1(cur, "SELECT COUNT(*) FROM silver.\"MasterCourseBatch\" WHERE is_latest_batch = 1")
    multi_batch   = q1(cur, """
        SELECT COUNT(*) FROM (
            SELECT bundle_id FROM silver.course_batches
            GROUP  BY bundle_id HAVING COUNT(*) > 1
        ) sub
    """)
    m("Total batches",                        fmt(total_batches))
    m("Batches flagged as latest per bundle",  fmt(latest_count))
    m("Bundles with more than one batch",      fmt(multi_batch))

    sub("Batch status breakdown (from API)")
    breakdown(cur, """
        SELECT COALESCE(batch_status, '(not set)'), COUNT(*)
        FROM   silver.course_batches
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Batch Status", "Batches")

    sub("Batch timeline (by date range)")
    ongoing  = q1(cur, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE  start_date_ist::DATE <= CURRENT_DATE
        AND    end_date_ist::DATE   >= CURRENT_DATE
    """)
    upcoming = q1(cur, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE  start_date_ist::DATE > CURRENT_DATE
    """)
    completed = q1(cur, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE  end_date_ist::DATE < CURRENT_DATE
    """)
    no_dates = q1(cur, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE  start_date_ist IS NULL AND end_date_ist IS NULL
    """)
    m("Ongoing  (today falls within batch dates)",  fmt(ongoing))
    m("Upcoming (start date is in the future)",      fmt(upcoming))
    m("Completed (end date is in the past)",         fmt(completed))
    m("No dates set",                                fmt(no_dates))

    sub("Batch enrollment figures")
    avg_admitted = q1(cur, "SELECT ROUND(AVG(admitted_students)) FROM silver.course_batches WHERE admitted_students > 0")
    max_admitted = q1(cur, "SELECT MAX(admitted_students)        FROM silver.course_batches")
    total_seats  = q1(cur, "SELECT SUM(admitted_students)        FROM silver.course_batches")
    m("Average admitted students per batch",  fmt(avg_admitted))
    m("Highest admitted students (one batch)", fmt(max_admitted))
    m("Total admitted seats (all batches)",   fmt(total_seats))

    sub("Average batch duration")
    avg_days = q1(cur, """
        SELECT ROUND(AVG(end_date_ist::DATE - start_date_ist::DATE))
        FROM   silver.course_batches
        WHERE  start_date_ist IS NOT NULL AND end_date_ist IS NOT NULL
    """)
    m("Average batch length (days)", fmt(avg_days))


# ─── SECTION 4: ENROLLMENTS ───────────────────────────────────────────────────

def section_enrollments(cur):
    hdr(4, "ENROLLMENTS")

    sub("Totals")
    total       = q1(cur, f"SELECT COUNT(*) FROM silver.transactions WHERE {STAFF_FILTER}")
    all_rows    = q1(cur,  "SELECT COUNT(*) FROM silver.transactions")
    uniq_stud   = q1(cur, f"SELECT COUNT(DISTINCT user_id) FROM silver.transactions WHERE {STAFF_FILTER}")
    uniq_bundle = q1(cur, f"SELECT COUNT(DISTINCT bundle_id) FROM silver.transactions WHERE {STAFF_FILTER} AND bundle_id IS NOT NULL")
    m("Total enrollment records (excl. staff)",  fmt(total))
    m("Total enrollment records (incl. staff)",  fmt(all_rows))
    m("Unique students enrolled",                fmt(uniq_stud))
    m("Unique courses with enrollments",         fmt(uniq_bundle))

    sub("By data source")
    breakdown(cur, f"""
        SELECT COALESCE(source, '(not set)'), COUNT(*)
        FROM   silver.transactions
        WHERE  {STAFF_FILTER}
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Source", "Enrollments")

    sub("Paid vs free (excl. staff)")
    paid  = q1(cur, f"SELECT COUNT(*) FROM silver.transactions WHERE {STAFF_FILTER} AND final_price  > 0")
    free  = q1(cur, f"SELECT COUNT(*) FROM silver.transactions WHERE {STAFF_FILTER} AND COALESCE(final_price, 0) = 0")
    m("Paid enrollments  (final_price > 0)",     fmt(paid))
    m("Free enrollments  (final_price = 0 / NULL)", fmt(free))

    sub("Time windows")
    this_year  = q1(cur, f"""
        SELECT COUNT(*) FROM silver.transactions
        WHERE  {STAFF_FILTER}
        AND    created_at_ist >= DATE_TRUNC('year', CURRENT_DATE)
    """)
    this_month = q1(cur, f"""
        SELECT COUNT(*) FROM silver.transactions
        WHERE  {STAFF_FILTER}
        AND    created_at_ist >= DATE_TRUNC('month', CURRENT_DATE)
    """)
    m("Enrollments this calendar year",  fmt(this_year))
    m("Enrollments this month",          fmt(this_month))

    sub("Monthly enrollment trend (last 12 months)")
    breakdown(cur, f"""
        SELECT TO_CHAR(DATE_TRUNC('month', created_at_ist), 'YYYY-MM'), COUNT(*)
        FROM   silver.transactions
        WHERE  {STAFF_FILTER}
        AND    created_at_ist >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months')
        GROUP  BY 1 ORDER BY 1
    """, "Month", "Enrollments")

    sub("Multi-course students")
    multi = q1(cur, f"""
        SELECT COUNT(*) FROM (
            SELECT user_id
            FROM   silver.transactions
            WHERE  {STAFF_FILTER}
            GROUP  BY user_id
            HAVING COUNT(DISTINCT bundle_id) > 1
        ) sub
    """)
    one_only = q1(cur, f"""
        SELECT COUNT(*) FROM (
            SELECT user_id
            FROM   silver.transactions
            WHERE  {STAFF_FILTER}
            GROUP  BY user_id
            HAVING COUNT(DISTINCT bundle_id) = 1
        ) sub
    """)
    max_courses = q1(cur, f"""
        SELECT MAX(cnt) FROM (
            SELECT COUNT(DISTINCT bundle_id) AS cnt
            FROM   silver.transactions WHERE {STAFF_FILTER}
            GROUP  BY user_id
        ) sub
    """)
    m("Students enrolled in > 1 course",        fmt(multi))
    m("Students enrolled in exactly 1 course",  fmt(one_only))
    m("Max courses by one student",             fmt(max_courses))


# ─── SECTION 5: STUDENTS (USERS) ──────────────────────────────────────────────

def section_students(cur):
    hdr(5, "STUDENTS")

    sub("Registration counts")
    total      = q1(cur, "SELECT COUNT(*) FROM silver.users")
    students   = q1(cur, f"SELECT COUNT(*) FROM silver.users WHERE {STAFF_FILTER}")
    staff      = q1(cur,  "SELECT COUNT(*) FROM silver.users WHERE email LIKE '%@vyoma%'")
    this_year  = q1(cur, f"SELECT COUNT(*) FROM silver.users WHERE {STAFF_FILTER} AND created_at_ist >= DATE_TRUNC('year',  CURRENT_DATE)")
    this_month = q1(cur, f"SELECT COUNT(*) FROM silver.users WHERE {STAFF_FILTER} AND created_at_ist >= DATE_TRUNC('month', CURRENT_DATE)")
    m("Total registered users",                 fmt(total))
    m("Students (non-@vyoma)",                   fmt(students))
    m("Staff / admins (@vyoma)",                 fmt(staff))
    m("New students registered this year",       fmt(this_year))
    m("New students registered this month",      fmt(this_month))

    sub("Profile completeness")
    with_contact = q1(cur, f"""
        SELECT COUNT(*) FROM silver.users
        WHERE  {STAFF_FILTER}
        AND    contact_number IS NOT NULL AND contact_number <> ''
    """)
    with_city    = q1(cur, f"SELECT COUNT(*) FROM silver.users WHERE {STAFF_FILTER} AND city  IS NOT NULL AND city  <> ''")
    with_state   = q1(cur, f"SELECT COUNT(*) FROM silver.users WHERE {STAFF_FILTER} AND state IS NOT NULL AND state <> ''")
    with_parent  = q1(cur, f"""
        SELECT COUNT(*) FROM silver.users
        WHERE  {STAFF_FILTER}
        AND    (parent_name IS NOT NULL OR parent_email IS NOT NULL)
    """)
    m("With contact number",          fmt(with_contact))
    m("With city filled in",          fmt(with_city))
    m("With state filled in",         fmt(with_state))
    m("With parent info (minors)",    fmt(with_parent))

    sub("Top 10 states by student count")
    breakdown(cur, f"""
        SELECT COALESCE(NULLIF(TRIM(state),''), '(not set)'), COUNT(*)
        FROM   silver.users
        WHERE  {STAFF_FILTER}
        GROUP  BY 1 ORDER BY 2 DESC LIMIT 10
    """, "State", "Students")

    sub("New student registrations by year")
    breakdown(cur, f"""
        SELECT EXTRACT(YEAR FROM created_at_ist)::INT, COUNT(*)
        FROM   silver.users
        WHERE  {STAFF_FILTER} AND created_at_ist IS NOT NULL
        GROUP  BY 1 ORDER BY 1
    """, "Year", "New Students")


# ─── SECTION 6: ATTENDANCE ────────────────────────────────────────────────────

def section_attendance(cur):
    hdr(6, "CLASS ATTENDANCE")

    sub("Coverage")
    total_records = q1(cur, "SELECT COUNT(*)             FROM silver.class_attendance")
    uniq_batches  = q1(cur, "SELECT COUNT(DISTINCT batch_id)   FROM silver.class_attendance")
    uniq_bundles  = q1(cur, "SELECT COUNT(DISTINCT bundle_id)  FROM silver.class_attendance")
    m("Total class-day records",            fmt(total_records))
    m("Unique batches with attendance data", fmt(uniq_batches))
    m("Unique courses with attendance data", fmt(uniq_bundles))

    sub("Overall attendance figures")
    avg_pct     = q1(cur, "SELECT ROUND(AVG(attendance_pct)::NUMERIC, 2)  FROM silver.class_attendance")
    tot_present = q1(cur, "SELECT SUM(present_count)                       FROM silver.class_attendance")
    tot_late    = q1(cur, "SELECT SUM(late_count)                          FROM silver.class_attendance")
    tot_absent  = q1(cur, "SELECT SUM(absent_count)                        FROM silver.class_attendance")
    m("Average attendance % (across all classes)",  pct(avg_pct))
    m("Total student-class present instances",      fmt(tot_present))
    m("Total student-class late instances",         fmt(tot_late))
    m("Total student-class absent instances",       fmt(tot_absent))

    sub("Attendance quality breakdown")
    high = q1(cur, """
        SELECT COUNT(*) FROM (
            SELECT batch_id FROM silver.class_attendance
            GROUP  BY batch_id HAVING AVG(attendance_pct) >= 80
        ) sub
    """)
    mid = q1(cur, """
        SELECT COUNT(*) FROM (
            SELECT batch_id FROM silver.class_attendance
            GROUP  BY batch_id HAVING AVG(attendance_pct) >= 50 AND AVG(attendance_pct) < 80
        ) sub
    """)
    low = q1(cur, """
        SELECT COUNT(*) FROM (
            SELECT batch_id FROM silver.class_attendance
            GROUP  BY batch_id HAVING AVG(attendance_pct) < 50
        ) sub
    """)
    m("Batches with avg attendance >= 80%",  fmt(high))
    m("Batches with avg attendance 50–79%",  fmt(mid))
    m("Batches with avg attendance < 50%",   fmt(low))

    sub("Class counts per batch")
    avg_classes = q1(cur, """
        SELECT ROUND(AVG(class_count)) FROM (
            SELECT batch_id, COUNT(*) AS class_count
            FROM   silver.class_attendance GROUP BY batch_id
        ) sub
    """)
    max_classes = q1(cur, """
        SELECT MAX(class_count) FROM (
            SELECT batch_id, COUNT(*) AS class_count
            FROM   silver.class_attendance GROUP BY batch_id
        ) sub
    """)
    m("Average classes held per batch",  fmt(avg_classes))
    m("Most classes held by one batch",  fmt(max_classes))

    sub("First-class vs last-class drop-off")
    first_avg = q1(cur, """
        SELECT ROUND(AVG(present_count)::NUMERIC, 1)
        FROM   silver.class_attendance
        WHERE  class_number = 1
    """)
    last_avg = q1(cur, """
        SELECT ROUND(AVG(present_count)::NUMERIC, 1)
        FROM   silver.class_attendance ca
        WHERE  class_number = (
            SELECT MAX(class_number) FROM silver.class_attendance c2 WHERE c2.batch_id = ca.batch_id
        )
    """)
    m("Average present count on first class",  fmt(first_avg))
    m("Average present count on last class",   fmt(last_avg))

    sub("Attendance by year")
    breakdown(cur, """
        SELECT EXTRACT(YEAR FROM class_date)::INT,
               COUNT(*)                          AS classes,
               ROUND(AVG(attendance_pct)::NUMERIC, 1) AS avg_pct,
               SUM(present_count)                AS total_present
        FROM   silver.class_attendance
        GROUP  BY 1 ORDER BY 1
    """, "Year", "Classes")


# ─── SECTION 7: ASSESSMENTS & CERTIFICATIONS ──────────────────────────────────

def section_assessments(cur):
    hdr(7, "ASSESSMENTS & CERTIFICATIONS")

    sub("Assessment submissions")
    total = q1(cur, "SELECT COUNT(*) FROM silver.assessments")
    tests = q1(cur, "SELECT COUNT(*) FROM silver.assessments WHERE event_type LIKE '%test%'")
    exer  = q1(cur, "SELECT COUNT(*) FROM silver.assessments WHERE event_type LIKE '%exercise%'")
    m("Total assessment events",     fmt(total))
    m("Test submissions",            fmt(tests))
    m("Exercise submissions",        fmt(exer))

    sub("Evaluation status")
    evaluated = q1(cur, "SELECT COUNT(*) FROM silver.assessments WHERE is_evaluated = 1")
    pending   = q1(cur, "SELECT COUNT(*) FROM silver.assessments WHERE COALESCE(is_evaluated, 0) = 0")
    avg_mark  = q1(cur, "SELECT ROUND(AVG(mark)::NUMERIC, 2) FROM silver.assessments WHERE mark IS NOT NULL")
    m("Evaluated",                   fmt(evaluated))
    m("Pending evaluation",          fmt(pending))
    m("Average mark (evaluated)",    fmt(avg_mark))

    sub("Certifications")
    total_certs = q1(cur, "SELECT COUNT(*) FROM silver.certificates")
    this_year   = q1(cur, "SELECT COUNT(*) FROM silver.certificates WHERE issued_at_ist >= DATE_TRUNC('year', CURRENT_DATE)")
    this_month  = q1(cur, "SELECT COUNT(*) FROM silver.certificates WHERE issued_at_ist >= DATE_TRUNC('month', CURRENT_DATE)")
    m("Total certificates issued",       fmt(total_certs))
    m("Certificates issued this year",   fmt(this_year))
    m("Certificates issued this month",  fmt(this_month))

    sub("Lifecycle tracker (Silver) — batch summary")
    lc_total     = q1(cur, "SELECT COUNT(*) FROM silver.course_lifecycle")
    lc_certified = q1(cur, "SELECT SUM(total_certified) FROM silver.course_lifecycle WHERE total_certified IS NOT NULL")
    lc_avg_att   = q1(cur, "SELECT ROUND(AVG(avg_attendance)::NUMERIC, 1) FROM silver.course_lifecycle WHERE avg_attendance IS NOT NULL")
    lc_avg_pass  = q1(cur, "SELECT ROUND(AVG(pass_percentage)::NUMERIC, 1) FROM silver.course_lifecycle WHERE pass_percentage IS NOT NULL")
    lc_avg_rating= q1(cur, "SELECT ROUND(AVG(overall_rating)::NUMERIC, 2) FROM silver.course_lifecycle WHERE overall_rating IS NOT NULL")
    m("Lifecycle records (batch-level)",            fmt(lc_total))
    m("Total students certified (lifecycle data)",  fmt(lc_certified))
    m("Average attendance % (lifecycle data)",       pct(lc_avg_att))
    m("Average pass percentage",                     pct(lc_avg_pass))
    m("Average overall course rating (out of 5)",    fmt(lc_avg_rating, 2))

    sub("Course completions (webhook events)")
    completions = q1(cur, "SELECT COUNT(*) FROM silver.courses")
    uniq_users  = q1(cur, "SELECT COUNT(DISTINCT user_id) FROM silver.courses")
    uniq_bundles= q1(cur, "SELECT COUNT(DISTINCT bundle_id) FROM silver.courses")
    m("Total course_completed webhook events",  fmt(completions))
    m("Unique students who completed a course", fmt(uniq_users))
    m("Unique courses with completions",        fmt(uniq_bundles))


# ─── SECTION 8: REVENUE ───────────────────────────────────────────────────────

def section_revenue(cur):
    hdr(8, "REVENUE & PRICING")

    sub("Overall")
    total_rev = q1(cur, f"""
        SELECT SUM(final_price) FROM silver.transactions
        WHERE  {STAFF_FILTER} AND final_price IS NOT NULL
    """)
    avg_price = q1(cur, f"""
        SELECT ROUND(AVG(final_price)::NUMERIC, 2) FROM silver.transactions
        WHERE  {STAFF_FILTER} AND final_price IS NOT NULL
    """)
    max_price = q1(cur, f"SELECT MAX(final_price) FROM silver.transactions WHERE {STAFF_FILTER}")
    paid_cnt  = q1(cur, f"SELECT COUNT(*) FROM silver.transactions WHERE {STAFF_FILTER} AND final_price  > 0")
    free_cnt  = q1(cur, f"SELECT COUNT(*) FROM silver.transactions WHERE {STAFF_FILTER} AND COALESCE(final_price, 0) = 0")
    m("Total revenue (all time, excl. staff)",  fmt(total_rev, 2))
    m("Average final price per enrollment",     fmt(avg_price, 2))
    m("Highest single enrollment price",        fmt(max_price, 2))
    m("Paid enrollments",                       fmt(paid_cnt))
    m("Free / zero-price enrollments",          fmt(free_cnt))

    sub("Revenue by year")
    breakdown(cur, f"""
        SELECT EXTRACT(YEAR FROM created_at_ist)::INT,
               COUNT(*),
               ROUND(SUM(final_price)::NUMERIC, 2)
        FROM   silver.transactions
        WHERE  {STAFF_FILTER} AND final_price > 0 AND created_at_ist IS NOT NULL
        GROUP  BY 1 ORDER BY 1
    """, "Year", "Enrollments")

    sub("Revenue by source")
    breakdown(cur, f"""
        SELECT COALESCE(source, '(not set)'),
               COUNT(*),
               ROUND(SUM(COALESCE(final_price,0))::NUMERIC, 2)
        FROM   silver.transactions WHERE {STAFF_FILTER}
        GROUP  BY 1 ORDER BY 3 DESC
    """, "Source", "Enrollments")

    sub("Currencies in use")
    breakdown(cur, f"""
        SELECT COALESCE(currency, '(not set)'), COUNT(*)
        FROM   silver.transactions WHERE {STAFF_FILTER}
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Currency", "Transactions")

    sub("Discount usage")
    with_discount = q1(cur, f"""
        SELECT COUNT(*) FROM silver.transactions
        WHERE  {STAFF_FILTER} AND discount IS NOT NULL AND discount > 0
    """)
    avg_discount = q1(cur, f"""
        SELECT ROUND(AVG(discount)::NUMERIC, 2) FROM silver.transactions
        WHERE  {STAFF_FILTER} AND discount > 0
    """)
    m("Enrollments with a discount applied",  fmt(with_discount))
    m("Average discount amount",              fmt(avg_discount, 2))


# ─── SECTION 9: LIVE SESSIONS ─────────────────────────────────────────────────

def section_sessions(cur):
    hdr(9, "LIVE SESSIONS")

    sub("Event counts")
    total    = q1(cur, "SELECT COUNT(*) FROM silver.sessions")
    uniq     = q1(cur, "SELECT COUNT(DISTINCT attendance_id) FROM silver.sessions")
    with_start = q1(cur, "SELECT COUNT(*) FROM silver.sessions WHERE actual_start_ist IS NOT NULL")
    m("Total session events received",          fmt(total))
    m("Unique session instances (attendance_id)", fmt(uniq))
    m("Sessions with actual start recorded",    fmt(with_start))

    sub("Cancellations & delays")
    cancelled = q1(cur, "SELECT COUNT(*) FROM silver.sessions WHERE cancellation_reason IS NOT NULL AND cancellation_reason <> ''")
    late_start = q1(cur, "SELECT COUNT(*) FROM silver.sessions WHERE delay_minutes IS NOT NULL AND delay_minutes > 0")
    avg_delay  = q1(cur, "SELECT ROUND(AVG(delay_minutes)::NUMERIC, 1) FROM silver.sessions WHERE delay_minutes IS NOT NULL AND delay_minutes > 0")
    m("Sessions cancelled",                            fmt(cancelled))
    m("Sessions that started late (delay > 0)",        fmt(late_start))
    m("Average delay when late (minutes)",             fmt(avg_delay, 1))

    sub("By virtual platform")
    breakdown(cur, """
        SELECT COALESCE(virtual_platform, '(not set)'), COUNT(*)
        FROM   silver.sessions
        GROUP  BY 1 ORDER BY 2 DESC
    """, "Platform", "Sessions")

    sub("Session events by year")
    breakdown(cur, """
        SELECT EXTRACT(YEAR FROM received_at)::INT, COUNT(*)
        FROM   silver.sessions
        GROUP  BY 1 ORDER BY 1
    """, "Year", "Session Events")


# ─── SECTION 10: TOP LISTS ────────────────────────────────────────────────────

def section_top_lists(cur):
    hdr(10, "TOP LISTS")

    sub("Top 15 courses by total enrollments")
    toplist(cur, f"""
        SELECT COALESCE(cm.course_name, t.course_name, '(unknown)'),
               COUNT(DISTINCT t.user_id) AS students,
               COUNT(*)                  AS enrollment_records
        FROM   silver.transactions t
        LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
        WHERE  t.{STAFF_FILTER}
        GROUP  BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Students", "Records"])

    sub("Top 15 courses by revenue")
    toplist(cur, f"""
        SELECT COALESCE(cm.course_name, t.course_name, '(unknown)'),
               COUNT(DISTINCT t.user_id)                   AS students,
               ROUND(SUM(t.final_price)::NUMERIC, 2)       AS total_revenue
        FROM   silver.transactions t
        LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
        WHERE  t.{STAFF_FILTER} AND t.final_price > 0
        GROUP  BY 1 ORDER BY 3 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Students", "Revenue"])

    sub("Top 15 batches by average attendance %")
    toplist(cur, """
        SELECT COALESCE(cm.course_name, '(unknown)'),
               cb.batch_name,
               ROUND(AVG(ca.attendance_pct)::NUMERIC, 1) AS avg_pct,
               COUNT(*)                                   AS classes
        FROM   silver.class_attendance ca
        JOIN   silver.course_batches   cb ON ca.batch_id  = cb.batch_id
        LEFT JOIN silver.course_metadata cm ON ca.bundle_id = cm.bundle_id
        GROUP  BY cm.course_name, cb.batch_name
        ORDER  BY 3 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Batch", "Avg Att%", "Classes"])

    sub("Top 10 subjects by total enrollments")
    toplist(cur, f"""
        SELECT COALESCE(cm.subject, '(unknown)'),
               COUNT(DISTINCT t.user_id) AS students
        FROM   silver.transactions t
        LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
        WHERE  t.{STAFF_FILTER}
        GROUP  BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 10
    """, ["Subject", "Students"])

    sub("Top 10 courses by number of batches")
    toplist(cur, """
        SELECT COALESCE(cm.course_name, '(unknown)'),
               COUNT(*)          AS batches,
               SUM(cb.admitted_students) AS total_admitted
        FROM   silver.course_batches cb
        LEFT JOIN silver.course_metadata cm ON cb.bundle_id = cm.bundle_id
        GROUP  BY cm.course_name ORDER BY 2 DESC NULLS LAST LIMIT 10
    """, ["Course Name", "Batches", "Total Admitted"])

    sub("Top 10 students by course count")
    toplist(cur, f"""
        SELECT COALESCE(u.full_name, t.email, '(unknown)'),
               COUNT(DISTINCT t.bundle_id) AS courses,
               COUNT(*)                    AS enrollments
        FROM   silver.transactions t
        LEFT JOIN silver.users u ON t.user_id = u.user_id
        WHERE  t.{STAFF_FILTER}
        GROUP  BY u.full_name, t.email
        ORDER  BY 2 DESC NULLS LAST LIMIT 10
    """, ["Student", "Courses", "Enrollments"])

    sub("Top 10 courses by average overall rating (lifecycle data)")
    toplist(cur, """
        SELECT COALESCE(cm.course_name, cl.course_name, '(unknown)'),
               COUNT(*)                                    AS batches_tracked,
               ROUND(AVG(cl.overall_rating)::NUMERIC, 2)  AS avg_rating,
               ROUND(AVG(cl.pass_percentage)::NUMERIC, 1) AS avg_pass_pct
        FROM   silver.course_lifecycle cl
        LEFT JOIN silver.course_metadata cm ON cl.course_id = cm.bundle_id
        WHERE  cl.overall_rating IS NOT NULL
        GROUP  BY cm.course_name, cl.course_name
        ORDER  BY 3 DESC NULLS LAST LIMIT 10
    """, ["Course Name", "Batches", "Avg Rating", "Avg Pass%"])

    sub("Enrollment funnel: first-class vs last-class attendance (lifecycle data)")
    toplist(cur, """
        SELECT COALESCE(cm.course_name, cl.course_name, '(unknown)'),
               cl.batch_name,
               cl.enrollments_on_fc       AS enroll_first_class,
               cl.enrollments_on_lc       AS enroll_last_class,
               cl.avg_attendance          AS avg_att_pct
        FROM   silver.course_lifecycle cl
        LEFT JOIN silver.course_metadata cm ON cl.course_id = cm.bundle_id
        WHERE  cl.enrollments_on_fc IS NOT NULL
        ORDER  BY cl.enrollments_on_fc DESC NULLS LAST LIMIT 10
    """, ["Course", "Batch", "Enroll@FC", "Enroll@LC", "Avg Att%"])


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "═" * 72)
    print("  VYOMA SAMSKRTA PATHASALA — DATA PIPELINE ANALYSIS")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    print("═" * 72)

    conn = connect()
    cur  = conn.cursor()

    section_data_health(cur)
    section_courses(cur)
    section_batches(cur)
    section_enrollments(cur)
    section_students(cur)
    section_attendance(cur)
    section_assessments(cur)
    section_revenue(cur)
    section_sessions(cur)
    section_top_lists(cur)

    cur.close()
    conn.close()

    print("\n" + "═" * 72)
    print("  Analysis complete.")
    print("═" * 72 + "\n")


if __name__ == "__main__":
    main()
