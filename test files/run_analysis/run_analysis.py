# ============================================================
# 15 — RUN ANALYSIS
# ============================================================
# What it does: Connects to the database and prints a 10-section
#               business metrics report directly to the terminal.
#
#   Section 1:  Data health — row counts and freshness
#   Section 2:  Course catalogue
#   Section 3:  Batches
#   Section 4:  Enrollments
#   Section 5:  Students
#   Section 6:  Class attendance
#   Section 7:  Assessments and certifications
#   Section 8:  Revenue and pricing
#   Section 9:  Live sessions
#   Section 10: Top lists (best courses, students, attendance)
#
# Why we need it: Provides a quick health check and summary
#                 of all key business metrics without opening
#                 Power BI or writing SQL manually.
#
# How to run:
#   python 15_run_analysis/run_analysis.py
#
#   Tip: pipe to a file to save the report:
#   python 15_run_analysis/run_analysis.py > report.txt
# ============================================================

import sys
import datetime
import psycopg2


# ── DATABASE SETTINGS ─────────────────────────────────────────
DB_HOST     = "localhost"
DB_NAME     = "edmingle_analytics"
DB_USER     = "postgres"
DB_PASSWORD = "Svyoma"
DB_PORT     = 5432
# ─────────────────────────────────────────────────────────────

# Staff emails contain '@vyoma' — exclude them from student metrics
STAFF_EMAIL_FILTER = "email NOT LIKE '%%@vyoma%%'"


# ── PRINT HELPERS ─────────────────────────────────────────────

def print_section_header(section_number, title):
    # Print a thick divider line with the section number and title
    print("")
    print("=" * 72)
    print("  " + str(section_number) + ". " + title)
    print("=" * 72)


def print_subsection(title):
    # Print a thin divider for a subsection within a section
    print("")
    print("  -- " + title)


def print_metric(label, value, pad=52):
    # Print one key-value metric line, padded for alignment
    padded_label = label.ljust(pad)
    print("    " + padded_label + " " + str(value))


def format_number(value, decimal_places=0):
    # Format a number with commas, returning a dash for None
    if value is None:
        return "--"
    try:
        if decimal_places > 0:
            return "{:,.{}f}".format(float(value), decimal_places)
        return "{:,}".format(int(value))
    except (TypeError, ValueError):
        return str(value)


def format_percent(value):
    # Format a number as a percentage string, e.g. "75.3%"
    if value is None:
        return "--"
    return "{:.1f}%".format(float(value))


def format_age(timestamp):
    # Format a timestamp as "N days ago" or "today" etc.
    if timestamp is None:
        return "--"
    delta = datetime.datetime.now().astimezone() - timestamp
    days = delta.days
    if days == 0:
        hours = delta.seconds // 3600
        if hours == 0:
            return "just now"
        return "today (" + str(hours) + "h ago)"
    if days == 1:
        return "yesterday"
    return str(days) + " days ago"


# ── QUERY HELPERS ─────────────────────────────────────────────

def query_one_value(cursor, sql, params=None):
    # Run a query and return the first column of the first row, or None
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if row:
        return row[0]
    return None


def query_all_rows(cursor, sql, params=None):
    # Run a query and return all rows as a list of tuples
    cursor.execute(sql, params)
    return cursor.fetchall()


def print_breakdown_table(cursor, sql, col1_label="Value", col2_label="Count", indent=4):
    # Print a simple two-column breakdown table from a query that returns (label, count) rows
    rows = query_all_rows(cursor, sql)
    if not rows:
        print(" " * indent + "(no data)")
        return

    # Find the widest label to align the columns nicely
    max_label_width = max(len(str(row[0])) for row in rows)
    max_label_width = max(max_label_width, len(col1_label))

    # Print the header
    header = " " * indent + col1_label.ljust(max_label_width + 2) + "  " + col2_label
    print(header)
    print(" " * indent + "-" * (max_label_width + 2 + 2 + 10))

    # Print each row
    for row in rows:
        label = str(row[0]) if row[0] is not None else "(not set)"
        count = format_number(row[1]) if len(row) > 1 else ""
        extra = "  " + format_number(row[2]) if len(row) > 2 else ""
        print(" " * indent + label.ljust(max_label_width + 2) + "  " + count + extra)


def print_top_list(cursor, sql, column_headers, indent=4):
    # Print a numbered top-N table with multiple columns
    rows = query_all_rows(cursor, sql)
    if not rows:
        print(" " * indent + "(no data)")
        return

    # Calculate the width of each column based on data and header lengths
    column_widths = []
    for col_index in range(len(column_headers)):
        header_width = len(str(column_headers[col_index]))
        data_width = max((len(str(row[col_index])) for row in rows), default=0)
        column_widths.append(max(header_width, data_width))

    # Print the header row
    header_parts = []
    for col_index in range(len(column_headers)):
        header_parts.append(column_headers[col_index].ljust(column_widths[col_index]))
    print(" " * indent + "    " + "  ".join(header_parts))

    # Print the separator line
    total_width = sum(column_widths) + 2 * len(column_widths) + 4
    print(" " * indent + "-" * total_width)

    # Print each data row with a rank number
    for rank_index in range(len(rows)):
        row = rows[rank_index]
        rank_number = str(rank_index + 1)
        data_parts = []
        for col_index in range(len(row)):
            cell_value = str(row[col_index]) if row[col_index] is not None else "--"
            data_parts.append(cell_value.ljust(column_widths[col_index]))
        print(" " * indent + rank_number.ljust(4) + "  ".join(data_parts))


# ── SECTION 1: DATA HEALTH ────────────────────────────────────

def section_data_health(cursor):
    print_section_header(1, "DATA HEALTH -- ROW COUNTS & FRESHNESS")

    print_subsection("Bronze tables")
    bronze_tables = [
        ("bronze.webhook_events",               "Main event store"),
        ("bronze.failed_events",                "Parse / DB failures"),
        ("bronze.course_catalogue_raw",         "Course catalogue CSV"),
        ("bronze.course_batches_raw",           "Batches CSV"),
        ("bronze.course_lifecycle_raw",         "MIS lifecycle tracker"),
        ("bronze.attendance_raw",               "Daily attendance pulls"),
        ("bronze.studentexport_raw",            "Student export CSV"),
        ("bronze.student_courses_enrolled_raw", "Enrollments CSV"),
        ("bronze.unresolved_students_raw",      "Unmatched CSV students"),
    ]
    for table_name, description in bronze_tables:
        count = query_one_value(cursor, "SELECT COUNT(*) FROM " + table_name)
        print_metric(table_name + "  (" + description + ")", format_number(count))

    print_subsection("Silver tables")
    silver_tables = [
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
    for table_name in silver_tables:
        count = query_one_value(cursor, "SELECT COUNT(*) FROM " + table_name)
        print_metric(table_name, format_number(count))

    print_subsection("Data freshness")
    last_event = query_one_value(cursor, "SELECT MAX(received_at) FROM bronze.webhook_events")
    last_txn   = query_one_value(cursor, "SELECT MAX(inserted_at) FROM silver.transactions")
    last_att   = query_one_value(cursor, "SELECT MAX(pull_date)   FROM silver.class_attendance")
    last_user  = query_one_value(cursor, "SELECT MAX(received_at) FROM silver.users")
    print_metric("Last webhook event received",        format_age(last_event))
    print_metric("Last transaction inserted",          format_age(last_txn))
    print_metric("Last attendance pull date",          str(last_att) if last_att else "--")
    print_metric("Last user event received",           format_age(last_user))

    failed   = query_one_value(cursor, "SELECT COUNT(*) FROM bronze.failed_events")
    unrouted = query_one_value(cursor,
        "SELECT COUNT(*) FROM bronze.webhook_events WHERE routed_to_silver = false")
    print_metric("Failed events in bronze.failed_events",  format_number(failed))
    print_metric("Bronze events not yet routed to Silver",  format_number(unrouted))


# ── SECTION 2: COURSE CATALOGUE ───────────────────────────────

def section_courses(cursor):
    print_section_header(2, "COURSE CATALOGUE -- BUNDLES")

    print_subsection("Bundle counts")
    total_bundles = query_one_value(cursor, "SELECT COUNT(*) FROM silver.course_metadata")
    with_batch    = query_one_value(cursor, """
        SELECT COUNT(DISTINCT cm.bundle_id)
        FROM   silver.course_metadata cm
        JOIN   silver.course_batches  cb ON cb.bundle_id = cm.bundle_id
    """)
    no_batch = query_one_value(cursor, """
        SELECT COUNT(*) FROM silver.course_metadata cm
        WHERE  NOT EXISTS (
            SELECT 1 FROM silver.course_batches cb WHERE cb.bundle_id = cm.bundle_id
        )
    """)
    in_count = query_one_value(cursor, """
        SELECT COUNT(DISTINCT bundle_id)
        FROM   silver."MasterCourseBatch"
        WHERE  include_in_course_count = 1
    """)
    print_metric("Total unique bundles (courses)",      format_number(total_bundles))
    print_metric("Bundles with at least one batch",     format_number(with_batch))
    print_metric("Bundles with no batch yet",           format_number(no_batch))
    print_metric("Bundles counted in course count",     format_number(in_count))

    print_subsection("By subject")
    print_breakdown_table(cursor, """
        SELECT COALESCE(subject, '(not set)'), COUNT(*)
        FROM   silver.course_metadata GROUP BY 1 ORDER BY 2 DESC
    """, "Subject", "Bundles")

    print_subsection("By course type")
    print_breakdown_table(cursor, """
        SELECT COALESCE(course_type, '(not set)'), COUNT(*)
        FROM   silver.course_metadata GROUP BY 1 ORDER BY 2 DESC
    """, "Course Type", "Bundles")

    print_subsection("By SSS category")
    print_breakdown_table(cursor, """
        SELECT COALESCE(sss_category, '(not set)'), COUNT(*)
        FROM   silver.course_metadata GROUP BY 1 ORDER BY 2 DESC
    """, "SSS Category", "Bundles")

    print_subsection("By adhyayanam category")
    print_breakdown_table(cursor, """
        SELECT COALESCE(adhyayanam_category, '(not set)'), COUNT(*)
        FROM   silver.course_metadata GROUP BY 1 ORDER BY 2 DESC
    """, "Adhyayanam Category", "Bundles")

    print_subsection("By position in funnel")
    print_breakdown_table(cursor, """
        SELECT COALESCE(position_in_funnel, '(not set)'), COUNT(*)
        FROM   silver.course_metadata GROUP BY 1 ORDER BY 2 DESC
    """, "Funnel Position", "Bundles")

    print_subsection("Data completeness")
    null_name    = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.course_metadata WHERE course_name IS NULL")
    null_subject = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.course_metadata WHERE subject    IS NULL")
    null_type    = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.course_metadata WHERE course_type IS NULL")
    print_metric("Bundles with no course name",  format_number(null_name))
    print_metric("Bundles with no subject",      format_number(null_subject))
    print_metric("Bundles with no course type",  format_number(null_type))


# ── SECTION 3: BATCHES ────────────────────────────────────────

def section_batches(cursor):
    print_section_header(3, "BATCHES")

    print_subsection("Batch counts")
    total_batches = query_one_value(cursor, "SELECT COUNT(*) FROM silver.course_batches")
    latest_count  = query_one_value(cursor,
        'SELECT COUNT(*) FROM silver."MasterCourseBatch" WHERE is_latest_batch = 1')
    multi_batch   = query_one_value(cursor, """
        SELECT COUNT(*) FROM (
            SELECT bundle_id FROM silver.course_batches
            GROUP BY bundle_id HAVING COUNT(*) > 1
        ) sub
    """)
    print_metric("Total batches",                        format_number(total_batches))
    print_metric("Batches flagged as latest per bundle",  format_number(latest_count))
    print_metric("Bundles with more than one batch",      format_number(multi_batch))

    print_subsection("Batch status breakdown")
    print_breakdown_table(cursor, """
        SELECT COALESCE(batch_status, '(not set)'), COUNT(*)
        FROM   silver.course_batches GROUP BY 1 ORDER BY 2 DESC
    """, "Batch Status", "Batches")

    print_subsection("Batch timeline (based on date range)")
    ongoing   = query_one_value(cursor, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE start_date_ist::DATE <= CURRENT_DATE AND end_date_ist::DATE >= CURRENT_DATE
    """)
    upcoming  = query_one_value(cursor, """
        SELECT COUNT(*) FROM silver.course_batches WHERE start_date_ist::DATE > CURRENT_DATE
    """)
    completed = query_one_value(cursor, """
        SELECT COUNT(*) FROM silver.course_batches WHERE end_date_ist::DATE < CURRENT_DATE
    """)
    no_dates  = query_one_value(cursor, """
        SELECT COUNT(*) FROM silver.course_batches
        WHERE start_date_ist IS NULL AND end_date_ist IS NULL
    """)
    print_metric("Ongoing  (today within batch dates)", format_number(ongoing))
    print_metric("Upcoming (start date in future)",      format_number(upcoming))
    print_metric("Completed (end date in past)",         format_number(completed))
    print_metric("No dates set",                         format_number(no_dates))

    print_subsection("Batch enrollment figures")
    avg_admitted = query_one_value(cursor,
        "SELECT ROUND(AVG(admitted_students)) FROM silver.course_batches WHERE admitted_students > 0")
    max_admitted = query_one_value(cursor, "SELECT MAX(admitted_students) FROM silver.course_batches")
    total_seats  = query_one_value(cursor, "SELECT SUM(admitted_students) FROM silver.course_batches")
    print_metric("Average admitted students per batch",   format_number(avg_admitted))
    print_metric("Highest admitted students (one batch)", format_number(max_admitted))
    print_metric("Total admitted seats (all batches)",    format_number(total_seats))


# ── SECTION 4: ENROLLMENTS ────────────────────────────────────

def section_enrollments(cursor):
    print_section_header(4, "ENROLLMENTS")

    print_subsection("Totals")
    total     = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER)
    all_rows  = query_one_value(cursor, "SELECT COUNT(*) FROM silver.transactions")
    uniq_stud = query_one_value(cursor,
        "SELECT COUNT(DISTINCT user_id) FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER)
    print_metric("Total enrollment records (excl. staff)",  format_number(total))
    print_metric("Total enrollment records (incl. staff)",  format_number(all_rows))
    print_metric("Unique students enrolled",                format_number(uniq_stud))

    print_subsection("By data source")
    print_breakdown_table(cursor,
        "SELECT COALESCE(source, '(not set)'), COUNT(*) FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " GROUP BY 1 ORDER BY 2 DESC",
        "Source", "Enrollments")

    print_subsection("Monthly trend (last 12 months)")
    print_breakdown_table(cursor,
        "SELECT TO_CHAR(DATE_TRUNC('month', created_at_ist), 'YYYY-MM'), COUNT(*) "
        "FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER + " "
        "AND created_at_ist >= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months') "
        "GROUP BY 1 ORDER BY 1",
        "Month", "Enrollments")

    print_subsection("Multi-course students")
    multi = query_one_value(cursor,
        "SELECT COUNT(*) FROM ("
        "SELECT user_id FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER + " "
        "GROUP BY user_id HAVING COUNT(DISTINCT bundle_id) > 1) sub")
    one_only = query_one_value(cursor,
        "SELECT COUNT(*) FROM ("
        "SELECT user_id FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER + " "
        "GROUP BY user_id HAVING COUNT(DISTINCT bundle_id) = 1) sub")
    print_metric("Students enrolled in > 1 course",       format_number(multi))
    print_metric("Students enrolled in exactly 1 course", format_number(one_only))


# ── SECTION 5: STUDENTS ───────────────────────────────────────

def section_students(cursor):
    print_section_header(5, "STUDENTS")

    print_subsection("Registration counts")
    total      = query_one_value(cursor, "SELECT COUNT(*) FROM silver.users")
    students   = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.users WHERE " + STAFF_EMAIL_FILTER)
    staff      = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.users WHERE email LIKE '%@vyoma%'")
    this_year  = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.users WHERE " + STAFF_EMAIL_FILTER +
        " AND created_at_ist >= DATE_TRUNC('year', CURRENT_DATE)")
    this_month = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.users WHERE " + STAFF_EMAIL_FILTER +
        " AND created_at_ist >= DATE_TRUNC('month', CURRENT_DATE)")
    print_metric("Total registered users",           format_number(total))
    print_metric("Students (non-@vyoma)",             format_number(students))
    print_metric("Staff / admins (@vyoma)",           format_number(staff))
    print_metric("New students this year",            format_number(this_year))
    print_metric("New students this month",           format_number(this_month))

    print_subsection("Top 10 states by student count")
    print_breakdown_table(cursor,
        "SELECT COALESCE(NULLIF(TRIM(state),''), '(not set)'), COUNT(*) "
        "FROM silver.users WHERE " + STAFF_EMAIL_FILTER +
        " GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
        "State", "Students")

    print_subsection("Registrations by year")
    print_breakdown_table(cursor,
        "SELECT EXTRACT(YEAR FROM created_at_ist)::INT, COUNT(*) "
        "FROM silver.users WHERE " + STAFF_EMAIL_FILTER +
        " AND created_at_ist IS NOT NULL GROUP BY 1 ORDER BY 1",
        "Year", "New Students")


# ── SECTION 6: ATTENDANCE ─────────────────────────────────────

def section_attendance(cursor):
    print_section_header(6, "CLASS ATTENDANCE")

    print_subsection("Coverage")
    total_records = query_one_value(cursor, "SELECT COUNT(*) FROM silver.class_attendance")
    uniq_batches  = query_one_value(cursor,
        "SELECT COUNT(DISTINCT batch_id) FROM silver.class_attendance")
    uniq_bundles  = query_one_value(cursor,
        "SELECT COUNT(DISTINCT bundle_id) FROM silver.class_attendance")
    print_metric("Total class-day records",             format_number(total_records))
    print_metric("Unique batches with attendance data", format_number(uniq_batches))
    print_metric("Unique courses with attendance data", format_number(uniq_bundles))

    print_subsection("Overall figures")
    avg_pct     = query_one_value(cursor,
        "SELECT ROUND(AVG(attendance_pct)::NUMERIC, 2) FROM silver.class_attendance")
    tot_present = query_one_value(cursor, "SELECT SUM(present_count) FROM silver.class_attendance")
    tot_late    = query_one_value(cursor, "SELECT SUM(late_count)    FROM silver.class_attendance")
    tot_absent  = query_one_value(cursor, "SELECT SUM(absent_count)  FROM silver.class_attendance")
    print_metric("Average attendance % (all classes)",  format_percent(avg_pct))
    print_metric("Total student-class present count",   format_number(tot_present))
    print_metric("Total student-class late count",      format_number(tot_late))
    print_metric("Total student-class absent count",    format_number(tot_absent))

    print_subsection("Attendance by year")
    print_breakdown_table(cursor, """
        SELECT EXTRACT(YEAR FROM class_date)::INT,
               COUNT(*) AS classes,
               ROUND(AVG(attendance_pct)::NUMERIC, 1) AS avg_pct
        FROM   silver.class_attendance
        GROUP  BY 1 ORDER BY 1
    """, "Year", "Classes")


# ── SECTION 7: ASSESSMENTS & CERTIFICATIONS ──────────────────

def section_assessments(cursor):
    print_section_header(7, "ASSESSMENTS & CERTIFICATIONS")

    print_subsection("Assessment submissions")
    total     = query_one_value(cursor, "SELECT COUNT(*) FROM silver.assessments")
    evaluated = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.assessments WHERE is_evaluated = 1")
    pending   = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.assessments WHERE COALESCE(is_evaluated, 0) = 0")
    avg_mark  = query_one_value(cursor,
        "SELECT ROUND(AVG(mark)::NUMERIC, 2) FROM silver.assessments WHERE mark IS NOT NULL")
    print_metric("Total assessment events",   format_number(total))
    print_metric("Evaluated",                 format_number(evaluated))
    print_metric("Pending evaluation",        format_number(pending))
    print_metric("Average mark (evaluated)",  format_number(avg_mark, 2))

    print_subsection("Certifications")
    total_certs = query_one_value(cursor, "SELECT COUNT(*) FROM silver.certificates")
    this_year   = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.certificates "
        "WHERE issued_at_ist >= DATE_TRUNC('year', CURRENT_DATE)")
    this_month  = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.certificates "
        "WHERE issued_at_ist >= DATE_TRUNC('month', CURRENT_DATE)")
    print_metric("Total certificates issued",       format_number(total_certs))
    print_metric("Certificates issued this year",   format_number(this_year))
    print_metric("Certificates issued this month",  format_number(this_month))

    print_subsection("Lifecycle tracker (Silver) -- batch summary")
    lc_total      = query_one_value(cursor, "SELECT COUNT(*) FROM silver.course_lifecycle")
    lc_certified  = query_one_value(cursor,
        "SELECT SUM(total_certified) FROM silver.course_lifecycle WHERE total_certified IS NOT NULL")
    lc_avg_att    = query_one_value(cursor,
        "SELECT ROUND(AVG(avg_attendance)::NUMERIC, 1) FROM silver.course_lifecycle WHERE avg_attendance IS NOT NULL")
    lc_avg_pass   = query_one_value(cursor,
        "SELECT ROUND(AVG(pass_percentage)::NUMERIC, 1) FROM silver.course_lifecycle WHERE pass_percentage IS NOT NULL")
    lc_avg_rating = query_one_value(cursor,
        "SELECT ROUND(AVG(overall_rating)::NUMERIC, 2) FROM silver.course_lifecycle WHERE overall_rating IS NOT NULL")
    print_metric("Lifecycle batch records",                   format_number(lc_total))
    print_metric("Total students certified (lifecycle data)", format_number(lc_certified))
    print_metric("Average attendance % (lifecycle data)",      format_percent(lc_avg_att))
    print_metric("Average pass percentage",                    format_percent(lc_avg_pass))
    print_metric("Average overall course rating (out of 5)",   format_number(lc_avg_rating, 2))


# ── SECTION 8: REVENUE ────────────────────────────────────────

def section_revenue(cursor):
    print_section_header(8, "REVENUE & PRICING")

    print_subsection("Overall")
    total_rev = query_one_value(cursor,
        "SELECT SUM(final_price) FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " AND final_price IS NOT NULL")
    avg_price = query_one_value(cursor,
        "SELECT ROUND(AVG(final_price)::NUMERIC, 2) FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " AND final_price IS NOT NULL")
    paid_cnt  = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " AND final_price > 0")
    free_cnt  = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " AND COALESCE(final_price, 0) = 0")
    print_metric("Total revenue (all time, excl. staff)",  format_number(total_rev, 2))
    print_metric("Average final price per enrollment",     format_number(avg_price, 2))
    print_metric("Paid enrollments",                       format_number(paid_cnt))
    print_metric("Free / zero-price enrollments",          format_number(free_cnt))

    print_subsection("Revenue by year")
    print_breakdown_table(cursor,
        "SELECT EXTRACT(YEAR FROM created_at_ist)::INT, COUNT(*), "
        "ROUND(SUM(final_price)::NUMERIC, 2) "
        "FROM silver.transactions "
        "WHERE " + STAFF_EMAIL_FILTER + " AND final_price > 0 AND created_at_ist IS NOT NULL "
        "GROUP BY 1 ORDER BY 1",
        "Year", "Enrollments")

    print_subsection("Currencies in use")
    print_breakdown_table(cursor,
        "SELECT COALESCE(currency, '(not set)'), COUNT(*) "
        "FROM silver.transactions WHERE " + STAFF_EMAIL_FILTER +
        " GROUP BY 1 ORDER BY 2 DESC",
        "Currency", "Transactions")


# ── SECTION 9: LIVE SESSIONS ──────────────────────────────────

def section_sessions(cursor):
    print_section_header(9, "LIVE SESSIONS")

    print_subsection("Event counts")
    total      = query_one_value(cursor, "SELECT COUNT(*) FROM silver.sessions")
    uniq       = query_one_value(cursor,
        "SELECT COUNT(DISTINCT attendance_id) FROM silver.sessions")
    with_start = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.sessions WHERE actual_start_ist IS NOT NULL")
    print_metric("Total session events received",            format_number(total))
    print_metric("Unique session instances (attendance_id)", format_number(uniq))
    print_metric("Sessions with actual start recorded",      format_number(with_start))

    print_subsection("Cancellations and delays")
    cancelled  = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.sessions "
        "WHERE cancellation_reason IS NOT NULL AND cancellation_reason <> ''")
    late_start = query_one_value(cursor,
        "SELECT COUNT(*) FROM silver.sessions WHERE delay_minutes IS NOT NULL AND delay_minutes > 0")
    avg_delay  = query_one_value(cursor,
        "SELECT ROUND(AVG(delay_minutes)::NUMERIC, 1) FROM silver.sessions "
        "WHERE delay_minutes IS NOT NULL AND delay_minutes > 0")
    print_metric("Sessions cancelled",                     format_number(cancelled))
    print_metric("Sessions that started late (delay > 0)", format_number(late_start))
    print_metric("Average delay when late (minutes)",      format_number(avg_delay, 1))

    print_subsection("By virtual platform")
    print_breakdown_table(cursor,
        "SELECT COALESCE(virtual_platform, '(not set)'), COUNT(*) "
        "FROM silver.sessions GROUP BY 1 ORDER BY 2 DESC",
        "Platform", "Sessions")


# ── SECTION 10: TOP LISTS ─────────────────────────────────────

def section_top_lists(cursor):
    print_section_header(10, "TOP LISTS")

    print_subsection("Top 15 courses by total enrollments")
    print_top_list(cursor, """
        SELECT COALESCE(cm.course_name, t.course_name, '(unknown)'),
               COUNT(DISTINCT t.user_id) AS students,
               COUNT(*) AS enrollment_records
        FROM   silver.transactions t
        LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
        WHERE  t.email NOT LIKE '%%@vyoma%%'
        GROUP  BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Students", "Records"])

    print_subsection("Top 15 courses by revenue")
    print_top_list(cursor, """
        SELECT COALESCE(cm.course_name, t.course_name, '(unknown)'),
               COUNT(DISTINCT t.user_id) AS students,
               ROUND(SUM(t.final_price)::NUMERIC, 2) AS total_revenue
        FROM   silver.transactions t
        LEFT JOIN silver.course_metadata cm ON t.bundle_id = cm.bundle_id
        WHERE  t.email NOT LIKE '%%@vyoma%%' AND t.final_price > 0
        GROUP  BY 1 ORDER BY 3 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Students", "Revenue"])

    print_subsection("Top 15 batches by average attendance %")
    print_top_list(cursor, """
        SELECT COALESCE(cm.course_name, '(unknown)'),
               cb.batch_name,
               ROUND(AVG(ca.attendance_pct)::NUMERIC, 1) AS avg_pct,
               COUNT(*) AS classes
        FROM   silver.class_attendance ca
        JOIN   silver.course_batches   cb ON ca.batch_id  = cb.batch_id
        LEFT JOIN silver.course_metadata cm ON ca.bundle_id = cm.bundle_id
        GROUP  BY cm.course_name, cb.batch_name
        ORDER  BY 3 DESC NULLS LAST LIMIT 15
    """, ["Course Name", "Batch", "Avg Att%", "Classes"])

    print_subsection("Top 10 students by course count")
    print_top_list(cursor, """
        SELECT COALESCE(u.full_name, t.email, '(unknown)'),
               COUNT(DISTINCT t.bundle_id) AS courses,
               COUNT(*) AS enrollments
        FROM   silver.transactions t
        LEFT JOIN silver.users u ON t.user_id = u.user_id
        WHERE  t.email NOT LIKE '%%@vyoma%%'
        GROUP  BY u.full_name, t.email
        ORDER  BY 2 DESC NULLS LAST LIMIT 10
    """, ["Student", "Courses", "Enrollments"])

    print_subsection("Top 10 courses by overall rating (lifecycle data)")
    print_top_list(cursor, """
        SELECT COALESCE(cm.course_name, cl.course_name, '(unknown)'),
               COUNT(*) AS batches_tracked,
               ROUND(AVG(cl.overall_rating)::NUMERIC, 2) AS avg_rating,
               ROUND(AVG(cl.pass_percentage)::NUMERIC, 1) AS avg_pass_pct
        FROM   silver.course_lifecycle cl
        LEFT JOIN silver.course_metadata cm ON cl.course_id = cm.bundle_id
        WHERE  cl.overall_rating IS NOT NULL
        GROUP  BY cm.course_name, cl.course_name
        ORDER  BY 3 DESC NULLS LAST LIMIT 10
    """, ["Course Name", "Batches", "Avg Rating", "Avg Pass%"])


# ── MAIN ─────────────────────────────────────────────────────

def main():
    # Print the report title with the current date and time
    now_string = datetime.datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    print("")
    print("=" * 72)
    print("  VYOMA SAMSKRTA PATHASALA -- DATA PIPELINE ANALYSIS")
    print("  Generated: " + now_string)
    print("=" * 72)

    # Connect to the database
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
        )
    except Exception as error:
        print("ERROR: Could not connect to database: " + str(error))
        sys.exit(1)

    cursor = conn.cursor()

    # Run all 10 sections in order
    section_data_health(cursor)
    section_courses(cursor)
    section_batches(cursor)
    section_enrollments(cursor)
    section_students(cursor)
    section_attendance(cursor)
    section_assessments(cursor)
    section_revenue(cursor)
    section_sessions(cursor)
    section_top_lists(cursor)

    cursor.close()
    conn.close()

    print("")
    print("=" * 72)
    print("  Analysis complete.")
    print("=" * 72)
    print("")


if __name__ == "__main__":
    main()
