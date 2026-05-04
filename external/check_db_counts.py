import psycopg2

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

TABLES = [
    'bronze.webhook_events',
    'bronze.failed_events',
    'bronze.studentexport_raw',
    'bronze.student_courses_enrolled_raw',
    'bronze.unresolved_students_raw',
    'silver.users',
    'silver.transactions',
    'silver.sessions',
    'silver.assessments',
    'silver.courses',
    'silver.announcements',
    'silver.certificates',
]


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    cur = conn.cursor()
    print(f"\n{'Table':<45} {'Row Count':>10}")
    print(f"{'-'*45} {'-'*10}")
    for tbl in TABLES:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        n = cur.fetchone()[0]
        print(f"{tbl:<45} {n:>10,}")
    print()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
