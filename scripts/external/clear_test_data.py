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

# WHERE conditions per table for identifying test/load data
CLEAR_TARGETS = [
    ("silver.users",
     "email LIKE '%%test%%' OR email LIKE '%%load%%' OR user_id >= 99990000"),
    ("silver.transactions",
     "email LIKE '%%test%%' OR email LIKE '%%load%%' OR user_id >= 99990000"),
    ("silver.assessments",
     "user_id >= 99990000"),
    ("silver.courses",
     "user_id >= 99990000"),
    ("silver.certificates",
     "user_id >= 99990000"),
    # Bronze: match by event_id prefix patterns used in tests
    ("bronze.webhook_events",
     "event_id ~ '^(e2e|dupe|resilience|conc|dbtest|constraint|manual|load)-'"),
]


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    cur = conn.cursor()

    print("\nTest data to delete:")
    total = 0
    for tbl, where in CLEAR_TARGETS:
        cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE {where}")
        n = cur.fetchone()[0]
        print(f"  {tbl:<45} {n:>8,}")
        total += n
    print(f"  {'TOTAL':<45} {total:>8,}")

    if total == 0:
        print("\nNothing to delete.")
        cur.close()
        conn.close()
        return

    confirm = input(f"\nDelete {total:,} rows? [y/N]: ").strip().lower()
    if confirm != 'y':
        print("Aborted.")
        cur.close()
        conn.close()
        return

    for tbl, where in CLEAR_TARGETS:
        cur.execute(f"DELETE FROM {tbl} WHERE {where}")
        print(f"  Deleted {cur.rowcount:,} from {tbl}")

    conn.commit()
    print("\nDone.")
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
