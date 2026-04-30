# MASTER RUNNER — Course Data Pipeline
# Run this daily: python api_scripts/run_course_pipeline.py
# Fetches catalogue + batches -> rebuilds master table
# Power BI reads from silver.course_master after this runs

import sys
import time

# ── CONFIG ──────────────────────────────────────
DB_HOST      = "localhost"
DB_NAME      = "edmingle_analytics"
DB_USER      = "postgres"
DB_PASSWORD  = "Svyoma"
DB_PORT      = 5432
API_KEY      = "590605228a847624e065a76e986803fa"
ORG_ID       = "683"
INST_ID      = "483"
# ─────────────────────────────────────────────────

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import api_scripts.fetch_course_catalogue as catalogue_script
import api_scripts.fetch_course_batches   as batches_script


def count_table(table):
    import psycopg2
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    return n


def main():
    t_start = time.time()

    print("=" * 45)
    print("COURSE DATA PIPELINE")
    print("=" * 45)
    print()

    # Step 1 — Catalogue
    cat_ok     = False
    cat_count  = 0
    cat_silver = 0
    try:
        print("--- Step 1: Course Catalogue ---")
        cat_ok, cat_count, cat_silver = catalogue_script.main()
        print()
    except SystemExit:
        print("  Catalogue script exited with error.")
    except Exception as e:
        print(f"  Catalogue script failed: {e}")

    # Step 2 — Batches + Master
    bat_ok      = False
    bat_count   = 0
    bat_silver  = 0
    master_rows = 0
    try:
        print("--- Step 2: Course Batches + Master ---")
        bat_ok, bat_count, bat_silver, master_rows = batches_script.main()
        print()
    except SystemExit:
        print("  Batches script exited with error.")
    except Exception as e:
        print(f"  Batches script failed: {e}")

    elapsed = time.time() - t_start

    print()
    print("  COURSE PIPELINE COMPLETE")
    print("  " + "-" * 41)
    print(f"  Catalogue API     : {'SUCCESS' if cat_ok  else 'FAILED'}")
    print(f"  Batches API       : {'SUCCESS' if bat_ok  else 'FAILED'}")
    print(f"  Silver rows       : {cat_silver} course_metadata, {bat_silver} course_batches")
    print(f"  Master table      : {master_rows} rows rebuilt")
    print(f"  Power BI ready    : {'YES' if cat_ok and bat_ok else 'NO -- check errors above'}")
    print(f"  Run time          : {elapsed:.1f}s")
    print("  " + "-" * 41)
    print("  Next run: schedule daily at 7:00 AM IST")
    print()


if __name__ == '__main__':
    main()
