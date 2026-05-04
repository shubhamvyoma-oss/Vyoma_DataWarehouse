# ONE-TIME MIGRATION — Course metadata Bronze load
# Loads course_catalogue, MIS_tracker, batches CSVs into Bronze
# Run once before csv_transform_course_silver.py

import os
import re
import pandas as pd
import psycopg2
import psycopg2.extras

# ── CONFIG ──────────────────────────────────────
DB_HOST      = "localhost"
DB_NAME      = "edmingle_analytics"
DB_USER      = "postgres"
DB_PASSWORD  = "Svyoma"
DB_PORT      = 5432
# ─────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_DIR  = os.path.join(BASE_DIR, 'CSV files')

SOURCES = [
    {
        'label':    'catalogue',
        'table':    'bronze.course_catalogue_raw',
        'filename': 'course_catalogue_data.csv',
    },
    {
        'label':    'lifecycle',
        'table':    'bronze.course_lifecycle_raw',
        'filename': 'Elearning MIS Merged Tracker - Course Lifecycle (1).csv',
    },
    {
        'label':    'batches',
        'table':    'bronze.course_batches_raw',
        'filename': 'batches_data.csv',
    },
]


def sanitize_col(col):
    c = col.lower().strip()
    c = re.sub(r'[^a-z0-9]+', '_', c)
    c = c.strip('_')
    c = re.sub(r'_+', '_', c)
    return c


def load_csv(conn, table, csv_path):
    df = pd.read_csv(csv_path, encoding='utf-8', encoding_errors='replace', dtype=str)
    df = df.where(pd.notna(df), None)

    seen = {}
    new_cols = []
    for col in df.columns:
        s = sanitize_col(col)
        if s in seen:
            seen[s] += 1
            s = f'{s}_{seen[s]}'
        else:
            seen[s] = 0
        new_cols.append(s)
    df.columns = new_cols

    col_list   = ', '.join(new_cols)
    ph         = ', '.join(['%s'] * len(new_cols))
    sql        = f"""
        INSERT INTO {table} (source_row, {col_list})
        VALUES (%s, {ph})
        ON CONFLICT (source_row) DO NOTHING
    """

    cur = conn.cursor()
    inserted = 0
    for idx, row in df.iterrows():
        values = [int(idx)] + [row[c] for c in new_cols]
        cur.execute(sql, values)
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    cur.close()
    return inserted, len(df)


def count_rows(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table}')
    n = cur.fetchone()[0]
    cur.close()
    return n


def main():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )

    results = {}
    for src in SOURCES:
        path = os.path.join(CSV_DIR, src['filename'])
        print(f"Loading {src['table']} from {src['filename']} ...", flush=True)
        inserted, total = load_csv(conn, src['table'], path)
        results[src['table']] = count_rows(conn, src['table'])
        print(f"  {inserted}/{total} new rows inserted", flush=True)

    conn.close()

    print()
    for tbl, n in results.items():
        print(f"  {tbl:<45} : {n:,} rows")


if __name__ == '__main__':
    main()
