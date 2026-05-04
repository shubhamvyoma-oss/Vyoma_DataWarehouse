# 01 — Fetch Attendance

## What it does

Downloads daily class attendance records from the Edmingle API and saves them to the database in two layers:

1. **Bronze** (`bronze.attendance_raw`) — exact copy of what the API returned, one row per student per class
2. **Silver** (`silver.class_attendance`) — cleaned and aggregated, one row per class per batch

## Why we need it

Power BI reads `silver.class_attendance` to show attendance trends and drop-off charts. Without this script running daily, those charts go stale.

## How it works — step by step

```
Edmingle API  ──────────────────────────────────────────────────────
  │
  │  GET /attendance (report_type=55, one date at a time)
  │
  ▼
bronze.attendance_raw   (raw rows — every student, every class)
  │
  │  Aggregate: count present / late / absent per class
  │  Renumber classes 1, 2, 3... within each batch
  │
  ▼
silver.class_attendance (one row per class per batch)
```

### Steps in code

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `parse_args()` | Read --date, --start, --end, --dry-run from command line |
| 2 | `decide_dates_to_pull()` | Pick which dates to fetch (default = yesterday) |
| 3 | `fetch_attendance_for_date()` | Call the API for one date, retry 3 times on failure |
| 4 | `save_to_bronze()` | Write each raw row to `bronze.attendance_raw` |
| 5 | `aggregate_to_silver()` | Count present/late/absent per class, write to Silver |
| 6 | `print_final_summary()` | Print a summary table of what was done |

## How to run

```bash
# Fetch yesterday (default — use this in the daily schedule)
python 01_fetch_attendance/fetch_attendance.py

# Fetch a specific date
python 01_fetch_attendance/fetch_attendance.py --date 2024-03-15

# Fetch a date range
python 01_fetch_attendance/fetch_attendance.py --start 2024-03-01 --end 2024-03-31

# See what would be fetched without writing to DB
python 01_fetch_attendance/fetch_attendance.py --dry-run
```

## What to check after

- `bronze.attendance_raw` should have new rows
- `silver.class_attendance` should have new rows
- Run `python 12_check_db_counts/check_db_counts.py` to see row counts

## Schedule

Run daily at **7:00 AM IST** to capture the previous day's classes.

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL is not running | Start PostgreSQL |
| `API error: HTTP 401` | API key is wrong or expired | Check `API_KEY` in the script |
| `API error: HTTP 500` | Edmingle server error | Wait and retry; check Edmingle status |
| `WARNING: API returned 0 rows` | No classes on that date | Normal for holidays |
| `Connection error (attempt 1/3)` | Network issue | Script retries automatically |
