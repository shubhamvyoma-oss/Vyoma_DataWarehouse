# Vyoma Samskrta Pathasala — Data Warehouse

This repository contains the complete data pipeline for Vyoma Samskrta Pathasala's e-learning platform. It collects student, course, attendance, and enrollment data from the Edmingle LMS and stores it in a PostgreSQL database that Power BI reads for reporting.

---

## How the data flows

```
Edmingle LMS (online platform)
  │
  ├── Live events (real-time)  ─────────────────────────────────────────
  │     Student registers, buys course, attends class, gets certificate
  │     │
  │     │  HTTP POST  (webhooks, sent instantly)
  │     │
  │     ▼
  │   [06] webhook_receiver.py   ← always running server
  │
  ├── Batch API (scheduled pull) ──────────────────────────────────────
  │     Course catalogue, batch details
  │     │
  │     ├── [02] fetch_course_catalogue.py   (weekly)
  │     └── [03] fetch_course_batches.py     (daily)
  │
  └── Attendance API (scheduled pull) ─────────────────────────────────
        Class attendance records
        │
        └── [01] fetch_attendance.py   (daily)

        ─────────────────────────────────────────────────────────────
                        PostgreSQL Database
        ─────────────────────────────────────────────────────────────

        BRONZE LAYER  (raw data — exact copy of source)
          bronze.webhook_events
          bronze.attendance_raw
          bronze.course_catalogue_raw
          bronze.course_batches_raw
          bronze.course_lifecycle_raw
          bronze.studentexport_raw
          bronze.student_courses_enrolled_raw

        SILVER LAYER  (cleaned, typed, deduplicated)
          silver.users
          silver.transactions
          silver.sessions
          silver.assessments
          silver.certificates
          silver.courses
          silver.announcements
          silver.course_metadata
          silver.course_batches
          silver.course_lifecycle
          silver.class_attendance

        REPORTING TABLES  (wide tables for Power BI)
          silver."MasterCourseBatch"
          silver.course_meta_data

        ─────────────────────────────────────────────────────────────
                        Power BI   (reads Silver)
        ─────────────────────────────────────────────────────────────
```

---

## Folder structure

Each script has its own numbered folder. The number shows the recommended run order.

### Daily scripts (run every morning at 7:00 AM IST)

| Folder | Script | What it does |
|--------|--------|-------------|
| `01_fetch_attendance/` | `fetch_attendance.py` | Downloads yesterday's class attendance from the API |
| `04_run_course_pipeline/` | `run_course_pipeline.py` | Runs scripts 02 and 03 together (catalogue + batches) |

### Weekly scripts (run when courses change)

| Folder | Script | What it does |
|--------|--------|-------------|
| `02_fetch_course_catalogue/` | `fetch_course_catalogue.py` | Downloads the list of all courses from the API |
| `03_fetch_course_batches/` | `fetch_course_batches.py` | Downloads all course batch details from the API |

### Always-running server

| Folder | Script | What it does |
|--------|--------|-------------|
| `06_webhook_receiver/` | `webhook_receiver.py` | Flask server that receives live events from Edmingle |

### On-demand / maintenance scripts

| Folder | Script | What it does |
|--------|--------|-------------|
| `05_build_courses/` | `build_courses.py` | Builds the two wide reporting tables for Power BI |
| `07_reprocess_bronze/` | `reprocess_bronze.py` | Re-routes all Bronze events to Silver (recovery tool) |
| `15_run_analysis/` | `run_analysis.py` | Prints a 10-section business metrics report |

### One-time migration scripts (run once to load historical data)

Run in this order:

| Step | Folder | Script | What it does |
|------|--------|--------|-------------|
| 1 | `08_load_students_csv/` | `load_students_csv.py` | Loads student export CSVs into Bronze |
| 2 | `09_load_courses_csv/` | `load_courses_csv.py` | Loads course catalogue/lifecycle/batches CSVs into Bronze |
| 3 | `10_transform_courses_silver/` | `transform_courses_silver.py` | Transforms Bronze course data to Silver |
| 4 | `11_backfill_transactions/` | `backfill_transactions.py` | Creates Silver users and transactions from CSV Bronze |

### Utility scripts

| Folder | Script | What it does |
|--------|--------|-------------|
| `12_check_db_counts/` | `check_db_counts.py` | Prints row counts for all Bronze and Silver tables |
| `13_clear_test_data/` | `clear_test_data.py` | Deletes test rows from all tables (with confirmation prompt) |
| `14_test_webhook_send/` | `test_webhook_send.py` | Sends a single test event to the webhook server |

### Test scripts

| Folder | Script | What it does |
|--------|--------|-------------|
| `16_test_all_events/` | `test_all_events.py` | Sends one test event per event type, checks Silver tables |
| `17_test_db_unavailability/` | `test_db_unavailability.py` | Tests zero data loss during a DB outage |
| `18_test_pipeline_e2e/` | `test_pipeline_e2e.py` | 9-test comprehensive end-to-end suite |

---

## Database configuration

All scripts connect to PostgreSQL using these settings (hardcoded in each script):

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5432` |
| Database | `edmingle_analytics` |
| User | `postgres` |
| Password | `Svyoma` |

---

## How to set up from scratch

### Prerequisites

- Python 3.9+
- PostgreSQL 14+
- Required packages: `pip install psycopg2-binary requests flask pandas`

### Steps

1. Create the database and run all migration SQL files to create the tables
2. Run the one-time CSV migrations (scripts 08, 09, 10, 11) to load historical data
3. Start the webhook server: `python 06_webhook_receiver/webhook_receiver.py`
4. Set up Edmingle to send webhooks to `http://your-server:5000/webhook`
5. Schedule the daily scripts (01 and 04) to run at 7:00 AM IST

---

## Database layers explained

### Bronze layer

- Holds data **exactly as received** from the API or webhooks — no changes
- Safe to re-load: all Bronze tables use `ON CONFLICT DO NOTHING` or `DO UPDATE`
- If a Silver transform breaks, you can always re-run it from Bronze

### Silver layer

- Holds **cleaned, typed, and deduplicated** data
- This is what Power BI reads
- Uses `ON CONFLICT ... DO UPDATE SET` (UPSERT) so re-running is always safe

### Reporting tables

- `silver."MasterCourseBatch"` and `silver.course_meta_data` are wide tables built by script 05
- They join course metadata + batch details + lifecycle stats into one place
- Power BI only needs to query these two tables for the main course dashboard

---

## Troubleshooting

**Bronze table is empty:**
Run the relevant fetch script (01, 02, or 03) and check for API errors.

**Silver table is empty but Bronze has data:**
Run script 07 (`reprocess_bronze.py`) to re-route all Bronze events to Silver.

**Webhook events not arriving:**
Check that the Edmingle webhook URL points to your server and that the server is running.

**Power BI shows stale data:**
Run the daily pipeline scripts (01 and 04) and then refresh Power BI.

**Events in `bronze.failed_events`:**
Call `POST http://localhost:5000/retry-failed` to re-process them, or check the failure reason and fix the underlying issue.

---

## Architecture Overview

The pipeline follows the **medallion architecture** (Bronze → Silver → Gold):

```
Edmingle LMS
      │
      ├── Webhooks (real-time)           ingestion/webhook_receiver.py
      │        └── bronze.webhook_events
      │             └── silver.users / transactions / sessions / assessments / certificates
      │
      ├── API Scripts (scheduled)        api_scripts/
      │        ├── fetch_course_catalogue.py  → bronze.course_catalogue_raw
      │        ├── fetch_course_batches.py    → bronze.course_batches_raw
      │        ├── fetch_attendance.py        → bronze.attendance_raw
      │        └── run_course_pipeline.py     (orchestrates catalogue + batches)
      │             └── silver.course_metadata / course_batches / course_master / class_attendance
      │
      └── CSV Imports (one-time historical)  scripts/migrations/
               ├── studentexport.csv          → bronze.studentexport_raw
               ├── studentCoursesEnrolled.csv → bronze.student_courses_enrolled_raw
               └── Course Lifecycle CSV       → bronze.course_lifecycle_raw
                    └── silver.users / transactions / course_lifecycle

Silver tables ──→ gold.* views ──→ Power BI dashboards
```

**Bronze** stores raw data exactly as received — never transformed, never deleted.
**Silver** stores cleaned, typed, deduplicated records — one row per real-world entity.
**Gold** contains SQL views that join Silver tables into shapes ready for Power BI.

---

## Database: Current Row Counts

| Table | Rows | Description |
|---|---|---|
| `silver.users` | ~93,000 | All registered learners |
| `silver.transactions` | ~424,000 | All course enrollments |
| `bronze.attendance_raw` | growing | Attendance backfill in progress (2024–2026) |
| `silver.class_attendance` | growing | Per-batch per-date attendance summaries |
| `silver.course_master` | 995 | All batches with course metadata |

---

## Folder Structure

```
api_scripts/          API pull scripts (attendance, catalogue, batches)
CSV files/            Historical CSV exports used for one-time backfill
database/
  schemas/            CREATE SCHEMA statements
  bronze/
    webhook/          Tables for webhook event data
    api/              Tables for API-pulled data
    manual/           Tables for CSV / manual imports
  silver/
    webhook/          Cleaned tables sourced from webhook events
    api/              Cleaned tables sourced from API scripts
    manual/           Cleaned tables sourced from CSV imports
  gold/
    webhook/          Gold views over course/enrollment data
    api/              Gold views over attendance data
  run_all.sql         Runs all DDL files in dependency order
docs/                 Architecture, data dictionary, runbook, decisions
ingestion/            Live webhook receiver (Flask server)
logs/                 Runtime logs (webhook receiver, API backfills)
PDFs/                 Edmingle API documentation
scripts/
  migrations/         One-time CSV migration scripts
  external/           Utility scripts (check DB counts, clear test data)
tests/                End-to-end and unavailability test suites
```

---

## Quick Start

### 1. Install requirements

```bash
pip install flask psycopg2-binary requests pandas openpyxl pdfplumber
```

### 2. Configure credentials

Each script has a `CONFIG` block at the top. Update `DB_PASSWORD`, `API_KEY`, and related values before running.

### 3. Create the database

```sql
-- In psql:
CREATE DATABASE edmingle_analytics;
```

### 4. Run all DDL (creates all tables and views)

```bash
psql -U postgres -d edmingle_analytics -f database/run_all.sql
```

### 5. Start the live webhook receiver

```bash
python ingestion/webhook_receiver.py
```

### 6. Run the daily course pipeline (catalogue + batches)

```bash
python api_scripts/run_course_pipeline.py
```

### 7. Run the attendance backfill (first time only)

```bash
python api_scripts/fetch_attendance.py --start 2024-01-01 --end 2026-04-30
```

### 8. Run attendance daily (ongoing)

```bash
python api_scripts/fetch_attendance.py
```
*(defaults to yesterday)*

---

## Data Sources

| Source | Type | What it provides |
|---|---|---|
| Edmingle Webhooks | Real-time push | User registrations, enrollments, sessions, assessments, certificates |
| `report_type=55` API | Daily scheduled pull | Per-student attendance for every class date |
| `/short/masterbatch` API | Daily scheduled pull | All batch metadata (name, dates, tutor, enrolled count) |
| `/institute/{id}/courses/catalogue` API | Daily scheduled pull | All course bundles with subject, category, funnel position |
| `studentexport.csv` | One-time historical | 93,000+ student profiles |
| `studentCoursesEnrolled.csv` | One-time historical | 478,000+ historical enrollments |
| Course Lifecycle CSV | One-time historical | Course metrics (first/last class dates, ratings, completion counts) |

---

## Tests

```bash
python tests/test_pipeline_e2e.py   # 38/38 PASS
python tests/test_db_unavailability.py
```

---

## Documentation

| File | Contents |
|---|---|
| `docs/architecture.md` | Full data flow diagram and layer descriptions |
| `docs/data_dictionary.md` | Every column in every table explained |
| `docs/runbook.md` | How to set up, run, recover, and deploy |
| `docs/decisions.md` | Why each design decision was made |
| `docs/api_endpoints.md` | All Edmingle API endpoints and which ones we use |
