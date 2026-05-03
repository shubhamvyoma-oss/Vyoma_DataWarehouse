# Vyoma E-Learning Data Warehouse

Automated data pipeline for **Vyoma Samskrta Pathasala** ([sanskritfromhome.org](https://www.sanskritfromhome.org)), built on top of the Edmingle LMS platform. The warehouse collects all student, enrollment, attendance, and course data into a structured PostgreSQL database and makes it available for Power BI dashboards.

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
