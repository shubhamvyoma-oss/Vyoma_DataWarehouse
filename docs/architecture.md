# System Architecture

## What This System Does and Why It Exists

Vyoma Samskrta Pathasala (sanskritfromhome.org) runs its online courses through a platform called Edmingle. Edmingle tracks students, enrollments, live sessions, assessments, and certificates — but it does not provide a way to run custom reports or build dashboards. This pipeline solves that problem: it collects all Edmingle data automatically, stores it in a structured PostgreSQL database, and makes it available for Power BI dashboards. The goal is to give Vyoma a clear picture of how many students are enrolled, how many attend sessions, how many complete courses, and where students drop off.

---

## Data Flow

```
Edmingle LMS
     │
     ├── Webhooks (real-time) ──────────→ ingestion/webhook_receiver.py
     │                                              │
     │                                    bronze.webhook_events
     │                                              │
     │                                    silver.users
     │                                    silver.transactions
     │                                    silver.sessions
     │                                    silver.assessments
     │                                    silver.courses
     │                                    silver.announcements
     │                                    silver.certificates
     │
     ├── API Scripts (scheduled, not yet built) ──→ api_scripts/
     │                                              │
     │                                    bronze.daily_attendance_raw (planned)
     │                                              │
     │                                    silver.daily_attendance (planned)
     │
     └── CSV (one-time historical backfill) ──→ scripts/migrations/
                                                        │
                                              bronze.studentexport_raw
                                              bronze.student_courses_enrolled_raw
                                                        │
                                              silver.users
                                              silver.transactions

silver.* tables ──→ gold.* views ──→ Power BI dashboards
```

---

## Bronze Layer

Bronze is the raw data store. Every event or CSV row is saved here exactly as it arrived, with no transformation. Bronze data is never deleted.

**Why keep raw data?** If the Silver transformation logic has a bug, you can re-run it against Bronze without needing to re-fetch from Edmingle. Bronze is also the audit trail — you can always prove what came in and when.

| Table | Contents |
|-------|----------|
| `bronze.webhook_events` | Every webhook event received from Edmingle, stored as raw JSON |
| `bronze.failed_events` | Requests that arrived but could not be parsed or stored (malformed JSON, DB down, etc.) |
| `bronze.studentexport_raw` | Raw copy of the studentexport.csv historical file |
| `bronze.student_courses_enrolled_raw` | Raw copy of the studentCoursesEnrolled.csv historical file |
| `bronze.unresolved_students_raw` | Students from CSV whose email could not be matched to an Edmingle user_id |

---

## Silver Layer

Silver is the cleaned, typed, and deduplicated store. Each row in a Silver table represents one real-world entity (one student, one enrollment, one session). Silver tables are rebuilt from Bronze using UPSERT logic — running the same event twice produces one row, not two.

**What cleaning happens in Silver:**
- Unix timestamps are converted to IST (Asia/Kolkata) datetimes
- Duplicate events for the same entity are merged (COALESCE keeps the most complete version)
- Null values from partial events are filled in when a later event provides them
- Typed columns replace the raw JSONB: BIGINT for IDs, NUMERIC for prices, BOOLEAN for flags

| Table | Contents |
|-------|----------|
| `silver.users` | One row per student; upsert key is `user_id` |
| `silver.transactions` | One row per enrollment (student + course + batch); upsert key is `(user_id, bundle_id, master_batch_id)` |
| `silver.sessions` | One row per live class instance; upsert key is `attendance_id` |
| `silver.assessments` | One row per assessment submission or evaluation; upsert key is `event_id` |
| `silver.courses` | One row per course completion; upsert key is `event_id` |
| `silver.announcements` | One row per announcement; raw JSONB stored (structure not yet documented) |
| `silver.certificates` | One row per certificate issued; upsert key is `event_id` |

---

## Gold Layer

Gold is the reporting layer. It contains SQL VIEWs that join and aggregate Silver tables into shapes that Power BI can consume directly. Gold views contain no new data — everything comes from Silver.

**Planned Gold views (not yet built):**
- `gold.student_summary` — one row per student with enrollment count, attendance rate, completion status
- `gold.monthly_enrollments` — enrollment counts by month and course
- `gold.session_attendance` — attendance rates by batch and date range
- `gold.certificate_pipeline` — how many students are at each stage toward certification

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Webhook receiver | Python 3, Flask |
| Database | PostgreSQL 18 (local dev), PostgreSQL on VPS (production) |
| DB connection pooling | psycopg2 ThreadedConnectionPool (min 2, max 20) |
| Dashboards | Power BI (live connection to gold.* views) |
| Hosting (planned) | VPS managed by Shankar, with systemd keeping the receiver running |

---

## Who Uses What

| Person | Role |
|--------|------|
| **Shankar** | Manages the VPS server and PostgreSQL instance in production. Handles server access and uptime. |
| **Aishwarya** | Uses Power BI connected to the Gold views to build and view dashboards. |
| **Shubham** | Built and maintains the entire pipeline — webhook receiver, migrations, tests, schema. |
| **Shashank** | Reviews code on GitHub before changes go to production. |
