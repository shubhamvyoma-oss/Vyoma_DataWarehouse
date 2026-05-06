# System Architecture

## What This System Does and Why It Exists

Vyoma Samskrta Pathasala (sanskritfromhome.org) runs its online courses through a platform called Edmingle. Edmingle tracks students, enrollments, live sessions, assessments, and certificates — but it does not provide a way to run custom reports or build dashboards. This pipeline solves that problem: it collects all Edmingle data automatically, stores it in a structured PostgreSQL database, and makes it available for Power BI dashboards. The goal is to give Vyoma a clear picture of how many students are enrolled, how many attend sessions, how many complete courses, and where students drop off.

---

## Data Flow

```
Edmingle LMS
     │
     ├── Webhooks (real-time) ──────────→ Webhook_scripts/
     │                                              │
     |                                     webhook_receiver.py     
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
     ├── API Scripts (scheduled) ──────────────→ api_scripts/
     │                                              │
     │                                    bronze.attendance_raw
     │                                    bronze.course_catalogue_raw
     │                                    bronze.course_batches_raw
     │                                              │
     │                                    silver.attendance_data
     │                                    silver.course_catalogue
     │                                    silver.batches_Data
     │                                    silver.course_batch_merge_data (course_catalogue + batches_Data)
     |                                    silver.course_metadata (course_batch_merge_data + course_lifecycle)
     │
     └── CSV (one-time historical backfill) ──→ scripts/migrations/
                                                        │
                                              bronze.studentexport_raw
                                              bronze.student_courses_enrolled_raw
                                              bronze.course_lifecycle_raw
                                                        │
                                              silver.users
                                              silver.transactions

bronze.*table ──→  silver.* tables ──→ gold.* views ──→ Power BI dashboards
```

---

## Bronze Layer

Bronze is the raw data store. Every event or CSV row is saved here exactly as it arrived, with no transformation. Bronze data is never deleted.

**Why keep raw data?** If the Silver transformation logic has a bug, you can re-run it against Bronze without needing to re-fetch from Edmingle. Bronze is also the audit trail — you can always prove what came in and when.

| Table | Source | Contents |
|-------|--------|----------|
| `bronze.webhook_events` | Webhook | Every webhook event received from Edmingle, stored as raw JSON |
| `bronze.failed_events` | Webhook | Requests that arrived but could not be parsed or stored |
| `bronze.studentexport_raw` | CSV import | Raw copy of the studentexport.csv historical file |
| `bronze.student_courses_enrolled_raw` | CSV import | Raw copy of the studentCoursesEnrolled.csv historical file |
| `bronze.unresolved_students_raw` | CSV import | Students from CSV whose email could not be matched to an Edmingle user_id |
| `bronze.course_lifecycle_raw` | CSV import | Course operations MIS tracker (107 columns per batch) |
| `bronze.course_catalogue_raw` | API | All course bundles with classification fields |
| `bronze.course_batches_raw` | API | All batch records (nested under bundles, flattened) |
| `bronze.attendance_raw` | API | One row per student per class session (attendance status P/L/A/-) |

---

## Silver Layer

Silver is the cleaned, typed, and deduplicated store. Each row in a Silver table represents one real-world entity (one student, one enrollment, one session). Silver tables are rebuilt from Bronze using UPSERT logic — running the same event twice produces one row, not two.

**What cleaning happens in Silver:**
- Unix timestamps are converted to IST (Asia/Kolkata) datetimes
- Duplicate events for the same entity are merged (COALESCE keeps the most complete version)
- Null values from partial events are filled in when a later event provides them
- Typed columns replace the raw JSONB: BIGINT for IDs, NUMERIC for prices, BOOLEAN for flags
- Merging two different dataset to create a master data table

| Table | Source | Contents |
|-------|--------|----------|
| `silver.users` | Webhook | One row per student; upsert key is `user_id` |
| `silver.transactions` | Webhook + CSV | One row per enrollment; upsert key is `(user_id, bundle_id, master_batch_id)` |
| `silver.sessions` | Webhook | One row per live class instance; upsert key is `attendance_id` |
| `silver.assessments` | Webhook | One row per assessment event; upsert key is `event_id` |
| `silver.course_completion` | Webhook | One row per course completion; upsert key is `event_id` |
| `silver.announcements` | Webhook | One row per announcement; raw JSONB stored |
| `silver.certificates` | Webhook | One row per certificate issued; upsert key is `event_id` |
| `silver.course_catalogue_data` | API | One row per bundle |
| `silver.batches_data` | API | One row per batch; typed dates, tutor, enrolled count |
| `silver.course_batch_merge_data` | API | One row per course bundle; joining batches_Data + course_catalogue_data |
| `silver.course_metadata` | API | Flat table joining course_batch_merge_data + lifecycle; rebuilt /weekly/monthly|
| `silver.attendance_data` | API | One row per batch per class date; present/late/absent counts and attendance_pct |

---

## Gold Layer

Gold is the reporting layer. It contains SQL VIEWs that join and aggregate Silver tables into shapes that Power BI can consume directly. Gold views contain no new data — everything comes from Silver.

**Gold views (built):**

Course / enrollment views (`gold/webhook/gold_views.sql`):
- `gold.course` — per course-batch: enrollment counts, lifecycle dates, classification
- `gold.learner` — new vs returning learners per course
- `gold.course_type`, `gold.launch_type`, `gold.subject`
- `gold.learning_model`, `gold.term`, `gold.funnel`, `gold.sss_domain`

Attendance views (`gold/api/attendance_views.sql`):
- `gold.batch_attendance` — per-batch: avg attendance %, first/last class counts
- `gold.bundle_attendance` — aggregated to course level
- `gold.attendance_by_year` — year-by-year attendance trends
- `gold.first_vs_last_class` — drop-off analysis per batch

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Webhook receiver | Python 3, Flask |
| Database | PostgreSQL 14 (local dev), PostgreSQL on VPS (production) |
| DB connection pooling | psycopg2 ThreadedConnectionPool (min 2, max 20) |
| Dashboards | Power BI (live connection to gold.* views) |
| Hosting | VPS with systemd keeping the receiver running |

