# Key Design Decisions

Every significant decision made in building this pipeline, with the reason and what alternatives were considered.

---

DECISION: Use the Bronze/Silver/Gold medallion architecture
WHY: Edmingle's webhook payloads are inconsistent — some fields are missing in some events, field names vary between event types, and Edmingle sometimes retries the same event multiple times. Separating raw storage (Bronze) from clean storage (Silver) means we can always go back and fix a transformation bug without losing data. If a routing function is wrong, we reset `routed_to_silver = false` and reprocess Bronze; we never need to re-fetch from Edmingle.
ALTERNATIVE CONSIDERED: Write directly to typed Silver tables from the webhook, skipping Bronze. Rejected because this would permanently lose the original payload. Any bug in transformation would be unrecoverable.

---

DECISION: Use webhooks as the primary real-time data source, with planned API scripts as a supplement
WHY: Edmingle supports webhooks for 16 event types covering users, transactions, sessions, assessments, and certificates. Webhooks deliver data the moment something happens, with no polling delay. The API is needed only for data that webhooks don't cover — specifically daily attendance summaries from `report_type=55`.
ALTERNATIVE CONSIDERED: Poll the Edmingle REST API on a schedule for all data. Rejected because webhooks are lower latency, have no API rate-limit risk, and require no scheduled job infrastructure during early development.

---

DECISION: Use Edmingle's `report_type=55` endpoint (one call per day for all students) instead of a per-student attendance loop
WHY: Edmingle has a rate limit. Calling an attendance endpoint for each of Vyoma's ~93,000 students individually would require thousands of API calls per day, risk rate-limiting, and take hours to complete. The `report_type=55` endpoint returns all students' attendance for a given date in one response.
ALTERNATIVE CONSIDERED: Per-student API loop. Rejected due to rate limit risk and execution time.

---

DECISION: The `silver.transactions` schema is designed around the webhook payload format, not the CSV column names
WHY: The webhook is the live, ongoing data source. New enrollments from this day forward arrive as `transaction.user_purchase_completed` events. The schema must match what the webhook sends cleanly and without transformation. The CSV was a one-time historical import — its column names (like `tutor_name`, `class_id`, `cu_status`) describe attendance data, not purchase data, so they belong in a different table.
ALTERNATIVE CONSIDERED: Design `silver.transactions` around the CSV columns and map webhooks into it. Rejected because CSV columns are attendance-oriented and don't map cleanly to purchase events.

---

DECISION: Drop CSV columns that have no equivalent in the webhook payload
WHY: The studentCoursesEnrolled.csv contains attendance-related columns (`total_classes`, `present`, `absent`, `late`, `excused`, `class_id`, `tutor_name`, `cu_status`, `cu_state`). These fields are never sent in `transaction.*` webhook events. Including them in `silver.transactions` would leave them permanently null for all live webhook rows, creating a misleading split table. They belong in a future `silver.daily_attendance` table fed by the `report_type=55` API.
ALTERNATIVE CONSIDERED: Add all CSV columns to `silver.transactions` as nullable columns. Rejected because it would mix two different kinds of data (enrollment vs attendance) in one table.

---

DECISION: Store the 22,834 unresolvable students in `bronze.unresolved_students_raw` instead of deleting them
WHY: During the CSV backfill, 22,834 students from `studentexport.csv` could not be matched to an Edmingle `user_id` because their email address did not appear in any webhook or enrollment record. These students are real people who enrolled historically. Deleting them permanently would mean losing that history. By storing them in a Bronze table, we preserve the option to match them later — for example, if Edmingle provides a user export with user IDs that can be joined on email.
ALTERNATIVE CONSIDERED: Discard unresolvable rows. Rejected because the data is real and may be matchable in future.

---

DECISION: Add a disk-based fallback queue (`ingestion/fallback_queue.jsonl`) for when the database is unavailable
WHY: When PostgreSQL is completely unreachable, both the Bronze insert and the `bronze.failed_events` insert fail — they use the same database. Without a fallback, events arriving during a DB outage are silently lost, with no recovery path. The fallback file writes each event's raw body to disk so it can be recovered after the DB comes back via `POST /retry-failed`.
ALTERNATIVE CONSIDERED: Log events to the application log only. Rejected because the log applies PII masking and truncation, making it unsuitable for recovery. An in-memory queue was also considered but would be lost on server restart.

---

DECISION: Gold layer = SQL VIEWs only, no physical ETL tables
WHY: All the data already exists in Silver in a clean, typed form. Creating separate physical Gold tables would mean duplicating data and running an ETL job to keep them in sync. VIEWs are always fresh, require no scheduling, and are transparent — Power BI users can trace any number back to Silver immediately.
ALTERNATIVE CONSIDERED: Materialised views or separate physical tables updated by a nightly job. Rejected unless query performance on raw Silver tables becomes a real problem (it is not a problem at current data volumes).

---

DECISION: Store all timestamps in IST (Asia/Kolkata, UTC+5:30)
WHY: All of Vyoma's students, teachers, and staff are in India. Every report, dashboard, and query will be interpreted in IST. Storing in UTC and converting on every query is error-prone and makes ad-hoc SQL queries hard to read. The `unix_to_ist()` helper function converts Edmingle's Unix timestamps to IST TIMESTAMPTZ at insert time.
ALTERNATIVE CONSIDERED: Store in UTC and apply timezone conversion in Power BI or Gold views. Rejected because it adds complexity at every layer with no benefit for a single-timezone organisation.

---

DECISION: Hardcode credentials in a CONFIG block in every script instead of using a `.env` file or environment variables
WHY: Anyone deploying the pipeline needs to open a single file, update four values, and run it — with no knowledge of dotenv, environment variables, or shell configuration. A CONFIG block at the top of each file is the most readable and least error-prone approach.
ALTERNATIVE CONSIDERED: `python-dotenv` with a `.env` file. Rejected because it requires an extra dependency, a separate file to manage, and knowledge of how dotenv works. Also considered environment variables set in the shell — rejected for the same reason.
NOTE: The CONFIG values should never be committed to git. Each file's CONFIG block is set locally on each machine. The `.env.example` file documents what values are needed.

---

DECISION: One repository for everything (webhook receiver, API scripts, migrations, tests, schema)
WHY: The pipeline is not large enough to justify splitting into separate repos. Keeping everything together means one `git clone`, one place to look for any file, and no cross-repo dependency management.
ALTERNATIVE CONSIDERED: Separate repos for ingestion, migrations, and infrastructure. Rejected as unnecessary complexity.
