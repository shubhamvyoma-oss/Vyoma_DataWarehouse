# Data Dictionary

Every table in the database, with every column explained.

---

## bronze.webhook_events

Every webhook event received from Edmingle is stored here first, unchanged. This table is never modified after insert (except the `routed_to_silver` flag). It is the permanent audit log of all incoming data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1`, `2`, `3` |
| `event_id` | TEXT | Unique ID for this event, assigned by Edmingle or derived from event type + timestamp | `user.user_created-2024-03-08T04:30:00+00:00` |
| `event_type` | TEXT | The Edmingle event name | `user.user_created`, `session.session_created` |
| `raw_payload` | JSONB | The complete original webhook body as received, stored without any modification | `{"event": {...}, "payload": {...}}` |
| `received_at` | TIMESTAMPTZ | When this row was inserted, in IST | `2024-03-08 10:00:00+05:30` |
| `is_live_mode` | BOOLEAN | True if this is a real production event; false for test/sandbox events | `true` |
| `routed_to_silver` | BOOLEAN | Set to true once the event has been successfully written to the Silver tables | `false` (default), `true` (after Silver insert) |

---

## bronze.failed_events

Any webhook request that arrived at the server but could not be processed is logged here. This includes malformed JSON, missing required fields, and DB errors. It is the safety net — nothing is silently dropped.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `received_at` | TIMESTAMPTZ | When the failed request arrived, in IST | `2024-03-08 10:01:00+05:30` |
| `failure_reason` | TEXT | A short description of why it failed | `JSON parse failed`, `Bronze insert failed: ...` |
| `raw_body` | TEXT | The first 10,000 characters of the raw request body | `{invalid json here}` |
| `content_type` | TEXT | The Content-Type header of the failed request | `application/json`, `text/plain` |

---

## bronze.studentexport_raw

A verbatim copy of the `studentexport.csv` file exported from Edmingle. Every column from the CSV is stored as TEXT to avoid any data loss during import. This was a one-time historical backfill. All 56,000+ rows from the CSV are here.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | 0-based row index from the pandas DataFrame (unique, used to prevent duplicate imports) | `0`, `1`, `2` |
| `row_number` | TEXT | Row number as it appeared in the CSV | `1` |
| `name` | TEXT | Student full name | `Ramesh Kumar` |
| `email` | TEXT | Student email address | `ramesh@example.com` |
| `registration_number` | TEXT | Edmingle registration number | `VYO-00123` |
| `contact_number_dial_code` | TEXT | Dial code for primary phone | `+91` |
| `contact_number` | TEXT | Primary phone number | `9876543210` |
| `alternate_contact_number_dial_code` | TEXT | Dial code for alternate phone | `+91` |
| `alternate_contact_number` | TEXT | Alternate phone number | `9123456789` |
| `date_of_birth` | TEXT | Date of birth as a string | `01/15/1990` |
| `parent_name` | TEXT | Parent or guardian name | `Suresh Kumar` |
| `parent_contact` | TEXT | Parent phone number | `9812345678` |
| `parent_email` | TEXT | Parent email address | `suresh@example.com` |
| `address` | TEXT | Student home address | `123 Main Street, Mumbai` |
| `city` | TEXT | City | `Mumbai` |
| `state` | TEXT | State | `Maharashtra` |
| `standard` | TEXT | Academic standard or grade | `Class 10` |
| `date_created` | TEXT | Account creation date as a string | `3/8/2024 10:30` |
| `username` | TEXT | Edmingle username | `ramesh123` |
| `gender` | TEXT | Gender | `Male` |
| `status` | TEXT | Account status | `active` |
| `username_1` | TEXT | Secondary username field (Edmingle quirk) | `ramesh_k` |
| `why_study_sanskrit` | TEXT | Custom field: reason for learning Sanskrit | `Interested in ancient texts` |
| `user_nice_name` | TEXT | Display name | `Ramesh` |
| `user_last_name` | TEXT | Last name | `Kumar` |
| `would_like_to_teach` | TEXT | Custom field | `Yes` |
| `teaching_experience` | TEXT | Custom field | `2 years` |
| `is_mainstream_education` | TEXT | Custom field | `Yes` |
| `objective` | TEXT | Custom field: learning objective | `Read Vedas` |
| `user_age` | TEXT | Age as entered by student | `34` |
| `persona` | TEXT | Custom field | `Learner` |
| `objective_package` | TEXT | Custom field | `Certificate` |
| `time_per_week_hours` | TEXT | Hours per week available for study | `5` |
| `age_` | TEXT | Duplicate age field (Edmingle exports two) | `34` |
| `facebook_profile_url` | TEXT | Facebook URL | `https://facebook.com/ramesh` |
| `instagram_profile_url` | TEXT | Instagram URL | `https://instagram.com/ramesh` |
| `pinterest_profile_url` | TEXT | Pinterest URL | *(usually null)* |
| `soundcloud_profile_url` | TEXT | SoundCloud URL | *(usually null)* |
| `tumblr_profile_url` | TEXT | Tumblr URL | *(usually null)* |
| `youtube_profile_url` | TEXT | YouTube URL | *(usually null)* |
| `wikipedia_url` | TEXT | Wikipedia URL | *(usually null)* |
| `twitter_username` | TEXT | Twitter/X username | `@ramesh_k` |
| `gst_number` | TEXT | GST number for billing | *(usually null)* |
| `myspace_profile_url` | TEXT | MySpace URL | *(usually null)* |
| `international_phone_number` | TEXT | International format phone | `+919876543210` |
| `website` | TEXT | Personal website | *(usually null)* |
| `educational_qualification` | TEXT | Highest education level | `B.Com` |
| `linkedin_profile_url` | TEXT | LinkedIn URL | *(usually null)* |
| `age_v2` | TEXT | Third age field (Edmingle exports multiple) | `34` |
| `gender_` | TEXT | Duplicate gender field | `Male` |
| `sanskrit_qualification` | TEXT | Custom field | `Beginner` |
| `areas_of_interest` | TEXT | Custom field | `Grammar, Literature` |
| `studying_sanskrit_currently` | TEXT | Custom field | `Yes` |
| `current_education_status` | TEXT | Custom field | `Working professional` |
| `country_name` | TEXT | Country | `India` |
| `loaded_at` | TIMESTAMPTZ | When this row was loaded into the database | `2024-04-29 14:00:00+05:30` |

---

## bronze.student_courses_enrolled_raw

A verbatim copy of the `studentCoursesEnrolled.csv` file from Edmingle. All timestamps are stored as TEXT here and converted to typed values in Silver. This was a one-time historical backfill.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | 0-based row index from the CSV (unique, prevents duplicate imports) | `0` |
| `user_id` | TEXT | Edmingle user ID (stored as text in CSV) | `12345` |
| `name` | TEXT | Student name | `Ramesh Kumar` |
| `email` | TEXT | Student email | `ramesh@example.com` |
| `class_id` | TEXT | Internal class/batch identifier | `121333` |
| `class_name` | TEXT | Name of the batch | `Batch A 2024` |
| `tutor_name` | TEXT | Name of the teacher | `Prof. Sharma` |
| `total_classes` | TEXT | Total number of scheduled classes | `48` |
| `present` | TEXT | Number of classes attended | `40` |
| `absent` | TEXT | Number of classes missed | `8` |
| `late` | TEXT | Number of late arrivals | `2` |
| `excused` | TEXT | Number of excused absences | `1` |
| `start_date` | TEXT | Enrollment start date as Unix timestamp string | `1709856600` |
| `end_date` | TEXT | Enrollment end date as Unix timestamp string | `1741392600` |
| `master_batch_id` | TEXT | Batch identifier | `1281` |
| `master_batch_name` | TEXT | Batch name | `Batch A 2024` |
| `classusers_start_date` | TEXT | When this student joined the class | `1709856600` |
| `classusers_end_date` | TEXT | When this student's access ends | `1741392600` |
| `batch_status` | TEXT | Status of the overall batch | `active` |
| `cu_status` | TEXT | Status of this student's enrollment | `active` |
| `cu_state` | TEXT | State of the enrollment | `enrolled` |
| `institution_bundle_id` | TEXT | Institution's internal course bundle ID | `363` |
| `archived_at` | TEXT | When the enrollment was archived, if applicable | *(usually null)* |
| `bundle_id` | TEXT | Edmingle course bundle ID | `12477` |
| `loaded_at` | TIMESTAMPTZ | When this row was loaded into the database | `2024-04-29 14:00:00+05:30` |

---

## bronze.unresolved_students_raw

Students from `studentexport.csv` whose email address could not be matched to any `user_id` in Edmingle's system at the time of the backfill. These 22,834 rows were not loaded into `silver.users` because without a `user_id` they cannot be joined to enrollment or attendance data.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `source_row` | INTEGER | Row index from the original CSV | `42` |
| `email` | TEXT | The email address that could not be resolved | `unknown@example.com` |
| `raw_row` | JSONB | The complete original CSV row stored as JSON | `{"name": "...", "email": "...", ...}` |
| `inserted_at` | TIMESTAMPTZ | When this row was inserted | `2024-04-29 14:00:00+05:30` |

---

## silver.users

One row per student. When Edmingle sends a `user.user_created` or `user.user_updated` event, this table is upserted — if the student already exists, their fields are updated using COALESCE (existing non-null values are never overwritten by null).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | The event_id from the most recent webhook that touched this row | `user.user_updated-2024-03-08T05:00:00+00:00` |
| `event_type` | TEXT | The event type that last updated this row | `user.user_updated` |
| `user_id` | BIGINT | Edmingle's unique ID for this student (upsert key) | `123456` |
| `email` | TEXT | Student email address | `ramesh@example.com` |
| `full_name` | TEXT | Student full name | `Ramesh Kumar` |
| `user_name` | TEXT | Edmingle username | `ramesh123` |
| `user_role` | TEXT | Role in the platform | `student` |
| `contact_number` | TEXT | Phone number | `9876543210` |
| `institution_id` | INTEGER | Vyoma's institution ID in Edmingle (483 for Vyoma) | `483` |
| `city` | TEXT | City (from system_fields in user_updated events) | `Mumbai` |
| `state` | TEXT | State | `Maharashtra` |
| `address` | TEXT | Full address | `123 Main Street` |
| `pincode` | TEXT | PIN code | `400001` |
| `parent_name` | TEXT | Parent name (from system_fields) | `Suresh Kumar` |
| `parent_email` | TEXT | Parent email (from system_fields) | `suresh@example.com` |
| `parent_contact` | TEXT | Parent phone (from system_fields) | `9812345678` |
| `custom_fields` | JSONB | Any custom fields sent in the event, stored as JSON | `{"standard": "Class 10"}` |
| `created_at_ist` | TIMESTAMPTZ | Account creation time in IST; set once, never overwritten | `2024-03-08 10:00:00+05:30` |
| `updated_at_ist` | TIMESTAMPTZ | Last profile update time in IST | `2024-03-09 11:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written to the database, in IST | `2024-03-09 11:00:05+05:30` |

---

## silver.transactions

One row per student-course-batch enrollment. When a student buys the same course in a different batch, that is a separate row. CSV backfill data and live webhook data for the same enrollment merge into a single row (upsert on `user_id + bundle_id + master_batch_id`).

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Event ID from the most recent webhook that touched this row | `transaction.user_purchase_completed-2024-03-08T04:30:00+00:00` |
| `event_type` | TEXT | Event type | `transaction.user_purchase_completed` |
| `event_timestamp_ist` | TIMESTAMPTZ | When the event occurred, in IST | `2024-03-08 10:00:00+05:30` |
| `user_id` | BIGINT | Student's Edmingle user ID (part of upsert key) | `123456` |
| `email` | TEXT | Student email at time of purchase | `ramesh@example.com` |
| `full_name` | TEXT | Student name at time of purchase | `Ramesh Kumar` |
| `contact_number` | TEXT | Phone number | `9876543210` |
| `bundle_id` | BIGINT | Edmingle course bundle ID (part of upsert key) | `12477` |
| `course_name` | TEXT | Name of the course | `Sanskrit Foundation Course` |
| `master_batch_id` | BIGINT | Batch ID within the course (part of upsert key) | `1281` |
| `master_batch_name` | TEXT | Batch name | `Batch A 2024` |
| `institution_bundle_id` | BIGINT | Vyoma's internal bundle ID | `363` |
| `original_price` | NUMERIC(12,2) | Listed price before discount | `5000.00` |
| `discount` | NUMERIC(12,2) | Discount amount applied | `500.00` |
| `final_price` | NUMERIC(12,2) | Amount actually paid | `4500.00` |
| `currency` | TEXT | Currency code | `INR` |
| `credits_applied` | NUMERIC(12,2) | Platform credits used | `0.00` |
| `payment_method` | TEXT | How the student paid | `razorpay`, `upi` |
| `transaction_id` | TEXT | Payment gateway transaction reference | `pay_abc123` |
| `start_date_ist` | TIMESTAMPTZ | Enrollment start date in IST | `2024-03-08 10:00:00+05:30` |
| `end_date_ist` | TIMESTAMPTZ | Enrollment end date in IST | `2025-03-08 10:00:00+05:30` |
| `created_at_ist` | TIMESTAMPTZ | When the enrollment was created in IST | `2024-03-08 09:55:00+05:30` |
| `source` | TEXT | Where this row came from | `webhook`, `csv` |
| `inserted_at` | TIMESTAMPTZ | When this row was last written to the database | `2024-03-08 10:00:05+05:30` |

---

## silver.sessions

One row per live class session. Multiple webhook events can refer to the same session (created, updated, started, cancelled) — they all upsert onto the same row using `attendance_id` as the key.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Event ID from the most recent webhook that touched this row | `session.session_started-2024-03-08T04:45:00+00:00` |
| `event_type` | TEXT | The event type that last updated this row | `session.session_started` |
| `attendance_id` | BIGINT | Edmingle's unique ID for this session instance (upsert key) | `8914546` |
| `class_id` | BIGINT | The recurring class this session belongs to | `121333` |
| `class_name` | TEXT | Human-readable class name | `Sanskrit Grammar — Lesson 1` |
| `class_type_formatted` | TEXT | Type of class | `Live Class`, `Recorded` |
| `scheduled_start_ist` | TIMESTAMPTZ | Planned start time in IST | `2024-03-08 10:00:00+05:30` |
| `scheduled_end_ist` | TIMESTAMPTZ | Planned end time in IST | `2024-03-08 11:00:00+05:30` |
| `actual_start_ist` | TIMESTAMPTZ | When the session actually started (from session_started event) | `2024-03-08 10:05:00+05:30` |
| `duration_minutes` | INTEGER | Planned duration in minutes | `60` |
| `teacher_id` | BIGINT | Edmingle user ID of the teacher | `15` |
| `teacher_name` | TEXT | Teacher's name | `Prof. Ramesh Sharma` |
| `teacher_email` | TEXT | Teacher's email | `ramesh@example.com` |
| `master_batches` | JSONB | List of batches attending this session | `[{"master_batch_id": 1281, ...}]` |
| `schedule_id` | BIGINT | ID of the schedule this session belongs to | `5001` |
| `is_recurring` | BOOLEAN | Whether this is part of a recurring schedule | `true` |
| `virtual_platform` | TEXT | Online class platform | `Zoom`, `GoogleMeet` |
| `zoom_meeting_id` | TEXT | Zoom meeting ID if applicable | `123456789` |
| `cancellation_reason` | TEXT | Why the session was cancelled (from session_cancel event) | `Teacher unavailable` |
| `cancelled_by` | BIGINT | User ID of whoever cancelled it | `15` |
| `status` | INTEGER | Session status code from Edmingle | `0` (scheduled), `1` (live), `2` (ended) |
| `is_late_signin` | BOOLEAN | True if the teacher signed in late | `true` |
| `delay_minutes` | INTEGER | How many minutes late the session started | `5` |
| `reminder_type` | TEXT | Type of reminder sent (from session_reminders event) | `1h_before` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 10:05:10+05:30` |

---

## silver.assessments

One row per assessment event. There are four event types: `test_submitted`, `test_evaluated`, `exercise_submitted`, `exercise_evaluated`. Each has a unique `event_id`, so each produces its own row.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `assess-test-sub-001` |
| `event_type` | TEXT | Which assessment event this is | `assessments.test_submitted` |
| `user_id` | BIGINT | Student who submitted | `123456` |
| `attempt_id` | BIGINT | Edmingle ID for this attempt | `55001` |
| `exercise_id` | BIGINT | ID of the exercise (null for test events, which use attempt_id only) | `7001` |
| `mark` | NUMERIC(8,2) | Score awarded | `85.50` |
| `is_evaluated` | INTEGER | 0 = not yet evaluated, 1 = evaluated by faculty | `1` |
| `faculty_comments` | TEXT | Feedback from the teacher (null until evaluated event arrives) | `Good work. Review chapter 3.` |
| `submitted_at_ist` | TIMESTAMPTZ | When the student submitted, in IST | `2024-03-08 11:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 11:00:10+05:30` |

---

## silver.courses

One row per course completion event. A student completing a course triggers a `course.user_course_completed` event.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `course-completed-001` |
| `event_type` | TEXT | Always `course.user_course_completed` | `course.user_course_completed` |
| `user_id` | BIGINT | Student who completed the course | `123456` |
| `bundle_id` | BIGINT | The course that was completed | `12477` |
| `completed_at_ist` | TIMESTAMPTZ | When the course was completed, in IST | `2025-03-08 10:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2025-03-08 10:00:05+05:30` |

---

## silver.announcements

One row per announcement. The announcement payload structure from Edmingle has not yet been fully documented, so the entire data object is stored as JSONB for now.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `announce-001` |
| `event_type` | TEXT | Always `announcement.announcement_created` | `announcement.announcement_created` |
| `raw_data` | JSONB | Complete data payload from the announcement event | `{"title": "Maintenance", "message": "..."}` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2024-03-08 10:00:05+05:30` |

---

## silver.certificates

One row per certificate issued to a student.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `id` | SERIAL | Internal auto-increment row ID | `1` |
| `event_id` | TEXT | Unique event ID (upsert key) | `cert-001` |
| `event_type` | TEXT | Always `certificate.certificate_issued` | `certificate.certificate_issued` |
| `certificate_id` | TEXT | Edmingle's certificate reference number | `CERT-2024-123456-12477` |
| `user_id` | BIGINT | Student who received the certificate | `123456` |
| `issued_at_ist` | TIMESTAMPTZ | When the certificate was issued, in IST | `2025-03-08 10:00:00+05:30` |
| `received_at` | TIMESTAMPTZ | When this row was last written in IST | `2025-03-08 10:00:05+05:30` |

---

## Tables Planned But Not Yet Built

| Table | Purpose |
|-------|---------|
| `bronze.course_metadata_raw` | Raw copy of a course classification CSV (maps bundle_id to subject area, level, language) |
| `silver.course_metadata` | Cleaned course classifications — joins to `silver.transactions` by `bundle_id` |
| `silver.daily_attendance` | Per-student attendance records pulled daily from Edmingle's `report_type=55` API endpoint |
| `gold.*` | SQL VIEWs over the Silver tables, shaped for Power BI consumption — enrollment summaries, attendance rates, completion funnels. Not yet built. |
