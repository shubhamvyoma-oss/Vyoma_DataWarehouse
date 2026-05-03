# database/silver/webhook/

Silver tables built from `bronze.webhook_events`. The webhook receiver (`ingestion/webhook_receiver.py`) reads each incoming event's `event_type` and routes it to the appropriate Silver table via UPSERT.

---

## users.sql

**Table**: `silver.users`

One row per registered student. Upsert key: `user_id`.

Built from: `user.user_created`, `user.user_updated` events.

COALESCE merge strategy: if a `user_updated` event arrives with a null city but a non-null city already exists in the row, the existing value is kept. Later events never blank out earlier data.

Key columns: `user_id`, `email`, `full_name`, `user_role`, `contact_number`, `institution_id`, `city`, `state`, `address`, `pincode`, `parent_name`, `parent_email`, `parent_contact`, `custom_fields` (JSONB), `created_at_ist`, `updated_at_ist`.

---

## transactions.sql

**Table**: `silver.transactions`

One row per student-course-batch enrollment. Upsert key: `(user_id, bundle_id, master_batch_id)`.

Built from: `transaction.user_purchase_completed`, `transaction.enrollment_cancelled` events, and CSV backfill.

The `source` column records `webhook` for live events and `csv.import` for backfilled historical data. Both sources merge into the same row on conflict.

Key columns: `user_id`, `email`, `full_name`, `bundle_id`, `course_name`, `master_batch_id`, `master_batch_name`, `original_price`, `discount`, `final_price`, `currency`, `payment_method`, `transaction_id`, `start_date_ist`, `end_date_ist`, `source`.

---

## sessions.sql

**Table**: `silver.sessions`

One row per live class session instance. Upsert key: `attendance_id`.

Built from: `session.session_created`, `session.session_updated`, `session.session_started`, `session.session_cancel`, `session.session_reminders` events.

Multiple events for the same session (created → updated → started → cancelled) all upsert onto the same row. Fields populated by later events (e.g., `actual_start_ist` from `session_started`) are added without overwriting earlier fields.

Key columns: `attendance_id`, `class_id`, `class_name`, `scheduled_start_ist`, `scheduled_end_ist`, `actual_start_ist`, `duration_minutes`, `teacher_id`, `teacher_name`, `master_batches` (JSONB), `virtual_platform`, `status`, `cancellation_reason`.

---

## assessments.sql

**Table**: `silver.assessments`

One row per assessment event. Upsert key: `event_id`.

Built from: `assessments.test_submitted`, `assessments.test_evaluated`, `assessments.exercise_submitted`, `assessments.exercise_evaluated` events.

Each of these four event types produces its own row (they have distinct `event_id` values). When a `test_evaluated` event arrives after `test_submitted`, it is a separate row — not an update to the submitted row.

Key columns: `user_id`, `attempt_id`, `exercise_id`, `mark`, `is_evaluated`, `faculty_comments`, `submitted_at_ist`.

---

## courses.sql

**Table**: `silver.courses`

One row per course completion. Upsert key: `event_id`.

Built from: `course.user_course_completed` events.

Key columns: `user_id`, `bundle_id`, `completed_at_ist`.

---

## announcements.sql

**Table**: `silver.announcements`

One row per announcement. Upsert key: `event_id`.

Built from: `announcement.announcement_created` events.

The announcement payload structure from Edmingle is stored as raw JSONB in `raw_data` since the field structure varies between announcement types.

---

## certificates.sql

**Table**: `silver.certificates`

One row per certificate issued. Upsert key: `event_id`.

Built from: `certificate.certificate_issued` events.

Key columns: `certificate_id`, `user_id`, `issued_at_ist`.
