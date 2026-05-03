# database/bronze/webhook/

Tables populated by the live webhook receiver (`ingestion/webhook_receiver.py`). Data arrives in real time whenever an event occurs in Edmingle (student registers, makes a purchase, attends a session, etc.).

---

## webhook_events.sql

**Table**: `bronze.webhook_events`

Stores every incoming webhook event exactly as received. Nothing is filtered, transformed, or discarded.

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Auto-increment row ID |
| `event_id` | TEXT | Unique event identifier from Edmingle (unique constraint) |
| `event_type` | TEXT | Event name: `user.user_created`, `transaction.user_purchase_completed`, etc. |
| `raw_payload` | JSONB | Complete webhook body, stored without modification |
| `received_at` | TIMESTAMPTZ | Insertion time in IST |
| `is_live_mode` | BOOLEAN | True for production events, false for test/sandbox events |
| `routed_to_silver` | BOOLEAN | Set to true after the event has been successfully written to Silver |

**Unique key**: `event_id`

**Supported event types** (16 total):
- `user.user_created`, `user.user_updated`
- `transaction.user_purchase_completed`, `transaction.enrollment_cancelled`
- `session.session_created`, `session.session_updated`, `session.session_started`, `session.session_cancel`, `session.session_reminders`
- `assessments.test_submitted`, `assessments.test_evaluated`, `assessments.exercise_submitted`, `assessments.exercise_evaluated`
- `course.user_course_completed`
- `announcement.announcement_created`
- `certificate.certificate_issued`

---

## failed_events.sql

**Table**: `bronze.failed_events`

Safety net for any webhook request that arrived but could not be processed. Captures the raw body and failure reason. Common failure causes:
- `JSON parse failed` — Edmingle sent a non-JSON body (e.g., a validation ping)
- `Bronze insert failed` — PostgreSQL was temporarily unreachable
- `Missing event_id or event_type` — Event structure was incomplete

| Column | Type | Description |
|---|---|---|
| `id` | SERIAL | Auto-increment row ID |
| `received_at` | TIMESTAMPTZ | When the request arrived |
| `failure_reason` | TEXT | Short description of why it failed |
| `raw_body` | TEXT | First 10,000 characters of the raw request body |
| `content_type` | TEXT | Content-Type header of the failed request |
