# Edmingle API Reference

All HTTP endpoints used by this project — both the webhook receiver endpoints (inbound, served by this pipeline) and the Edmingle REST API endpoints (outbound, called by scripts in this pipeline).

---

## Part 1: Webhook Receiver Endpoints

These are endpoints served by `ingestion/webhook_receiver.py` running on port 5000.

---

### POST /webhook

The main entry point. Edmingle calls this URL every time a tracked event occurs.

**What it does:**
1. Reads the raw request body
2. Parses JSON (regardless of Content-Type header)
3. Extracts `event_id` and `event_type`
4. Inserts the raw payload into `bronze.webhook_events`
5. Routes to the appropriate Silver table based on `event_type`
6. Always returns HTTP 200 — Edmingle permanently disables webhooks that return any other status

**Request:**
```
POST /webhook
Content-Type: application/json
X-Webhook-Signature: <hmac-sha256 hex of body using webhook secret>

Body (real Edmingle format):
{
  "event": {
    "event": "user.user_created",
    "event_ts": "2024-03-08T04:30:00+00:00",
    "livemode": true
  },
  "payload": {
    "user_id": 123456,
    "email": "ramesh@example.com",
    ...
  }
}
```

**Response:**
```json
{"status": "received"}
```
HTTP 200 always.

**16 supported event types:**

| Event Type | Silver Table |
|------------|-------------|
| `user.user_created` | `silver.users` |
| `user.user_updated` | `silver.users` |
| `transaction.user_purchase_initiated` | `silver.transactions` |
| `transaction.user_purchase_completed` | `silver.transactions` |
| `transaction.user_purchase_failed` | `silver.transactions` |
| `session.session_created` | `silver.sessions` |
| `session.session_update` | `silver.sessions` |
| `session.session_cancel` | `silver.sessions` |
| `session.session_started` | `silver.sessions` |
| `session.session_start` | `silver.sessions` |
| `session.session_reminders` | `silver.sessions` |
| `session.session_reminder` | `silver.sessions` |
| `assessments.test_submitted` | `silver.assessments` |
| `assessments.test_evaluated` | `silver.assessments` |
| `assessments.exercise_submitted` | `silver.assessments` |
| `assessments.exercise_evaluated` | `silver.assessments` |
| `course.user_course_completed` | `silver.courses` |
| `announcement.announcement_created` | `silver.announcements` |
| `certificate.certificate_issued` | `silver.certificates` |
| `url.validate` | Bronze only (Edmingle health check ping) |

---

### POST /retry-failed

Recovers events that were written to the disk fallback file (`ingestion/fallback_queue.jsonl`) during a DB outage.

**When to call this:** After PostgreSQL comes back up following an outage. Call it once; it processes all queued events and deletes the fallback file when done.

**Request:**
```
POST /retry-failed
(no body required)
```

**Response:**
```json
{"retried": 10, "remaining": 0}
```

- `retried` — number of events successfully inserted into Bronze
- `remaining` — number that still failed (DB may still be unreachable, or the event body was corrupt)

**What it does:**
1. Reads every line from `ingestion/fallback_queue.jsonl`
2. Parses each event's raw body
3. Inserts into `bronze.webhook_events`
4. Removes successfully recovered lines from the file
5. Deletes the file entirely if all lines were recovered

---

### GET /health

Returns the current health of the webhook receiver and its database connection.

**Request:**
```
GET /health
```

**Response (healthy):**
```json
{"status": "ok", "database": "connected"}
```
HTTP 200

**Response (DB unreachable):**
```json
{"status": "error", "database": "connection refused"}
```
HTTP 500

---

### GET /status

Returns the 10 most recently received webhook events.

**Request:**
```
GET /status
```

**Response:**
```json
{
  "count": 10,
  "last_10_events": [
    {
      "event_id": "user.user_created-2024-03-08T04:30:00+00:00",
      "event_type": "user.user_created",
      "received_at": "2024-03-08T10:00:05+05:30",
      "is_live_mode": true,
      "routed_to_silver": true
    },
    ...
  ]
}
```

---

### GET /failed

Returns the 10 most recently failed webhook requests from `bronze.failed_events`.

**Request:**
```
GET /failed
```

**Response:**
```json
{
  "count": 3,
  "last_10_failed": [
    {
      "id": 1,
      "received_at": "2024-03-08T10:01:00+05:30",
      "failure_reason": "JSON parse failed",
      "content_type": "text/plain",
      "raw_body_preview": "{invalid json..."
    },
    ...
  ]
}
```

---

## Part 2: Edmingle REST API Endpoints

These are endpoints on Edmingle's servers that this project calls. Base URL: `https://vyoma-api.edmingle.com/nuSource/api/v1`

All requests require headers: `apikey: <api_key>` and `ORGID: 683`

---

### GET /report/csv?report_type=55 — Global Student Attendance

**Status: Built. Script: `api_scripts/fetch_attendance.py`**

Returns all students' attendance status for every class session that falls within a given IST date window. One call per day covers all students across all active batches.

**Request:**
```
GET /report/csv
  ?report_type=55
  &organization_id=683
  &start_time=<unix IST 00:00:00>
  &end_time=<unix IST 23:59:59>
  &response_type=1
Headers: apikey, ORGID
```

**Parameters:**
| Parameter | Description | Example |
|---|---|---|
| `report_type` | Always 55 | `55` |
| `organization_id` | Vyoma's organisation ID | `683` |
| `start_time` | IST midnight as Unix timestamp | `1728864000` |
| `end_time` | IST end-of-day as Unix timestamp | `1728950399` |
| `response_type` | `1` = JSON response (default is CSV) | `1` |

**Response:** JSON array of objects. One object per student per class session. Key fields: `student_Id`, `studentName`, `studentEmail`, `batch_Id`, `batchName`, `class_Id`, `className`, `bundle_Id`, `bundleName`, `course_Id`, `courseName`, `studentAttendanceStatus` (P/L/A/-/E/OL/NA), `classDate`, `startTime`, `endTime`, `classDuration`, `teacher_Id`, `teacherName`, `attendanceId`, `studentRating`, `studentComments`.

---

### GET /institute/{INST_ID}/courses/catalogue — Course Catalogue

**Status: Built. Script: `api_scripts/fetch_course_catalogue.py`**

Returns all course bundles with full metadata including Vyoma-specific custom fields.

**Request:**
```
GET /institute/483/courses/catalogue
Headers: apikey, ORGID
```

**Response:** Paginated list of bundle objects. Includes `bundle_id`, `bundle_name`, and all custom classification fields (subject, SSS category, adhyayanam category, funnel position, term of course, viniyoga, division).

---

### GET /short/masterbatch — All Batches

**Status: Built. Script: `api_scripts/fetch_course_batches.py`**

Returns all batches grouped under their parent bundle. Used to get current batch metadata (dates, tutor, enrollment count).

**Request:**
```
GET /short/masterbatch
  ?status=0
  &page=1
  &per_page=100
  &organization_id=683
  &bundle_id=0
Headers: apikey, ORGID
```

**Response structure:**
```json
{
  "bundles": [
    {
      "bundle_id": 1234,
      "bundle_name": "Sanskrit Foundation",
      "batch": [
        { "class_id": 5678, "class_name": "Batch A 2024", "mb_admitted_students": 45, ... }
      ]
    }
  ],
  "page_context": { "has_more_page": true }
}
```

Note: In the API response, `class_id` and `class_name` at the batch level refer to what we call `batch_id` and `batch_name` in the database.

---

### GET /organization/students — Student List

Returns a paginated list of all registered students.

**Request:**
```
GET /organization/students?organization_id=683&page=1&per_page=100
Headers: apikey, ORGID
```

Not currently used by any script (historical backfill used CSV instead). Useful for resolving the 22,834 unmatched students in `bronze.unresolved_students_raw`.

---

### GET /institution/dataexport?type=N — Data Export (Not yet implemented)

Returns bulk data exports by type. High-value types not yet built into the pipeline:

| Type | Description |
|---|---|
| `5` | User engagement stats (videos watched, time spent, login count per student) |
| `8` | Material view stats per student (requires `start_date` param) |
| `10` | Exercise/quiz completion stats per student |

**Request:**
```
GET /institution/dataexport?type=5&organization_id=683&page=1&per_page=100
Headers: apikey, ORGID
```

---

## Part 3: HMAC Signature Verification

Edmingle signs every webhook request body with HMAC-SHA256 using the shared webhook secret. The pipeline verifies this signature before processing.

**How it works:**
1. Edmingle sends the signature in the `X-Webhook-Signature` header
2. The receiver computes `hmac.new(SECRET, body, sha256).hexdigest()`
3. If the computed value does not match the header, the request is rejected

**Why this matters:** Without signature verification, anyone who knows your webhook URL could send fake events and inject bogus data into the database.

**Note:** The current implementation in `webhook_receiver.py` has the signature check in place. The webhook secret is set in the CONFIG block at the top of the file. The secret in `.env.example` (`your_webhook_secret_here`) must be replaced with the real value from Edmingle's webhook settings page.
