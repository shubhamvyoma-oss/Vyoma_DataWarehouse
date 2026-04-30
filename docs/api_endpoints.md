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

All requests require the API key in the header: `X-API-KEY: <api_key>`

---

### GET /report/csv?report_type=55 — Attendance Report (Planned)

**Status: Not yet built. Script will be at `api_scripts/report55_attendance.py`.**

Returns a CSV-formatted attendance report for all students on a given date. This replaces a per-student loop — one call covers all ~93,000 students.

**Request:**
```
GET /report/csv?report_type=55&date=2024-03-08&org_id=683
X-API-KEY: <api_key>
```

**Parameters:**
| Parameter | Description | Example |
|-----------|-------------|---------|
| `report_type` | Always 55 for the attendance report | `55` |
| `date` | The date to pull attendance for | `2024-03-08` |
| `org_id` | Vyoma's organisation ID in Edmingle | `683` |

**What the script will do:**
1. Call this endpoint for today's date
2. Parse the CSV response
3. Insert each row into `bronze.daily_attendance_raw`
4. Transform and upsert into `silver.daily_attendance`

**Planned schedule:** Run once per day via cron or systemd timer, shortly after midnight IST.

---

### GET /organization/students — Student List (Planned)

**Status: Not yet built. Would be used for registration backfill.**

Returns a paginated list of all registered students in the organisation.

**Request:**
```
GET /organization/students?org_id=683&page=1&per_page=100
X-API-KEY: <api_key>
```

**Why this is useful:** The `studentexport.csv` from the historical backfill could not resolve 22,834 students. This endpoint could provide a `user_id`-to-email mapping that would let us match and load those students into `silver.users`.

---

### GET /reports/sales?reportDetailsType=3 — Enrollment History (Planned)

**Status: Not yet built.**

Returns month-by-month enrollment history. Could be used to verify that the CSV backfill captured all historical transactions correctly.

**Request:**
```
GET /reports/sales?reportDetailsType=3&org_id=683&from=2020-01-01&to=2024-12-31
X-API-KEY: <api_key>
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
