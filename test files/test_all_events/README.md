# 16 — Test All Events

## What it does

Sends one test webhook event for every supported Edmingle event type to the running webhook server, then checks that all Silver tables have data.

## Why we need it

Smoke test to verify the webhook server handles all event types correctly, end-to-end from HTTP request to database row. Run after any change to the webhook server or Silver routing logic.

## Event types tested

| Event type | Silver table |
|-----------|-------------|
| `transaction.user_purchase_initiated` | `silver.transactions` |
| `transaction.user_purchase_failed` | `silver.transactions` |
| `session.session_created` | `silver.sessions` |
| `session.session_update` | `silver.sessions` |
| `session.session_cancel` | `silver.sessions` |
| `session.session_started` | `silver.sessions` |
| `session.session_reminders` | `silver.sessions` |
| `assessments.test_submitted` | `silver.assessments` |
| `assessments.test_evaluated` | `silver.assessments` |
| `assessments.exercise_submitted` | `silver.assessments` |
| `assessments.exercise_evaluated` | `silver.assessments` |
| `course.user_course_completed` | `silver.courses` |
| `announcement.announcement_created` | `silver.announcements` |
| `certificate.certificate_issued` | `silver.certificates` |

## How to run

```bash
# Step 1: Start the webhook server
python 06_webhook_receiver/webhook_receiver.py

# Step 2: In another terminal, run the test
python 16_test_all_events/test_all_events.py
```

## Example output

```
SENDING TEST EVENTS
====================
  OK    transaction.user_purchase_initiated  [txn-initiated-001]
  OK    transaction.user_purchase_failed     [txn-failed-001]
  ...
  All 14 events returned HTTP 200.

SILVER TABLE ROW COUNTS
========================
  OK     silver.users:          1 row(s)
  OK     silver.transactions:   2 row(s)
  OK     silver.sessions:       1 row(s)
  OK     silver.assessments:    4 row(s)
  OK     silver.courses:        1 row(s)
  OK     silver.announcements:  1 row(s)
  OK     silver.certificates:   1 row(s)

  All 7 Silver tables have data.
```

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `server unreachable` | Webhook server not running | Start it first with script 06 |
| `FAIL  event.type  HTTP 500` | Routing error in server | Check server terminal for traceback |
| `EMPTY  silver.table` | Routing didn't work | Check server logs |
