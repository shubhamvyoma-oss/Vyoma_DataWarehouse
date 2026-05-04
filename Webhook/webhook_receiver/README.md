# 06 — Webhook Receiver

## What it does

Runs a Flask web server that listens for real-time events from Edmingle and saves them to the database.

Every time a student registers, buys a course, attends a class, or completes a course, Edmingle sends an HTTP POST request to this server. The server:

1. Saves the raw event to **Bronze** (`bronze.webhook_events`) immediately
2. Routes the event to the correct **Silver** table based on the event type
3. If the database is down, saves the event to a disk fallback queue

## Why we need it

Without this server, we would only see historical data. With it, the database is updated in real time — within seconds of an event happening in Edmingle.

## How it works — data flow

```
Edmingle Platform
  │
  │  POST /webhook  (JSON payload)
  │
  ▼
Flask server (port 5000)
  │
  ├── Save raw JSON → bronze.webhook_events
  │
  ├── Look up event type in EVENT_ROUTER
  │
  ├── Route to Silver table:
  │     user.user_created         → silver.users
  │     user.user_updated         → silver.users
  │     transaction.*             → silver.transactions
  │     session.*                 → silver.sessions
  │     assessments.*             → silver.assessments
  │     course.user_course_*      → silver.courses
  │     announcement.*            → silver.announcements
  │     certificate.*             → silver.certificates
  │
  └── If DB is down → write to fallback_queue.jsonl (disk)
```

### Key endpoints

| Endpoint | Method | What it does |
|----------|--------|-------------|
| `/webhook` | POST | Main event receiver |
| `/health` | GET | Returns `{"status": "ok"}` — used to check if server is up |
| `/status` | GET | Shows Bronze/Silver row counts |
| `/failed` | GET | Shows last 10 failed events |
| `/retry-failed` | POST | Re-processes events from the disk fallback queue |

## How to run

### Development (single process, auto-reload)

```bash
python 06_webhook_receiver/webhook_receiver.py
```

Server starts on `http://localhost:5000`

### Production (Gunicorn, multiple workers)

```bash
gunicorn --workers 4 --bind 0.0.0.0:5000 "06_webhook_receiver.webhook_receiver:app"
```

## What to check after starting

- Visit `http://localhost:5000/health` — should return `{"status": "ok"}`
- Visit `http://localhost:5000/status` — should show row counts
- Run `python 16_test_all_events/test_all_events.py` to send test events

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Address already in use` | Port 5000 is taken | Stop the other process or change the port |
| `Could not initialize DB pool` | PostgreSQL not running | Start PostgreSQL |
| Events not appearing in Silver | Event type not in EVENT_ROUTER | Check the event type string |
| `fallback_queue.jsonl growing` | DB was down | Call `POST /retry-failed` to flush the queue |
