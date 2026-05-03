# Runbook — How to Run Everything

This document explains how to set up, run, and recover the pipeline from scratch.

---

## 1. Initial Setup (Fresh Machine or VPS)

### Install requirements

```bash
# Python 3.10 or newer
python --version

# Install Python packages
pip install flask psycopg2-binary requests pandas openpyxl

# PostgreSQL 14 or newer must be installed and running
# On Ubuntu/Debian:
sudo apt install postgresql postgresql-contrib
```

### Clone the repository

```bash
git clone https://github.com/shubhamvyoma-oss/E-Learning-Vyoma-DataWarehouse.git
cd E-Learning-Vyoma-DataWarehouse
```

### Set credentials in each script

Every script has a `CONFIG` block near the top of the file. Open each file and update these values:

```python
DB_HOST        = "localhost"       # or your VPS IP
DB_NAME        = "edmingle_analytics"
DB_USER        = "postgres"
DB_PASSWORD    = "your_password"
DB_PORT        = 5432
WEBHOOK_SECRET = "your_webhook_secret_here"
API_KEY        = "your_edmingle_api_key"
```

Files that need their CONFIG block updated:
- `ingestion/webhook_receiver.py`
- `ingestion/reprocess_bronze.py`
- `api_scripts/fetch_attendance.py`
- `api_scripts/fetch_course_catalogue.py`
- `api_scripts/fetch_course_batches.py`
- `api_scripts/run_course_pipeline.py`
- `scripts/external/check_db_counts.py`
- `scripts/external/clear_test_data.py`
- `scripts/external/reprocess_bronze.py`
- `scripts/external/test_webhook_send.py`
- `scripts/migrations/csv_load_bronze.py`
- `scripts/migrations/csv_backfill_transactions.py`
- `tests/test_pipeline_e2e.py`
- `tests/test_db_unavailability.py`

### Create the PostgreSQL database

```bash
# Connect as the postgres superuser
psql -U postgres

# Inside psql:
CREATE DATABASE edmingle_analytics;
\q
```

### Run database/run_all.sql to create all tables

```bash
# Run from the project root directory
psql -U postgres -d edmingle_analytics -f database/run_all.sql
```

This creates all schemas, the `unix_to_ist()` helper function, all Bronze tables, and all Silver tables.

### Run the CSV migration scripts (one-time, historical backfill only)

These scripts import the historical CSV exports. Run them once on a fresh database. Do not run them again — they will skip duplicate rows, but it is unnecessary.

```bash
# Step 1: Load the raw CSV files into Bronze tables
python scripts/migrations/csv_load_bronze.py

# Step 2: Backfill Silver tables from the raw Bronze data
python scripts/migrations/csv_backfill_transactions.py
```

The CSV files must be present in the `CSV files/` folder. They are not in the repo (gitignored) because they contain student PII.

---

## 2. Daily Operations

### Start the webhook receiver

```bash
python ingestion/webhook_receiver.py
```

The receiver starts on port 5000. It stays running and processes every incoming Edmingle event. For production, this should be managed by systemd (see Section 5).

**To expose it to Edmingle during local development:**

```bash
# In a separate terminal
ngrok http 5000
```

Copy the ngrok HTTPS URL and paste it into Edmingle's webhook settings. Edmingle will send all events to that URL.

### Check that data is coming in

```bash
# Shows row counts for every table
python scripts/external/check_db_counts.py

# Or hit the health endpoint
curl http://localhost:5000/health

# Or check recent events
curl http://localhost:5000/status
```

### Attendance pull (daily)

```bash
python api_scripts/fetch_attendance.py
```

Pulls yesterday's attendance by default. Calls Edmingle's `report_type=55` endpoint and stores data in `bronze.attendance_raw` → `silver.class_attendance`.

---

## 3. If Something Breaks

### Webhook receiver crashed

```bash
# Restart it
python ingestion/webhook_receiver.py

# Check for events that arrived while it was down
cat ingestion/fallback_queue.jsonl
```

If `fallback_queue.jsonl` exists and has content, events arrived while the DB was unreachable. Recover them:

```bash
curl -X POST http://localhost:5000/retry-failed
```

This reads the fallback file, inserts each event into Bronze, and deletes the file when done. The response tells you how many were recovered: `{"retried": 10, "remaining": 0}`.

### DB went down (PostgreSQL stopped)

Events that arrived while the DB was down are in `ingestion/fallback_queue.jsonl`. After restarting PostgreSQL:

```bash
# Confirm DB is back
curl http://localhost:5000/health

# Recover queued events
curl -X POST http://localhost:5000/retry-failed
```

Then run the Silver reprocessor to ensure all Bronze events are in Silver:

```bash
python scripts/external/reprocess_bronze.py
```

### Silver table has wrong data or is missing rows

This happens when a Silver routing bug is fixed after events have already been processed. Re-run all Bronze events through the router:

```bash
# Standalone version (no project imports needed)
python scripts/external/reprocess_bronze.py
```

This only processes Bronze rows where `routed_to_silver = false`. If you need to reprocess everything, connect to the DB first and reset the flag:

```sql
UPDATE bronze.webhook_events SET routed_to_silver = false;
```

Then run the script again.

### Test data is polluting the DB

After running tests, clean up with:

```bash
python scripts/external/clear_test_data.py
```

This shows you how many test rows exist and asks for confirmation before deleting. It identifies test data by email patterns (`%test%`, `%load%`) and high `user_id` values (>= 99,990,000).

### Events in bronze.failed_events

Check what failed:

```bash
curl http://localhost:5000/failed
```

Common causes:
- `JSON parse failed` — Edmingle sent a non-JSON body. Usually safe to ignore.
- `Bronze insert failed` — DB was temporarily unavailable. Use `/retry-failed`.
- `Missing event_id or event_type` — Edmingle sent a validation ping (`url.validate` event). Normal.

---

## 4. How to Run Tests

### Full end-to-end test suite (38 tests)

The webhook receiver must be running before you run this.

```bash
python ingestion/webhook_receiver.py   # in one terminal
python tests/test_pipeline_e2e.py      # in another terminal
```

Expected result: `38/38 PASS`

The test suite covers: HMAC signature validation, duplicate deduplication, all 16 event types, Silver upsert logic, Bronze `routed_to_silver` flag, the `/failed` endpoint, and concurrent request handling.

### DB unavailability test

This test simulates a complete PostgreSQL outage and verifies that no events are lost.

```bash
python tests/test_db_unavailability.py
```

Expected result: `DB UNAVAILABILITY TEST PASS` with `Data lost: 0`.

What the test does:
1. Sends 5 baseline events (DB up) — verifies they land in Bronze
2. Blocks all new DB connections using `ALTER DATABASE ... ALLOW_CONNECTIONS false`
3. Sends 10 events while DB is down — verifies the server returns HTTP 200 (no crash)
4. Restores DB connections
5. Calls `POST /retry-failed` to recover events from `fallback_queue.jsonl`
6. Verifies all 10 events are now in Bronze

### Event coverage test

Sends one event of every type and checks all Silver tables have data:

```bash
python tests/test_all_events.py
```

---

## 5. How to Deploy to VPS

### SSH and clone

```bash
ssh user@vps-ip-address
git clone https://github.com/ShubhamK0802/E-Learning-Vyoma-DataWarehouse.git
cd E-Learning-Vyoma-DataWarehouse
pip install flask psycopg2-binary requests
```

### Set CONFIG values

Edit each file's CONFIG block with the production DB password and webhook secret.

### Run database setup

```bash
psql -U postgres -d edmingle_analytics -f database/run_all.sql
```

### Set up systemd to keep the receiver running

Create `/etc/systemd/system/webhook-receiver.service`:

```ini
[Unit]
Description=Edmingle Webhook Receiver
After=network.target postgresql.service

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/E-Learning-Vyoma-DataWarehouse
ExecStart=/usr/bin/python3 ingestion/webhook_receiver.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable webhook-receiver
sudo systemctl start webhook-receiver
sudo systemctl status webhook-receiver
```

### Point Edmingle webhook URL to VPS

In Edmingle settings, set the webhook URL to:

```
http://VPS_IP:5000/webhook
```

Or if a domain and SSL are configured:

```
https://yourdomain.com/webhook
```
