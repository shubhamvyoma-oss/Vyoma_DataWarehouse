# ingestion/

Contains the live webhook receiver and the Bronze reprocessor. This is the real-time data ingestion layer — it runs continuously as a server and handles all incoming Edmingle webhook events.

---

## Files

| File | Description |
|---|---|
| `webhook_receiver.py` | Flask server that receives, validates, stores, and routes all webhook events |
| `reprocess_bronze.py` | Standalone script that re-routes Bronze events to Silver (for fixing Silver bugs) |
| `webhook_receiver.log` | Runtime log (rotated, PII-masked) |

---

## webhook_receiver.py

A Flask application listening on port 5000. It handles:

1. **HMAC signature validation** — Verifies the `X-Edmingle-Signature` header against the webhook secret to reject forged requests.
2. **Bronze insert** — Every valid event is inserted into `bronze.webhook_events` as raw JSONB. Duplicate `event_id` values are ignored via `ON CONFLICT DO NOTHING`.
3. **Silver routing** — Based on `event_type`, the event is parsed and upserted into the appropriate Silver table. The `routed_to_silver` flag is set to `true` on success.
4. **Fallback queue** — If PostgreSQL is unreachable, the raw event body is appended to `ingestion/fallback_queue.jsonl` so it can be recovered when the DB comes back.
5. **Failed events log** — Any event that fails Bronze insert or Silver routing is recorded in `bronze.failed_events`.

### Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/webhook` | POST | Main event receiver — Edmingle sends all events here |
| `/health` | GET | Returns `{"status": "ok"}` and DB connection status |
| `/status` | GET | Returns recent event counts from Bronze |
| `/failed` | GET | Returns recent `bronze.failed_events` rows |
| `/retry-failed` | POST | Reads `fallback_queue.jsonl` and re-inserts events into Bronze |

### How to Run

```bash
python ingestion/webhook_receiver.py
```

Starts on port 5000. For local development, expose it to Edmingle using ngrok:

```bash
ngrok http 5000
# Then set the ngrok HTTPS URL as the webhook URL in Edmingle settings
```

### Production (systemd)

```ini
[Unit]
Description=Edmingle Webhook Receiver
After=network.target postgresql.service

[Service]
WorkingDirectory=/path/to/repo
ExecStart=/usr/bin/python3 ingestion/webhook_receiver.py
Restart=always
RestartSec=5
```

### Connection Pooling

Uses `psycopg2.ThreadedConnectionPool` with min=2, max=20 connections. This handles concurrent webhook events without exhausting PostgreSQL connections.

### Log Rotation

Uses Python's `RotatingFileHandler` to limit log file size. PII fields (email, phone, name) are masked before writing to the log. Full payloads are stored in Bronze, not in the log.

---

## reprocess_bronze.py

Standalone script that re-processes all Bronze events with `routed_to_silver = false`. Used when a Silver routing bug is fixed and you need to re-push previously received events into Silver.

```bash
python ingestion/reprocess_bronze.py
```

To force-reprocess all Bronze events (not just unrouted ones):

```sql
UPDATE bronze.webhook_events SET routed_to_silver = false;
```

Then run the script again.

A duplicate version of this script also lives in `scripts/external/reprocess_bronze.py` — that version is fully self-contained with no project imports, suitable for running from any directory.
