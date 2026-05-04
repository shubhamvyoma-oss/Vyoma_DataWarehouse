# 17 — Test DB Unavailability

## What it does

Tests that the webhook server handles database outages gracefully — events must **not** be lost when the database is temporarily unavailable.

The test runs in 5 phases:

| Phase | What happens |
|-------|-------------|
| 1 | Send 5 events while DB is up (baseline check) |
| 2 | Block all new DB connections (`ALTER DATABASE ALLOW_CONNECTIONS false`) |
| 3 | Send 10 events while DB is blocked (these go to disk fallback queue) |
| 4 | Restore DB connections |
| C | Call `/retry-failed` to flush the disk fallback queue into Bronze |
| 5 | Verify all 10 outage events made it into `bronze.webhook_events` |

## Why we need it

Confirms the disk-fallback queue in the webhook server is working. If this test passes, **zero events are lost** during a database outage.

## IMPORTANT — development only

This test uses `ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS false` which affects ALL connections to the database, including any open Power BI or pgAdmin sessions. Only run this in a **development environment**, never on a production server.

## How to run

```bash
# Step 1: Start the webhook server
python 06_webhook_receiver/webhook_receiver.py

# Step 2: In another terminal, run the test
python 17_test_db_unavailability/test_db_unavailability.py
```

## Example output (passing)

```
Phase 1 -- Baseline (DB up): sending 5 events ...
  HTTP 200s received        : 5/5
  Phase 1: PASS

Phase 2 -- Blocking new connections ...
  DB confirmed unreachable: FATAL:  ...

Phase 3 -- Sending 10 events while DB is blocked ...
  HTTP 200s while DB blocked   : 10/10
  Server did NOT crash: YES

Phase 4 -- Restoring DB connections ...
  DB connection restored successfully.

Phase C -- Calling /retry-failed ...
  retried=10  remaining=0

Phase 5 -- Verifying all 10 outage events are in Bronze ...
  Phase-3 events found in bronze: 10/10

  Data lost: 0 (must be zero)

DB UNAVAILABILITY TEST: PASS
```

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Server unreachable` | Webhook server not running | Start it with script 06 |
| `Data lost: N` | Disk fallback queue not configured | Check `FALLBACK_QUEUE_FILE` in the webhook server config |
| `DB still unreachable` in Phase 4 | PostgreSQL took time to accept connections | Script waits 2s — increase if needed |
