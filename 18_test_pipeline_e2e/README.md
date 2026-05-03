# 18 — Test Pipeline End-to-End

## What it does

Runs a comprehensive 9-test suite against the live webhook server and database to verify the entire pipeline is correct and robust.

| Test | What it verifies |
|------|-----------------|
| 1 | **Data integrity** — no duplicates, no Bronze events missing their Silver rows |
| 2 | **Field mapping** — real API payload fields reach the correct Silver columns |
| 3 | **Duplicate protection** — sending the same event twice produces exactly 1 row |
| 4 | **Failed event recovery** — the `/failed` endpoint shows and clears failed events |
| 5 | **Server resilience** — server restarts cleanly and accepts events immediately |
| 6 | **IST timestamps** — all Silver timestamps show the `+05:30` offset |
| 7 | **NULL audit** — important columns don't have excessive NULL values |
| 8 | **Concurrent load** — 20 simultaneous events are all stored without data loss |
| 9 | **Constraint violation** — a bad event is rejected from Silver but preserved in Bronze |

## Why we need it

Catches regressions after any change to the routing logic, Silver table schemas, or webhook server configuration. Run before deploying changes to production.

## How to run

```bash
# Step 1: Start the webhook server
python 06_webhook_receiver/webhook_receiver.py

# Step 2: In another terminal, run all tests
python 18_test_pipeline_e2e/test_pipeline_e2e.py
```

## Example output (all passing)

```
==============================================================
  Edmingle Webhook Pipeline -- End-to-End Test Suite
==============================================================

  Server healthy at http://localhost:5000

==============================================================
  TEST 1 -- Data Integrity Check
==============================================================
  [PASS]  1a. No duplicate event_ids in Bronze
  [PASS]  1b. bronze.failed_events is empty
  [PASS]  1c. silver.assessments: all routed Bronze events have Silver rows
  ...

==============================================================
  FINAL RESULTS: 32/32 passed, 0 failed
==============================================================
```

## Notes

- **Test 2** (field mapping) only runs if there are real Edmingle events (`is_live_mode=true`) in Bronze. It is silently skipped in a fresh dev environment.
- **Test 5** (server resilience) stops and restarts the server process. After it completes, the server will be running again on port 5000.
- **Test 8** uses Python threads to send 20 events simultaneously. It cleans up after itself.

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Cannot connect to http://localhost:5000` | Webhook server not running | Start it with script 06 |
| `[FAIL] 1a. No duplicate event_ids` | Bronze has duplicates | Check if the UPSERT ON CONFLICT clause is working |
| `[FAIL] 6. All Silver timestamps have IST offset` | Time zone not set correctly | Check `unix_to_ist()` function in the database |
| `[FAIL] 8b. All 20 events stored in Bronze` | Connection pool too small | Increase `maxconn` in the webhook server config |
