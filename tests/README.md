# tests/

End-to-end and integration test suites. The webhook receiver must be running on localhost:5000 before running most tests.

---

## Files

| File | Tests | Run time |
|---|---|---|
| `test_pipeline_e2e.py` | 38 tests covering the full webhook pipeline | ~30 seconds |
| `test_db_unavailability.py` | Simulates a complete PostgreSQL outage | ~60 seconds |
| `test_all_events.py` | Sends one event of every type and verifies Silver routing | ~10 seconds |

---

## test_pipeline_e2e.py

The main test suite. Covers:

- HMAC signature validation (accepts valid, rejects tampered)
- Duplicate event deduplication (`event_id` uniqueness enforced)
- All 16 Edmingle event types routed to correct Silver tables
- Silver upsert logic (second event for same entity merges, not duplicates)
- Bronze `routed_to_silver` flag set after successful Silver write
- `/failed` endpoint returns failed event records
- Concurrent request handling (thread safety)

Expected result: `38/38 PASS`

```bash
# In terminal 1
python ingestion/webhook_receiver.py

# In terminal 2
python tests/test_pipeline_e2e.py
```

---

## test_db_unavailability.py

Tests the fallback queue mechanism for complete PostgreSQL outages. Steps:

1. Sends 5 baseline events (DB up) — verifies Bronze insert
2. Blocks all DB connections: `ALTER DATABASE edmingle_analytics ALLOW_CONNECTIONS false`
3. Sends 10 events while DB is down — verifies server returns HTTP 200 (no crash)
4. Restores DB connections
5. Calls `POST /retry-failed` to recover events from `fallback_queue.jsonl`
6. Verifies all 10 events landed in Bronze

Expected result: `DB UNAVAILABILITY TEST PASS` with `Data lost: 0`

**Note**: This test temporarily blocks all database connections. Do not run it on a production database.

---

## test_all_events.py

Sends one synthetic event of each of the 16 supported event types and verifies that the correct Silver table has a new row. Useful for quickly checking that all routes are still working after a code change.

Expected result: all event types routed successfully.

---

## Test Data Cleanup

After running tests, remove test rows:

```bash
python scripts/external/clear_test_data.py
```
