# scripts/external/

Utility and diagnostic scripts for maintaining the database and debugging the pipeline. All scripts are self-contained (no project imports) — they can be run from any directory.

---

## Files

| File | Description |
|---|---|
| `check_db_counts.py` | Prints row counts for every table in the warehouse |
| `clear_test_data.py` | Removes test data inserted by the test suite |
| `reprocess_bronze.py` | Re-routes unprocessed Bronze webhook events to Silver |
| `test_webhook_send.py` | Sends test webhook events to the receiver for manual testing |

---

## check_db_counts.py

Connects to the database and prints a formatted summary of row counts for all Bronze, Silver, and Gold tables. Useful for quickly verifying a pipeline run completed successfully.

```bash
python scripts/external/check_db_counts.py
```

Sample output:
```
bronze.webhook_events          :   470
bronze.studentexport_raw       : 116,113
silver.users                   :  93,284
silver.transactions            : 424,313
...
```

---

## clear_test_data.py

Removes test rows from Bronze and Silver tables. Test data is identified by email patterns (`%test%`, `%load%`) and high synthetic user IDs (`>= 99,990,000`). Shows a count of rows to be deleted and asks for confirmation before proceeding.

```bash
python scripts/external/clear_test_data.py
```

Run this after `tests/test_pipeline_e2e.py` if you want a clean database.

---

## reprocess_bronze.py

Self-contained version of `ingestion/reprocess_bronze.py`. Reads all `bronze.webhook_events` rows where `routed_to_silver = false` and re-routes them through the Silver logic. Use this when a Silver routing bug is fixed and you need to backfill previously received events.

```bash
python scripts/external/reprocess_bronze.py
```

To force-reprocess all Bronze events:
```sql
UPDATE bronze.webhook_events SET routed_to_silver = false;
```
Then run the script.

---

## test_webhook_send.py

Sends HTTP POST requests with test webhook payloads to the receiver running on localhost:5000. Useful for manually verifying that a specific event type is being processed correctly without needing real Edmingle events.

```bash
# Webhook receiver must be running first
python ingestion/webhook_receiver.py

# Then in another terminal
python scripts/external/test_webhook_send.py
```
