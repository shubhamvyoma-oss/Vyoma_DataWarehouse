# 14 — Test Webhook Send

## What it does

Sends a single test event to the running webhook server to confirm it is accepting events correctly. The event simulates a `user.user_created` notification.

## Why we need it

Quick sanity check after starting or restarting the webhook server. Confirms the server is up, accepts events, and routes them to Silver.

## How to run

```bash
# Start the webhook server first (in another terminal):
python 06_webhook_receiver/webhook_receiver.py

# Then run the test:
python 14_test_webhook_send/test_webhook_send.py
```

## Example output

```
=== test_webhook_send.py ===

Sending test event to http://localhost:5000/webhook ...
  Event ID   : test-event-001
  Event type : user.user_created
  User ID    : 99999
  Email      : test@example.com

Response:
  HTTP status : 200
  Body        : {"status": "ok", "event_id": "test-event-001"}

Test event sent successfully.
```

## What to check after

- HTTP status should be **200**
- Check `silver.users` has a new row for `user_id = 99999`
- Run `python 12_check_db_counts/check_db_counts.py` to verify

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Connection refused` | Webhook server is not running | Start it with `python 06_webhook_receiver/webhook_receiver.py` |
| `HTTP 500` | Server error | Check the server terminal for the error traceback |
| `HTTP 400` | Bad request format | Check the event payload structure in the script |
