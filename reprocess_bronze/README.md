# 07 — Reprocess Bronze

## What it does

Reads every event in `bronze.webhook_events` and routes each one to the correct Silver table — exactly the same routing logic as the live webhook server.

After processing, each event is marked `routed_to_silver = true` in Bronze.

## Why we need it

This is a **recovery tool**. Use it when:

- The webhook server was down and some events never made it to Silver
- You changed the Silver table schema and need to re-populate it from existing Bronze data
- You want to verify that all Bronze events are reflected in Silver

## How it works — step by step

```
bronze.webhook_events   (all events, sorted by id)
  │
  │  For each event:
  │    1. Look up event_type in EVENT_ROUTER
  │    2. Parse the raw_payload to extract data fields
  │    3. Call the correct route_* function
  │    4. Mark the event routed_to_silver = true
  │
  ▼
silver.users            (from user.user_created / user.user_updated)
silver.transactions     (from transaction.*)
silver.sessions         (from session.*)
silver.assessments      (from assessments.*)
silver.courses          (from course.user_course_completed)
silver.announcements    (from announcement.*)
silver.certificates     (from certificate.*)
```

Progress is printed every 50 events so you can see it working on large datasets.

## How to run

```bash
python 07_reprocess_bronze/reprocess_bronze.py
```

This is safe to re-run — all Silver INSERTs use `ON CONFLICT ... DO UPDATE SET` so no duplicates are created.

## What to check after

- The output should show `Errors: 0`
- `bronze.webhook_events` rows should all have `routed_to_silver = true`
- Run `python 12_check_db_counts/check_db_counts.py` to verify Silver counts increased

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `Errors: N` | Some events failed to route | Check the error messages above the summary |
| Many events in `Skipped (unknown type)` | Old event types not in EVENT_ROUTER | Expected — url.validate and others are intentionally skipped |
