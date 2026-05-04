# 13 — Clear Test Data

## What it does

Deletes test data rows (rows with test event IDs or test email addresses) from Bronze and Silver tables. Shows a preview first and asks for confirmation before deleting anything.

## Why we need it

When running test scripts (16, 17, 18), fake events are written to the database. Before showing the data to stakeholders or connecting Power BI, you should clean up these test rows so they don't distort metrics.

## How to run

```bash
python 13_clear_test_data/clear_test_data.py
```

The script will show a count of rows to be deleted and ask:
```
Are you sure you want to delete these rows? (y/n):
```

Type `y` and press Enter to confirm. Type `n` or just press Enter to cancel.

## What gets deleted

Rows matching any of these patterns:

- `event_id` starting with `e2e-`, `dupe-`, `conc-`, `resilience-`, `constraint-`, `dbtest-`
- `email` matching `*@test.com`, `*@example.com`
- `user_id` in the test range 92000000–99999999

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `Cancelled — nothing was deleted` | User typed 'n' | Normal — this is the safety check working |
