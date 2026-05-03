# 04 — Run Course Pipeline

## What it does

Runs scripts 02 and 03 in sequence with a single command, then prints a summary showing whether both steps succeeded.

- Step 1: Course Catalogue (script 02) — fetches what courses exist
- Step 2: Course Batches + Master Table (script 03) — fetches batch details, rebuilds the master table

## Why we need it

Power BI reads `silver.course_master` which depends on both catalogue data (script 02) AND batch data (script 03) being fresh. Running them together guarantees they are always in sync.

## How it works

```
python 04_run_course_pipeline/run_course_pipeline.py
  │
  ├── subprocess: python 02_fetch_course_catalogue/fetch_course_catalogue.py
  │     │
  │     └── [waits for it to finish]
  │
  └── subprocess: python 03_fetch_course_batches/fetch_course_batches.py
        │
        └── [waits for it to finish]
```

Each script runs as a separate process. If one fails, the script reports it clearly but still continues with the next step.

## How to run

```bash
python 04_run_course_pipeline/run_course_pipeline.py
```

## What to check after

Both steps should say **SUCCESS** in the output:

```
  Catalogue API     : SUCCESS
  Batches API       : SUCCESS
  Power BI ready    : YES
```

If either step fails, look at the error output from that step above the summary line.

## Schedule

Run daily at **7:00 AM IST**.

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `Course Catalogue FAILED (exit code 1)` | Script 02 crashed | Run script 02 alone and check its error output |
| `Batches + Master Table FAILED (exit code 1)` | Script 03 crashed | Run script 03 alone and check its error output |
| `Power BI ready: NO` | One or both steps failed | Fix the failing step then re-run |
