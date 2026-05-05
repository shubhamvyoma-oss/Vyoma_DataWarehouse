# run_course_pipeline.py

Orchestrates the full course data refresh: catalogue → batches → master table. Run this daily to keep course metadata and batch records up to date for Power BI.

---

## Steps

1. **Step 1 — Course Catalogue**: Calls `fetch_course_catalogue.main()` which pulls all bundles from the API, upserts `bronze.course_catalogue_raw`, and promotes to `silver.course_metadata`.

2. **Step 2 — Course Batches + Master**: Calls `fetch_course_batches.main()` which pulls all batches, upserts `bronze.course_batches_raw`, promotes to `silver.course_batches`, and fully rebuilds `silver.course_master`.

---

## Output

```
=============================================
COURSE DATA PIPELINE
=============================================

--- Step 1: Course Catalogue ---
  ...

--- Step 2: Course Batches + Master ---
  ...

  COURSE PIPELINE COMPLETE
  -----------------------------------------
  Catalogue API     : SUCCESS
  Batches API       : SUCCESS
  Silver rows       : 541 course_metadata, 995 course_batches
  Master table      : 995 rows rebuilt
  Power BI ready    : YES
  Run time          : 14.3s
  -----------------------------------------
  Next run: schedule daily at 7:00 AM IST
```

---

## Usage

```bash
python api_scripts/run_course_pipeline.py
```

---

## Scheduling (Windows Task Scheduler or cron)

```bash
# cron example — 7:00 AM IST daily
30 1 * * * cd /path/to/repo && python api_scripts/run_course_pipeline.py >> logs/course_pipeline.log 2>&1
```

*(7:00 AM IST = 01:30 UTC)*
