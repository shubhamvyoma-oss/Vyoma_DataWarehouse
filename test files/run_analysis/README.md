# 15 — Run Analysis

## What it does

Connects to the database and prints a 10-section business metrics report to the terminal.

| Section | What it shows |
|---------|--------------|
| 1 | Data health — row counts and data freshness |
| 2 | Course catalogue — bundles by subject, type, SSS category |
| 3 | Batches — status breakdown, timeline, enrollment figures |
| 4 | Enrollments — totals, monthly trend, multi-course students |
| 5 | Students — registrations by state and year |
| 6 | Attendance — overall figures, quality breakdown by year |
| 7 | Assessments and certifications — pass rates, ratings |
| 8 | Revenue and pricing — totals, by year, currencies |
| 9 | Live sessions — cancellations, delays, platform breakdown |
| 10 | Top lists — best courses by enrollment, revenue, attendance |

## Why we need it

Quick way to get the big picture without opening Power BI or writing SQL. Good for weekly check-ins and ad-hoc questions.

## How to run

```bash
python 15_run_analysis/run_analysis.py
```

Save the output to a file:

```bash
python 15_run_analysis/run_analysis.py > report.txt
```

## Example output (partial)

```
========================================================================
  VYOMA SAMSKRTA PATHASALA -- DATA PIPELINE ANALYSIS
  Generated: 2024-03-15  09:30:00
========================================================================

========================================================================
  1. DATA HEALTH -- ROW COUNTS & FRESHNESS
========================================================================

  -- Bronze tables
    bronze.webhook_events  (Main event store)           12,847
    bronze.failed_events   (Parse / DB failures)             0
    ...
```

## Common errors

| Error message | Likely cause | Fix |
|---------------|-------------|-----|
| `ERROR: Could not connect to database` | PostgreSQL not running | Start PostgreSQL |
| `-- (no data)` in a section | That Silver table is empty | Run the relevant pipeline scripts first |
