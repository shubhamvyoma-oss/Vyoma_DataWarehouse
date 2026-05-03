# 05_courses — Course Master Merge

This script joins all course-related Silver and Bronze tables into two wide reporting tables:
**silver.MasterCourseBatch** and **silver.course_meta_data**.

---

## What the script does

```
python 05_courses/courses.py
```

1. Checks that all source tables exist in the database
2. Runs **Merge 1** → creates `silver.MasterCourseBatch`
3. Prints the row count for MasterCourseBatch
4. Runs **Merge 2** → creates `silver.course_meta_data`
5. Prints the row count for course_meta_data
6. Prints `Done. Both tables are ready.`

The script is safe to re-run. Both tables use `ON CONFLICT (bundle_id, batch_id) DO UPDATE`, so running it again refreshes data in place without creating duplicates.

---

## Computed flags

Both tables include three computed flag columns (values are 0 or 1):

| Column | Value = 1 when… |
|---|---|
| `is_latest_batch` | This batch has the highest `end_date` for its bundle (the most recent batch). Ties broken by `batch_id DESC`. |
| `include_in_course_count` | This batch is either **active** (`batch_status = 'active'`) **or** is the latest batch for its bundle. Use this to avoid double-counting bundles with multiple historical batches. |
| `has_batch` | This bundle has at least one batch record. Always `1` in these tables because both are driven from `silver.course_batches`. |

---

## Merge 1 — silver.MasterCourseBatch

**Goal:** One row per bundle-batch pair, combining course metadata with batch schedule information and three computed flags.

**Join diagram:**

```
silver.course_metadata          silver.course_batches
 bundle_id (UNIQUE)      ─────>  bundle_id  ─┐
 course_name                     batch_id    ├─ UNIQUE (bundle_id, batch_id)
 subject                         bundle_name │
 course_type                     batch_name  │
 status                          batch_status│
 term_of_course                  start_date  │
 position_in_funnel              end_date    │
 adhyayanam_category             tutor_name  │
 sss_category                    admitted_students
 viniyoga
 course_division                 bronze.course_catalogue_raw
 division                        bundle_id (TEXT → cast to BIGINT)
 level                           tutors
 language                        tutord_ids  → stored as tutor_ids
 num_students                    course_ids
 cost                            texts
                                 type
                                 certificate
                                 course_sponsor
                                 number_of_lectures
                                 duration
                                 personas

                         Computed (window function)
                                 is_latest_batch
                                 include_in_course_count
                                 has_batch
```

**Join key:** `silver.course_batches.bundle_id = silver.course_metadata.bundle_id`

**Why three sources?**
- `silver.course_metadata` — course classification (subject, SSS category, funnel position, etc.)
  Populated by `api_scripts/fetch_course_catalogue.py`
- `silver.course_batches` — batch-level data (dates, tutor, enrollment count)
  Populated by `api_scripts/fetch_course_batches.py`
- `bronze.course_catalogue_raw` — descriptive fields (tutors list, texts, personas, etc.) that
  were never promoted to Silver because they are not needed for webhook processing

**How `is_latest_batch` is computed:**
A `ROW_NUMBER()` window function ranks batches within each bundle by `end_date DESC NULLS LAST`.
The row ranked 1 (newest end_date) gets `is_latest_batch = 1`. All others get 0.
When two batches share the same end_date, the higher `batch_id` wins.

**Note on `tutord_ids`:** The Bronze catalogue table has a typo in the column name
(`tutord_ids` instead of `tutor_ids`). The script reads from `tutord_ids` and stores it
as `tutor_ids` in MasterCourseBatch.

---

## Merge 2 — silver.course_meta_data

**Goal:** Add lifecycle performance data and the computed flags to every batch row.
This is the final wide reporting table used by Power BI.

**Join diagram:**

```
silver.MasterCourseBatch
 bundle_id  ─┬──────────────────────────>  silver.course_lifecycle
 batch_name ─┘  (LEFT JOIN)                course_id  (= bundle_id)
                                           batch_name (= batch_name)
                                           type_of_launch
                                           learning_model
                                           persona
                                           first_class_date
                                           last_class_date
                                           enrollments_on_fc
                                           enrollments_on_lc
                                           avg_attendance → avg_attendance_all_classes
                                           total_classes_held
                                           total_certified → total_students_certified
                                           pass_percentage
                                           overall_rating → overall_course_rating

              (second LEFT JOIN)           bronze.course_lifecycle_raw
                                           course_id (TEXT → cast to BIGINT)
                                           batch_name
                                           primary_teacher_name_sfh_link → primary_teacher_name
                                           ela
                                           assessment_start_date
                                           assessment_end_date
                                           total_assessment_attendees

              (propagated from MCB)        is_latest_batch
                                           include_in_course_count
                                           has_batch
```

**Join key:**
- `silver.course_lifecycle.course_id = silver.MasterCourseBatch.bundle_id`
- `silver.course_lifecycle.batch_name = silver.MasterCourseBatch.batch_name`

**Why LEFT JOIN?**
Some batches have no lifecycle record (upcoming courses, or the tracker wasn't filled in).
A LEFT JOIN keeps every batch row; lifecycle columns are `NULL` when there is no match.

**Why pull from Bronze lifecycle too?**
`silver.course_lifecycle` only stores summary metrics (averages, totals, pass %).
Richer fields like the primary teacher name, ELA contact, and assessment dates are only in
`bronze.course_lifecycle_raw`. The script joins both to get a complete picture.

---

## Prerequisites

All source tables must exist and be populated before running this script:

| Table | Populated by |
|---|---|
| `silver.course_metadata` | `api_scripts/fetch_course_catalogue.py` |
| `silver.course_batches` | `api_scripts/fetch_course_batches.py` |
| `silver.course_lifecycle` | `scripts/migrations/csv_backfill_transactions.py` |
| `bronze.course_catalogue_raw` | `scripts/migrations/csv_load_bronze.py` |
| `bronze.course_lifecycle_raw` | `scripts/migrations/csv_load_bronze.py` |

---

## How to run

```bash
# 1. Update the CONFIG block at the top of courses.py with your DB password
# 2. Run the script
python 05_courses/courses.py
```

Expected output:
```
All source tables found.
Running Merge 1 — creating silver.MasterCourseBatch...
silver.MasterCourseBatch: 312 rows
Running Merge 2 — creating silver.course_meta_data...
silver.course_meta_data: 312 rows
Done. Both tables are ready.
```

Row counts will always match — `course_meta_data` is a LEFT JOIN from `MasterCourseBatch`
so it never has fewer rows.

---

## What to check in pgAdmin

After running:

```sql
-- Check Merge 1: one row per batch, with computed flags
SELECT bundle_id, batch_id, bundle_name, batch_name,
       is_latest_batch, include_in_course_count, has_batch
FROM silver."MasterCourseBatch"
ORDER BY bundle_id, end_date DESC NULLS LAST
LIMIT 20;

-- Verify flags: for each bundle, exactly one batch should have is_latest_batch = 1
SELECT bundle_id, COUNT(*) FILTER (WHERE is_latest_batch = 1) AS latest_count
FROM silver."MasterCourseBatch"
GROUP BY bundle_id
HAVING COUNT(*) FILTER (WHERE is_latest_batch = 1) <> 1;
-- Should return 0 rows if flags are correct

-- Check Merge 2 (course_meta_data): lifecycle data where available, NULL where not
SELECT bundle_id, batch_name, is_latest_batch, include_in_course_count,
       type_of_launch, first_class_date, avg_attendance_all_classes,
       total_students_certified, overall_course_rating
FROM silver.course_meta_data
ORDER BY bundle_id, end_date DESC NULLS LAST
LIMIT 20;

-- How many batches have lifecycle data vs. no lifecycle data
SELECT
    COUNT(*) FILTER (WHERE type_of_launch IS NOT NULL) AS with_lifecycle,
    COUNT(*) FILTER (WHERE type_of_launch IS NULL)     AS no_lifecycle
FROM silver.course_meta_data;

-- Verify Bronze tutor_ids came through (note: source column was misnamed tutord_ids)
SELECT bundle_id, tutors, tutor_ids FROM silver."MasterCourseBatch" LIMIT 5;
```
