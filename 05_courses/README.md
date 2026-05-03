# 05_courses — Course Master Merge

This script joins all three course-related Silver tables into two wide reporting tables:
**silver.MasterCourseBatch** and **silver.MasterMerge**.

---

## What the script does

```
python 05_courses/courses.py
```

1. Checks that all source tables exist in the database
2. Runs **Merge 1** → creates `silver.MasterCourseBatch`
3. Prints the row count for MasterCourseBatch
4. Runs **Merge 2** → creates `silver.MasterMerge`
5. Prints the row count for MasterMerge
6. Prints `Done. Both tables are ready.`

The script is safe to re-run. Both tables use `ON CONFLICT (bundle_id, batch_id) DO UPDATE`, so running it again refreshes data in place without creating duplicates.

---

## Merge 1 — silver.MasterCourseBatch

**Goal:** One row per bundle-batch pair, combining course metadata with batch schedule information.

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
```

**Join key:** `silver.course_metadata.bundle_id = silver.course_batches.bundle_id`

**Why three sources?**
- `silver.course_metadata` has the course classification (subject, SSS category, etc.)
- `silver.course_batches` has the batch-level data (dates, tutor, enrollment count)
- `bronze.course_catalogue_raw` has descriptive fields (tutors list, texts, personas, etc.) that were never promoted to Silver because they are not needed for webhook processing — but they are useful for reporting

**Note on `tutord_ids`:** The Bronze catalogue table has a typo in the column name (`tutord_ids` instead of `tutor_ids`). The script reads from `tutord_ids` and stores it as `tutor_ids` in MasterCourseBatch.

---

## Merge 2 — silver.MasterMerge

**Goal:** Add lifecycle performance data to every batch row.

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
```

**Join key:**
- `silver.course_lifecycle.course_id = silver.MasterCourseBatch.bundle_id`
- `silver.course_lifecycle.batch_name = silver.MasterCourseBatch.batch_name`

**Why LEFT JOIN?**
Some batches have no lifecycle record yet (they may be upcoming or the tracker wasn't filled in). A LEFT JOIN keeps every batch row and leaves lifecycle columns as `NULL` when there is no match. This is intentional — missing data is better than missing rows.

**Why pull from Bronze lifecycle too?**
`silver.course_lifecycle` only stores the summary metrics (averages, totals, pass %). Richer fields like the primary teacher name, ELA contact, and assessment dates are only in `bronze.course_lifecycle_raw`. The script joins both to get a complete picture.

---

## Prerequisites

All source tables must exist before running this script:
- `silver.course_metadata` — populated by `api_scripts/fetch_course_catalogue.py`
- `silver.course_batches` — populated by `api_scripts/fetch_course_batches.py`
- `silver.course_lifecycle` — populated by `scripts/migrations/csv_backfill_transactions.py`
- `bronze.course_catalogue_raw` — populated by `scripts/migrations/csv_load_bronze.py`
- `bronze.course_lifecycle_raw` — populated by `scripts/migrations/csv_load_bronze.py`

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
Running Merge 2 — creating silver.MasterMerge...
silver.MasterMerge: 312 rows
Done. Both tables are ready.
```

Row counts will match (MasterMerge is a LEFT JOIN from MasterCourseBatch, so it never has fewer rows).

---

## What to check in pgAdmin

After running:

```sql
-- Check Merge 1: should have one row per batch
SELECT bundle_id, batch_id, bundle_name, batch_name, subject, tutor_name
FROM silver."MasterCourseBatch"
ORDER BY bundle_id, batch_id
LIMIT 20;

-- Check Merge 2: should show lifecycle data where available, NULL where not
SELECT bundle_id, batch_name, type_of_launch, first_class_date,
       avg_attendance_all_classes, total_students_certified, overall_course_rating
FROM silver."MasterMerge"
ORDER BY bundle_id, batch_id
LIMIT 20;

-- Check how many batches have lifecycle data vs. no lifecycle data
SELECT
    COUNT(*) FILTER (WHERE type_of_launch IS NOT NULL) AS with_lifecycle,
    COUNT(*) FILTER (WHERE type_of_launch IS NULL)     AS no_lifecycle
FROM silver."MasterMerge";

-- Verify Bronze tutor_ids came through (note: source column was misnamed tutord_ids)
SELECT bundle_id, tutors, tutor_ids FROM silver."MasterCourseBatch" LIMIT 5;
```
