# database/gold/

Gold is the reporting layer. It contains SQL VIEWs that join and aggregate Silver tables into shapes that Power BI consumes directly. Gold views contain no new data — everything comes from Silver. Views are always fresh and require no ETL scheduling.

---

## Subfolders

| Folder | Views | Power BI use |
|---|---|---|
| `webhook/` | Course and enrollment analytics views | Course catalogue, learner breakdown, term/funnel/SSS dashboards |
| `api/` | Attendance analytics views | Batch attendance trends, first vs last class drop-off |

---

## webhook/

**File**: `gold_views.sql`

Nine views built from `silver.course_metadata`, `silver.transactions`, `silver.course_lifecycle`:

| View | Description |
|---|---|
| `gold.course_summary` | One row per course-batch: all classification dimensions + enrollment counts (total / live webhook / historical CSV) |
| `gold.learner_summary` | New vs returning learners per course (new = first-ever enrollment, returning = enrolled before) |
| `gold.course_type_summary` | Enrollment counts grouped by course type and status |
| `gold.launch_type_summary` | Enrollment counts grouped by launch type (Repeat / Reopen / Relaunch) |
| `gold.subject_summary` | Enrollment and course counts by academic subject |
| `gold.learning_model_summary` | Vyoma's pyramid: Bhashadhyayanam / Granthadhyayanam / Shastradhyayanam / Viniyoga |
| `gold.term_summary` | New and returning learners grouped by term length (Very Short / Short / Mid / Long) |
| `gold.funnel_summary` | New and returning learners grouped by position in funnel |
| `gold.sss_domain_summary` | Enrollment counts in the Samskrta / Samskara / Samskriti Venn diagram |

All views filter out Vyoma staff rows using `WHERE email NOT LIKE '%@vyoma%'`.

---

## api/

**File**: `attendance_views.sql`

Four views built from `silver.class_attendance`, `silver.course_metadata`, `silver.course_batches`:

| View | Description |
|---|---|
| `gold.batch_attendance_summary` | One row per batch: avg attendance %, total classes held, first/last class present counts |
| `gold.bundle_attendance_summary` | One row per course bundle: aggregated across all batches |
| `gold.attendance_by_year` | Attendance metrics aggregated by calendar year |
| `gold.first_vs_last_class` | Drop-off analysis: first class attendance vs last class attendance per batch |

---

## Why Views Instead of Tables?

Gold uses SQL VIEWs rather than physical ETL tables because:
- All data already exists in Silver in a clean, typed form
- VIEWs are always fresh — no ETL job needed to keep them in sync
- Any number in a Power BI dashboard can be traced directly back to Silver
- At current data volumes (93K users, 424K transactions), live queries against Silver are fast enough
