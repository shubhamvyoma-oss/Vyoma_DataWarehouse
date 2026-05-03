# database/gold/webhook/

Gold views built over Silver tables that are populated by webhook events and the course API pipeline.

---

## gold_views.sql

Creates 9 views for course and enrollment analytics. All views filter `email NOT LIKE '%@vyoma%'` to exclude staff.

### gold.course_summary

One row per course-batch. Joins:
- `silver.course_metadata` — classification fields (type, subject, SSS category, funnel position, etc.)
- `silver.course_lifecycle` — operational history (first/last class dates, avg attendance, certifications, rating)
- `silver.transactions` — enrollment counts split by source (live webhook vs historical CSV)

Used by most other Gold views as the base layer.

### gold.learner_summary

Classifies each enrollment as `new` (this is the student's first ever course) or `returning` (they enrolled in another course first) using a CTE that finds each user's earliest `start_date_ist` across all transactions.

### gold.course_type_summary

Aggregates `gold.course_summary` by `(course_type, status)`. Outputs `total_courses`, `total_enrollments`, `enrollments_on_fc`, `enrollments_on_lc`.

### gold.launch_type_summary

Aggregates by `type_of_launch` (Repeat / Reopen / Relaunch). Used for the launch type breakdown dashboard page.

### gold.subject_summary

Aggregates by `(subject, status)`. Shows how many courses and enrollments exist per academic subject.

### gold.learning_model_summary

Maps courses to Vyoma's educational model (Bhashadhyayanam / Granthadhyayanam / Shastradhyayanam / Viniyoga) using `COALESCE(CASE WHEN viniyoga = 'True' THEN 'Viniyoga' END, adhyayanam_category)`.

### gold.term_summary

Joins `gold.course_summary` with `gold.learner_summary` to show new vs returning learners per term length category.

### gold.funnel_summary

Same structure as term_summary but grouped by `position_in_funnel`.

### gold.sss_domain_summary

Groups by `sss_category` (Samskrta / Samskara / Samskriti). Used for the SSS domain Venn diagram in Power BI.
