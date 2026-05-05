# fetch_course_catalogue.py

Pulls all course bundles from the Edmingle catalogue API and stores them in the Bronze and Silver layers.

---

## API Endpoint

```
GET /institute/{INST_ID}/courses/catalogue
Headers: apikey, ORGID
```

Returns all course bundles (not batches) with metadata: name, description, subject, course type, Vyoma-specific custom fields (SSS category, funnel position, adhyayanam category, viniyoga, division, term of course).

---

## Bronze Table: `bronze.course_catalogue_raw`

**Unique key**: `source_row` (0-based row index from the API response). All 59+ fields from the API are stored as TEXT columns. Upserted on every run — re-running does not create duplicates.

Key custom fields specific to Vyoma:
- `subject` — the academic subject area
- `sss_category` — Samskrta / Samskara / Samskriti classification
- `adhyayanam_category` — Bhashadhyayanam / Granthadhyayanam / Shastradhyayanam
- `viniyoga` — whether the course is in the Viniyoga tier
- `position_in_funnel` — Bottom / Lower Middle / Middle / Upper Middle / Top
- `term_of_course` — Very Short / Short / Mid / Long
- `division` — organisational division

---

## Silver Table: `silver.course_catalogue`

One row per bundle. Promotes typed and cleaned columns from Bronze. Key columns: `bundle_id` (BIGINT), `course_name`, `course_type`, `status`, `subject`, `term_of_course`, `position_in_funnel`, `adhyayanam_category`, `sss_category`, `viniyoga`, `division`.

---

## Key Behaviour

- Unwraps the API response by trying keys: `response`, `data`, `courses`, `bundles`, `result`, `items` in order.
- Pagination is handled with `page_context.has_more_page`.
- Returns `(success: bool, bronze_count: int, silver_count: int)` for the orchestrator.

---

## Usage

```bash
python api_scripts/fetch_course_catalogue.py
```

Run daily or on demand. Typically called by `run_course_pipeline.py` as Step 1.
