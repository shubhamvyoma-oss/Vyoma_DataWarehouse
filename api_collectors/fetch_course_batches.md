# fetch_course_batches.py

Pulls all batch records from the Edmingle masterbatch API, flattens the nested bundle→batch structure, and stores results in Bronze and Silver. Also rebuilds `silver.course_master` after every run.

---

## API Endpoint

```
GET /short/masterbatch
  ?status=0
  &page=1
  &per_page=100
  &organization_id=683
  &bundle_id=0
Headers: apikey, ORGID
```

The API returns a paginated list of **bundles**, each containing a nested `batch` array. The script iterates all pages until `page_context.has_more_page` is false.

---

## Nested Structure Flattening

The raw API response groups batches under their parent bundle:

```json
{
  "bundles": [
    {
      "bundle_id": 1234,
      "bundle_name": "Sanskrit Foundation",
      "batch": [
        { "class_id": 5678, "class_name": "Batch A 2024", ... },
        { "class_id": 5679, "class_name": "Batch B 2024", ... }
      ]
    }
  ]
}
```

The script promotes `bundle_id` and `bundle_name` onto each batch item and flattens into a list of batch dicts. Note that in Edmingle's API, what they call `class_id` and `class_name` at the batch level are what we call `batch_id` and `batch_name` in our database.

---

## Bronze Table: `bronze.course_batches_raw`

**Unique key**: `source_row` (0-based index from flattened batch list). All fields stored as TEXT. Upserted on every run.

Key fields: `bundle_id`, `bundle_name`, `batch_id`, `batch_name`, `batch_status`, `start_date`, `end_date`, `tutor_id`, `tutor_name`, `admitted_students`.

---

## Silver Table: `silver.course_batches`

One row per batch. Typed columns: `batch_id` (BIGINT), `bundle_id` (BIGINT), `start_date_ist` (TIMESTAMPTZ), `end_date_ist` (TIMESTAMPTZ), `admitted_students` (INTEGER).

Also populates `silver.course_master` by joining `silver.course_catalogue` with `silver.course_batches` and `silver.course_lifecycle` into a single denormalised table that Power BI reads directly.

---

## silver.course_master Rebuild

After every batch upsert, `silver.course_master` is fully rebuilt by truncating and re-inserting from a join of:
- `silver.course_catalogue` — course classification fields
- `silver.course_batches` — batch dates, tutor, enrolled count
- `silver.course_lifecycle` — first/last class dates, ratings, certification counts

Computed flags:
- `is_latest_batch` — 1 if this is the most recent batch for the bundle
- `include_in_course_count` — 1 for active/latest batches
- `has_batch` — 1 if at least one batch exists for the bundle

---

## Usage

```bash
python api_scripts/fetch_course_batches.py
```

Run daily or on demand. Typically called by `run_course_pipeline.py` as Step 2.
