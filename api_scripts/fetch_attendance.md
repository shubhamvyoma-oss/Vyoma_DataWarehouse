# fetch_attendance.py

Pulls daily student attendance from Edmingle's `report_type=55` endpoint and stores it in the Bronze and Silver layers.

---

## API Endpoint

```
GET /report/csv
  ?report_type=55
  &organization_id=683
  &start_time=<unix IST 00:00:00>
  &end_time=<unix IST 23:59:59>
  &response_type=1
```

One call per day. Returns a JSON array of objects — one object per student per class session that fell within the time window. The API returns students across all active batches simultaneously.

---

## Attendance Status Codes

| Code | Meaning | Counted as |
|---|---|---|
| `P` | Present | Present (in `present_count`, in `attendance_pct` numerator) |
| `L` | Late | Present (in `late_count`, in `attendance_pct` numerator) |
| `A` | Absent | Absent (in `absent_count`) |
| `-` | Not yet marked | Excluded from Silver aggregation |
| `E` | Excused | Excluded from Silver aggregation |
| `OL` | On Leave | Excluded from Silver aggregation |
| `NA` | Not Applicable | Excluded from Silver aggregation |

---

## Bronze Table: `bronze.attendance_raw`

**Unique key**: `(student_id, class_id)` — one row per student per class session. Re-pulling the same date updates the attendance status in-place via `ON CONFLICT DO UPDATE`. This means pulling data for a day after teachers have marked attendance will correctly change `-` to `P` or `A`.

Key fields stored:
- `student_id`, `student_name`, `student_email`, `reg_no`
- `batch_id`, `batch_name`, `class_id`, `class_name`
- `bundle_id`, `bundle_name`, `course_id`, `course_name`
- `attendance_id`, `teacher_id`, `teacher_name`, `teacher_email`
- `attendance_status` (P / L / A / - / E / OL / NA)
- `class_date` (raw string: "16 Mar 2026"), `class_date_parsed` (DATE)
- `start_time`, `end_time`, `class_duration`
- `student_rating`, `student_comments`
- `raw_payload` (full JSON object)

Staff rows (email contains `@vyoma`) are silently skipped and not inserted into Bronze.

---

## Silver Table: `silver.class_attendance`

**Unique key**: `(batch_id, class_date)` — one row per batch per class date. Aggregated from Bronze.

Aggregation formula per batch per date:
- `present_count` = rows where `attendance_status = 'P'`
- `late_count` = rows where `attendance_status = 'L'`
- `absent_count` = rows where `attendance_status = 'A'`
- `attendance_pct` = `(present + late) / (present + late + absent) * 100`

`class_number` is computed as `ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY class_date ASC)` — the sequence number of each class within its batch, recomputed after every Silver upsert.

---

## Command-line Usage

```bash
# Pull yesterday (default daily run)
python api_scripts/fetch_attendance.py

# Pull one specific date
python api_scripts/fetch_attendance.py --date 2026-03-16

# Backfill a range
python api_scripts/fetch_attendance.py --start 2024-01-01 --end 2026-04-30

# Dry run (prints API structure, no DB writes)
python api_scripts/fetch_attendance.py --date 2026-03-16 --dry-run
```

---

## Rate Limiting

25 calls per minute. The script automatically sleeps 60 seconds after every 25 dates. This is printed to the log:
```
Rate limit: sleeping 60s after 25 calls ...
```

---

## Logging (backfill)

When running as a background process, redirect output:
```bash
python -u api_scripts/fetch_attendance.py --start 2024-01-01 --end 2026-04-30 > logs/attendance_backfill.log 2> logs/attendance_backfill.err
```

Progress is printed every 10 dates and at every rate-limit sleep.

---

## IST Timestamp Calculation

The API requires Unix timestamps in IST (UTC+5:30). The script calculates:

```python
IST   = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
day   = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
start = int(day.replace(hour=0,  minute=0,  second=0).timestamp())   # IST midnight
end   = int(day.replace(hour=23, minute=59, second=59).timestamp())  # IST end of day
```

---

## Error Handling

- Each date is retried 3 times with 10-second delays between attempts.
- 401 Unauthorized stops immediately (API key has expired).
- Failed dates are collected and printed in the final summary.
- The error log (`attendance_backfill.err`) captures any uncaught exceptions.
