# PDFs/

Edmingle API documentation provided by the Edmingle support team.

---

## Files

| File | Contents |
|---|---|
| `Edmingle API_01.pdf` | Complete Edmingle REST API reference (118 pages) |
| `Edmingle API_02.pdf` | Identical copy of the same document |

---

## Key Endpoints Documented (relevant to this pipeline)

| Endpoint | What it does | Used in pipeline |
|---|---|---|
| `GET /report/csv?report_type=55` | Global student + attendance by date range | Yes — `fetch_attendance.py` |
| `GET /short/masterbatch` | All batches (nested under bundles) | Yes — `fetch_course_batches.py` |
| `GET /institute/{id}/courses/catalogue` | All course bundles | Yes — `fetch_course_catalogue.py` |
| `GET /class/attendance?class_id=X` | Per-batch attendance history | Not used (report_type=55 is more efficient) |
| `GET /institution/dataexport?type=5` | Per-student engagement stats | **Not yet implemented** |
| `GET /institution/dataexport?type=8` | Material view stats per student | **Not yet implemented** |
| `GET /institution/dataexport?type=10` | Exercise completion stats | **Not yet implemented** |
| `GET /reports/enrollment` | Enrollment trends per course | **Not yet implemented** |
| `GET /reports/sales` | Revenue report | **Not yet implemented** |
| `GET /ses/listlogs/timebased` | Email delivery logs | **Not yet implemented** |

---

## report_type=55 Attendance Status Codes

As documented in the PDF:

| Code | Meaning |
|---|---|
| `P` | Present |
| `L` | Late |
| `A` | Absent |
| `-` | Not yet marked |
| `E` | Excused |
| `OL` | On Leave |
| `NA` | Not Applicable |

Our pipeline counts `P` and `L` as present in `attendance_pct`. `E`, `OL`, `NA`, and `-` are stored in Bronze but excluded from the Silver aggregation denominator.

---

## Data Export Types (type parameter for /institution/dataexport)

| Type | Description | Notes |
|---|---|---|
| 1 | Course bundle list | We use the catalogue API instead |
| 2 | Course list | We use the catalogue API instead |
| 3 | Batch listing | We use the masterbatch API instead |
| 4 | Quiz details | Not implemented |
| 5 | User stats (engagement per student) | High value — not yet implemented |
| 6 | User stats for quiz | Not implemented |
| 7 | User stats for section | Not implemented |
| 8 | Material view stats | High value — not yet implemented |
| 9 | Exercise list | Not implemented |
| 10 | User stats for exercise | High value — not yet implemented |
| 11 | Course bundle teaching material list | Not implemented |
| 12 | Class list | We use the masterbatch API instead |
| 18 | Schedule class attendance | Documented as "not available" |
