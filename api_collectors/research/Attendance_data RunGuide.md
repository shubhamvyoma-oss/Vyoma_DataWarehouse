# How to use Date Range to get attendance data
## Formula used for *attendance_pct = ((Present Count + Late Count) / Total Records) × 100*
## Single day
python attendance_data.py --date 2026-05-01

## Last 7 days
python attendance_data.py --start 2026-04-01 --end 2026-04-07

## Whole month of April
python attendance_data.py --start 2026-04-01 --end 2026-04-30