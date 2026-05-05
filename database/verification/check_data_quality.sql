-- This script helps us check if our data tables were created correctly and have data in them.
-- It is like a "Quality Check" to make sure everything looks right after we run our scripts.

-- ── 1. LIST ALL TABLES ──────────────────────────────────────────────
-- This query shows us a list of all tables that exist in the 'silver' layer.
-- We do this to make sure no tables are missing.
SELECT 
    table_name AS silver_table_name
FROM information_schema.tables
-- We only want to see tables in the silver schema.
WHERE table_schema = 'silver';


-- ── 2. CHECK COURSE BATCH MERGE TABLE ───────────────────────────────
-- This query shows us the names of all columns in the 'course_batch_merge' table.
-- We do this to verify that the table has all the expected fields.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'course_batch_merge';

-- This query shows us the first 10 rows of data in the 'course_batch_merge' table.
-- We do this to see if the data looks correct and isn't empty.
SELECT * 
FROM silver.course_batch_merge 
-- We limit to 10 rows so we don't get too much information at once.
LIMIT 10;


-- ── 3. CHECK COURSE LIFECYCLE TABLE ────────────────────────────────
-- Checking the column names for the 'course_lifecycle' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'course_lifecycle';

-- Checking a sample of 10 rows from the 'course_lifecycle' table.
SELECT * 
FROM silver.course_lifecycle 
LIMIT 10;


-- ── 4. CHECK USERS TABLE ───────────────────────────────────────────
-- Checking the column names for the 'users' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'users';

-- Checking a sample of 10 rows from the 'users' table.
SELECT * 
FROM silver.users 
LIMIT 10;


-- ── 5. CHECK TRANSACTIONS TABLE ────────────────────────────────────
-- Checking the column names for the 'transactions' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'transactions';

-- Checking a sample of 10 rows from the 'transactions' table.
SELECT * 
FROM silver.transactions 
LIMIT 10;


-- ── 6. CHECK ATTENDANCE DATA TABLE ─────────────────────────────────
-- Checking the column names for the 'attendance_data' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'attendance_data';

-- Checking a sample of the most recent 10 rows from the 'attendance_data' table.
SELECT * 
FROM silver.attendance_data 
-- Sorting by class date so we see the newest records first.
ORDER BY class_date DESC 
LIMIT 10;


-- ── 7. CHECK BATCHES DATA TABLE ────────────────────────────────────
-- Checking the column names for the 'batches_data' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'batches_data';

-- Checking a sample of 10 rows from the 'batches_data' table.
SELECT * 
FROM silver.batches_data 
LIMIT 10;


-- ── 8. CHECK COURSE CATALOGUE TABLE ─────────────────────────────────
-- Checking the column names for the 'course_catalogue' table.
SELECT 
    column_name AS field_name
FROM information_schema.columns
WHERE table_schema = 'silver'
AND table_name = 'course_catalogue';

-- Checking a sample of 10 rows from the 'course_catalogue' table.
SELECT * 
FROM silver.course_catalogue 
LIMIT 10;
