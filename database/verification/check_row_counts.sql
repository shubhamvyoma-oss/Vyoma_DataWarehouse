-- This script acts as a "Health Inspector" for our data warehouse.
-- It counts the total number of rows in every important table.
-- We use this to quickly see if our data ingestion scripts are working or if a table is accidentally empty.

-- We use SELECT and UNION ALL to combine the counts from different tables into one list.

-- 1. Count rows in the raw webhook events table (Bronze layer).
SELECT 
    'bronze.webhook_events' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM bronze.webhook_events

UNION ALL

-- 2. Count rows in the raw attendance data table (Bronze layer).
SELECT 
    'bronze.attendance_raw' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM bronze.attendance_raw

UNION ALL

-- 3. Count rows in the structured users table (Silver layer).
SELECT 
    'silver.users' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM silver.users

UNION ALL

-- 4. Count rows in the structured attendance data table (Silver layer).
SELECT 
    'silver.class_attendance' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM silver.attendance_data

UNION ALL

-- 5. Count rows in the course master merge table (Silver layer).
SELECT 
    'silver.course_master' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM silver.course_batch_merge

UNION ALL

-- 6. Count rows in the course metadata table (Silver layer).
SELECT 
    'silver.course_catalogue' AS database_table_name, 
    COUNT(*) AS total_row_count 
FROM silver.course_catalogue

-- We sort the final list by the table name to make it easy to read.
ORDER BY database_table_name;
