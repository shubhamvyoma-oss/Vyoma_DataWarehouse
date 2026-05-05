# Verification: Data Quality Checks

Welcome to the **Verification** folder! This is where we act like "Health Inspectors" for our data. Before we trust our reports, we use these scripts to make sure the data is complete and accurate.

## What's in here?

1.  **`check_row_counts.sql`**: 
    *   This script counts how many rows (records) are in every table.
    *   **Goal**: To make sure no table is accidentally empty and that data is actually moving from the Bronze layer to the Silver layer.
2.  **`check_data_quality.sql`**: 
    *   This script looks inside the tables to see the column names and a few sample rows.
    *   **Goal**: To verify that the columns exist and that the data inside looks "healthy" (no weird symbols, correct dates, etc.).

## Why is Verification important?

Imagine building a house on a shaky foundation—that's what happens if you build reports on bad data! 
*   **Catching Bugs**: If `silver.users` has 0 rows but `bronze.webhook_events` has 10,000, we know something is wrong with our cleaning script.
*   **Confidence**: It gives the team confidence that the numbers they see in Power BI are real and reliable.

## Common Errors & Solutions

| Error Message | What it means | How to fix it |
| :--- | :--- | :--- |
| `count = 0` (for any table) | The table exists but it is completely empty. | Check if the ingestion script (like a Python collector) was run. Make sure the source data (CSV or API) isn't empty. |
| `relation "..." does not exist` | You are trying to check a table that hasn't been created yet. | Run the setup scripts in `bronze` or `silver` folders first. |
| `column "..." does not exist` | You are looking for a column that has been renamed or deleted. | Run the `check_data_quality.sql` script to see the current list of columns in that table. |
| `unexpected null values` | A column that should have data (like an email or name) is blank. | Investigate the Bronze layer to see if the raw data was missing that information. |

## Beginner Tip
You should run these scripts **every time** you update the database. If the row counts look roughly the same as last time (or higher), you are likely in good shape!
