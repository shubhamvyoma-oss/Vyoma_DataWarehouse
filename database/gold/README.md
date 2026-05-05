# Gold Layer: Reporting Views

Welcome to the **Gold Layer**! This is the final stage of our data journey. 

In this folder, we have SQL scripts that create **Views**. You can think of a View as a "Saved Report" or a "Virtual Table". Instead of writing long, complicated queries every time we want to see course performance or student attendance, we save those queries here so we can use them easily in tools like Power BI.

## What's in here?

1.  **`course_views.sql`**: 
    *   Creates views that summarize course information.
    *   Tells us things like: "How many students are in each course?", "Which courses are the most popular?", and "Are students new or returning?"
2.  **`attendance_views.sql`**: 
    *   Creates views that focus on class attendance.
    *   Tells us things like: "What is the average attendance for a batch?" and "Are students dropping off after the first class?"

## Why do we use Views?

*   **Simplicity**: They hide the messy details of joining multiple tables.
*   **Consistency**: Everyone uses the same definition for "Total Enrollments" or "Average Attendance".
*   **Security**: We automatically filter out staff emails (like anyone with `@vyoma`) so our reports only show real student data.

## Common Errors & Solutions

| Error Message | What it means | How to fix it |
| :--- | :--- | :--- |
| `relation "silver.transactions" does not exist` | The View is trying to find a table in the Silver layer that hasn't been created yet. | Run the scripts in the `database/silver` folder first, or use `run_all.sql`. |
| `column "..." does not exist` | A column name in the Silver table was changed or is missing. | Check the Silver table definition and update the View script to match the new name. |
| `permission denied for schema gold` | Your database user doesn't have the right to create things in the Gold folder. | Ask your database administrator for "CREATE" permissions on the `gold` schema. |
| `view ... is not a table` | You are trying to delete or update data directly in a View. | You cannot change data in a View. You must change the data in the original tables (Bronze/Silver). |

## Beginner Tip
If you want to see the results of a view, just run:
```sql
SELECT * FROM gold.course LIMIT 10;
```
It's just like querying a normal table!
