# Gold Layer: API Views (Ready for Power BI)

## What is this?
The **API** folder in the Gold layer contains "Views." A View is like a saved search. It combines data from different Silver tables to create a final report that is easy for Power BI to read.

## Why do we need it?
Power BI works best when it has one simple table to look at. Instead of asking Power BI to do complex math or combine tables, we do it here in the Gold layer. This makes our reports faster and more accurate.

## How it works (Step-by-Step)
1. **Combine:** The script joins different tables (e.g., matching "Students" with their "Attendance").
2. **Calculate:** It does the math (e.g., calculating "Attendance Percentage").
3. **Format:** It gives columns nice names (e.g., renaming `std_id` to `Student ID`).
4. **Publish:** It creates a "View" that Power BI can see.
5. **Result:** A beautiful, easy-to-use report is ready!

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **View takes too long to load** | The math or the join is too complicated. | We might need to "materialize" the view (save it as a table) to speed it up. |
| **Missing Column in Power BI** | A column was renamed or deleted in the SQL script. | Check the SQL script and update the Power BI report to use the new name. |
| **Incorrect Calculations** | The math in the view is wrong (e.g., dividing by zero). | Update the SQL script logic to handle special cases. |
