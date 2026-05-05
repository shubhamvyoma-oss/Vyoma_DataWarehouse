# Bronze Layer: Manual Data

## What is this?
The **Manual** folder in the Bronze layer holds data that is uploaded by a person, usually from **CSV files** (like Excel spreadsheets). 

This includes things that aren't easily available through the API, such as student export lists or custom course tracking sheets.

## Why do we need it?
Not all data comes to us automatically. Sometimes we have to manually export information from Edmingle or other tools. This folder gives that manual data a home in our database so it can be combined with our automatic data.

## How it works (Step-by-Step)
1. **Export:** A person downloads a CSV file from Edmingle (like "Student Export").
2. **Upload:** A Python script reads that CSV file.
3. **Load:** This SQL script creates a "Raw" table in the `bronze` schema.
4. **Result:** The manual spreadsheet data is now safely stored in our database.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Missing Columns** | The CSV file doesn't have the columns the script expected. | Check if the CSV was exported correctly and hasn't been edited. |
| **Encoding Error** | The computer is having trouble reading special characters. | Save the CSV file as "CSV UTF-8 (Comma delimited)" in Excel. |
| **Invalid Date Format** | A date in the spreadsheet looks weird (e.g., 13/13/2024). | Fix the date format in the spreadsheet before uploading. |
