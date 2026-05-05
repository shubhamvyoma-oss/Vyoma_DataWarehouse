# Silver Layer: Manual Data (Cleaned)

## What is this?
The **Manual** folder in the Silver layer cleans and organizes the spreadsheet data that was uploaded to the Bronze Manual folder.

## Why do we need it?
Manual spreadsheets often have typos, missing information, or inconsistent formats. This step ensures that the manual data is high-quality and matches the format of our other data.

## How it works (Step-by-Step)
1. **Extract:** The script reads from the `bronze` Manual tables.
2. **Standardize:** It makes sure things like "Phone Numbers" all look the same.
3. **Filter:** It removes empty rows or tests that shouldn't be in the final report.
4. **Load:** It saves the perfect data into the `silver` schema.
5. **Result:** Manual data is now just as clean as our automatic data.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Invalid Date** | A date in the manual file was entered incorrectly. | Correct the date in the original CSV and re-upload to Bronze. |
| **Duplicate ID** | The same student appears twice in the manual list. | The script will try to pick the best one, or you can clean the CSV. |
| **Lookup Failed** | We couldn't match a student in the manual list to our main user list. | Check if the student's email or ID is correct in the CSV. |
