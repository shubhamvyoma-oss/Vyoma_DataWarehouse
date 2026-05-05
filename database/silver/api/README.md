# Silver Layer: API Data (Cleaned)

## What is this?
The **API** folder in the Silver layer takes the raw, messy data from the Bronze API folder and "cleans it up." 

It transforms JSON data into a clean, easy-to-read table format with proper columns.

## Why do we need it?
Raw data from an API can be very complicated to read. We clean it so that we can easily use it for reports without having to deal with the messy original format every time.

## How it works (Step-by-Step)
1. **Extract:** The script looks at the raw data in the `bronze` API tables.
2. **Transform:** It picks out the important pieces (like "Course Name" and "Student ID").
3. **Clean:** It fixes things like dates or names that might be formatted strangely.
4. **Load:** It saves this clean data into a new table in the `silver` schema.
5. **Result:** We have a clean, structured table ready for analysis.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Data Type Mismatch** | The script expected a number but found text. | Check if the raw data in the Bronze layer has changed format. |
| **Missing Raw Data** | The Bronze table is empty, so there is nothing to clean. | Run the Bronze API scripts first to download the data. |
| **Key Error** | A specific field name changed in the Edmingle API. | Update the SQL script to use the new field name. |
