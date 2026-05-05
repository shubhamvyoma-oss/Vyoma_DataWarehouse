# Database Setup

Welcome! This folder contains all the tools needed to build our "Data Warehouse." 

##  What does this folder do?
This folder builds the digital "filing cabinet" for our school's data. It creates organized tables (which look like spreadsheets) where we can store student names, attendance records, course details, and more.

## Why do we need it?
1.  **Organization**: Instead of having data scattered in different files, we put it all in one safe, central place.
2.  **Speed**: It makes finding information (like "Who missed class today?") very fast, even with thousands of records.
3.  **Cleanliness**: It helps us fix messy data (like misspelled names or empty records) so our final reports are always accurate.

## How it works (The System)

Here is a simple diagram of how the setup works:

```text
      +----------------+
      |  SQL Folders   |  <-- These contain the "Instructions"
      | (setup, bronze,|      Written in SQL (the database language)
      | silver, gold)  |
      +-------+--------+
              |
              | 1. Read Instructions
              v
      +-------+--------+
      |  run_all.py    |  <-- The "Robot" (Python Script)
      | (Python Script)|      that reads and follows the instructions
      +-------+--------+
              |
              | 2. Build Tables
              v
      +-------+--------+
      |   PostgreSQL   |  <-- The "Filing Cabinet" 
      |   Database     |      (Where all our data is safely stored)
      +----------------+
```

### Step-by-Step Process:
We use a 3-step cleaning process called the "Medallion Architecture":

1.  **Setup Phase**: We create the "rooms" (called **Schemas**) in our database to keep different types of data separate.
2.  **Bronze Phase (Raw Stage)**: We create tables for "raw" data. This is data exactly as it comes from the internet, before it is cleaned.
3.  **Silver Phase (Clean Stage)**: We create "clean" tables. The "Robot" follows instructions to fix mistakes and remove duplicates.
4.  **Gold Phase (Final Stage)**: We create "ready-to-use" views. These are special tables designed specifically for making charts and reports.
5.  **Verification**: The script runs a final check to make sure every table was built correctly and is ready for use.

##  How to run it
You only need to run one single script to do everything!

1.  **Open your terminal** (this is the Command Prompt on Windows or Terminal on Mac).
2.  **Navigate to the project folder** (E-Learning-Vyoma-DataWarehouse).
3.  **Run the script** by typing this command and pressing Enter:
    ```bash
    python database/run_all.py
    ```
4.  **Watch the progress**: The script will list each file it runs. When it finishes, it will say "INITIALIZATION COMPLETE".

## What to check after running
*   **Success Messages**: You should see the word "SUCCESS" next to every file name on your screen.
*   **Final Message**: Look for the "INITIALIZATION COMPLETE" message at the very bottom.
*   **Database Tables**: If you use a tool like **pgAdmin**, you should now see new tables inside the `bronze`, `silver`, and `gold` schemas.

## Common Errors (How to fix them)

| Error Message | What it means | How to fix it |
| :--- | :--- | :--- |
| `Could not connect to the database` | The script can't find your PostgreSQL "filing cabinet". | Make sure PostgreSQL is installed and currently running on your computer. |
| `Password authentication failed` | The secret password used to log in is incorrect. | Open `run_all.py` and check the `DB_PASSWORD` line at the top. |
| `No module named 'psycopg2'` | You are missing a "helper" tool that Python needs. | Type `pip install psycopg2` in your terminal and press Enter. |
| `File not found` | The script cannot find its SQL instructions. | Ensure you are running the command while inside the main `E-Learning-Vyoma-DataWarehouse` folder. |
| `ERROR in ...` | There is a mistake in one of the SQL instruction files. | Read the error message below the "ERROR" line; it usually tells you exactly what is wrong. |
