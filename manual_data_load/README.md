# Manual Data Load

## What does this folder do?
This folder contains simple Python scripts that take data from **CSV files** (which are like Excel spreadsheets) and put them into our **Database**. 

Think of this process like moving books from a messy box into a organized library. 

## Why do we need it?
Before we had this system, we kept all our student and course information in spreadsheets. To see everything in one place, we need to move that old information into our new database. We call this "backfilling" data.

## A Simple Diagram of the Process

```text
  [ CSV Files ]  --->  [ Bronze Tables ]  --->  [ Silver Tables ]
 (Spreadsheets)        (Raw, Unchanged)         (Clean & Ready)
      |                       |                        |
      |   (Step 1: Load)      |   (Step 2: Clean)      |
      +-----------------------+------------------------+
```

## How it works (Step-by-Step)

### 1. The "Load" Stage (Raw Storage)
We start by copying the spreadsheets exactly as they are into the database. We call this the **Bronze** layer. 
*   `csv_load_bronze.py`: Copies student and enrollment spreadsheets.
*   `csv_load_course_bronze.py`: Copies course catalogue and batch spreadsheets.

### 2. The "Transform" Stage (Cleaning Up)
Next, we take that raw data and clean it up so it's easier to use. For example, we change dates into a format the database likes and turn text like "$100" into the number 100. We save this clean data in the **Silver** layer.
*   `csv_transform_course_silver.py`: Cleans up the course information.
*   `csv_backfill_transactions.py`: Links students to their courses using their emails.

## How to run the scripts
To move the data, you need to run these commands in your terminal (one after the other). 

**Important:** Make sure your database is turned on before you start!

1.  **Load the Raw Data:**
    ```bash
    python csv_load_bronze.py
    python csv_load_course_bronze.py
    ```

2.  **Clean and Link the Data:**
    ```bash
    python csv_transform_course_silver.py
    python csv_backfill_transactions.py
    ```

## What to check after you run them
After each script finishes, read the messages on your screen:
*   **Success:** You should see a message like "Added 50 new rows."
*   **Already Done:** If it says "Added 0 new rows," it usually means the data was already put in the database before.
*   **Database Check:** You can use a tool like pgAdmin to look at the `silver.users` table. You should see student names and emails there now!

## Common Errors Table

| Error Message | What it means | How to fix it |
| :--- | :--- | :--- |
| `File not found` | The script cannot find your CSV file. | Check that your spreadsheets are in the folder named `CSV files`. |
| `Could not connect to database` | The script cannot talk to the database. | Make sure PostgreSQL is running and your password is correct. |
| `Permission denied` | Another program is using the file. | Close the CSV file in Excel before running the script. |
| `Column not found` | The spreadsheet is missing a column. | Make sure you are using the correct, updated CSV files. |
| `Try/Except` messages | The script found a row it didn't understand. | Usually, the script will just skip that one row and keep going. |

## Jargon Buster (Simple Definitions)
*   **CSV:** A simple file that stores table data (like a spreadsheet).
*   **Database:** A powerful system for storing and finding large amounts of information.
*   **Script:** A small computer program that performs a specific task.
*   **SQL:** The language used to talk to databases.
*   **Backfill:** The process of adding old, historical data into a new system.
