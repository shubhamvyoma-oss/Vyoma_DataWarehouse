# Silver Layer: Cleaned and Polished Data

Welcome to the **Silver Layer**! This is the second step in our data journey. 

## What is the Silver Layer?

In the first step (Bronze), we collected raw data that was often messy, incomplete, or repetitive. In the **Silver Layer**, we take that raw data and "polish" it. 

Think of it like a kitchen:
*   **Bronze** is the raw vegetables delivered from the farm (they might have dirt on them).
*   **Silver** is when the vegetables are washed, peeled, and chopped, ready for cooking!

### Why do we need Silver?
1.  **No Duplicates:** We use "Upsert" logic. This means if we get the same student info twice, we don't create two rows; we just update the one we already have.
2.  **Standard Dates:** We convert all times to India Standard Time (IST) so everyone is looking at the same clock.
3.  **Correct Types:** We make sure numbers are treated as numbers and dates are treated as dates.
4.  **Readable Names:** We use clear names for columns so anyone can understand what the data represents.

---

## Important Tables in Silver

| Table Name | What it Stores | Why it's Important |
|:---|:---|:---|
| **`users`** | A list of all our students. | Helps us know exactly who our learners are. |
| **`transactions`** | Records of enrollments and payments. | Tells us which students joined which courses and how much they paid. |
| **`sessions`** | Info about live class sessions. | Helps us track when classes happen and how long they last. |
| **`course_lifecycle`**| The "story" of a course batch. | Shows how many students started, finished, and how they felt about the course. |
| **`course_catalogue`**| The "Master" course table. | Combines all course details into one place for easy reporting. |

---

## Common Errors and How to Fix Them

If you run into trouble while working with Silver tables, check this table for help!

| Error Message / Issue | What it Means | How to Fix It |
|:---|:---|:---|
| **`duplicate key value violates unique constraint`** | You are trying to add a row that already exists. | Use an `INSERT ... ON CONFLICT` (Upsert) statement instead of a plain `INSERT`. |
| **`column "..." does not exist`** | You typed the name of a column wrong or it hasn't been added yet. | Check the SQL file for that table to see the exact spelling of the column names. |
| **`invalid input syntax for type numeric`** | You are trying to put text (like "Free") into a price column. | Make sure the data is a clean number before inserting it. |
| **`relation "silver.table_name" does not exist`** | The table hasn't been created yet. | Run the `.sql` file for that table to create it in the database. |
| **`null value in column "user_id" violates not-null constraint`** | You are trying to add a record without a required ID. | Check your source data to make sure every record has a valid ID. |

---

## How to use these files

To create or update these tables in your database, you can run them using a tool like `psql` or through our automation scripts.

Example command:
```bash
psql -d edmingle_analytics -f database/silver/users.sql
```
