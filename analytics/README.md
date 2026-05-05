# Analytics Tools

This folder contains simple computer scripts that help us understand our data. It takes messy data and turns it into clean reports that humans can read and understand.

## What it does
These scripts do two main things:
1. **Build Reports:** They combine different lists of data (like student lists and course lists) into one big "Master List".
2. **Analyze Data:** They count things, like how many students we have, which courses are the most popular, and how much revenue (money) we have made.

## Why we need it
We have data in many different places. It is hard to see the "big picture" just by looking at one list at a time. These tools bring everything together so we can make better decisions for Vyoma Samskrta Pathasala.

## How it works (The Big Picture)

```text
  [ Raw Data Tables ]       [ Merge Script ]       [ Analysis Script ]
  +-----------------+      +------------------+     +-------------------+
  | - Courses List  | ===> | build_courses.py | ==> |  run_analysis.py  |
  | - Batches List  |      +------------------+     +-------------------+
  | - Student List  |               ||                       ||
  +-----------------+               \/                       \/
                           [ Master Report ]        [ Final Text Report ]
                           (One big table)          (Shown on screen)
```

## Step-by-Step Process
1. **Connect:** The script talks to the database (a digital filing cabinet where all our data is stored).
2. **Check:** It makes sure all the lists it needs are ready to use.
3. **Combine (Merge):** It takes information from different lists and matches them up. For example, it matches a student's name with the course they are taking.
4. **Calculate:** It does math to find averages and totals (like total revenue or average attendance).
5. **Report:** It prints everything out on the screen in a clean format for you to read.

## How to run it
You need to have Python installed on your computer.

1. Open your terminal (the black screen where you type commands).
2. To build the big master lists, type:
   ```
   python analytics/build_courses.py
   ```
3. To see the analysis report on your screen, type:
   ```
   python analytics/run_analysis.py
   ```

## What to check after
- After running `build_courses.py`, it should say "Success!" and show you how many rows it created.
- After running `run_analysis.py`, you should see a long report with 10 different sections. Check if the numbers (like "Total unique courses") look reasonable.

## Common Errors
| Error | What it means | How to fix it |
|-------|---------------|---------------|
| "Could not connect to database" | The script cannot talk to the database. | Check if your database is running and if the password in the script is correct. |
| "Missing table" | A list of data was not found. | Make sure you have run the earlier setup and data load scripts first. |
| "ModuleNotFoundError" | A Python tool (like `psycopg2`) is missing. | You need to install the tool by typing `pip install psycopg2-binary`. |
| "Permission denied" | The computer is blocking the script. | Try running your terminal as an "Administrator". |

## Technical Terms Explained
- **Database:** A digital filing cabinet where we store all our information.
- **SQL:** A special language used to talk to databases and ask them questions.
- **Script:** A file containing instructions that tells the computer what to do.
- **Table:** A list of data with rows and columns, very much like an Excel sheet.
- **Merge:** The process of joining two or more tables together into one bigger table.
- **Revenue:** The total amount of money received for our courses.
