# Vyoma E-Learning Data Pipeline (A Complete Beginner's Guide)

Welcome! If you are new to programming or data engineering, you are in the right place. This project is a **Data Pipeline**. A data pipeline is like a digital conveyor belt that moves information from one place to another, cleaning it up along the way.

## What is this project for?

Vyoma uses a website called **Edmingle** to run its online courses. Edmingle tracks student names, classes, and payments. However, Edmingle doesn't let us make custom charts and reports.

To solve this, we built this pipeline to:
1. **Collect** data from Edmingle automatically.
2. **Clean** the data so it is neat and organized.
3. **Store** it in our own database (a digital filing cabinet).
4. **Report** on it so we can see how many students are learning!

## How the data flows (The Big Picture)

Here is a simple map of how information travels from the Edmingle website into our database:

```text
[ Edmingle Website ]
       |
       |  (Data comes out in 3 ways)
       |
       V
   +-------------------+       +-------------------+       +-------------------+
   | 1. LIVE MESSAGES  |       | 2. DAILY PULLS    |       | 3. OLD FILES      |
   | (Webhooks)        |       | (API Collection)  |       | (Manual Load)     |
   | Whenever someone  |       | Every night we    |       | We upload old     |
   | joins a course.   |       | ask for updates.  |       | Excel spreadsheets|
   +---------|---------+       +---------|---------+       +---------|---------+
             |                           |                           |
             V                           V                           V
   +---------------------------------------------------------------------------+
   |                        THE DATA PIPELINE (Python)                         |
   |  Our scripts receive the data, clean it, and sort it into folders.        |
   +--------------------------------------|------------------------------------+
                                          |
                                          V
   +---------------------------------------------------------------------------+
   |                        THE DATABASE (PostgreSQL)                          |
   |  BRONZE: The raw, messy data (just in case we need to see the original).  |
   |  SILVER: The clean, polished data (ready for reading).                    |
   |  GOLD:   Final reports (The results used for charts).                     |
   +--------------------------------------|------------------------------------+
                                          |
                                          V
                                [ Power BI Dashboards ]
                                 (Beautiful charts!)
```

## The Folder Structure (The Order of Work)

We have organized our folders to show the logical flow of data:

- **`database/`**: This is where the digital filing cabinet (the database) is built and stored.
- **`Webhook_scripts/`**: This is a "Live Listener" that stays on all day to catch updates as they happen.
- **`api_collectors/`**: These scripts go out every day to fetch the latest course and attendance lists from Edmingle's API.
- **`manual_data_load/`**: Use this if you have old Excel files that need to be put into the system manually.
- **`analytics/`**: This creates the final summary reports and merges that the team uses to make decisions.
- **`tests/`**: These are safety scripts to make sure everything is working correctly.

## How to get started (Step-by-Step)

If you are running this for the first time, follow these steps:

### Step 1: Install the tools
You need to install Python and a database called PostgreSQL. Then, run this command in your terminal to get the extra tools we use:
```bash
pip install flask requests psycopg2-binary pandas
```

### Step 2: Set up the Database
Go into the database folder and run the setup script:
```bash
python database/run_all.py
```

### Step 3: Start the Live Listener
Open a terminal and run the receiver:
```bash
python Webhook_scripts/webhook_receiver.py
```

### Step 4: Run the Daily Pull
To grab the current lists, run:
```bash
python api_collectors/run_course_pipeline.py
```

## Common Errors & Jargon

If you get stuck, look at this table.

| Term / Error | Simple Explanation | How to Fix |
| :--- | :--- | :--- |
| **Database (PostgreSQL)** | A digital filing cabinet for your data. | Make sure the PostgreSQL program is running. |
| **API** | A way for two computers to talk to each other. | Check that your internet is connected. |
| **Webhook** | A "push" message sent instantly when something happens. | Make sure the `Webhook_scripts` receiver is running. |
| `ModuleNotFoundError` | Your computer is missing a tool. | Run the `pip install` command shown in Step 1. |
| `ConnectionRefused` | The script cannot find your database. | Check your database username and password. |

---
*For more details, open any of the folders and read the `README.md` inside them!*
