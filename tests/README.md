# Tests Directory

Welcome! This folder contains "Test Scripts." Think of these as a team of tiny robots that check if our data factory is working correctly.

## What does this folder do?
This folder is where we keep our tools for checking the health of our system. It does two main things:
1. **Sends Test Messages:** It sends "fake" data to our system to see if it handles it correctly.
2. **Checks the Database:** It looks inside the database to make sure the data arrived safely and in the right place.

## Why do we need it?
*   **To find mistakes early:** It's better to find a bug with "fake" data than with a real student's information!
*   **To save time:** These scripts can check 100 things in the time it takes a human to check 1.
*   **To give us confidence:** When all the tests pass, we know our system is ready to use.

## How it works (The Big Picture)

Imagine our system is like a post office. These tests are like sending a test letter to make sure it gets delivered to the right mailbox.

### Simple Diagram
```text
  [ Test Script ] ----> [ Webhook Server ] ----> [ Database ]
      (Sends)               (Receives)            (Stores)
         |                                           ^
         |                                           |
         +----------( Checks if data is here )-------+
```

### Step-by-Step Process:
1.  **Preparation:** You turn on the "Webhook Server" (our receiver).
2.  **Sending:** You run a test script. It creates a fake piece of data (like a new student enrollment).
3.  **Transport:** The script "posts" this data to the server.
4.  **Wait:** We wait a few seconds for the computer to finish its work.
5.  **Verification:** The script asks the database: "Hey, did you get a letter for a student named 'Test Student'?"
6.  **Results:** If the database says "Yes!", the script prints **OK**. If not, it prints **FAIL**.

## How to run the tests

### 1. Make sure the server is on!
Before running tests, you must have the `webhook_receiver.py` script running in another window.

### 2. Open your Terminal
Open your "Command Prompt" or "Terminal."

### 3. Run a Script
Type `python` followed by the name of the script you want to run.

**To run the main test:**
```bash
python tests/test_all_events.py
```

**To see how much data you have:**
```bash
python tests/check_db_counts.py
```

**To clean up your test data:**
```bash
python tests/clear_test_data.py
```

## What to check after running
*   **Success Messages:** Look for words like `OK`, `SUCCESS`, or `200`. These are good!
*   **Error Messages:** If you see `FAIL`, `ERROR`, or `Connection Refused`, something is wrong.
*   **Row Counts:** If you run `check_db_counts.py`, notice if the numbers get bigger after you send data.

## Common Errors & Jargon (Technical Words)

| Word / Error | What it means in simple English | How to fix it |
| :--- | :--- | :--- |
| **Webhook Server** | A program that "listens" for data coming from the internet. | Make sure `webhook_receiver.py` is running! |
| **Database** | A digital filing cabinet where we store all our information. | Make sure PostgreSQL is turned on. |
| `ConnectionRefused` | Your script tried to talk to the server, but nobody answered. | Start your `webhook_receiver.py` script. |
| `OperationalError` | Your script tried to talk to the database, but couldn't get in. | Check if your database password is correct in the script. |
| `ModuleNotFoundError`| You are missing a "plugin" or "library" that Python needs. | Run `pip install requests psycopg2` in your terminal. |

---
*Remember: Don't be afraid of errors! They are just the computer's way of telling you what to fix next.*
