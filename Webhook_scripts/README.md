# Webhook Ingestion

## What does this do?
This folder contains scripts that help our computer "listen" to messages from another system called **Edmingle**. 

Whenever something happens in Edmingle—like a new student signing up—it sends a message called a **Webhook** to our computer. Our scripts catch these messages and save them safely in our **Database** (a digital filing cabinet).

## Why do we need it?
We want our database to have the latest information automatically. 
- **Without these scripts:** We would have to manually download student lists every day and type them into our system.
- **With these scripts:** Information flows in instantly. When a student pays for a course, our database knows about it a second later!

## Simple Terms Explained
- **Webhook:** A message sent automatically from one website to another when something happens.
- **JSON:** A simple way to organize data using names and values (like `{"name": "John"}`).
- **Database:** A software where we store all our important information in tables.
- **Server:** A program that stays running and waits for other computers to talk to it.
- **Script:** A file containing instructions for the computer to follow.

## How it works (The Big Picture)

```text
  +------------+       +-----------------------+       +-----------------+
  |  Edmingle  | ----> |  webhook_receiver.py  | ----> |  Our Database   |
  | (Website)  |       |    (The Listener)     |       | (Filing Cabinet)|
  +------------+       +-----------------------+       +-----------------+
        ^                         |                           |
        |                         v                           |
        |               +-----------------------+             |
        |               |  reprocess_bronze.py  | <-----------+
        |               |    (The Organizer)    |
        +---------------+-----------------------+
```

## Step-by-Step Process
1. **The Receiver Listens:** The script `webhook_receiver.py` runs a small server. It sits and waits for messages on a specific "doorway" (called a Port).
2. **A Message Arrives:** When someone joins a course, Edmingle knocks on that door and sends a JSON message.
3. **Saving the "First Draft":** We save the message exactly as it is into a table called **Bronze**. This is our backup.
4. **Organizing the Data:** Later (or instantly), we run `reprocess_bronze.py`. It reads the "Bronze" messages, picks out the important bits (like student names), and puts them into clean tables called **Silver**.

## How to run the scripts

### 1. Start the Listener
This script must be running to catch new messages. Open your terminal and type:
```bash
python Webhook_scripts/webhook_receiver.py
```
*Tip: You must keep this terminal window open!*

### 2. Run the Organizer
If you want to move data from the "Bronze" backup to the clean "Silver" tables, run:
```bash
python Webhook_scripts/reprocess_bronze.py
```

## What to check after starting
1. **Check the Health:** Open your web browser and go to `http://localhost:5000/health`. If you see `{"status": "ok"}`, it is working!
2. **Watch the Terminal:** When a message arrives, the terminal might show new lines or errors.
3. **Check the Backup File:** If the database is broken, look for a file called `webhook_backup.jsonl`. It holds the messages we couldn't save to the database.

## Common Errors Table

| Error Message | What it means | How to fix it |
| :--- | :--- | :--- |
| `Database connection failed` | The database is turned off or the password is wrong. | Make sure PostgreSQL is running and check the password in the script. |
| `Address already in use` | Another program is already using the "doorway" (Port 5000). | Close any other running Python windows and try again. |
| `ModuleNotFoundError` | You are missing some Python tools. | Type `pip install flask psycopg2` in your terminal. |
| `Not JSON` | The message received was not in the right format. | This usually means something went wrong at Edmingle. Check the `failed_events` table. |
| `Failed to process...` | The Organizer couldn't understand one specific message. | Look at the error message in the terminal. It might be missing a student ID. |
