# Silver Layer: Webhook Data (Processed)

## What is this?
The **Webhook** folder in the Silver layer takes the real-time "ping" messages from the Bronze layer and turns them into organized lists of events (like a list of new users or new course sales).

## Why do we need it?
Webhook messages arrive one-by-one and are hard to read. We process them so we can see the "Big Picture"—for example, seeing all transactions in one clean table.

## How it works (Step-by-Step)
1. **Listen:** The script watches the `webhook_events` table in Bronze.
2. **Unpack:** It opens the "message" and extracts the details (User, Amount, Date).
3. **Deduplicate:** If the same notification was sent twice, the script keeps only one.
4. **Load:** It saves the event into the correct table (like `silver.users` or `silver.transactions`).
5. **Result:** Real-time data is now organized and ready for reporting.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Missing Event Type** | A notification arrived for an event we don't recognize. | We may need to update the script to handle new types of events. |
| **JSON Path Error** | The script couldn't find the "price" or "email" inside the message. | Edmingle might have changed how they send their messages. |
| **Delayed Processing** | Events are piling up in Bronze but not appearing in Silver. | Check if the Silver processing script is running or if it's stuck on an error. |
