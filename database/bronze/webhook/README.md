# Bronze Layer: Webhook Data

## What is this?
The **Webhook** folder in the Bronze layer holds data that is sent to us in **real-time** by Edmingle.

Think of a webhook like a "notification" on your phone. Whenever something happens in Edmingle (like a student signing up), Edmingle immediately sends a "ping" to our server with the details.

## Why do we need it?
Webhooks allow us to see what is happening *right now* instead of waiting for a daily download. We store these notifications exactly as they arrive so we have a record of every event.

## How it works (Step-by-Step)
1. **Event:** Something happens in Edmingle (e.g., a student buys a course).
2. **Ping:** Edmingle sends a message (webhook) to our Webhook Receiver.
3. **Capture:** The Receiver script catches the message.
4. **Load:** This SQL script saves that message into the `webhook_events` table in our `bronze` schema.
5. **Result:** We have a live record of the event.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Duplicate Event** | The same notification was sent twice. | This is common; we use the Silver layer to filter out duplicates. |
| **JSON Parse Error** | The message from Edmingle was scrambled or broken. | Check the `failed_events` table to see what the broken message looked like. |
| **Connection Timeout** | Our server was too busy to hear the notification. | The "Webhook Receiver" script might need to be restarted. |
