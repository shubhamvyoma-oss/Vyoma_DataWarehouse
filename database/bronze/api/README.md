# Bronze Layer: API Data

## What is this?
The **API** folder in the Bronze layer holds data that is automatically pulled from **Edmingle** using their Application Programming Interface (API). 

Think of an API as a waiter in a restaurant: we ask for specific information (like a list of courses), and the waiter (API) brings it back to us from the kitchen (Edmingle's database).

## Why do we need it?
We need this folder to store the "raw" or "original" version of the data. By keeping a copy of exactly what Edmingle sent us, we can always go back and check if something went wrong during the cleanup process later on.

## How it works (Step-by-Step)
1. **Request:** A Python script asks Edmingle for data (like attendance or course lists).
2. **Download:** The data is downloaded as a JSON file (a common way to share data).
3. **Load:** This SQL script creates a table and puts that raw data into the `bronze` schema in our database.
4. **Result:** We now have a "snapshot" of the Edmingle data inside our own database.

## Common Errors

| Error | What it means | How to fix it |
| :--- | :--- | :--- |
| **Table already exists** | You are trying to create a table that is already there. | This is usually okay; the script often "drops" (deletes) the old table first. |
| **Permission Denied** | You don't have the right "key" to talk to the database. | Make sure your database user has "Owner" or "Write" permissions. |
| **Null values in raw data** | Edmingle sent an empty field for something important. | This is normal for raw data; we will fix or "clean" this in the Silver layer. |
