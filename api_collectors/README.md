# API Data Collection Center

Welcome! This folder is like a "post office" for our data. It contains programs that automatically go to the Edmingle website, pick up our course and attendance information, and bring it home to our database.

## 1. What does this do?
These programs (we call them "scripts") are designed to:
- Connect to the Edmingle system over the internet.
- Ask for lists of courses, batches, and student attendance records.
- Clean up the information so it is neat and tidy.
- Save the final information into our database so we can make reports later.

## 2. Why do we need this?
Imagine if you had to log into a website and download 50 different files every single morning. You might get tired, forget a file, or make a mistake!
- **Automation:** The computer does the boring work for us.
- **Accuracy:** The computer doesn't make "copy-paste" mistakes.
- **Speed:** What takes a human an hour takes the computer a few seconds.

## 3. How it works (The Big Picture)

Here is a simple diagram showing how the data moves:

```
[ Edmingle Website ]  <-- (1) Script asks for data
       |
       | (2) Data travels over the internet
       v
[ Our Python Script ] <-- (3) Script cleans and fixes the data
       |
       | (4) Script saves data into the "Bronze" table (Raw Backup)
       v
[ Our Database (SQL) ]
       |
       | (5) Script organizes data into the "Silver" table (Ready for Reports)
       v
[ Final Power BI Report ]
```

### Step-by-Step Details:
1.  **The Request:** The script uses a "Secret Key" (like a password) to tell Edmingle it's allowed to see our data.
2.  **The Download:** Edmingle sends the data back in a format called "JSON" (which looks like a long list of notes).
3.  **The Cleaning:** The script removes extra spaces, fixes dates that look weird, and handles missing information.
4.  **Bronze Save:** We save a "Raw" copy first. This is like keeping the original receipt just in case.
5.  **Silver Save:** We organize the data into clean rows and columns. This is what we use to build our charts and graphs.

## 4. How to run the programs
You can run these programs using your Command Prompt or Terminal.

### To get Attendance information:
- **Normal Run (get yesterday's data):**
  `python fetch_attendance.py`
- **Get data for a specific day:**
  `python fetch_attendance.py --date 2024-05-15`
- **Get data for a whole month:**
  `python fetch_attendance.py --start 2024-05-01 --end 2024-05-31`

### To get Course and Batch information:
- **Run everything at once (recommended):**
  `python run_course_pipeline.py`

## 5. What to check after you run a script
Always look at the text that appears on your screen. 
- **Success:** Look for words like **"SUCCESS"**, **"DONE"**, or **"PIPELINE FINISHED"**.
- **Numbers:** It will tell you how many records it saved (e.g., "Bronze Table Updated: 125 records").
- **Errors:** If you see red text or the word "FAILED", something went wrong. Check the table below!

## 6. Common Errors (and how to fix them)

| Error Message | What it usually means | How to fix it |
| :--- | :--- | :--- |
| **Unauthorized (401)** | Your Secret API Key is wrong or has expired. | Ask your manager for the latest API Key and update it in the script. |
| **Network Error** | Your computer is not connected to the internet. | Check your Wi-Fi or plug in your internet cable and try again. |
| **Database Connection Failed** | Your database "Postgres" is turned off. | Open your database software and make sure it is "Running". |
| **No courses found** | There was no new data to download for that day. | This might be normal if there were no classes that day! |
| **File not found** | You are running the command from the wrong folder. | Make sure you are inside the `api_collectors` folder. |

---
**Fun Tip:** We have added comments to almost every line of code in these files. If you open them with a text editor (like Notepad or VS Code), you can read exactly how the computer "thinks" step-by-step!
