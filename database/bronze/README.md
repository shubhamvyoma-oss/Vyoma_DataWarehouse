# Bronze Layer: The Digital Filing Cabinet

Welcome to the **Bronze Layer** of our data warehouse! 

### What is the Bronze Layer?
Imagine you have a big office. Every day, people bring you stacks of papers: receipts, student forms, and attendance sheets. Some are messy, some have coffee stains, and some are missing information.

The **Bronze Layer** is like a giant **Digital Filing Cabinet**. 
1. We take every "paper" (data) exactly as it arrives.
2. We don't try to fix the coffee stains or fill in missing names yet.
3. We just file them away so we never lose them.

**Rule #1 of Bronze:** Keep the data raw. We don't change anything here. We want to keep a perfect copy of what the original source sent us.

---

### Why do we do this?
- **Safety:** if we ever make a mistake while cleaning the data later, we can always come back here to the "original" and start over.
- **History:** We have a permanent record of what the data looked like the moment it reached us.
- **Flexibility:** If we decide we want to look at the data in a new way next year, we still have the raw files ready to go.

---

### What's inside this folder?
Each `.sql` file here creates a "Drawer" (Table) in our filing cabinet:
- `attendance_raw`: Raw attendance logs.
- `batches_data_raw`: Information about groups of students (batches).
- `course_catalogue_data_raw`: The list of all courses we offer.
- `webhook_events`: Live messages sent to us by the Edmingle system.
- `studentexport_raw`: Profile information for every student.

---

### Common Errors & How to Solve Them

| Error Message | What it Means | How to Fix It |
| :--- | :--- | :--- |
| `Unique constraint violation` | You are trying to file a paper that is already in the drawer. | Check if you are loading the same CSV file twice. |
| `Relation "bronze..." does not exist` | The filing cabinet hasn't been built yet. | Run the `.sql` script to create the table first. |
| `Column "..." does not exist` | You are looking for a folder that isn't in the drawer. | Check the table definition to see the correct column names. |
| `Data type mismatch` | You are trying to put a picture into a text-only folder. | In Bronze, we mostly use `TEXT` or `JSONB` to avoid this! |
| `Permission denied` | You don't have the key to the filing cabinet. | Ask your database administrator for "Write" permissions. |

---

### Remember:
If the data looks messy in Bronze, **that is okay!** We have a "Silver Layer" where we will do all the cleaning, washing, and tidying up. Bronze is just for storage.
