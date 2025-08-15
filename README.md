# RFIDZapier

A Python daemon that runs the **Exclusions** and **Submissions** jobs on a 12 hour schedule, leaving its logs in the daemonLogger folder. You can modify its behavior under the Services app in System (search "services" in windows search bar)

---

## Quick start

### 1) Install dependencies
- python-dotenv
- mysql-connector-python
- pandas
- pywin32
- requests

### 2) Create folders
- ./daemonLogger/
- ./Unsuccessful Exclusion Rows/
- ./Unsuccessful Submission Rows/
- ./Load old submission values outputs/

### 3) Add a .env file, add (or adjust using the loadold...py files to get up-to-date) the last_seen file, and adjust the load_dotenv file paths to your specific .env directory
1. Create a .env file with your db credentials for mysql connector to use:
(remember to add .env to .gitignore)

MYSQL_HOST=your-mysql-host
MYSQL_USER=your-username
MYSQL_PASSWORD=your-password
MYSQL_DATABASE=your-database

2. Create exclusion_last_seen.txt and submissions_last_seen.txt to keep track of current position in the database. Simply leave a number in these files as the index of what the program should begin with when next executing. 

Use the two SQL queries below to get the latest exclusion/submission numbers
- SELECT MAX(submission_number) FROM the exclusion table (Exclusion)
- SELECT MAX(submission) FROM the general_form_subs table (Submissions)

3. Change the ```load_dotenv(r"C:\....")``` to your specific directory. These are located in the files: ```main.py and db.py``` .
    
### 4) Starting the daemon
The daemon is defined in daemonRunner.py. Run these commands in an elevated PowerShell (Run as Administrator)

```
# Install the service onto the computer (or update it)
python daemonRunner.py Install

# start / stop / remove the daemon (respectively)
python daemonRunner.py start
python daemonRunner.py stop
python daemonRunner.py remove
```

### 5) Miscellaneous
We have two other files (loadOldSubmissionValues.py and loadOldUPCValues.py) to regain any potentially missed submission and exclusion values. 

Run them with 
- python loadOldSubmissionValues.py
- python loadOldUPCValues.py

To modify the daemon, the content to retrieve exclusions and submissions are in main.py and submissions.py respectively. 



### Here is the structure of the code:
```
RFIDZapier/
├─ daemonRunner.py          # Windows service wrapper (runs both jobs on a schedule)
├─ main.py                  # Exclusions job entrypoint
├─ submissions.py           # Submissions job entrypoint
├─ downloader.py            # Helper(s) for fetching external data (e.g., UPC)
├─ parser.py                # CSV/row parsing helpers
├─ db.py                    # MySQL connection helpers
├─ exclusion_last_seen.txt  # Cursor/checkpoint state (exclusions)
├─ submission_last_seen.txt # Cursor/checkpoint state (submissions)
├─ daemonLogger/            # Logs written here
├─ Unsuccessful Exclusion Rows/
├─ Unsuccessful Submission Rows/
└─ Load old submission values output/

```
