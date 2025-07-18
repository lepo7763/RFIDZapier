import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
databaseExclusion = os.getenv("MYSQL_DATABASE_EXCLUSION")
databaseSubmission = os.getenv("MYSQL_DATABASE_SUBMISSION")

# ---------------------------------------------
# ------------ EXCLUSION FUNCTIONS ------------ 
# ---------------------------------------------

def getExclusionRows(start, end):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseExclusion 
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT DISTINCT submission_id, submission_number, item_file 
                   FROM alec_site.exclusion 
                   WHERE submission_number 
                   BETWEEN %s AND %s""", (start, end)) 
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

def insertExcludedUPCToSQL(submissionID, UPC):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseExclusion 
    )

    cursor = conn.cursor()
    cursor.execute("INSERT INTO alec_site.excluded_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, UPC))
    conn.commit()
    cursor.close()
    conn.close()

def getLatestExclusionSubmissionNumber():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseExclusion 
    )

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(submission_number) FROM alec_site.exclusion") 
    maxNumber = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    return int(maxNumber)

def exclusionLoadLastSeen():
    try: 
        with open("exclusion_last_seen.txt", "r") as f:
            return int(f.read().strip())
        
    except FileNotFoundError:
        return 0
    
def exclusionSaveLastSeen(number):
    with open("exclusion_last_seen.txt", "w") as f:
        f.write(str(number))


# ---------------------------------------------
# ------------ SUBMISSION FUNCTIONS -----------
# ---------------------------------------------

def getSubmissionRows(start, end):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT submission_id, submission, item_file, gtin
                   FROM alec_site.general_form_subs 
                   WHERE submission
                   BETWEEN %s AND %s""", (start, end)) 
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

def insertSubmissionUPCtoSQL(submissionID, value):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("INSERT INTO alec_testing.general_item_file_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, value)) 
    conn.commit()
    cursor.close()  
    conn.close()

def getLatestSubmissionSubmissionNumber():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(submission) FROM alec_site.general_form_subs") 
    maxNumber = cursor.fetchone()[0]
    cursor.close()
    conn.close

    return int(maxNumber)

def submissionLoadLastSeen():
    try: 
        with open("submission_last_seen.txt", "r") as f:
            return int(f.read().strip())
        
    except FileNotFoundError:
        return 0

def submissionSaveLastSeen(number):
    with open("submission_last_seen.txt", "w") as f:
        f.write(str(number))
    