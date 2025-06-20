import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")

def getExclusionRows(start, end):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database 
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT submission_id, submission_number, item_file 
                   FROM exclusion 
                   WHERE submission_number 
                   BETWEEN %s AND %s""", (start, end)) 
    rows = cursor.fetchall()

    conn.close()
    return rows

def insertUPCToSQL(submissionID, UPC):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database 
    )

    cursor = conn.cursor()
    cursor.execute("INSERT INTO excluded_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, UPC))
    conn.commit()
    conn.close()

def getLatestSubmissionNumber():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = database 
    )

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(submission_number) FROM exclusion") 
    maxNumber = cursor.fetchone()[0]
    conn.close()

    return maxNumber

def loadLastSeen():
    try: 
        with open("last_seen.txt", "r") as f:
            return int(f.read().strip())
        
    except FileNotFoundError:
        return 0
    
def saveLastSeen(number):
    with open("last_seen.txt", "w") as f:
        f.write(str(number))