import mysql.connector
import os
from dotenv import load_dotenv
from mysql.connector import pooling, IntegrityError

load_dotenv(r"C:\Users\Ranfe\Music\RFIDZapier\.env") # TODO: change to C:\programdata

HOST = os.getenv("MYSQL_HOST")
USER = os.getenv("MYSQL_USER")
PASSWORD = os.getenv("MYSQL_PASSWORD")
DB = os.getenv("MYSQL_DATABASE")

# connection pool to borrow connections from
POOL = pooling.MySQLConnectionPool(
    pool_name="db_pool",
    pool_size=5,
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB,
    autocommit=True        
)


# ---------------------------------------------
# ------------ EXCLUSION FUNCTIONS ------------ 
# ---------------------------------------------

def getExclusionRows(start, end):
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""SELECT DISTINCT submission_id, submission_number, item_file 
                   FROM alec_site.exclusion 
                   WHERE submission_number 
                   BETWEEN %s AND %s""", (start, end)) 
        return cursor.fetchall()

def insertExcludedUPCToSQL(submissionID, UPC):
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("INSERT INTO alec_site.excluded_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, UPC))

def getLatestExclusionSubmissionNumber():
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT MAX(submission_number) FROM alec_site.exclusion") 
        maxNumber = cursor.fetchone()[0]

    return int(maxNumber)

def exclusionLoadLastSeen():
    try: 
        with open(r"C:\Users\Ranfe\Music\RFIDZapier\exclusion_last_seen.txt", "r") as f:
            return int(f.read().strip())
        
    except FileNotFoundError:
        return 0
    
def exclusionSaveLastSeen(number):
    with open(r"C:\Users\Ranfe\Music\RFIDZapier\exclusion_last_seen.txt", "w") as f:
        f.write(str(number))


# ---------------------------------------------
# ------------ SUBMISSION FUNCTIONS -----------
# ---------------------------------------------

def getSubmissionRows(start, end):
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""SELECT submission_id, submission, item_file, gtin
                    FROM alec_site.general_form_subs 
                    WHERE submission
                    BETWEEN %s AND %s""", (start, end)) 
        return cursor.fetchall()


def insertSubmissionUPCtoSQL(submissionID, value):
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("INSERT INTO alec_site.general_item_file_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, value)) 
    

def getLatestSubmissionSubmissionNumber():
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT MAX(submission) FROM alec_site.general_form_subs") 
        maxNumber = cursor.fetchone()[0]
    return int(maxNumber)

def submissionLoadLastSeen():
    try: 
        with open(r"C:\Users\Ranfe\Music\RFIDZapier\submission_last_seen.txt", "r") as f:
            return int(f.read().strip())
        
    except FileNotFoundError:
        return 0

def submissionSaveLastSeen(number):
    with open(r"C:\Users\Ranfe\Music\RFIDZapier\submission_last_seen.txt", "w") as f:
        f.write(str(number))
    