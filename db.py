import mysql.connector
import os
from dotenv import load_dotenv

# add a way to filter amount of rows extracted and after what row it should start from
# must be updating after each iteration
#

load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")

def getExclusionRows(start, end):
    # move this stuff to a .env file maybe?
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
    # maybe change this to just start and to the (unspecified) end later when it runs autonomously on a server?
    rows = cursor.fetchall()

    conn.close()
    return rows

#function to write specific data into the exclusion table
#def somethingsomething():
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