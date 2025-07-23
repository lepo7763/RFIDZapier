import mysql.connector, os, csv, datetime
from dotenv import load_dotenv
from parser import isValidSubmissionCSV
from downloader import retrieveUPC

# Searches from september to present day for any missing UPC/GTIN values in submission


# --------------------------------------------------------------------------------
# ---------------------------------TEST BEFORE RUNNING----------------------------
# --------------------------------------------------------------------------------
# RESULT: duplicate rule not on !

load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
databaseExclusion = os.getenv("MYSQL_DATABASE_EXCLUSION")
databaseSubmission = os.getenv("MYSQL_DATABASE_SUBMISSION")



def test():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT * FROM alec_site.general_item_file_upc 
                   WHERE submission_id = '2027007028899' AND upc = '8906111350862' LIMIT 1""")
    exists = cursor.fetchone() is not None

    cursor.close()
    conn.close()
    return exists

if __name__ == "__main__":
    try:
        print(test())
    except mysql.connector.IntegrityError:
        print(mysql.connector.IntegrityError)