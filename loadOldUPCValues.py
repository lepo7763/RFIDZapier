import mysql.connector
import os
from dotenv import load_dotenv

from parser import isValidExclusionCSV
from downloader import retrieveUPC
import csv, datetime

# searches from september to present day for any UPCs missing from table in alec_site exclusion


load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
databaseExclusion = os.getenv("MYSQL_DATABASE_EXCLUSION")
databaseSubmission = os.getenv("MYSQL_DATABASE_SUBMISSION")

def getRows():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseExclusion # change?
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT submission_id, submission_number, item_file
                   FROM alec_site.exclusion
                   WHERE submission_date
                   BETWEEN '2025-06-30' 
                   and '2025-07-30'""") # set dates
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

def thing(submissionID, UPC):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission #change?
    )

    cursor = conn.cursor()
    cursor.execute("INSERT INTO alec_site.exclusion_upc_list (submission_id, upc) VALUES (%s, %s)", (submissionID, UPC))
    conn.commit()
    cursor.close()
    conn.close()


def doThing():    
    currentDate = datetime.datetime.now() # get current date and time
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")

    with open(f"Unsuccessful Exclusion Rows/{dayDate} at {timeDate}.csv", "w", newline='') as csvfile: # write unsuccessful rows to a CSV file
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getRows() 

        for submissionID, submissionNumber, itemFile in rows:
            if not isValidExclusionCSV(itemFile):
                print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                continue

            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
                
            try:
                UPCs, badUPCs = retrieveUPC(itemFile) 
                if UPCs: # if UPC column isn't empty
                    for upc in UPCs:
                        print(f"Found UPC: {upc}")
                        try:
                            thing(submissionID, upc)
                        except mysql.connector.IntegrityError:
                            print(mysql.connector.IntegrityError)
                            writer.writerow([submissionNumber, submissionID, itemFile, "Already Exists in Table"])
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, submissionID, itemFile, f"Bad UPC Value - {bad}"])
                
                elif badUPCs: # if UPC column is empty
                    print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, submissionID, itemFile, "All UPCs Invalid"])
                
                else: # if both UPC and badUPC columns are empty
                    print(f"({submissionNumber}) missing UPC Column entirely")
                    writer.writerow([submissionNumber, submissionID, itemFile, "Missing UPC Column"])

            except Exception as e:
                print(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, submissionID, itemFile, "Failed to Retrieve UPC Value(s)"])


if __name__ == "__main__":
    doThing()