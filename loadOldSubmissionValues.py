import mysql.connector, os, csv, datetime
from dotenv import load_dotenv
from parser import isValidSubmissionCSV
from downloader import retrieveUPC

# Searches from september to present day for any missing UPC/GTIN values in submission

load_dotenv()

host = os.getenv("MYSQL_HOST")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
databaseExclusion = os.getenv("MYSQL_DATABASE_EXCLUSION")
databaseSubmission = os.getenv("MYSQL_DATABASE_SUBMISSION")

def getSubmissionRows():
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("""SELECT submission_id, submission, item_file, gtin
                   FROM alec_site.general_form_subs 
                   WHERE submission_date >= '2024-09-01' 
                   AND submission_date < '2024-10-01'""") 
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

def insertMissingIntoSQL(submissionID, value):
    conn = mysql.connector.connect(
        host = host,
        user = user,
        password = password,
        database = databaseSubmission
    )

    cursor = conn.cursor()
    cursor.execute("INSERT INTO alec_site.general_item_file_upc (submission_id, upc) VALUES (%s, %s)", (submissionID, value)) #add query
    conn.commit()
    cursor.close()
    conn.close()

def runScript():
    currentDate = datetime.datetime.now() # get current date and time
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")

    with open(f"Unsuccessful Submission Rows/{dayDate} at {timeDate}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getSubmissionRows()

        for submissionID, submissionNumber, itemFile, gtin in rows:
            if not (itemFile and itemFile.strip()):
                print(f"{submissionNumber} itemFile is not present, using GTIN instead: {gtin}")
                writer.writerow([submissionNumber, f"{submissionID}", "itemFile not Present", "Used GTIN as UPC"])
                insertMissingIntoSQL(submissionID, gtin)
                continue

            elif not isValidSubmissionCSV(itemFile):
                print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                continue
                
            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV")

            try:
                UPCs, badUPCs = retrieveUPC(itemFile)

                if UPCs: # ifUPC Column isn't empty
                    for upc in UPCs:
                        print(f"Found UPC: {upc}")
                        insertMissingIntoSQL(submissionID, upc)
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, f"{submissionID}", itemFile, f"Bad UPC Value - {bad}"])
                
                elif badUPCs: # if UPC column is empty
                    print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "All UPCs Invalid"])

                else: #if both good and bad upcs columns are empty
                    print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Missing UPC Column"])



            except Exception as e:
                print(f"Failed to process {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Failed to Retrieve UPC Value(s)"])


if __name__ == "__main__":
    runScript()