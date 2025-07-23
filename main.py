from db import getExclusionRows, insertExcludedUPCToSQL, getLatestExclusionSubmissionNumber, exclusionLoadLastSeen, exclusionSaveLastSeen
from parser import isValidExclusionCSV
from downloader import retrieveUPC
import csv, datetime, mysql.connector, os, logging
from dotenv import load_dotenv
from mysql.connector import pooling, IntegrityError

# actively take new submissions for exclusion (excluded upcs) and add them

# kept prints for debugging, but replaced with logging.info()
load_dotenv(r"C:\Users\Ranfe\Music\RFIDZapier\.env") # TODO: change to C:\programdata

HOST      = os.getenv("MYSQL_HOST")
USER      = os.getenv("MYSQL_USER")
PASSWORD  = os.getenv("MYSQL_PASSWORD")
DB        = os.getenv("MYSQL_DATABASE")

POOL = pooling.MySQLConnectionPool(
    pool_name="excl_pool",
    pool_size=5,           # adjust if we need more concurrency
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB,
    autocommit=True        # TODO: check
)

def checkExclusionSQL(submissionID, value):
    with POOL.get_connection() as conn, conn.cursor() as cursor:
        cursor.execute("""SELECT submission_id, upc FROM alec_site.excluded_upc
                       WHERE submission_id = %s AND upc = %s LIMIT 1""", (submissionID, value))
        exists = cursor.fetchone() is not None
    return exists

def mainFunction():
    startIndex = exclusionLoadLastSeen()
    maxIndex = getLatestExclusionSubmissionNumber()

    if startIndex > maxIndex: # check for new rows
        logging.info("No new rows to process for exclusions.")
        # print("No new rows to process for exclusions.") 
        return
    
    currentDate = datetime.datetime.now() # get current date and time
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")

    with open(f"Unsuccessful Exclusion Rows/{dayDate} at {timeDate}.csv", "w", newline='') as csvfile: # write unsuccessful rows to a CSV file
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getExclusionRows(startIndex, maxIndex) 

        for submissionID, submissionNumber, itemFile in rows:
            if not isValidExclusionCSV(itemFile):
                logging.info(f"({submissionNumber}) has a bad URL")
                # print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                exclusionSaveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 
                continue
            
            logging.info(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV")
            # print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
                
            try:
                UPCs, badUPCs = retrieveUPC(itemFile) 
                if UPCs: # if UPC column isn't empty
                    for upc in UPCs:
                        logging.info(f"Found UPC: {upc}")
                        # print(f"Found UPC: {upc}")
                        if not checkExclusionSQL(submissionID, upc):
                            insertExcludedUPCToSQL(submissionID, upc)
                        else:
                            logging.error(mysql.connector.IntegrityError)
                            # print(mysql.connector.IntegrityError)
                            writer.writerow([submissionNumber, submissionID, itemFile, "Already Exists in Table"])
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, submissionID, itemFile, f"Bad UPC Value - {bad}"])
                
                elif badUPCs: # if UPC column is empty
                    logging.info(f"({submissionNumber}) all UPC values are invalid")
                    # print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, submissionID, itemFile, "All UPCs Invalid"])
                
                else: # if both UPC and badUPC columns are empty
                    logging.info(f"({submissionNumber}) missing UPC Column entirely")
                    # print(f"({submissionNumber}) missing UPC Column entirely")
                    writer.writerow([submissionNumber, submissionID, itemFile, "Missing UPC Column"])

            except Exception as e:
                logging.error(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
                # print(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, submissionID, itemFile, "Failed to Retrieve UPC Value(s)"])

            exclusionSaveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 
    logging.info(f"""------------------------------------------------------------\n
                     Finished Exclusion for {dayDate} at {timeDate}\n
                     ------------------------------------------------------------\n\n\n""")
    # print(f"Finished Exclusion for {dayDate} at {timeDate}")
    
if __name__ == "__main__":
    mainFunction()