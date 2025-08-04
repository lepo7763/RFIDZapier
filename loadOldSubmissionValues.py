import mysql.connector, os, csv, datetime
from dotenv import load_dotenv
from parser import isValidSubmissionCSV
from downloader import retrieveUPC
from mysql.connector import pooling, IntegrityError

# change months before running 
# Searches each month for any missing UPC/GTIN values in submission

# revised version (uses pooling and batch inserts)

# 163650 (before running)
# 164380 (after running for september)
# 165451 (after running for october)
# 166214 (after running november)
# 170116 (after running december)
# 170650 (after running january)
# 171470 (after running february)
# 174195 (after running march)
# 180419 (after runnning april)
# 187602 (after running may)
# 205740 (after running june)


load_dotenv()


HOST = os.getenv("MYSQL_HOST")
USER = os.getenv("MYSQL_USER")
PASSWORD = os.getenv("MYSQL_PASSWORD")
DB = os.getenv("MYSQL_DATABASE")

POOL = pooling.MySQLConnectionPool(
    pool_name='loadOldSubmissionValuesPool',
    pool_size=5,
    host=HOST,
    user=USER,
    password=PASSWORD,
    database=DB,
    autocommit=False
)

# Helper functions:
def getSubmissionRows():
    conn = POOL.get_connection()
    cursor = conn.cursor()

    cursor.execute("""SELECT submission_id, submission, item_file, gtin
                   FROM alec_site.general_form_subs 
                   WHERE submission_date >= '2025-7-01' 
                   AND submission_date < '2025-8-01'""") # July
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

def insertMissingIntoSQL(batch): # Insert batch (1000) into the sql db
    if not batch:
        return
    
    conn = POOL.get_connection()
    cursor = conn.cursor(prepared=True)

    cursor.executemany("INSERT IGNORE INTO alec_site.general_item_file_upc (submission_id, upc) "
                   "VALUES (%s, %s)", batch)
    conn.commit()

    cursor.close()
    conn.close()
    batch.clear()
    print(f"""---------------------------------------------------
              Inserted 1000 values into the database
              ---------------------------------------------------""")

def checkSQL(submissionID, value): # check for duplicates in the sql db
    conn = POOL.get_connection()
    cursor = conn.cursor(prepared=True)

    cursor.execute("""SELECT submission_id, upc FROM alec_site.general_item_file_upc 
                   WHERE submission_id = %s AND upc = %s LIMIT 1""", (submissionID, value))
    exists = cursor.fetchone() is not None

    cursor.close()
    conn.close()
    return exists

def runScript():
    currentDate = datetime.datetime.now() # get current date and time
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")

# {dayDate} at {timeDate}.csv

    with open(f"Unsuccessful Submission Rows/July Run for loading old submissions.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getSubmissionRows()
        batch = []
        BATCH_SIZE = 1000

        for submissionID, submissionNumber, itemFile, gtin in rows:
            if not (itemFile and itemFile.strip()) and not gtin:
                print(f"{submissionNumber} does not have itemFile and GTIN. Skipping...")
                writer.writerow([submissionNumber, f"{submissionID}", "itemFile not present", "Both GTIN and itemFile not present (skipped)"])
                continue
            elif not (itemFile and itemFile.strip()):
                if not checkSQL(submissionID, gtin): #check if submissionid doesn't have the gtin, if true -> execute
                    print(f"({submissionNumber}) itemFile is not present, using GTIN instead: {gtin}")
                    writer.writerow([submissionNumber, f"{submissionID}", "itemFile not Present", f"Inserted GTIN value ({gtin}) (no UPCs present)"])
                    try:
                        batch.append((submissionID, gtin))
                        if len(batch) >= BATCH_SIZE:
                            insertMissingIntoSQL(batch)
                    except mysql.connector.Error as e:
                        print(f"MySql Error: {e}")
                        writer.writerow([submissionNumber, f"{submissionID}", "—", f"{e}"])
                else:
                    print(f"{submissionID} itemFile is not present, but used GTIN ({gtin}) already") 
                    writer.writerow([submissionNumber, f"{submissionID}", "—", "GTIN already present as UPC (handled and skipped)"])
                continue

            elif not isValidSubmissionCSV(itemFile):
                print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                continue
                
            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV")

            try:
                UPCs, badUPCs = retrieveUPC(itemFile)

                if UPCs: # if UPC Column isn't empty
                    if not checkSQL(submissionID, gtin):
                        batch.append((submissionID, gtin)) # add gtin to the submissionID along with the UPC values
                    for upc in UPCs:
                        print(f"Found UPC: {upc}")
                        if not checkSQL(submissionID, upc): # check for duplicates, if not, insert into sql
                            batch.append((submissionID, upc))
                            if len(batch) >= BATCH_SIZE:
                                insertMissingIntoSQL(batch)
                        else: 
                            print(mysql.connector.IntegrityError)  
                            writer.writerow([submissionNumber, submissionID, itemFile, "Already Exists in Table"])

                    # Logging invalid UPC values
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, f"{submissionID}", itemFile, f"Bad UPC Value - {bad}"])
                
                if UPCs:
                    print(f"Queued {len(UPCs)} UPCs for submission {submissionID}")

                elif badUPCs: # if UPC column is empty
                    print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "All UPCs Invalid"])

                else: #if both good and bad upcs columns are empty
                    print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Missing UPC Column"])

            except Exception as e:
                print(f"Failed to process {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Failed to Retrieve UPC Value(s)"])

        # push what's left in batch to sql
        insertMissingIntoSQL(batch)


if __name__ == "__main__":
    runScript()