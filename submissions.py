import csv, datetime, mysql.connector, logging
from parser import isValidSubmissionCSV
from db import submissionLoadLastSeen, submissionSaveLastSeen, getSubmissionRows, insertSubmissionUPCtoSQL, getLatestSubmissionSubmissionNumber
from downloader import retrieveUPC

# note: submission id can end in 00 or 99, in any case, just 
# scrape the UPCs from the excel if the excel file is present
# else, just add the gtin as the upc in the sql db with the submission id

# takes in upc values from excel files submitted to 
# alec_site.general_item_file_upc table

# kept prints for debugging, but replaced with logging.info()


def submissionsFunction():
    startIndex = submissionLoadLastSeen()
    maxIndex = getLatestSubmissionSubmissionNumber()

    if startIndex > maxIndex: 
        logging.info("No new rows to process for submissions.") 
        # print("No new rows to process for submissions.")
        return
    
    currentDate = datetime.datetime.now()
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")
    
    with open(f"Unsuccessful Submission Rows/{dayDate} at {timeDate}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"]) 

        rows = getSubmissionRows(startIndex, maxIndex)

        for submissionID, submissionNumber, itemFile, gtin in rows: 
            if not (itemFile and itemFile.strip()) and not gtin:
                logging.info(f"{submissionNumber} does not have itemFile and GTIN. Skipping...")
                # print(f"{submissionNumber} does not have itemFile and GTIN. Skipping...")
                writer.writerow([submissionNumber, f"{submissionID}", "itemFile not present", "Both GTIN and itemFile not present (skipped)"])
                continue
            elif not (itemFile and itemFile.strip()):
                logging.info(f"{submissionNumber} itemFile is not present, using GTIN instead: {gtin}")
                # print(f"{submissionNumber} itemFile is not present, using GTIN instead: {gtin}")
                writer.writerow([submissionNumber, f"{submissionID}", "itemFile Not Present", "Used GTIN As UPCs"])
                try:
                    insertSubmissionUPCtoSQL(submissionID, gtin)
                except mysql.connector.Error as e:
                    logging.info(f"MySql Error: {e}")
                    # print(f"MySql Error: {e}")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, f"{e}"])

                submissionSaveLastSeen(int(submissionNumber) + 1) # if script crashes, this saves where it left off
                continue
            elif not isValidSubmissionCSV(itemFile):
                logging.info(f"{submissionNumber}) has a bad URL")
                # print(f"{submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Bad URL"])
                submissionSaveLastSeen(int(submissionNumber) + 1) 
                continue
            
            logging.info(f"{submissionNumber} Submission ID [{submissionID}] has a valid CSV")
            # print(f"{submissionNumber} Submission ID [{submissionID}] has a valid CSV")

            try:
                UPCs, badUPCs = retrieveUPC(itemFile)

                if UPCs: # if UPC column isn't empty
                    insertSubmissionUPCtoSQL(submissionID, gtin)
                    for upc in UPCs:
                        logging.info(f"Found UPC: {upc}")
                        # print(f"Found UPC: {upc}")
                        insertSubmissionUPCtoSQL(submissionID, upc)
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, f"{submissionID}", itemFile, f"Bad UPC Value - {bad}"])

                elif badUPCs: # if UPC column is empty
                    logging.info(f"({submissionNumber}) all UPC values are invalid")
                    # print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "All UPCs Invalid"])

                else: # if both UPC and badUPC columns are empty
                    logging.info(f"({submissionNumber}) missing UPC column entirely")
                    # print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Missing UPC Column"])

            except Exception as e:
                logging.info(f"Failed to process {submissionNumber} with ID [{submissionID}]: {e}")
                # print(f"Failed to process {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Failed to Retrieve UPC Value(s)"])

            submissionSaveLastSeen(int(submissionNumber) + 1)  # Save progress in case of crash
    logging.info(f"""------------------------------------------------------------\n
                     Finished Submissions for {dayDate} at {timeDate}\n
                     ------------------------------------------------------------\n\n\n""")
    # print(f"Finished Submissions for {dayDate} at {timeDate}")


if __name__ == "__main__":
    submissionsFunction()