import csv, datetime
from parser import isValidSubmissionCSV
from db import submissionLoadLastSeen, submissionSaveLastSeen, getSubmissionRows, insertSubmissionUPCtoSQL, getLatestSubmissionSubmissionNumber
from downloader import retrieveUPC

# note: submission id can end in 00 or 99, in any case, just 
# scrape the UPCs from the excel if the excel file is present
# else, just add the gtin as the upc in the sql db with the submission id


def submissions():
    startIndex = submissionLoadLastSeen()
    maxIndex = getLatestSubmissionSubmissionNumber()

    if startIndex > maxIndex:  
        print("No new rows to process for submissions.")
        return
    
    currentDate = datetime.datetime.now()
    dayDate = currentDate.strftime("%Y-%m-%d")
    timeDate = currentDate.strftime("%H-%M-%S")
    with open(f"Unsuccessful Submission Rows/{dayDate} at {timeDate}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"]) 

        rows = getSubmissionRows(startIndex, maxIndex)

        for submissionID, submissionNumber, itemFile, gtin in rows: 
            if not (itemFile and itemFile.strip()):
                print(f"{submissionNumber} itemFile is not present, using GTIN: {gtin}")
                writer.writerow([submissionNumber, f"{submissionID}", "itemFile Not Present", "Used GTIN As UPCs"])
                # insertSubmissionUPCtoSQL(submissionID, gtin)
                submissionSaveLastSeen(int(submissionNumber) + 1) # if script crashes, this saves where it left off
                continue

            elif not isValidSubmissionCSV(itemFile):
                print(f"{submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Bad URL"])
                submissionSaveLastSeen(int(submissionNumber) + 1) 
                continue
            
            print(f"{submissionNumber} Submission ID [{submissionID}] has a valid CSV")

            try:
                UPCs, badUPCs = retrieveUPC(itemFile)

                if UPCs: # if UPC column isn't empty
                    for upc in UPCs:
                        print(f"Found UPC: {upc}")
                        # insertSubmissionUPCtoSQL(submissionID, upc)
                    for bad in badUPCs:
                        writer.writerow([submissionNumber, f"{submissionID}", itemFile, f"Bad UPC Value - {bad}"])

                elif badUPCs: # if UPC column is empty
                    print(f"({submissionNumber}) all UPC values are invalid")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "All UPCs Invalid"])

                else: # if both UPC and badUPC columns are empty
                    print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Missing UPC Column"])

            except Exception as e:
                print(f"Failed to process {submissionNumber} with ID [{submissionID}]: {e}")
                writer.writerow([submissionNumber, f"{submissionID}", itemFile, "Failed to Retrieve UPC Value(s)"])

            submissionSaveLastSeen(int(submissionNumber) + 1)  # Save progress in case of crash

if __name__ == "__main__":
    submissions()