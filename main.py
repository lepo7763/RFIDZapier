from db import getExclusionRows, insertExcludedUPCToSQL, getLatestExclusionSubmissionNumber, exclusionLoadLastSeen, exclusionSaveLastSeen
from parser import isValidExclusionCSV
from downloader import retrieveUPC
import csv, datetime

def main():
    startIndex = exclusionLoadLastSeen()
    maxIndex = getLatestExclusionSubmissionNumber()

    if startIndex > maxIndex: # check for new rows
        print("No new rows to process for exclusions.")
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
                print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                exclusionSaveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 
                continue

            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
                
            try:
                UPCs, badUPCs = retrieveUPC(itemFile) 
                if UPCs: # if UPC column isn't empty
                    for upc in UPCs:
                        print(f"Found UPC: {upc}")
                        # insertExcludedUPCToSQL(submissionID, upc)
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

            exclusionSaveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 

if __name__ == "__main__":
    main()