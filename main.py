from db import getExclusionRows, insertUPCToSQL, getLatestSubmissionNumber, loadLastSeen, saveLastSeen
from parser import isValidCSV
from downloader import retrieveUPC
from daemonRunner import runDaemon
import csv, datetime

def main():
    startIndex = loadLastSeen()
    maxIndex = getLatestSubmissionNumber()

    if startIndex > maxIndex:
        print("No new rows to process.")
        return
    
    currentDate = datetime.datetime.now()
    formatDate = currentDate.strftime("%Y-%m-%d %H-%M-%S")

    with open(f"Unsuccessful Rows/{formatDate}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getExclusionRows(startIndex, maxIndex) 

        for submissionID, submissionNumber, itemFile in rows:
            if not isValidCSV(itemFile):
                print(f"({submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                saveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 
                continue

            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
                
            try:
                UPCs, badUPCs = retrieveUPC(itemFile) 
                if UPCs:
                    for value in UPCs:
                        print(f"Found UPC: {value}")
                        # insertUPCToSQL(submissionID, value)

                if badUPCs and not UPCs:
                    print(f"({submissionNumber}) all UPCs are invalid")
                    writer.writerow([submissionNumber, submissionID, itemFile, "All UPCs invalid"])

                elif not UPCs and not badUPCs:
                    print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, submissionID, itemFile, "Missing UPC column"])

            except Exception as e:
                print(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
        
            saveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 

if __name__ == "__main__":
    runDaemon()