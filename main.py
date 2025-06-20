from db import getExclusionRows, insertUPCToSQL, getLatestSubmissionNumber, loadLastSeen, saveLastSeen
from parser import isValidCSV
from downloader import retrieveUPC
import csv, datetime

# add a method to export a csv after each run, printing all of the rows that were erroneous
# make the csv only export failed itemFiles

# then run for the entire table and look for errors in terminal + exported csv

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
        writer.writerow(["Submission Number", "Submission ID", "itemFile"])

        rows = getExclusionRows(startIndex, maxIndex) 

        for submissionID, submissionNumber, itemFile in rows:
            if not isValidCSV(itemFile):
                writer.writerow([submissionNumber, submissionID, "Bad URL"])
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
                    writer.writerow([submissionNumber, submissionID, "All UPCs invalid"])

                elif not UPCs and not badUPCs:
                    print(f"({submissionNumber}) missing UPC column entirely")
                    writer.writerow([submissionNumber, submissionID, "Missing UPC column"])

            except Exception as e:
                print(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
        
            saveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 

if __name__ == "__main__":
    main()