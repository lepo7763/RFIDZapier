from db import getExclusionRows, insertUPCToSQL, getLatestSubmissionNumber, loadLastSeen, saveLastSeen
from parser import isValidCSV
from downloader import retrieveUPC
import csv
import datetime

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

        for row in rows:
            submissionID, submissionNumber, itemFile = row
            if isValidCSV(itemFile):
                print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
                
                try:
                    UPCs = retrieveUPC(itemFile) # add retrieveUPC function here? to write the submissionID with the scraped UPC to the exclusion_UPC table
                    for value in UPCs:
                        print(f"Found {value}") # delete
                        # insertUPCToSQL(submissionID, value)
                        # ^^ uncomment later
                except Exception as e:
                    print(f"Failed to print {submissionNumber} with ID [{submissionID}]: {e}")
        
            else:
                print(f"({submissionNumber}) Submission ID [{submissionID}] produced an error with its item file")
                writer.writerow([submissionNumber, submissionID, itemFile]) # write to csv file
                
            saveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 

if __name__ == "__main__":
    main()