from db import getExclusionRows, insertUPCToSQL, getLatestSubmissionNumber, loadLastSeen, saveLastSeen
from parser import isValidCSV
from downloader import retrieveUPC

# add a method to export a csv after each run, printing all of the rows that were erroneous, then another section below 
# to show all of the rows that were successful
# should the csv show only failed csv or two sections to show all csvs?


def main():
    startIndex = loadLastSeen()
    maxIndex = getLatestSubmissionNumber()

    if startIndex > maxIndex:
        print("No new rows to process.")
        return
    
    rows = getExclusionRows(startIndex, maxIndex) 

    for row in rows:
        submissionID, submissionNumber, itemFile = row
        if isValidCSV(itemFile):
            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
            UPCs = retrieveUPC(itemFile) # add retrieveUPC function here? to write the submissionID with the scraped UPC to the exclusion_UPC table
            for value in UPCs:
                print(f"Found {value}") # delete
                # insertUPCToSQL(submissionID, value)
                # ^^ uncomment later
    
        else:
            print(f"({submissionNumber}) Submission ID [{submissionID}] produced an error with its item file")
            
        saveLastSeen(submissionNumber + 1) # if script crashes, this saves where it left off. 

if __name__ == "__main__":
    main()