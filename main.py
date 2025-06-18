from db import getExclusionRows, insertUPCToSQL, getLatestSubmissionNumber
from parser import isValidCSV
from downloader import retrieveUPC

# add logic to automatically sort from starting submission number to target submission number or just the end

# add a method to export a csv after each run, printing all of the rows that were erroneous, then another section below 
# to show all of the rows that were successful
def main():
    rows = getExclusionRows(11536, maxIndex)
    for row in rows:
        submissionID, submissionNumber, itemFile = row
        if isValidCSV(itemFile):  
            print(f"({submissionNumber}) Submission ID [{submissionID}] has a valid CSV") # validates csv file to be downloadable
            UPCs = retrieveUPC(itemFile) # add retrieveUPC function here? to write the submissionID with the scraped UPC to the exclusion_UPC table
            for value in UPCs:
                print(f"Found {value}") # replace w/ a function here from db.py to write the submissioID + UPCValue to the exclusion_UPC table
                # insertUPCToSQL(submissionID, value)
                # ^^uncomment later
    
        else:
            print(f"({submissionNumber}) Submission ID [{submissionID}] produced an error with its item file")
            
        # keep track of already visited rows?

if __name__ == "__main__":
    maxIndex = getLatestSubmissionNumber()
    print(maxIndex)
    main()