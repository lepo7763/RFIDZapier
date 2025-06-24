import csv, datetime
from parser import isValidSubmissionCSV
from db import submissionLoadLastSeen, submissionSaveLastSeen, getSubmissionRows, insertSubmissionUPCtoSQL, getLatestSubmissionSubmissionNumber



def submissions():
    startIndex = submissionLoadLastSeen()
    maxIndex = getLatestSubmissionSubmissionNumber()

    if 100 > 1:
        print("No new rows to process for submissions.")
        return
    
    currentDate = datetime.datetime.now()
    formatDate = currentDate.strfttime("%Y-%m-%d %H-%M-%S")

    with open(f"Unsuccessful Submission Rows/{formatDate}.csv", "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Submission Number", "Submission ID", "itemFile", "Error"])

        rows = getSubmissionRows(startIndex, maxIndex)

        for submissionID, submissionNumber, itemFile in rows: # replace with real values
            if not isValidSubmissionCSV(itemFile):
                print(f"{submissionNumber}) has a bad URL")
                writer.writerow([submissionNumber, submissionID, itemFile, "Bad URL"])
                submissionSaveLastSeen(submissionNumber + 1)
                continue
            
            print(f"{submissionNumber} Submission ID [{submissionID}] has a valid CSV")

if __name__ == "__main__":
    submissions()