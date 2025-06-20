import csv
import requests
import io
# retrieve the UPC
# download the csv file
# when writing to the "excluded_upc" table, insert submission ID and the UPC value 

def retrieveUPC(url):
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print(response.status_code)
        return []
    
    fileStream = io.StringIO(response.text, newline='')
    reader = csv.reader(fileStream)
    header = next(reader)

    try:  
        UPCColumn = header.index("UPC") # get column location of UPC value
    except ValueError:
        print("UPC column not present")
        return []
    
    UPCValues = []

    for row in reader:
        if len(row) > UPCColumn and row[UPCColumn].strip(): # check if there are enough columns to get to UPCColumn
            UPCValues.append(row[UPCColumn]) # append any upc values to UPCValues
            # print(row[UPCColumn])         <- test

    return UPCValues
