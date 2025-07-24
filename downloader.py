import csv, io, requests, re, logging

# retrieve the UPC
# download the csv file

# kept prints for debugging, but replaced with logging.info()

def retrieveUPC(url):
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print(response.status_code)
        return [], []
    
    reader = csv.reader(io.StringIO(response.text, newline=""))
    header = next(reader, [])
    
    try:  
        UPCColumn = header.index("UPC") # get column location of UPC value
    except ValueError:
        logging.info("UPC column not present")
        # print("UPC column not present")
        return [], []
    
    UPCValues, badUPCs = [], []

    for row in reader:
        rawValue = str(row[UPCColumn]).strip()

        if "e" in rawValue.lower():
            badUPCs.append(rawValue)
            logging.info(f"Skipped invalid UPC: {rawValue}")
            # print(f"Skipped invalid UPC: {rawValue}")
            continue

        cleanedValue = re.sub(r"[^0-9]", "", rawValue)

        if cleanedValue:
            UPCValues.append(cleanedValue)
        else:
            badUPCs.append(rawValue)    

    return UPCValues, badUPCs
