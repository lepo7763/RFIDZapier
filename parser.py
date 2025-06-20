import re

def isValidCSV(url):
    pattern = r'^https://www\.jotform\.com/uploads/rfidlab/.+\.csv$'

    return re.match(pattern, url) is not None
