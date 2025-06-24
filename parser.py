import re

def isValidExclusionCSV(url):
    pattern = r'^https://www\.jotform\.com/uploads/rfidlab/.+\.csv$'
    return re.match(pattern, url) is not None

def isValidSubmissionCSV(url):
    pattern = r'^https://drive\.google\.com/uc\?export=download&id=[\w-]+$'
    return re.match(pattern, url) is not None

