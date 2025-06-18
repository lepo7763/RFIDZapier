import re

def isValidCSV(url):
    pattern = r'^https://www\.jotform\.com/uploads/rfidlab/.+\.csv$'

    return re.match(pattern, url) is not None

#url = 'https://www.jotform.com/uploads/rfidlab/232856442170152/6062900855452145150/UPC%20810129870305%20VENDOR%20SKU%20PFSCLRR16.csv'
#url = 'https://www.jotform.com/uploads/rfidlab/232856442170152/6062203050265976078/2166060%20exclusion.csv'
#url = 'https://www.jotform.com/uploads/rfidlab/232856442170152/6060459420362253488/CMC%20Temporary%20Exclusion%20List.csv'
