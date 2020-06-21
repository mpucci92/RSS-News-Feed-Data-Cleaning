# Single Day Script - Aggregate all of these at the end #

# Single Day Script - Aggregate all of these at the end #

import hashlib
import json
from glob import glob
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime

# Local Time setting
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

# logging information #
log_path = "C:\\Users\\mpucci\\Desktop\\LJZP inv\\finBERT Git\\OscraP\\Merge_logs\\logs.log"
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

source = 'GoogleNews'
count = 0

if datetime.datetime.today().weekday() == 6:
    unique_hashs = []

else:



line_list = []
logger_lines = []

# data to be merged #
data_to_be_merged = "/home/mp0941745/scripts/google_tickers/google_data/*.csv"


for csvFile in glob(data_to_be_merged):
    try:
        for line in open(csvFile, 'r'):
            line_split = line.split(',')
            lines = line_split[3:]
            data_md5 = str(hashlib.md5(json.dumps(lines, sort_keys=True).encode('utf-8')).hexdigest())

            if data_md5 in unique_hashs:
                pass

            else:
                unique_hashs.append(data_md5)
                line_list.append(line)

    except Exception as e:
        logging.warning('Error Message: %s:%s', csvFile,e)
        logging.warning('Failed to write line: %s', line)
        pass
# Writing the unique hashlist at the of the operation #
with open("C:\\Users\\mpucci\\Desktop\\LJZP inv\\finBERT Git\\OscraP\\Hashlist File\\Hashlist.txt", 'w') as output:
    for row in unique_hashs:
        output.write(str(row) + '\n')

# Writing the lines from list to a csv file #

with open('D:\\GoogleNews Data\\Merged.csv', 'w') as Merged:
    for line in line_list:
        Merged.write(line)
