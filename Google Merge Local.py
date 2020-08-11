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
log_path = "D:\\Merge Project\\Logs\\merge_logs_Google_%s.log" %(localtime)
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

source = 'Google'
count = 0

if (datetime.datetime.today().weekday() == 5):
     f = open("D:\\Merge Project\\Hashlists\\Google_Hashlist.txt", "r") # load in the existing text file
     unique_hashs = list(f)

else:
    unique_hashs = []

line_list = []
logger_lines = []

# data to be merged #
data_to_be_merged = "D:\\GCP DATA BACKUP\\GoogleNews\\To Merge Csvs\\*.csv"


for csvFile in glob(data_to_be_merged):
    for line in open(csvFile, 'r',encoding="utf8"):
            try:
                line_split = line.split(',')
                lines = line_split[1]
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
with open("D:\\Merge Project\\Hashlists\\Google_Hashlist.txt", 'w') as output:
    for row in unique_hashs:
        output.write(str(row) + '\n')

# Writing the lines from list to a csv file #

with open('D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_Merged_%s' % localtime +'.csv', 'w') as Merged:
    for line in line_list:
        Merged.write(line)

import os
import tarfile

file_name = "D:\\GCP DATA BACKUP\\GoogleNews\\Final Tar\\Google_Merged_%s" % localtime + ".tar"

tar = tarfile.open(file_name, "w:gz")

os.chdir("D:\\GCP DATA BACKUP\\GoogleNews\\Final")

for name in os.listdir("."):
         tar.add(name)
tar.close()
