from const import DIR, date_today, logger
from bs4 import BeautifulSoup
import requests
import sys, os
import hashlib
import json
from glob import glob
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
from google.cloud import storage

# logging information #
log_path = "/home/zqretrace/scripts/merge_logs/CNBC_Merged_logs/merge_logs_CNBC.log"
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

source = 'CNBC'
count = 0

if (datetime.datetime.today().weekday() == 5):
    unique_hashs = []

else:

    f = open("/home/zqretrace/scripts/Hashfiles/CNBC_Hashlist.txt", "r") # load in the existing text file
    unique_hashs = list(f)

line_list = []
logger_lines = []

# data to be merged #
data_to_be_merged = "/home/zqretrace/scripts/News_data/*/*.csv"


for csvFile in glob(data_to_be_merged):
    try:
        for line in open(csvFile, 'r'):
            line_split = line.split(',')
            lines = line_split[0]
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
with open("/home/zqretrace/scripts/Hashfiles/CNBC_Hashlist.txt", 'w') as output:
    for row in unique_hashs:
        output.write(str(row) + '\n')

# Writing the lines from list to a csv file #

with open('/home/zqretrace/scripts/CNBC_MERGED_DATA/CNBC_Merged_%s' % localtime +'.csv', 'w') as Merged:
    for line in line_list:
        Merged.write(line)

import os
import tarfile

file_name = "/home/zqretrace/scripts/cnbc_zipped_data/CNBC_Merged_%s" % localtime + ".tar"

tar = tarfile.open(file_name, "w:gz")

os.chdir("/home/zqretrace/scripts/CNBC_MERGED_DATA")

for name in os.listdir("."):
         tar.add(name)
tar.close()

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

path = "/home/zqretrace/scripts/cnbc_zipped_data"
dirs = os.listdir(path)
slash = '/'

for file in dirs:
    upload_blob("cnbc-storage",("%s" % (path) + '%s' % (slash) + "%s" %(str(file))), "CNBCNews/CNBC_Merged_%s" % localtime)
