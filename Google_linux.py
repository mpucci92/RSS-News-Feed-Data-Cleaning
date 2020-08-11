from socket import gaierror
from time import strftime, localtime
import feedparser
import requests
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
import re
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime
import hashlib
import json
from glob import glob
import pandas as pd
import numpy as np
import logging
from time import gmtime, strftime, localtime
import datetime as datetime



log_path = "/home/zqretrace/scripts/logs/Google_logs.log"
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

from time import gmtime, strftime, localtime

localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())
ticker_list = []
article_list = []
name_list = []
name_list_nospace = []
clean_names = []
rss_date = []
list_ticker = []
percent = '%'

def alphanumeric(text):
    clean = re.sub(r'[^A-Za-z0-9 ]+', '', text)
    return clean

df = pd.read_csv('/home/zqretrace/data/clean_tickers.csv')

tickers = list(df.Ticker)

time = datetime.date.today().strftime("%B %d, %Y")

for ticker in tickers:
    try:
        rss_url = 'https://news.google.com/rss/search?q=''"%s"' % (str(ticker)) + '+when:7d&hl=en-CA&gl=CA&ceid=CA:en'

        news_feed = feedparser.parse(rss_url)

        df_news_feed = json_normalize(news_feed.entries)

        data_path = "/home/zqretrace/scripts/google_data/" + "rss_%s" % ticker + "_%s" % localtime + ".csv"

        if (len(df_news_feed)) == 0:
            continue

        else:
            df_news_feed.to_csv(data_path)  # input to save data

        logging.info('%s:Completed',ticker)
        list_ticker.append(ticker)
        current_complete = (len(list_ticker)/len(tickers))*100
        logging.info('Current Percentage: %f %s', current_complete, percent)

    except Exception as e:
        logging.warning('Error Message: %s:%s', ticker,e)

percent_successful = (len(list_ticker)/len(tickers))*100
logging.info('Percentage of successful tickers: %f  %s', percent_successful, percent)
logging.info('-----END OF FILE-----')

# Local Time setting
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

# logging information #
log_path = "/home/zqretrace/scripts/logs/Google_merge_logs.log"
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

source = 'GoogleNews'
count = 0

if (datetime.datetime.today().weekday() == 5):
    unique_hashs = []

else:
    f = open("/home/zqretrace/scripts/hashlist/Google_Hashlist.txt", "r") # load in the existing text file
    unique_hashs = list(f)

line_list = []
logger_lines = []

# data to be merged #
data_to_be_merged = "/home/zqretrace/scripts/google_data/*.csv"


for csvFile in glob(data_to_be_merged):
    try:
        for line in open(csvFile, 'r'):
            line_split = line.split(',')
            lines = line_split[3]
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
with open("/home/zqretrace/scripts/hashlist/Google_Hashlist.txt", 'w') as output:
    for row in unique_hashs:
        output.write(str(row) + '\n')

# Writing the lines from list to a csv file #

with open('/home/zqretrace/scripts/GOOGLE_MERGED_DATA/Merged_%s' % localtime +'.csv', 'w') as Merged:
    for line in line_list:
        Merged.write(line)

import os
import tarfile

file_name = "/home/zqretrace/scripts/tar_google/Google_Merged_%s" % localtime + ".tar"

tar = tarfile.open(file_name, "w:gz")
os.chdir("/home/zqretrace/scripts/GOOGLE_MERGED_DATA")
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

path = "/home/zqretrace/scripts/tar_google"
dirs = os.listdir(path)
slash = '/'

for file in dirs:
    upload_blob("cnbc-storage",("%s" % (path) + '%s' % (slash) + "%s" %(str(file))), "GoogleNews/Tar_Google_%s" % localtime)
