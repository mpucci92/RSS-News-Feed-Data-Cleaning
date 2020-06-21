from socket import gaierror
from time import strftime, localtime
import feedparser
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pandas.io.json import json_normalize
import re
import logging
import datetime
import os

log_path = "/home/mp0941745/scripts/logs/logs.log"
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

df = pd.read_csv('/home/mp0941745/scripts/google_tickers/clean_tickers.csv')

tickers = list(df.Ticker)

time = datetime.date.today().strftime("%B %d, %Y")

for ticker in tickers:
    try:
        rss_url = 'https://news.google.com/rss/search?q=''"%s"' % (str(ticker)) + '+when:7d&hl=en-CA&gl=CA&ceid=CA:en'

        news_feed = feedparser.parse(rss_url)

        df_news_feed = json_normalize(news_feed.entries)

        data_path = "/home/mp0941745/scripts/google_tickers/google_data/" + "rss_%s" % ticker + "_%s" % localtime + ".csv"

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
