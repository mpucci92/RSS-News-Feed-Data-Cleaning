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
# from google.cloud import storage

# logging information #
log_path = "D:\\GoogleNews Data\\Test\\logs_CNBC.log"
logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.DEBUG)

# Local Time setting
localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"
path ="C:\\Users\\mpucci\\Desktop\\LJZP inv\\finBERT Git\\tickers_yf_1.csv"
df = pd.read_csv(path)
TICKERS = list(df.Ticker)
ticker_list = []
percent = '%'

def get_news(ticker):

	url = URL.format(ticker = ticker)
	page = requests.get(url).content
	page = BeautifulSoup(page)

	page = page.find("h3", text="latest news")
	page = page.parent.parent.parent

	articles = []
	notes = page.find_all("span", {"class" : "note"})
	for note in notes:
	    parent = note.parent

	    a = parent.find("a")

	    href = a.get_attribute_list("href")[0]
	    if 'https' not in href:
	        continue

	    title = a.find("span").text

	    articles.append([title, note.text, href,localtime])

	df = pd.DataFrame(articles, columns = ['title', 'date', 'link','acquisition time'])
	if len(df) == 0:
		pass

	else:
		df.to_csv("D:\\GoogleNews Data\\CNBC TEST\\{ticker}_{localtime}.csv", sep=',', index=False)

if __name__ == '__main__':


	for ticker in TICKERS:

		try:
			get_news(ticker)
			logging.info('%s:Completed',ticker)
			ticker_list.append(ticker)
			current_complete = (len(ticker_list)/len(TICKERS))*100
			logging.info('Current Percentage: %f %s', current_complete, percent)

		except Exception as e:
			logging.warning('Error Message: %s:%s', ticker,e)
			continue

	percent_successful = (len(ticker_list)/len(TICKERS))*100
	logging.info('Percentage of successful tickers: %f  %s', percent_successful,percent)
