from const import DIR, date_today, logger, DELIM
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys, os


URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"
path ="/home/mp0941745/data/tickers_yf.csv"
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
	    articles.append([title, note.text, href])

	df = pd.DataFrame(articles, columns = ['title', 'date', 'link'])
	if len(df) == 0:
		pass

	else:
		df.to_csv(f'{DIR}/News_data/{date_today}/{ticker}_{date_today}.csv', sep=DELIM, index=False)

def init_folders():

	os.mkdir(f'{DIR}/News_data/{date_today}')

if __name__ == '__main__':

	init_folders()

	for ticker in TICKERS:

		try:
			get_news(ticker)
			logger.info('%s:Completed',ticker)
			ticker_list.append(ticker)
			current_complete = (len(ticker_list)/len(TICKERS))*100
			logger.info('Current Percentage: %f %s', current_complete, percent)

		except Exception as e:
			logger.warning('Error Message: %s:%s', ticker,e)
			continue

	percent_successful = (len(ticker_list)/len(TICKERS))*100
	logger.info('Percentage of successful tickers: %f  %s', percent_successful,percent)

