import re
import logging
from datetime import datetime
import os

date_today = datetime.today().strftime("%Y-%m-%d")
percent = '%'

def logger(ticker,list_ticker,tickers):

# ticker: Iterative value from tickers
# tickers: contains all the tickers
# list_ticker: appends tickers to a list

    try:
        log_path = "C:\\Users\\mpucci\\Desktop\\LJZP inv\\finBERT Git\\OscraP\\GoogleNewsRSS\\logs\\logs_%s.log", date_today
        log_level = 'info'
        logging.basicConfig(filename=log_path,format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%m/%d/%Y %H:%M:%S',level=logging.CRITICAL)
        logging.info('%s:Completed',ticker)
        list_ticker.append(ticker)
        current_complete = (len(list_ticker)/len(tickers))*100
        logging.info('Current Percentage: %f %s', current_complete, percent)

    except Exception as e:
        logging.warning('Error Message: %s:%s', ticker,e)

    percent_successful = (len(list_ticker)/len(tickers))*100
    logging.info('Percentage of successful tickers: %f  %s', percent_successful, percent)

