import pandas as pd
import numpy as np 
import datetime as dt
import yfinance as yf
import random
from time import sleep
import io
import os
import logging


import init_logging as il
import help_functions as hf

# SET UP LOGGING
il.init_logging()



# set up parameters
#swe_holidays = holidays.Sweden()

STOCKMARKET_CLOSES = hf.STOCKMARKET_CLOSES
WAITFORMARKET = hf.WAITFORMARKET
START_DATE = hf.START_DATE


# check if stock market has closed
date_today = dt.date.today()
datetime_today = dt.datetime.today()
timeconstraint = dt.datetime.combine(date_today,STOCKMARKET_CLOSES) + dt.timedelta(minutes=WAITFORMARKET)
if (datetime_today.time() <= timeconstraint.time()):
    logging.warning(f"Warning: Stock market closes around *{STOCKMARKET_CLOSES}* during weekdays.")


# Connect to database
logging.info('Connecting to database to get a list of tickers to sync.')
conn = hf.get_conn()
try:
    # Get list of stocks to sync from database
    sql_command = f"SELECT DISTINCT(ticker_intl) FROM public.ticker_list;"
    cur = conn.cursor()
    cur.execute(sql_command)
    ticker_list = cur.fetchall()
    conn.commit()
    conn.rollback()
    cur.close()
    # create DF so it fits into for-loop below
    ticker_list = pd.DataFrame(ticker_list, columns=['ticker_intl'])
except(Exception) as err:
    errmsg =  'Could not get list of stock tickers from database.'
    logging.error(errmsg)
    logging.error(err.args)
    raise(Exception(errmsg))

logging.info('Starting to sync ticker prices.')
# start syncing ticker prices
try:

    for ticker_name in ticker_list.ticker_intl:
        # 
        logging.info('='*40)
        logging.info(f'Get dates to sync ticker {ticker_name}')
        dates_to_sync = hf.get_dates_to_sync(ticker_name, 
                                    conn,
                                    table='public.ticker_prices',
                                    ) 
        if not dates_to_sync:
            continue
        else:
            (sdate, edate), (sdate_str, edate_str) = dates_to_sync
        # 
        logging.info(f'Getting {ticker_name}')
        ticker_obj = yf.Ticker(ticker_name)
        #
        logging.info(f"Syncinc {ticker_name} from {sdate_str} to {edate_str}.")
        ticker_olhc = ticker_obj.history(start=sdate_str,
                                        end=None,#edate_str,
                                        )
        if ticker_olhc.empty:
            # if it is empty
            logging.warning(f"Data for ticker {ticker_name} not available on server.")
            continue
        # prepare data to be written to the database
        ticker_olhc = ticker_olhc.reset_index()
        ticker_olhc['ticker_intl'] = ticker_name
        ticker_olhc.columns = [i.lower().replace(' ','_') for i in ticker_olhc.columns]
        # now reorder columns so it is the same as in database
        ticker_olhc = ticker_olhc[['date','ticker_intl','open','low','high','close','volume','stock_splits','dividends']]
        # write data to database
        logging.info('Writing data to database.')
        try:
            ret = hf.execute_values(conn, ticker_olhc, 'ticker_prices')
        except(Exception) as err:
            conn.close()
            logging.error(f'Exception {err.args}')
            raise(err)
        if not ret:
            warnmsg = f'Could not write ticker {ticker_name} to database.'
            logging.warning(warnmsg)
        # sleeping a bit between requests
        sleep(random.randint(0,1))
        
except(Exception) as err:
    errmsg = f"Could not sync tickers"
    logging.error(errmsg)
    logging.error(err.args)
    conn.close()

logging.info('Scraping done!')
