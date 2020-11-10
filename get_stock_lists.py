import requests as req
import pandas as pd
from lxml import html
import os
from sqlalchemy import create_engine
import psycopg2
import random
import io
from time import sleep
import numpy as np


SYNC_URLS = ['http://www.nasdaqomxnordic.com/shares/listed-companies/stockholm',
             'http://www.nasdaqomxnordic.com/shares/listed-companies/first-north-premier',
             'http://www.nasdaqomxnordic.com/shares/listed-companies/first-north']

data = []

try:
    for sync_url in SYNC_URLS:
        print(sync_url)
        page = req.get(sync_url, 
            timeout=3.0,
                      )
        # CHECK response, so its reasonable
        if 'Access Denied' in page.text:
            print('Access Denied!')
            raise(StandardError)
        tree = html.fromstring(page.content)
        page.close
        # fix links
        tree.make_links_absolute('http://www.nasdaqomxnordic.com')
        # get table rows with stocks
        trs = tree.xpath('//tbody//tr')
        # get the data
        print('Parsing data...')
        data_i = pd.DataFrame(
                [[j.text_content() for j in i.getchildren()[:-1]] for i in trs],
                columns = ['name', 'ticker', 'currency', 'isin', 'category', 'icb']
                )
        data_i['ticker_intl'] = ["-".join(i.split(" "))+".ST" for i in data_i['ticker'].values]
        data_i['market'] = sync_url.split('/')[-1]
        data_i.drop('currency', axis=1, inplace=True)
        data_i.drop('category', axis=1, inplace=True)
        print('Done! Saving.')

        # CHECK data, so that its as expected

        # then append it to the list
        data.append(data_i)
        sleep(random.random()*3)

    data = pd.concat(data)
except(req.ReadTimeout):
    print('Timed out, trying local file...')
    file_list = ['Stockholm - Listed Companies - Nasdaq.html',
                 'First North Premier - Nasdaq.html',
                 'First North - Listed Companies - Nasdaq.html']
    basepath = '/PATH/WHERE/TO/SAVE/WEBPAGE'
    
    file_list = [os.path.join(basepath,f) for f in file_list]
    
    if not np.array([os.path.isfile(f) for f in file_list]).all():
        print("File(s) doesn't exists")
        raise(FileNotFoundError)
    
    def get_market(s):
        if 'premier' in s.lower():
            return 'first-north-premier'
        elif 'stockholm' in s.lower():
            return 'stockholm'
        elif 'north' in s.lower():
            return 'first-north'
        else:
            raise(StandardError)
    data = []
    
    for file_path in file_list:
        data_i = pd.read_html(file_path)[0]    
        data_i.drop('Sector',inplace=True, axis=1)
        data_i.drop('Fact Sheet',inplace=True, axis=1)
        data_i.drop('Currency',inplace=True, axis=1)
        data_i.dropna(inplace=True)
        data_i.rename(columns={'Name':'name','Symbol':'ticker','ISIN':'isin','ICB Code':'icb'}, inplace=True)
        data_i['ticker_intl'] = ["-".join(i.split(" "))+".ST" for i in data_i['ticker'].values]
        data_i['market'] = get_market(file_path)
        print('Done! Saving.')
        data_i.head()
        # CHECK data, so that its as expected

        # then append it to the list
        data.append(data_i)

    data = pd.concat(data)

engine = create_engine('postgresql+psycopg2://PG_USER:PG_PASSWORD@PG_IP:5432/STOCK_DB')

# Put the header to the table?
data.head(0).to_sql('ticker_list', engine, if_exists='replace',index=False) #truncates the table

# Now put the data in the 
conn = engine.raw_connection()
cur = conn.cursor()
output = io.StringIO()
data.to_csv(output, sep='\t', header=False, index=False)
output.seek(0)
contents = output.getvalue()
cur.copy_from(output, 'ticker_list', null="") # null values become ''
conn.commit()
