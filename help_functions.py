import datetime as dt
import holidays
import random
from time import sleep
from configparser import ConfigParser
import psycopg2
from psycopg2 import extras
import logging


swe_holidays = holidays.Sweden()


# first define how to read config
# read config files
def read_config(filename='database.ini', section='postgresql'):
    # create a parser
    
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


# then read in some configs...
# a bit horrible programming, but just want a POC atm...

#try:
sync_settings = read_config(filename='config.ini', section='stocks')
# move to config file
STOCKMARKET_CLOSES = dt.time( int(sync_settings['market_closes']),00,00)
# in minutes
WAITFORMARKET = int(sync_settings['waitformarket'])
# attempt to sync this far back

START_DATE = dt.date(*[int(i) for i in sync_settings['start_date'].split('-')] )


def get_last_date(ticker, conn, table='public.ticker_prices'):
    #conn = get_conn()
    sql_command = f"SELECT MAX(date) FROM {table} WHERE ticker_intl=\'{ticker}\';"
    cur = conn.cursor()
    cur.execute(sql_command)
    last_date = cur.fetchone()[0]
    conn.commit()
    conn.rollback()
    cur.close()
    return last_date

def get_start_date(ticker, conn, table='public.ticker_prices'):
    # try to get the last date stored, if there
    last_date = get_last_date(ticker, conn, table='public.ticker_prices')
    # starting date is last date + one day
    try:
        # if it doesn't exist, last_date will be np.nan value.
        # the following will raise a TypeError
        # the date column is stored as a date dtype
        sdate = last_date + dt.timedelta(days=1)
        #print('There\'s already data stored. Checking last day synced.')
    except(TypeError):
        # if the ticker doesn't exist, start syncing the full thing (well, since START_DATE at least)
        #logging.WARN(f"Ticker {ticker} not found in database, will try to sync it.")
        logging.warning(f"Could not find ticker {ticker} in database, will try to sync it from {START_DATE}.")
        sdate = START_DATE
    return sdate

def get_dates_to_sync(ticker, 
		conn,  
		table='public.ticker_prices',
		):
    
    date_today = dt.date.today()
    datetime_today = dt.datetime.today()
    # get last date synced, if any, else use global param START_DATE
    sdate = get_start_date(ticker, 
	    conn, 
	    table='public.ticker_prices',
	    )
    
    timeconstraint = dt.datetime.combine(date_today,STOCKMARKET_CLOSES) + dt.timedelta(minutes=WAITFORMARKET)
    # always sync until today unless
    # the market hasn't closed yet, sync until yesterday
    if (datetime_today.time() <= timeconstraint.time()):
        #print('\n  *Stock market closes at {0}*, so lets only sync until yesterday.'.format(STOCKMARKET_CLOSES))
        edate = date_today - dt.timedelta(days=1)        
    else:
        edate = date_today
    # check that last sync date is not a (bank) holiday
    # NOTE that this will not take care of Saturdays
    while edate in swe_holidays:
        if edate in swe_holidays:
            logging.info('Last sync day is a public holiday ({0}), testing the day before...'.format(edate.strftime('%Y-%m-%d')) )
        edate -= dt.timedelta(days=1)
    # number of days to sync
    daystosync = (edate-sdate).days + 1
    # if number of days to sync is negative it means last sync date is today.
    if daystosync <= 0:
        logging.info('\n   *No data (dates) to sync for ticker: {0}*'.format(ticker))
        #print('    - It was last synced {0} day(s) ago (on {1})'.format((edate-sdate).days + 1, sdate))
        #print('\n*Continuing to next ticker...*')
        return False
    # If start is on saturday/sunday, and we are trying to sync sat, or sat+sun
    # we have to wait.
    if sdate.weekday() > 4 and daystosync < 2:
        logging.info(f'*Stock market not open on weekends!* Skipping {ticker}')
        #print('\n*Continuing to next ticker...*')
        return False
    #
    #
    #
    # FOR TESTING, REMOVE after checking syncing
    #
    #edate = date_today - dt.timedelta(days=3)
    #
    #
    #
    #
    if sdate>edate:
        logging.info('{0}: No data to sync.'.format(key))
        return False
    # format input for Yahoo
    edate = edate + dt.timedelta(days=1)
    sdate_str = sdate.strftime('%Y-%m-%d')
    edate_str = edate.strftime('%Y-%m-%d')
    return (sdate,edate),(sdate_str,edate_str)


# db connect

def connect(filename='database.ini'):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = read_config(filename=filename)

        # connect to the PostgreSQL server
        logging.info(f"Connecting to the PostgreSQL database as {params.get('user')}...")
        conn = psycopg2.connect(**params)
        
        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        logging.info('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        logging.info(db_version)
        
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()
            logging.info('Database connection closed.')

            
def get_conn(filename='database.ini'):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = read_config(filename=filename)

        # connect to the PostgreSQL server
        logging.info(f"Connecting to the PostgreSQL database as {params.get('user')}...")
        conn = psycopg2.connect(**params)
        
        # create a cursor
        #cur = conn.cursor()
        
        # execute a statement
        #print('PostgreSQL database version:')
        #cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        #db_version = cur.fetchone()
        #print(db_version)
        # close the communication with the PostgreSQL
        #cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        
    return conn

# write data to database
def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    https://naysan.ca/2020/05/16/pandas-to-postgresql-using-psycopg2-bulk-insert-using-execute_values/
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    #query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    query = f"INSERT INTO {table}({cols}) VALUES %s"
    #print(query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error: {error}")
        # try to catch it.
        if 'duplicate' in error.pgerror:
            if error.pgcode == '23505':
                logging.info('Entry with identical primary key already exists. Uniqueness broken.')
            else:
                logging.info("There\'s a duplicate entry/row/column/? in the db")
        conn.rollback()
        cursor.close()
        return 0
    #print("execute_values() done")
    cursor.close()
    return 1
