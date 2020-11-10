import logging
import datetime as dt
import os
from help_functions import read_config

logging_config = read_config(filename='config.ini', section='logging')
LOGPATH = logging_config['logpath']

def init_logging():
    logging.basicConfig(filename=os.path.join(LOGPATH,'stock_syncing-{0}.log'.format(dt.datetime.now().strftime('%y%m%d-%H%M%S')) 
                                          ), 
                    filemode='w', format='%(asctime)s-%(levelname)s: %(message)s',
                    level=logging.INFO)


