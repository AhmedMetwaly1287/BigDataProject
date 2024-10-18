import logging as log
from datetime import datetime
import os

CURR_DATE = datetime.now()
CURR_DATE_FORMATTED = CURR_DATE.strftime("%d_%m_%Y")
LOG_PATH = "./Logs"


def Initialize():
    """This function initializes the logger to be ready to be used."""
   
    logger = log.getLogger('Logging')
    logger.setLevel(log.INFO)

    log_file_path = os.path.join(LOG_PATH, CURR_DATE_FORMATTED + '.log')
    
    fileToStoreLogIn = log.FileHandler(log_file_path)
    
    formatter = log.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fileToStoreLogIn.setFormatter(formatter)

    logger.addHandler(fileToStoreLogIn)
    
    return logger
