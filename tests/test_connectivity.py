from src.lib import mssql
from src.lib import aws
import logging


# assuming we can initiate a connection to S3 and both source and target DBs
def connect():
    try:
        mssql.General.init()
        assertion = 1
    except ConnectionError:
        logging.info(f'Connection to MSSQL failed')
        pass

    try:
        aws.S3.init()
        assertion = assertion + 1
    except ConnectionError:
        logging.info(f'Connection to AWS S3 failed')
        pass

    try:
        aws.RedShift.init()
        assertion = assertion + 1
    except ConnectionError:
        logging.info(f'Connection to AWS Redshift failed')
        pass

    assert assertion == 3
