from src.lib import mssql
from src.lib import aws
import logging


# assuming we can initiate a connection to S3 and both source and target DBs
def test_connect():
    assertion = 0

    try:
        mssql.init()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to MSSQL failed')
        pass

    try:
        aws.init_s3()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to AWS S3 failed')
        pass

    try:
        aws.init_redshift()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to AWS Redshift failed')
        pass

    assert assertion == 3
