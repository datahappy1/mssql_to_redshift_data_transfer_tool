""" connectivity test """

import logging
from src.lib import mssql
from src.lib import aws


# assuming we can initiate a connection to S3 and both source and target DBs
def test_connect():
    """
    main test connectivity function
    :return:
    """

    assertion = 0

    try:
        mssql.init()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to MSSQL failed')

    try:
        aws.init_s3()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to AWS S3 failed')

    try:
        aws.init_redshift()
        assertion = assertion + 1
    except ConnectionError:
        logging.info('Connection to AWS Redshift failed')

    assert assertion == 3
