import os
import logging
from src import mssql_to_redshift
from io import StringIO


# assuming we can extract a test table to a csv file, upload to S3 and the Redshift copy command is as expected
def test_integrate():

    targetdir = os.getcwd().rstrip('tests') + 'files'
    log_stream = StringIO()
    logging.basicConfig(stream=log_stream, level=logging.INFO)

    mssql_to_redshift.main('MSSQL_to_Redshift', 'mngmt', targetdir, 'TRUE')

    s3commandfound = 0
    redshiftcommandfound = 0

    for line in log_stream.getvalue().splitlines():
        if 'Dryrun S3 copy Integration_test_table' in line:
            s3commandfound = 1
        elif 'Dryrun copy Integration_test_table Integration_test_table_' in line:
            redshiftcommandfound = 1
        else:
            pass
    print(s3commandfound)
    assert  s3commandfound == 1
    #assert s3commandfound + redshiftcommandfound == 2
