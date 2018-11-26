""" integration test """

import os
import logging
from io import StringIO
from src import mssql_to_redshift


# assuming we can extract a test table to a csv file, upload to S3
# the Redshift copy command is looking as expected and program returns success
def test_integrate():
    """
    main test integration function
    :return:
    """
    target_dir = os.getcwd().rstrip('tests') + 'files'
    log_stream = StringIO()
    logging.basicConfig(stream=log_stream, level=logging.INFO)

    obj = mssql_to_redshift.Runner(database_name='MSSQL_to_Redshift', schema_name='mngmt',
                                   target_directory=target_dir, dry_run=1, dry_run_str_prefix='',
                                   files='', ret='')

    mssql_to_redshift.Runner.main(obj)

    success_found = 0

    for line in log_stream.getvalue().splitlines():
        print(line)
        if 'Program ran successfully!' in line:
            success_found = 1
        else:
            pass
    #print(success_found)

    assert success_found == 1


#test_integrate()
