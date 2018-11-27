""" integration test """

import os
from src import mssql_to_redshift


# assuming we can extract a test table to a csv file, upload to S3
# the Redshift copy command is looking as expected and program returns success
def test_integrate():
    """
    main test integration function
    :return:
    """
    target_dir = os.getcwd().rstrip('tests') + 'files'

    obj = mssql_to_redshift.Runner(database_name='MSSQL_to_Redshift', schema_name='mngmt',
                                   target_directory=target_dir, dry_run=1, dry_run_str_prefix='',
                                   files='', ret='', conn_mssql='', conn_s3='', conn_redshift='')

    ret = mssql_to_redshift.Runner.main(obj)
    success_found = 0

    if ret == 0:
        success_found = 1
    else:
        pass

    assert success_found == 1
