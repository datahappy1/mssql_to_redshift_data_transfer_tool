""" integration test """

import os
import logging
from io import StringIO
from src import mssql_to_redshift


# assuming we can extract a test table to a csv file, upload to S3
# and the Redshift copy command is looking as expected
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

    s3_command_found = 0
    # redshift_command_found = 0

    for line in log_stream.getvalue().splitlines():
        print(line)
        if 'Dryrun S3 copy Integration_test_table' in line:
            s3_command_found = 1
        # elif 'Dryrun copy Integration_test_table Integration_test_table_' in line:
            # redshift_command_found = 1
        else:
            pass
    print(s3_command_found)
    # assert s3_command_found == 1
    # assert s3_command_found + redshift_command_found == 2


test_integrate()
