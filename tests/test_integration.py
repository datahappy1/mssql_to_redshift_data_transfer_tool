""" integration test """

import os
from mssql_to_redshift_data_transfer_tool import mssql_to_redshift


# assuming we can run the full dry run flow and the program returns success
def test_integrate():
    """
    main test integration function
    :return:
    """
    target_dir = os.getcwd().rstrip('tests') + 'files'
    obj = mssql_to_redshift.Runner(database_name='MSSQL_to_Redshift', schema_name='mngmt',
                                   generated_csv_files_target_directory=target_dir, dry_run=1)

    ret = mssql_to_redshift.Runner.run(obj)

    assert ret == 0
