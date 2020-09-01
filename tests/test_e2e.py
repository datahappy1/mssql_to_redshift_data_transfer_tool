""" end to end test """

import os

from mssql_to_redshift_data_transfer_tool import __main__

def test_dry_run():
    """
    test dry run
    :return:
    """
    target_dir = os.path.join(os.getcwd(), "files")

    prepared_args = {'database_name': 'MSSQL_to_Redshift',
                     'schema_name': 'mngmt',
                     'generated_csv_files_target_directory': target_dir,
                     'dry_run': True}

    runner_obj = __main__.Runner(**prepared_args)
    actual_result = __main__.Runner.run(runner_obj)

    assert actual_result == 0
