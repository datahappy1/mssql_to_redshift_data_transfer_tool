import os
from src import mssql_to_redshift


# assuming we can extract a test table to a csv file, upload to S3 and the Redshift copy command is as expected
def test_integrate():
    # TODO
    # 1 if not exists write row to config mssql table with referring a test table from buildscript
    # 2 run main program mssql_to_redshift.py
    # 3 check that the s3 and Redshift commands are like expected
    targetdir = os.getcwd().rstrip('tests') + 'files'

    mssql_to_redshift.main('MSSQL_to_Redshift', 'mngmt', targetdir, 'TRUE')

    assert 1 == 1
