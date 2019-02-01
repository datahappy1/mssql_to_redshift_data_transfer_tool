""" connectivity test """
from src.lib import mssql
from src.lib import aws


# assuming we can initiate and query a connection cursor to MSSQL
def test_connect_mssql():
    """
    MSSQL test connectivity function
    :return:
    """
    conn_mssql = mssql.init()
    cursor = conn_mssql.cursor()
    cursor.execute("SELECT 1")
    test_mssql = str(cursor.fetchone())
    mssql.close(conn_mssql)
    assert test_mssql == "(1,)"


# assuming we can initiate and query a connection cursor to AWS Redshift
def test_connect_redshift():
    """
    AWS Redshift test connectivity function
    :return:
    """
    conn_redshift = aws.init_redshift()
    cursor = conn_redshift.cursor()
    cursor.execute("SELECT 1")
    test_rs = str(cursor.fetchone())
    aws.close_redshift(conn_redshift)
    assert test_rs == "(1,)"


# assuming we can initiate a connection to AWS S3 and checking that
# the bucket name we declare in settings.py exists and that we have access
# if success, variable "test_s3" returns 0 used for assertion
def test_s3_init():
    """
    AWS S3 test connectivity function
    :return:
    """
    test_s3 = None
    try:
        conn_s3 = aws.init_s3()
        test_s3 = aws.check_bucket(conn_s3)
    finally:
        assert test_s3 == 0
