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
    cursor.execute("SELECT 0")
    test = str(cursor.fetchone())
    mssql.close(conn_mssql)
    assert test == "(0,)"


# assuming we can initiate and query a connection cursor to AWS Redshift
def test_connect_redshift():
    """
    AWS Redshift test connectivity function
    :return:
    """
    conn_redshift = aws.init_redshift()
    cursor = conn_redshift.cursor()
    cursor.execute("SELECT 0")
    test = str(cursor.fetchone())
    aws.close_redshift(conn_redshift)
    assert test == "(0,)"


# assuming we can initiate a connection to AWS S3 and checking
# the bucket we setup in settings.py exists and that we can connect
# if success, test_pass returns 0 used in assertion
def test_s3_init():
    """
    AWS S3 test connectivity function
    :return:
    """
    conn_s3 = aws.init_s3()
    test = aws.check_bucket(conn_s3)
    assert test == 0
