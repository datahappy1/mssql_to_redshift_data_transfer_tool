""" healthcheck connectivity test """
from mssql_to_redshift_data_transfer_tool.lib.mssql import MsSql
from mssql_to_redshift_data_transfer_tool.lib.aws import Aws

AWS = Aws()


def test_connect_mssql():
    """
    MSSQL test connectivity function
    :return:
    """
    conn_mssql = MsSql().__repr__()
    cursor = conn_mssql.cursor()
    cursor.execute("SELECT 0")
    actual_result = str(cursor.fetchone())
    conn_mssql.close()
    assert actual_result == "(0, )"


def test_connect_redshift():
    """
    AWS Redshift test connectivity function
    :return:
    """
    redshift_conn = Aws().__repr__()['redshift_conn']
    cursor = redshift_conn.cursor()
    cursor.execute("SELECT 0")
    actual_result = str(cursor.fetchone())
    AWS.disconnect_redshift()
    assert actual_result == "(0,)"


def test_s3_init():
    """
    AWS S3 test connectivity function
    :return:
    """
    actual_result = AWS.check_bucket()
    assert actual_result == 0
