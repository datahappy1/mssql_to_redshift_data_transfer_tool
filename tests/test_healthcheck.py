""" healthcheck connectivity test """
from mssql_to_redshift_data_transfer_tool.lib.mssql import MsSql
from mssql_to_redshift_data_transfer_tool.lib.aws import S3, Redshift


def test_connect_mssql():
    """
    MSSQL test connectivity function
    :return:
    """
    mssql_client = MsSql()
    conn_mssql = mssql_client.__repr__()

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
    aws_redshift_client = Redshift()
    redshift_conn = aws_redshift_client.__repr__()

    cursor = redshift_conn.cursor()
    cursor.execute("SELECT 0")

    actual_result = str(cursor.fetchone())

    aws_redshift_client.disconnect()

    assert actual_result == "(0,)"


def test_s3_init():
    """
    AWS S3 test connectivity function
    :return:
    """
    aws_s3_client = S3()

    actual_result = aws_s3_client.check_bucket()

    assert actual_result == 0
