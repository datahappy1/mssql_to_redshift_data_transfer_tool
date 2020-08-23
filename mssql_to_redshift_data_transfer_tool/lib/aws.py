""" AWS Library """
import logging
import boto3
import psycopg2

from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool.settings import S3_BUCKET_NAME, S3_TARGET_DIR, REDSHIFT_DB
from mssql_to_redshift_data_transfer_tool.lib.utils import get_env_var_value

logging.getLogger().setLevel(logging.INFO)


def init_s3():
    """
    Initiate AWS S3 bucket
    :return: conn_s3 on success, sys.exit on error
    """
    aws_access_key_id = get_env_var_value("aws_access_key_id")
    aws_secret_access_key = get_env_var_value("aws_secret_access_key")

    try:
        conn_s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

        logging.info('AWS S3 set boto3.client success')
        return conn_s3
    except ClientError:
        logging.error('AWS S3 set boto3.client failed, ClientError')
        raise MsSqlToRedshiftBaseException(ClientError)


def upload_to_s3(conn_s3, full_file_name, file_name):
    """
    Upload to S3
    :param conn_s3
    :param full_file_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    try:
        transfer = S3Transfer(conn_s3)
        transfer.upload_file(full_file_name, S3_BUCKET_NAME, S3_TARGET_DIR + "/" + file_name)
        logging.info('%s file uploaded successfully, target bucket: %s, target folder: %s',
                     file_name, S3_BUCKET_NAME, S3_TARGET_DIR)
        return 0
    except S3UploadFailedError:
        logging.error('AWS S3 upload failed, S3UploadFailedError')
        raise MsSqlToRedshiftBaseException(S3UploadFailedError)


def check_bucket(conn_s3):
    """
    Check if the s3 bucket set in settings.py exists and is available
    :param conn_s3
    :return: 0 if the bucket exists and we can access it
    """
    try:
        conn_s3.head_bucket(Bucket=S3_BUCKET_NAME)
        return 0
    except ClientError:
        logging.error('AWS S3 bucket used in settings.py not exists or not available')
        raise MsSqlToRedshiftBaseException(ClientError)


def init_redshift():
    """
    Initiate AWS Redshift
    :return: conn_redshift on success, sys.exit on error
    """
    redshift_host = get_env_var_value("redshift_host")
    redshift_port = get_env_var_value("redshift_port")
    redshift_user = get_env_var_value("redshift_user")
    redshift_pass = get_env_var_value("redshift_pass")

    try:
        redshift_database = REDSHIFT_DB

        conn_redshift = psycopg2.connect(dbname=redshift_database, host=redshift_host,
                                         port=redshift_port, user=redshift_user,
                                         password=redshift_pass)

        logging.info('AWS Redshift connection initiated')
        return conn_redshift
    except psycopg2.Error as pse:
        logging.error(f'AWS Redshift connection failed, {pse}')
        raise MsSqlToRedshiftBaseException(pse)


def close_redshift(conn_redshift):
    """
    Close AWS Redshift connection
    :param conn_redshift
    :return: 0 on success
    """
    conn_redshift.close()
    logging.info('AWS Redshift connection closed')
    return 0


def copy_to_redshift(conn_redshift, table_name, file_name):
    """
    Fire "Copy" commands to load AWS Redshift
    :param conn_redshift:
    :param table_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    aws_access_key_id = get_env_var_value("aws_access_key_id")
    aws_secret_access_key = get_env_var_value("aws_secret_access_key")

    try:
        cur = conn_redshift.cursor()
        cur.execute("begin;")

        copy_redshift_string = 'copy ' + table_name + " from 's3://" + S3_BUCKET_NAME + '/' \
                               + S3_TARGET_DIR + '/' \
                               + file_name + "' credentials " \
                               + "'aws_access_key_id=" + aws_access_key_id \
                               + "; aws_secret_access_key=" + aws_secret_access_key + "' csv;"
        cur.execute(copy_redshift_string)
        cur.execute("commit;")
        return 0
    except psycopg2.Error as pse_err:
        logging.error(f'AWS Redshift copy command failed, {pse_err}')
        raise MsSqlToRedshiftBaseException(pse_err)
