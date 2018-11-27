""" AWS Library """
import logging
import sys
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.exceptions import Boto3Error, S3UploadFailedError
import psycopg2
from src.settings import S3_BUCKET_NAME, S3_TARGET_DIR, REDSHIFT_DB
from src.lib.utils import decode_env_vars

# set logging levels for aws module console output
logging.getLogger().setLevel(logging.INFO)


def init_s3():
    """
    Initiate AWS S3 bucket
    :return: 0 on success, sys.exit on error
    """
    global CONN_S3

    try:
        aws_access_key_id = decode_env_vars("aws_access_key_id")
        aws_secret_access_key = decode_env_vars("aws_secret_access_key")

        CONN_S3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

        logging.info('AWS S3 set boto3.client success')
        return 0
    except Boto3Error:
        logging.error('AWS S3 set boto3.client failed, Boto3Error')
        sys.exit(1)


def upload_to_s3(full_file_name, file_name):
    """
    Upload to S3
    :param full_file_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    try:
        bucket_name = S3_BUCKET_NAME
        target_dir = S3_TARGET_DIR

        transfer = S3Transfer(CONN_S3)
        transfer.upload_file(full_file_name, bucket_name, target_dir + "/" + file_name)
        logging.info('%s file uploaded successfully, target bucket: %s, target folder: %s',
                     file_name, bucket_name, target_dir)
        return 0
    except S3UploadFailedError:
        logging.error('AWS S3 upload failed, S3UploadFailedError')
        sys.exit(1)


def init_redshift():
    """
    Initiate AWS Redshift
    :return: 0 on success, sys.exit on error
    """
    global CONN_REDSHIFT

    try:
        redshift_host = decode_env_vars("redshift_host")
        redshift_port = decode_env_vars("redshift_port")
        redshift_user = decode_env_vars("redshift_user")
        redshift_pass = decode_env_vars("redshift_pass")

        redshift_database = REDSHIFT_DB

        CONN_REDSHIFT = psycopg2.connect(dbname=redshift_database, host=redshift_host,
                                         port=redshift_port, user=redshift_user,
                                         password=redshift_pass)

        logging.info('AWS Redshift connection initiated')
        return 0
    except ConnectionError:
        logging.error('AWS Redshift connection failed, ConnectionError')
        sys.exit(1)


def close_redshift():
    """
    Close AWS Redshift connection
    :return: 0 on success
    """
    CONN_REDSHIFT.close()
    logging.info('AWS Redshift connection closed')
    return 0


def copy_to_redshift(table_name, file_name):
    """
    Fire "Copy" commands to load AWS Redshift
    :param table_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    try:
        aws_access_key_id = decode_env_vars("aws_access_key_id")
        aws_secret_access_key = decode_env_vars("aws_secret_access_key")

        cur = CONN_REDSHIFT.cursor()
        cur.execute("begin;")

        copy_redshift_string = 'copy ' + table_name + " from 's3://" + S3_BUCKET_NAME + '/' \
                               + S3_TARGET_DIR + '/' \
                               + file_name + "' credentials " \
                               + "'aws_access_key_id=" + aws_access_key_id \
                               + "; aws_secret_access_key=" + aws_secret_access_key + "' csv;"
        cur.execute(copy_redshift_string)
        cur.execute("commit;")
        return 0
    except ConnectionError:
        logging.error('AWS Redshift copy command failed, ConnectionError')
        sys.exit(1)
