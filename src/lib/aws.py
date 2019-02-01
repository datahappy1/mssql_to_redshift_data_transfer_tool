""" AWS Library """
import logging
import sys
import boto3
from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
import psycopg2
from src.settings import S3_BUCKET_NAME, S3_TARGET_DIR, REDSHIFT_DB
from src.lib.utils import decode_env_vars

# set logging levels for aws module console output
logging.getLogger().setLevel(logging.INFO)


def init_s3():
    """
    Initiate AWS S3 bucket
    :return: conn_s3 on success, sys.exit on error
    """
    try:
        aws_access_key_id = decode_env_vars("aws_access_key_id")
        aws_secret_access_key = decode_env_vars("aws_secret_access_key")

        conn_s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)

        logging.info('AWS S3 set boto3.client success')
        return conn_s3
    except ClientError:
        logging.error('AWS S3 set boto3.client failed, ClientError')
        sys.exit(1)


def upload_to_s3(conn_s3, full_file_name, file_name):
    """
    Upload to S3
    :param conn_s3
    :param full_file_name:
    :param file_name:
    :return: 0 on success, sys.exit on error
    """
    try:
        bucket_name = S3_BUCKET_NAME
        target_dir = S3_TARGET_DIR

        transfer = S3Transfer(conn_s3)
        transfer.upload_file(full_file_name, bucket_name, target_dir + "/" + file_name)
        logging.info('%s file uploaded successfully, target bucket: %s, target folder: %s',
                     file_name, bucket_name, target_dir)
        return 0
    except S3UploadFailedError:
        logging.error('AWS S3 upload failed, S3UploadFailedError')
        sys.exit(1)


def list_bucket(conn_s3):
    """
    List objects available in my bucket set in settings.py
    :param conn_s3:
    :return: all the keys located in the bucket
    """
    try:
        bucket_name = S3_BUCKET_NAME
        for key in conn_s3.list_objects(Bucket=bucket_name)['Contents']:
            keys = key['Key']
        return keys
    except boto3.exceptions.ResourceNotExistsError:
        logging.warning('AWS S3 %s bucket is empty', bucket_name)


def init_redshift():
    """
    Initiate AWS Redshift
    :return: conn_redshift on success, sys.exit on error
    """
    try:
        redshift_host = decode_env_vars("redshift_host")
        redshift_port = decode_env_vars("redshift_port")
        redshift_user = decode_env_vars("redshift_user")
        redshift_pass = decode_env_vars("redshift_pass")

        redshift_database = REDSHIFT_DB

        conn_redshift = psycopg2.connect(dbname=redshift_database, host=redshift_host,
                                         port=redshift_port, user=redshift_user,
                                         password=redshift_pass)

        logging.info('AWS Redshift connection initiated')
        return conn_redshift
    except ConnectionError:
        logging.error('AWS Redshift connection failed, ConnectionError')
        sys.exit(1)


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
    try:
        aws_access_key_id = decode_env_vars("aws_access_key_id")
        aws_secret_access_key = decode_env_vars("aws_secret_access_key")

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
    except ConnectionError:
        logging.error('AWS Redshift copy command failed, ConnectionError')
        sys.exit(1)
