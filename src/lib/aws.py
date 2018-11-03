from boto3.s3.transfer import S3Transfer
from boto3.exceptions import Boto3Error, S3UploadFailedError
from src.settings import s3_bucketname, s3_targetdir, redshift_db
from src.lib.utils import decode_env_vars
import boto3
import psycopg2
import logging
import sys

# set logging levels for aws module console output
logging.getLogger().setLevel(logging.INFO)


class S3:
    global conn_s3

    try:
        aws_access_key_id = decode_env_vars("aws_access_key_id")
        aws_secret_access_key = decode_env_vars("aws_secret_access_key")

        conn_s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                              aws_secret_access_key=aws_secret_access_key)

        logging.info(f'AWS S3 connection initiated')

    except Boto3Error:
        logging.error('AWS S3 connection failed, Boto3Error')
        sys.exit(1)

    @staticmethod
    def upload(fullfilename, filename):
        try:
            bucketname = s3_bucketname
            targetdir = s3_targetdir

            transfer = S3Transfer(conn_s3)
            transfer.upload_file(fullfilename, bucketname, targetdir + "/" + filename)
            logging.info(f'File {filename} uploaded successfully, target bucket: {bucketname}, '
                         f'target folder: {targetdir}')
        except S3UploadFailedError:
            logging.error('AWS S3 upload failed, S3UploadFailedError')
            sys.exit(1)


class RedShift:
    global conn_redshift

    try:
        redshift_host = decode_env_vars("redshift_host")
        redshift_port = decode_env_vars("redshift_port")
        redshift_user = decode_env_vars("redshift_user")
        redshift_pass = decode_env_vars("redshift_pass")

        redshift_database = redshift_db

        conn_redshift = psycopg2.connect(dbname=redshift_database, host=redshift_host, port=redshift_port,
                                user=redshift_user, password=redshift_pass)

        logging.info(f'AWS Redshift connection initiated')

    except ConnectionError:
        logging.error('AWS Redshift connection failed, ConnectionError')
        sys.exit(1)

    @staticmethod
    def close():
        conn_redshift.close()
        logging.info(f'AWS Redshift connection closed')
        return 0

    @staticmethod
    def copy(tablename, filename):
        try:
            aws_access_key_id = decode_env_vars("aws_access_key_id")
            aws_secret_access_key = decode_env_vars("aws_secret_access_key")

            cur = conn_redshift.cursor()
            cur.execute("begin;")

            copy_redshift_string = 'copy ' + tablename + " from 's3://" + s3_bucketname + '/' + s3_targetdir + '/' \
                                   + filename + "' credentials " \
                                   + "'aws_access_key_id=" + aws_access_key_id + "; aws_secret_access_key=" \
                                   + aws_secret_access_key + "' csv;"
            cur.execute(copy_redshift_string)
            cur.execute("commit;")
        except ConnectionError:
            logging.error('AWS Redshift copy command failed, ConnectionError')
            sys.exit(1)
