""" AWS module """
import boto3
import psycopg2
from os import getenv

from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool.settings import S3_BUCKET_NAME, S3_TARGET_DIR, REDSHIFT_DB


def _init_s3_client(aws_access_key_id, aws_secret_access_key):
    try:
        return boto3.client('s3', aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    except ClientError:
        raise MsSqlToRedshiftBaseException(ClientError)


def _connect_to_redshift():
    try:
        return psycopg2.connect(dbname=REDSHIFT_DB, host=getenv("redshift_host"),
                                port=getenv("redshift_port"), user=getenv("redshift_user"),
                                password=getenv("redshift_pass"))
    except psycopg2.Error as pse:
        raise MsSqlToRedshiftBaseException(pse)


class Aws:
    def __init__(self):
        self.aws_access_key_id = getenv('aws_access_key_id')
        self.aws_secret_access_key = getenv('aws_secret_access_key')
        self.s3_client = _init_s3_client(self.aws_secret_access_key, self.aws_secret_access_key)
        self.redshift_conn = _connect_to_redshift()

    def upload_to_s3(self, full_file_name, file_name):
        try:
            transfer = S3Transfer(self.s3_client)
            transfer.upload_file(full_file_name, S3_BUCKET_NAME, S3_TARGET_DIR + "/" + file_name)
        except S3UploadFailedError:
            raise MsSqlToRedshiftBaseException(S3UploadFailedError)

    def check_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError:
            raise MsSqlToRedshiftBaseException(ClientError)

    def disconnect_redshift(self):
        self.redshift_conn.disconnect()

    def copy_to_redshift(self, table_name, file_name):
        try:
            cur = self.redshift_conn.cursor()
            cur.execute("begin;")

            copy_redshift_cmd = 'copy ' + table_name + " from 's3://" + S3_BUCKET_NAME + '/' \
                                + S3_TARGET_DIR + '/' \
                                + file_name + "' credentials " \
                                + "'aws_access_key_id=" + self.aws_access_key_id \
                                + "; aws_secret_access_key=" + self.aws_secret_access_key + "' csv;"
            cur.execute(copy_redshift_cmd)
            cur.execute("commit;")
        except psycopg2.Error as pse_err:
            raise MsSqlToRedshiftBaseException(pse_err)
