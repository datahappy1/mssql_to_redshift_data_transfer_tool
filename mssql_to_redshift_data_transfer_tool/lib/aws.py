""" AWS module """
import boto3
import psycopg2

from boto3.s3.transfer import S3Transfer
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from mssql_to_redshift_data_transfer_tool.lib import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, \
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_USER, REDSHIFT_PASS
from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool.settings import S3_BUCKET_NAME, S3_TARGET_DIR, REDSHIFT_DB


def _init_s3_client():
    try:
        return boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    except ClientError as cli_err:
        raise MsSqlToRedshiftBaseException(cli_err)


def _connect_to_redshift():
    try:
        return psycopg2.connect(dbname=REDSHIFT_DB, host=REDSHIFT_HOST,
                                port=REDSHIFT_PORT, user=REDSHIFT_USER,
                                password=REDSHIFT_PASS)
    except psycopg2.Error as pse:
        raise MsSqlToRedshiftBaseException(pse)


class S3:
    """
    AWS S3 class
    """
    def __init__(self):
        self.s3_client = _init_s3_client()

    def __repr__(self):
        return self.s3_client

    def upload_to_s3(self, local_file_name, remote_file_name):
        """
        method for uploading a file to S3
        :param local_file_name:
        :param remote_file_name:
        :return:
        """
        try:
            transfer = S3Transfer(self.s3_client)
            transfer.upload_file(local_file_name, S3_BUCKET_NAME,
                                 S3_TARGET_DIR + "/" + remote_file_name)
        except S3UploadFailedError as s3_upload_err:
            raise MsSqlToRedshiftBaseException(s3_upload_err)

    def check_bucket(self):
        """
        method to verify S3 bucket is available
        :return:
        """
        try:
            self.s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            return 0
        except ClientError as cli_err:
            raise MsSqlToRedshiftBaseException(cli_err)


class Redshift:
    """
    AWS Redshift class
    """
    def __init__(self):
        self.redshift_conn = _connect_to_redshift()

    def __repr__(self):
        return self.redshift_conn

    def disconnect(self):
        """
        method for closing connection
        :return:
        """
        self.redshift_conn.close()

    def copy_to_redshift(self, dry_run, table_name, file_name):
        """
        method for executing COPY command to load .csv file to a Redshift table
        :param dry_run:
        :param table_name:
        :param file_name:
        :return:
        """
        copy_redshift_cmd = 'copy ' + table_name + " from 's3://" + S3_BUCKET_NAME + '/' \
                            + S3_TARGET_DIR + '/' \
                            + file_name + "' credentials " \
                            + "'aws_access_key_id=" + AWS_ACCESS_KEY_ID \
                            + ";aws_secret_access_key=" + AWS_SECRET_ACCESS_KEY + "' csv;"

        try:
            cur = self.redshift_conn.cursor()
            cur.execute("begin;")
            cur.execute(copy_redshift_cmd)

            if dry_run:
                cur.execute("rollback;")
            else:
                cur.execute("commit;")

        except psycopg2.Error as pse_err:
            raise MsSqlToRedshiftBaseException(pse_err)
