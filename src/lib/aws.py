from boto3.s3.transfer import S3Transfer
from boto3.exceptions import Boto3Error, S3UploadFailedError
from src.settings import s3_bucketname, s3_targetdir, redshift_db
from src.lib.utils import env_vars
import boto3
import psycopg2
import logging
import json
import sys


class S3:
    @staticmethod
    def init():
        try:
            env_var_json = json.loads(env_vars())

            aws_access_key_id = env_var_json["aws_access_key_id"]
            aws_secret_access_key = env_var_json["aws_secret_access_key"]

            client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key)
            return client

        except Boto3Error:
            logging.error('AWS S3 connection failed, Boto3Error')
            sys.exit(1)

    @staticmethod
    def upload(fullfilename, filename):
        try:
            bucketname = s3_bucketname
            targetdir = s3_targetdir

            client = S3.init()

            transfer = S3Transfer(client)
            transfer.upload_file(fullfilename, bucketname, targetdir + "/" + filename)
            logging.info(f'File {filename} uploaded successfully, target bucket: {bucketname}, '
                         f'target folder: {targetdir}')

        except S3UploadFailedError:
            logging.error('AWS S3 upload failed, S3UploadFailedError')
            sys.exit(1)


class RedShift:
    @staticmethod
    def init():
        try:
            env_var_json = json.loads(env_vars())

            redshift_host = env_var_json["redshift_host"]
            redshift_port = env_var_json["redshift_port"]
            redshift_user = env_var_json["redshift_user"]
            redshift_pass = env_var_json["redshift_pass"]
            redshift_database = redshift_db

            conn = psycopg2.connect(dbname=redshift_database, host=redshift_host, port=redshift_port,
                                    user=redshift_user, password=redshift_pass)
            return conn

        except ConnectionError:
            logging.error('AWS Redshift connection failed, ConnectionError')
            sys.exit(1)

    @staticmethod
    def copy(tablename, filename):
        try:
            RedShift.init()

            env_var_json = json.loads(env_vars())
            aws_access_key_id = env_var_json["aws_access_key_id"]
            aws_secret_access_key = env_var_json["aws_secret_access_key"]

            cur = RedShift.init().cursor()
            cur.execute("begin;")

            copy_redshift_string = 'copy ' + tablename + " from 's3://" + s3_bucketname + '/' + s3_targetdir + '/' \
                                   + filename + "' credentials " \
                                   + "'aws_access_key_id=" + aws_access_key_id + "; aws_secret_access_key=" \
                                   + aws_secret_access_key + "' csv;"
            cur.execute(copy_redshift_string)
            #cur.execute("copy kpi_kpireport from 's3://clab-migration/kpi.csv' credentials 'aws_access_key_id=ID;aws_secret_access_key=KEY/KEY/pL/KEY' csv;")
            #print(copy_redshift_string)
            cur.execute("commit;")

        except ConnectionError:
            logging.error('AWS Redshift copy command failed, ConnectionError')
            sys.exit(1)
