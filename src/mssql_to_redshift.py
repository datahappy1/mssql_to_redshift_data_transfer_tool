""" MSSQL to Redshift data transfer tool """

import argparse
import logging
import os
import sys
from src import settings
from src.lib import mssql
from src.lib import aws
from src.lib import utils


def prepare_args():
    ###########################################################################################
    """ 0: The arguments parser and main launcher """
    ###########################################################################################

    parser = argparse.ArgumentParser()
    parser.add_argument('-dn', '--databasename', type=str, required=True)
    parser.add_argument('-sn', '--schemaname', type=str, required=True)
    parser.add_argument('-td', '--targetdirectory', type=str, required=True)
    parser.add_argument('-dr', '--dryrun', type=str, required=True)
    parsed = parser.parse_args()

    database_name = parsed.databasename
    schema_name = parsed.schemaname

    target_directory = parsed.targetdirectory
    target_directory = target_directory.replace('\f', '\\f')

    dry_run = parsed.dryrun
    # arg parse bool data type known bug workaround
    if dry_run.lower() in ('no', 'false', 'f', 'n', 0):
        dry_run = False
        dry_run_str_prefix = ''
    else:
        dry_run = True
        dry_run_str_prefix = 'Dry run '

    obj = Runner(database_name, schema_name, target_directory, dry_run, dry_run_str_prefix,
                 files='', ret='', conn_mssql='', conn_s3='', conn_redshift='')
    Runner.main(obj)


class Runner:
    """
    Class Runner handling the functions for the program flow
    """
    def __init__(self, database_name, schema_name, target_directory, dry_run,
                 dry_run_str_prefix, files, ret, conn_mssql, conn_s3, conn_redshift):
        self.database_name = database_name
        self.schema_name = schema_name
        self.target_directory = target_directory
        self.dry_run = dry_run
        self.dry_run_str_prefix = dry_run_str_prefix
        self.files = files
        self.ret = ret
        self.conn_mssql = conn_mssql
        self.conn_s3 = conn_s3
        self.conn_redshift = conn_redshift

    def prepare_dir(self):
        ###########################################################################################
        """ 1: preparations and creating a working folder for the .csv files """
        ###########################################################################################
        target_directory = self.target_directory
        try:
            if not os.path.exists(target_directory):
                os.makedirs(target_directory)
                logging.info('%s folder created', target_directory)
            else:
                logging.info('%s folder already exists', target_directory)
        except OSError:
            logging.error('Could not create the %s folder, OSError', target_directory)
            sys.exit(1)

    def ms_sql(self):
        ###########################################################################################
        """ 2: execute the [mngmt].[Extract_Filter_BCP] MS SQL Stored Procedure with pymssql """
        ###########################################################################################
        database_name = self.database_name
        schema_name = self.schema_name
        target_directory = self.target_directory
        dry_run = self.dry_run
        dry_run_str_prefix = self.dry_run_str_prefix

        self.conn_mssql = mssql.init()

        logging.info('%s Generating .csv files using bcp in a stored procedure starting',
                     dry_run_str_prefix)

        ret = mssql.run_extract_filter_bcp(self.conn_mssql, database_name, schema_name,
                                           target_directory, dry_run)
        self.ret = ret

        if "MS SQL error, details:" in ret:
            logging.error('SQL code error in the stored procedure')
            logging.error('%s', ret)
            sys.exit(1)
        elif "No .csv files generated" in ret:
            logging.error('%s No .csv files generated', dry_run_str_prefix)
            sys.exit(1)
        else:
            logging.info('%s Generating .csv files using bcp in a stored procedure '
                         'finished', dry_run_str_prefix)

    def file_size(self):
        ###########################################################################################
        """ 3: check the .csv file size """
        ###########################################################################################

        files = utils.str_split(self.ret)
        self.files = files

        for file_name in files:
            file_name = file_name.strip("'")
            file_size = os.path.getsize(file_name)
            # check that the file size in MB is not greater than settings.CSV_MAX_FILE_SIZE
            if file_size / 1048576 > settings.CSV_MAX_FILE_SIZE:
                logging.error('The file %s has file size %s MB and that is larger than '
                              'CSV_MAX_FILE_SIZE set in settings.py', file_name, str(file_size))
                sys.exit(1)
            else:
                pass

        logging.info('All files passed the max file size check, value %s declared in settings.py',
                     settings.CSV_MAX_FILE_SIZE)

    def aws_s3(self):
        ###########################################################################################
        """ 4: upload csv files to S3 """
        ###########################################################################################
        dry_run_str_prefix = self.dry_run_str_prefix
        dry_run = self.dry_run
        files = self.files
        database_name = self.database_name
        schema_name = self.schema_name

        self.conn_s3 = aws.init_s3()

        logging.info('%s Upload of the .csv files to the S3 bucket location %s / %s starting',
                     dry_run_str_prefix, settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

        for file_name in files:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]

            if bool(dry_run):
                logging.info('%s S3 copy %s', dry_run_str_prefix, file_name)
            else:
                aws.upload_to_s3(self.conn_s3, full_file_name, file_name)
                mssql.write_log_row(self.conn_mssql, 'S3 UPLOAD', database_name, schema_name,
                                    '#N/A', settings.S3_BUCKET_NAME + '/' + settings.S3_TARGET_DIR,
                                    file_name, 'S', 'file ' + file_name + ' copied to S3 bucket')

        logging.info('%s Upload of the .csv files to the S3 bucket location %s / %s finished',
                     dry_run_str_prefix, settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

    def aws_redshift(self):
        ###########################################################################################
        """ 5: run Redshift COPY commands """
        ###########################################################################################
        dry_run_str_prefix = self.dry_run_str_prefix
        files = self.files
        dry_run = self.dry_run
        database_name = self.database_name
        schema_name = self.schema_name

        self.conn_redshift = aws.init_redshift()

        logging.info('%s Copy of the .csv files to the AWS Redshift cluster starting',
                     dry_run_str_prefix)

        for file_name in files:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]
            table_name = file_name[0:(len(file_name)-24)]

            if bool(dry_run):
                logging.info('%s copy %s from s3://%s/%s/%s',
                             dry_run_str_prefix, table_name, settings.S3_BUCKET_NAME,
                             settings.S3_TARGET_DIR, file_name)
            else:
                aws.copy_to_redshift(self.conn_redshift, table_name, file_name)
                mssql.write_log_row(self.conn_mssql, 'RS UPLOAD', database_name, schema_name,
                                    table_name, settings.S3_BUCKET_NAME + '/' +
                                    settings.S3_TARGET_DIR, file_name, 'S', 'file ' + file_name +
                                    ' copied to a Redshift table ' + table_name +
                                    ' from the S3 bucket')

        logging.info('%s Copy of the .csv files to the AWS Redshift cluster finished',
                     dry_run_str_prefix)

    def close_conn(self):
        ###########################################################################################
        """ 6: close DB connections """
        ###########################################################################################
        mssql.close(self.conn_mssql)
        aws.close_redshift(self.conn_redshift)

    def main(self):
        ###########################################################################################
        """ 7: orchestrate project flow """
        ###########################################################################################
        # set logging levels for main function console output
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger("botocore").setLevel(logging.WARNING)

        Runner.prepare_dir(self)
        Runner.ms_sql(self)
        Runner.file_size(self)
        Runner.aws_s3(self)
        Runner.aws_redshift(self)
        Runner.close_conn(self)

        logging.info('Program ran successfully!')
        return 0


if __name__ == "__main__":
    prepare_args()
