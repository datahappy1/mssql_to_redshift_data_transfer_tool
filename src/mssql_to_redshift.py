""" MSSQL to Redshift data transfer tool """
import os
import sys
import argparse
import logging

from src import settings
from src.lib import mssql, aws, utils


def prepare_args():
    """
    The arguments parser function
    :return:
    """

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
    else:
        dry_run = True

    return {"database_name": database_name,
            "schema_name": schema_name,
            "target_directory": target_directory,
            "dry_run": dry_run}


class Runner:
    """
    Class Runner handling the functions for the program flow
    """

    @staticmethod
    def _dry_run_str_add_prefix(dry_run):
        dry_run_str_prefix = 'Dry run ' if dry_run else ''
        return dry_run_str_prefix

    @staticmethod
    def set_logging():
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger("botocore").setLevel(logging.WARNING)

    def __init__(self, database_name, schema_name, target_directory, dry_run):
        self.database_name = database_name
        self.schema_name = schema_name
        self.target_directory = target_directory
        self.dry_run = dry_run
        self.dry_run_str_prefix = Runner._dry_run_str_add_prefix(self.dry_run)
        self.file_names = None
        self.mssql_sp_return_value = None
        self.conn_mssql = None
        self.conn_s3 = None
        self.conn_redshift = None

    def prepare_dir(self):
        """
        1: creating a working folder for the .csv files
        :return:
        """

        try:
            if not os.path.exists(self.target_directory):
                os.makedirs(self.target_directory)
                logging.info('%s folder created', self.target_directory)
            else:
                logging.info('%s folder already exists', self.target_directory)
        except OSError:
            logging.error('Could not create the %s folder, OSError', self.target_directory)
            sys.exit(1)

    def run_ms_sql_phase(self):
        """
        2: execute the [mngmt].[Extract_Filter_BCP] MS SQL Stored Procedure using pyodbc
        :return:
        """

        self.conn_mssql = mssql.init()

        logging.info('%s Generating .csv files using bcp in a stored procedure starting',
                     self.dry_run_str_prefix)

        self.mssql_sp_return_value = mssql.run_extract_filter_bcp(self.conn_mssql,
                                                                  self.database_name,
                                                                  self.schema_name,
                                                                  self.target_directory,
                                                                  self.dry_run)

        if "MS SQL error, details:" in self.mssql_sp_return_value:
            logging.error('SQL code error in the stored procedure')
            logging.error('%s', self.mssql_sp_return_value)
            sys.exit(1)
        elif "No .csv files generated" in self.mssql_sp_return_value:
            logging.error('%s No .csv files generated', self.dry_run_str_prefix)
            sys.exit(1)
        else:
            logging.info('%s Generating .csv files using bcp in a stored procedure '
                         'finished', self.dry_run_str_prefix)

    def check_file_size(self):
        """
        3: check the .csv file size
        :return:
        """

        self.file_names = utils.str_split(self.mssql_sp_return_value)

        for file_name in self.file_names:
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

    def run_aws_s3_phase(self):
        """
        4: upload csv files to S3
        :return:
        """

        self.conn_s3 = aws.init_s3()

        logging.info('%s Upload of the .csv files to the S3 bucket location %s / %s starting',
                     self.dry_run_str_prefix, settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

        for file_name in self.file_names:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]

            if bool(self.dry_run):
                logging.info('%s S3 copy %s', self.dry_run_str_prefix, file_name)
            else:
                aws.upload_to_s3(self.conn_s3, full_file_name, file_name)
                mssql.write_log_row(self.conn_mssql, 'S3 UPLOAD', self.database_name, self.schema_name,
                                    '#N/A', settings.S3_BUCKET_NAME + '/' + settings.S3_TARGET_DIR,
                                    file_name, 'S', 'file ' + file_name + ' copied to S3 bucket')

        logging.info('%s Upload of the .csv files to the S3 bucket location %s / %s finished',
                     self.dry_run_str_prefix, settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

    def run_aws_redshift_phase(self):
        """
        5: run Redshift COPY commands
        :return:
        """

        self.conn_redshift = aws.init_redshift()

        logging.info('%s Copy of the .csv files to the AWS Redshift cluster starting',
                     self.dry_run_str_prefix)

        for file_name in self.file_names:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]
            table_name = file_name[0:(len(file_name) - 24)]

            if bool(self.dry_run):
                logging.info('%s copy %s from s3://%s/%s/%s',
                             self.dry_run_str_prefix, table_name, settings.S3_BUCKET_NAME,
                             settings.S3_TARGET_DIR, file_name)
            else:
                aws.copy_to_redshift(self.conn_redshift, table_name, file_name)
                mssql.write_log_row(self.conn_mssql, 'RS UPLOAD', self.database_name, self.schema_name,
                                    table_name, settings.S3_BUCKET_NAME + '/' +
                                    settings.S3_TARGET_DIR, file_name, 'S', 'file ' + file_name +
                                    ' copied to a Redshift table ' + table_name +
                                    ' from the S3 bucket')

        logging.info('%s Copy of the .csv files to the AWS Redshift cluster finished',
                     self.dry_run_str_prefix)

    def close_conn(self):
        """
        6: close DB connections
        :return:
        """

        mssql.close(self.conn_mssql)
        aws.close_redshift(self.conn_redshift)

    def main(self):
        """
        7: orchestrate project flow
        :return:
        """
        Runner.set_logging()
        Runner.prepare_dir(self)
        Runner.run_ms_sql_phase(self)
        Runner.check_file_size(self)
        Runner.run_aws_s3_phase(self)
        Runner.run_aws_redshift_phase(self)
        Runner.close_conn(self)

        logging.info('Program ran successfully!')

        return 0


if __name__ == "__main__":
    PREPARED_ARGS = prepare_args()
    RUNNER_OBJ = Runner(**PREPARED_ARGS)
    Runner.main(RUNNER_OBJ)
