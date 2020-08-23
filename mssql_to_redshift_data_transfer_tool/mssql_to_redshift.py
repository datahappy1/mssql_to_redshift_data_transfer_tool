""" MSSQL to Redshift data transfer tool """
import os
import argparse
import logging

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool import settings
from mssql_to_redshift_data_transfer_tool.lib import mssql, aws, utils


def setup_logger(dry_run=None):
    """
    setup_logger function
    :param dry_run:
    :return:
    """
    logging.basicConfig()
    logging.getLogger("botocore").setLevel(logging.WARNING)

    _name = 'DRY RUN ' + __name__ if bool(dry_run) else __name__

    logger = logging.getLogger(_name)
    return logger


class Runner:
    """
    Class Runner handling the functions for the program flow
    """

    def __init__(self, database_name, schema_name, generated_csv_files_target_directory, dry_run):
        self.database_name = database_name
        self.schema_name = schema_name
        self.logger = setup_logger(dry_run=dry_run)
        self.dry_run = dry_run
        self.generated_csv_files_target_directory = generated_csv_files_target_directory
        self.generated_csv_file_names = None

    @property
    def is_dry_run(self):
        return True if self.dry_run else False

    def prepare_dir(self):
        """
        create a working folder for the .csv files
        :return:
        """

        if not os.path.exists(self.generated_csv_files_target_directory):
            try:
                os.makedirs(self.generated_csv_files_target_directory)
                self.logger.info('%s folder created', self.generated_csv_files_target_directory)
            except OSError as os_err:
                self.logger.error('Could not create the %s folder, OSError', self.generated_csv_files_target_directory)
                raise MsSqlToRedshiftBaseException(os_err)
        else:
            self.logger.info('%s folder already exists', self.generated_csv_files_target_directory)

    def run_ms_sql_phase(self):
        """
        execute the [mngmt].[Extract_Filter_BCP] MS SQL Stored Procedure using pyodbc
        :return:
        """

        conn_mssql = mssql.init()

        self.logger.info('Generating .csv files using bcp in a stored procedure started')

        mssql_sp_return_value = mssql.run_extract_filter_bcp(conn_mssql,
                                                             self.database_name,
                                                             self.schema_name,
                                                             self.generated_csv_files_target_directory,
                                                             self.dry_run)

        if "MS SQL error, details:" in mssql_sp_return_value:
            self.logger.error('SQL code error in the stored procedure %s', mssql_sp_return_value)
            raise MsSqlToRedshiftBaseException

        elif "No .csv files generated" in mssql_sp_return_value:
            self.logger.error('No .csv files generated')
            raise MsSqlToRedshiftBaseException

        else:
            self.logger.info('Generating .csv files using BCP in a stored procedure finished')

        mssql.close(conn_mssql)

        return mssql_sp_return_value

    def check_file_size(self, mssql_sp_return_value):
        """
        check the .csv file size
        :return:
        """

        self.generated_csv_file_names = utils.str_splitter(mssql_sp_return_value)

        for file_name in self.generated_csv_file_names:
            file_name = file_name.strip("'")
            file_size = os.path.getsize(file_name)
            # check that the file size in MB is not greater than settings.CSV_MAX_FILE_SIZE
            if file_size / 1048576 > settings.CSV_MAX_FILE_SIZE:
                self.logger.error('The file %s has file size %s MB and that is larger than '
                                  'CSV_MAX_FILE_SIZE set in settings.py', file_name, str(file_size))
                raise MsSqlToRedshiftBaseException

        self.logger.info('All files passed the max file size check, value %s declared in settings.py',
                         settings.CSV_MAX_FILE_SIZE)

    def run_aws_s3_phase(self):
        """
        upload csv files to S3
        :return:
        """

        conn_s3 = aws.init_s3()

        self.logger.info('Upload of the .csv files to the S3 bucket location %s / %s starting',
                         settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

        for file_name in self.generated_csv_file_names:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]

            if self.is_dry_run:
                logging.info('S3 copy %s', file_name)
            else:
                aws.upload_to_s3(conn_s3, full_file_name, file_name)
                logging.info('S3 UPLOAD of %s to S3 bucket passed', file_name)

        self.logger.info('%s Upload of the .csv files to the S3 bucket location %s / %s finished',
                         settings.S3_BUCKET_NAME, settings.S3_TARGET_DIR)

    def run_aws_redshift_phase(self):
        """
        run Redshift COPY commands
        :return:
        """

        conn_redshift = aws.init_redshift()

        self.logger.info('Copy of the .csv files to the AWS Redshift cluster starting')

        for file_name in self.generated_csv_file_names:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]
            table_name = file_name[0:(len(file_name) - 24)]

            if self.is_dry_run:
                self.logger.info('%s copy %s from s3://%s/%s/%s',
                                 table_name, settings.S3_BUCKET_NAME,
                                 settings.S3_TARGET_DIR, file_name)
            else:
                aws.copy_to_redshift(conn_redshift, table_name, file_name)

        self.logger.info('Copy of the .csv files to the AWS Redshift cluster finished')

        aws.close_redshift(conn_redshift)

    def run(self):
        """
        orchestrate flow
        :return:
        """
        Runner.prepare_dir(self)
        mssql_sp_return_value = Runner.run_ms_sql_phase(self)
        Runner.check_file_size(self, mssql_sp_return_value)
        Runner.run_aws_s3_phase(self)
        Runner.run_aws_redshift_phase(self)

        self.logger.info('Program finished successfully!')

        return 0


def prepare_args():
    """
    The arguments parser function
    :return:
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-dn', '--databasename', type=str, required=True)
    parser.add_argument('-sn', '--schemaname', type=str, required=True)
    parser.add_argument('-td', '--generated_csv_files_target_directory', type=str, required=True)
    parser.add_argument('-dr', '--dryrun', type=str, required=True)
    parsed = parser.parse_args()

    database_name = parsed.databasename
    schema_name = parsed.schemaname
    generated_csv_files_target_directory = parsed.generated_csv_files_target_directory.replace('\f', '\\f')

    dry_run = parsed.dryrun
    # arg parse bool data type known bug workaround
    dry_run = False if dry_run.lower() in ('no', 'false', 'f', 'n', 0) else True

    return {"database_name": database_name,
            "schema_name": schema_name,
            "generated_csv_files_target_directory": generated_csv_files_target_directory,
            "dry_run": dry_run}


if __name__ == "__main__":
    PREPARED_ARGS = prepare_args()
    RUNNER_OBJ = Runner(**PREPARED_ARGS)
    Runner.run(RUNNER_OBJ)
