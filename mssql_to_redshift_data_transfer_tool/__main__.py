""" __main__ module """
import os
import argparse
import logging

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool.settings import CSV_MAX_FILE_SIZE
from mssql_to_redshift_data_transfer_tool.lib.mssql import MsSql
from mssql_to_redshift_data_transfer_tool.lib.aws import Aws


def get_logger(dry_run=None):
    logging.basicConfig(level=logging.INFO)
    # logging.getLogger("botocore").setLevel(logging.WARNING)

    _name = 'DRY RUN ' + __name__ if bool(dry_run) else __name__
    logger = logging.getLogger(_name)

    return logger


class Runner:
    def __init__(self, database_name, schema_name, generated_csv_files_target_directory, dry_run):
        self.database_name = database_name
        self.schema_name = schema_name
        self.is_dry_run = dry_run
        self.generated_csv_files_target_directory = generated_csv_files_target_directory
        self.logger = get_logger(dry_run=self.is_dry_run)
        self.aws_obj = Aws()

    def prepare_target_dir_if_not_exists(self):
        if not os.path.exists(self.generated_csv_files_target_directory):
            try:
                os.makedirs(self.generated_csv_files_target_directory)
                self.logger.info('%s folder created', self.generated_csv_files_target_directory)
            except OSError as os_err:
                self.logger.error('Could not create the %s folder, OSError', self.generated_csv_files_target_directory)
                raise MsSqlToRedshiftBaseException(os_err)

    def run_ms_sql_extract_stored_procedure(self):
        conn_mssql = MsSql()

        self.logger.info('Generating .csv files using BCP in a stored procedure started')

        _return_sp_value = conn_mssql.run_extract_filter_bcp_stored_procedure(
            self.database_name,
            self.schema_name,
            self.generated_csv_files_target_directory,
            self.is_dry_run)

        conn_mssql.disconnect()

        if "MS SQL error, details:" in _return_sp_value:
            self.logger.error('SQL code error in the stored procedure %s', _return_sp_value)
            raise MsSqlToRedshiftBaseException

        elif "No .csv files generated" in _return_sp_value:
            self.logger.error('No .csv files generated')
            raise MsSqlToRedshiftBaseException

        else:
            self.logger.info('Generating .csv files using BCP in a stored procedure finished')

        created_csv_file_names_list = _return_sp_value[0][0].strip('()').split(',')

        return created_csv_file_names_list

    def check_files_size(self, created_csv_file_names_list):

        for file_name in created_csv_file_names_list:
            if os.path.isfile(file_name):
                file_size = os.path.getsize(file_name)

                if file_size / 1048576 > CSV_MAX_FILE_SIZE:
                    self.logger.error('The file %s has file size %s MB and that is larger than '
                                      'CSV_MAX_FILE_SIZE set in settings.py', file_name, str(file_size))
                    raise MsSqlToRedshiftBaseException
            else:
                self.logger.error('Invalid file %s', file_name)
                raise MsSqlToRedshiftBaseException

        self.logger.info('All generated .csv files passed the max file size check')

    def run_aws_s3_file_uploads(self, created_csv_file_names_list):
        self.logger.info('Upload of .csv files to S3 bucket starting')

        for raw_file_name in created_csv_file_names_list:
            full_file_name = raw_file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]

            self.aws_obj.upload_to_s3(self.is_dry_run, full_file_name, file_name)

            self.logger.info('Upload of %s to S3 bucket passed', file_name)

        self.logger.info('Upload of .csv files to S3 bucket finished')

    def run_aws_redshift_copy_commands(self, created_csv_file_names_list):
        self.logger.info('Copy of the .csv files to the AWS Redshift cluster started')

        for file_name in created_csv_file_names_list:
            full_file_name = file_name.strip("'")
            file_name = full_file_name.rsplit('\\', 1)[1]
            table_name = file_name[0:(len(file_name) - 24)]

            self.aws_obj.copy_to_redshift(self.is_dry_run, table_name, file_name)

        self.logger.info('Copy of the .csv files to the AWS Redshift cluster finished')

        self.aws_obj.disconnect_redshift()

    def run(self):
        try:
            self.prepare_target_dir_if_not_exists()
            mssql_sp_return_value = self.run_ms_sql_extract_stored_procedure()
            self.check_files_size(created_csv_file_names_list=mssql_sp_return_value)
            self.run_aws_s3_file_uploads(created_csv_file_names_list=mssql_sp_return_value)
            self.run_aws_redshift_copy_commands(created_csv_file_names_list=mssql_sp_return_value)

            self.logger.info('Success')

            return 0

        except MsSqlToRedshiftBaseException as exc:
            self.logger.info('Failure %s', exc)

            return 1


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
