""" __main__ module """
import os
import argparse
import logging

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException, \
    UnknownLogLevelException
from mssql_to_redshift_data_transfer_tool.settings import LOGGING_LEVEL, CSV_MAX_FILE_SIZE_MB
from mssql_to_redshift_data_transfer_tool.lib.mssql import MsSql
from mssql_to_redshift_data_transfer_tool.lib.aws import S3, Redshift


def get_logger(dry_run=None):
    """
    function getting logger and based on dry_run argument
    sets the logger name
    :param dry_run:
    :return:
    """
    logging_level_uppercase = LOGGING_LEVEL.upper()

    if logging_level_uppercase not in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
        raise UnknownLogLevelException(f'Unknown logging level {LOGGING_LEVEL}')

    logging.basicConfig(level=logging_level_uppercase)

    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    _name = 'DRY RUN ' + __name__ if bool(dry_run) else __name__
    logger = logging.getLogger(_name)

    return logger


class Runner:
    """
    main execution class
    """

    def __init__(self, database_name, schema_name, generated_csv_files_target_directory, dry_run):
        self.database_name = database_name
        self.schema_name = schema_name
        self.is_dry_run = dry_run
        self.generated_csv_files_target_directory = generated_csv_files_target_directory
        self.logger = get_logger(dry_run=self.is_dry_run)
        self.created_csv_file_names_list = None

    def prepare_local_folder_if_not_exists(self):
        """
        method creating local folder to store csv files
        generated by the SQL Server Stored Procedure
        :return:
        """
        if not os.path.exists(self.generated_csv_files_target_directory):
            try:
                os.makedirs(self.generated_csv_files_target_directory)
                self.logger.debug('%s folder created', self.generated_csv_files_target_directory)
            except OSError as os_err:
                self.logger.error('Could not create the %s folder, OSError',
                                  self.generated_csv_files_target_directory)
                raise MsSqlToRedshiftBaseException(os_err)

    def run_ms_sql_extract_stored_procedure(self):
        """
        method running the MsSql Stored Procedure that generates using BCP
        the .csv files and stores them in the local folder
        :return:
        """
        ms_sql_client = MsSql()

        self.logger.info('Generating .csv files using BCP in a stored procedure started')

        command = """\
            EXEC [MSSQL_to_Redshift].[mngmt].[Extract_Filter_BCP] 
            @DatabaseName=?, @SchemaName=?, @TargetDirectory=?, @DryRun=?
            """
        params = (self.database_name, self.schema_name,
                  self.generated_csv_files_target_directory, self.is_dry_run)

        try:
            _return_sp_value = ms_sql_client.run_stored_procedure(command, params)
        except MsSqlToRedshiftBaseException as exc:
            self.logger.error('Generating .csv files using BCP in a stored procedure failed '
                              'with exception: %s', exc)
            raise exc

        ms_sql_client.disconnect()

        self.logger.info('Generating .csv files using BCP in a stored procedure finished')

        if not _return_sp_value:
            self.logger.error('No .csv files generated, check if '
                              'database name: %s , schema name: %s '
                              'are set in MSSQL mngmt.ControlTable',
                              self.database_name, self.schema_name)
            raise MsSqlToRedshiftBaseException

        self.created_csv_file_names_list = [x[0].strip("()") for x in _return_sp_value]

        for item in self.created_csv_file_names_list:
            self.logger.debug(".csv file %s generated", item)

    def check_files_size(self):
        """
        method checking all of the generated .csv files meet the
        defined max file size in settings.py
        :return:
        """
        _file_size_list = []

        for file_name in self.created_csv_file_names_list:
            if os.path.isfile(file_name):
                file_size = os.path.getsize(file_name)
                file_size_mb = int(file_size / 1048576)

                _file_size_list.append(file_size_mb)

                if file_size_mb > CSV_MAX_FILE_SIZE_MB:
                    self.logger.error('The file %s has file size %s MB and that is larger than '
                                      'CSV_MAX_FILE_SIZE set in settings.py',
                                      file_name, str(file_size))
                    raise MsSqlToRedshiftBaseException
            else:
                self.logger.error('Invalid file %s', file_name)
                raise MsSqlToRedshiftBaseException

        file_size_max = max(_file_size_list) if _file_size_list else 0

        self.logger.info('All generated .csv files passed the max file size check, '
                         'max file size is %s MB', str(file_size_max))

    def run_aws_s3_file_uploads(self):
        """
        method running the s3 file upload for each of the generated .csv files
        :return:
        """
        aws_s3_client = S3()

        self.logger.info('Upload of .csv files to S3 bucket started')

        for file_abs_path in self.created_csv_file_names_list:
            file_name = os.path.basename(file_abs_path)

            try:
                aws_s3_client.upload_to_s3(file_abs_path, file_name)
            except MsSqlToRedshiftBaseException as exc:
                self.logger.error('Upload of .csv files to S3 bucket failed '
                                  'with exception: %s', exc)
                raise exc

            self.logger.debug('Upload of %s to S3 bucket passed', file_abs_path)

        self.logger.info('Upload of .csv files to S3 bucket finished')

    def run_aws_redshift_copy_commands(self):
        """
        method running the AWS Redshift COPY commands to load the .csv files
        from S3 to corresponding Redshift tables
        :return:
        """
        aws_redshift_client = Redshift()

        self.logger.info('Copy of the .csv files to the AWS Redshift cluster started')

        for file_abs_path in self.created_csv_file_names_list:
            base_file_name = os.path.basename(file_abs_path)
            table_name = base_file_name[0:(len(base_file_name) - 24)]

            try:
                aws_redshift_client.copy_to_redshift(self.is_dry_run, table_name, base_file_name)
            except MsSqlToRedshiftBaseException as exc:
                self.logger.error('Copy of the .csv files to the AWS Redshift cluster failed '
                                  'with exception: %s', exc)
                raise exc

            self.logger.debug('.csv file %s copied to AWS Redshift table %s',
                              file_abs_path, table_name)

        self.logger.info('Copy of the .csv files to the AWS Redshift cluster finished')

        aws_redshift_client.disconnect()

    def run(self):
        """
        main orchestrator method
        :return:
        """
        try:
            self.prepare_local_folder_if_not_exists()
            self.run_ms_sql_extract_stored_procedure()
            self.check_files_size()
            self.run_aws_s3_file_uploads()
            self.run_aws_redshift_copy_commands()

            self.logger.info('Success')
            return 0

        except MsSqlToRedshiftBaseException:
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
    generated_csv_files_target_directory = \
        parsed.generated_csv_files_target_directory.replace('\f', '\\f')

    # arg parse bool data type known bug workaround
    if parsed.dryrun.lower() in ('no', 'false', 'f', 'n', 0):
        dry_run = False
    else:
        dry_run = True

    return {"database_name": database_name,
            "schema_name": schema_name,
            "generated_csv_files_target_directory": generated_csv_files_target_directory,
            "dry_run": dry_run}


if __name__ == "__main__":
    PREPARED_ARGS = prepare_args()

    RUNNER_OBJ = Runner(**PREPARED_ARGS)
    RUNNER_OBJ.run()
