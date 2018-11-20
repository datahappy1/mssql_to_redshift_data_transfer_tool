""" MSSQL Library """

import logging
import sys
import pymssql
from src.settings import MS_SQL_DB
from src.lib.utils import decode_env_vars

# set logging levels for mssql module console output
logging.getLogger().setLevel(logging.INFO)


def init():
    """
    Initiate MSSQL Server
    :return: 0 on success, sys.exit on error
    """
    global CONN

    try:
        mssql_host = decode_env_vars("mssql_host")
        mssql_port = decode_env_vars("mssql_port")
        mssql_user = decode_env_vars("mssql_user")
        mssql_pass = decode_env_vars("mssql_pass")

        CONN = pymssql.connect(server=mssql_host,
                               port=mssql_port,
                               user=mssql_user,
                               password=mssql_pass,
                               database=MS_SQL_DB,
                               autocommit=True)

        logging.info('SQL Server connection initiated')
        return 0
    except ConnectionError:
        logging.error('SQL Server connection failed')
        sys.exit(1)


def close():
    """
    Close MSSQL Connection
    :return: 0
    """
    CONN.close()
    logging.info('SQL Server connection closed')
    return 0


def run_extract_filter_bcp(database_name, schema_name, target_directory, dry_run):
    """
    Fire the extract_filter_bcp stored procedure
    :param database_name:
    :param schema_name:
    :param target_directory:
    :param dry_run:
    :return: ret on success, sys.exit on error
    """
    try:
        cursor = CONN.cursor()
        cursor.execute("EXEC [mngmt].[Extract_Filter_BCP] '"
                       + database_name + "','"
                       + schema_name + "','"
                       + target_directory + "','"
                       + str(dry_run) + "'"
                       )
        ret = cursor.fetchone()[0]
        return ret
    except ConnectionError:
        logging.error("Stored Procedure Extract_Filter_BCP failed, ConnectionError")
        sys.exit(1)


def write_log_row(execution_step, database_name, schema_name, table_name, target_directory,
                  file_name, status, message):
    """
    Fire the ExecutionLogs_Insert stored procedure
    :param execution_step:
    :param database_name:
    :param schema_name:
    :param table_name:
    :param target_directory:
    :param file_name:
    :param status:
    :param message:
    :return: 0 on success, logging.warning on error
    """
    try:
        cursor = CONN.cursor()
        cursor.execute("EXEC [mngmt].[ExecutionLogs_Insert]"
                       + " @executionstep='" + execution_step + "',"
                       + " @databasename='" + database_name + "',"
                       + " @schemaname='" + schema_name + "',"
                       + " @tablename='" + table_name + "',"
                       + " @targetdirectory='" + target_directory + "',"
                       + " @filename='" + file_name + "',"
                       + " @status='" + status + "',"
                       + " @message='" + message + "'"
                       )
        return 0
    except ConnectionError:
        logging.warning("Row not logged, ConnectionError")
