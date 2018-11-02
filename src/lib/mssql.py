import pymssql
import logging
import sys
from src.settings import mssql_db
from src.lib.utils import decode_env_vars


class General:
    @staticmethod
    def init():
        try:
            mssql_host = decode_env_vars("mssql_host")
            mssql_port = decode_env_vars("mssql_port")
            mssql_user = decode_env_vars("mssql_user")
            mssql_pass = decode_env_vars("mssql_pass")

            conn = pymssql.connect(server=mssql_host,
                                   port=mssql_port,
                                   user=mssql_user,
                                   password=mssql_pass,
                                   database=mssql_db,
                                   autocommit=True)

            logging.info(f'SQL Server connection initiated')
            return conn
        except ConnectionError:
            logging.error('SQL Server connection failed, ConnectionError')
            sys.exit(1)


class StoredProc:
    @staticmethod
    def run_extract_filter_bcp(databasename, schemaname, targetdirectory, dryrun):
        try:
            cursor = General.init().cursor()
            cursor.execute("EXEC [mngmt].[Extract_Filter_BCP] '"
                           + databasename + "','"
                           + schemaname + "','"
                           + targetdirectory + "','"
                           + str(dryrun) + "'"
                           )

            ret = cursor.fetchone()[0]
            return ret

        except ConnectionError:
            logging.error(f"Stored Procedure Extract_Filter_BCP failed, ConnectionError")
            sys.exit(1)

    @staticmethod
    def write_log_row(executionstep, databasename, schemaname, tablename, targetdirectory, filename, status, message):
        try:
            cursor = General.init().cursor()
            cursor.execute("EXEC [mngmt].[ExecutionLogs_Insert]"
                           + " @executionstep='" + executionstep + "',"
                           + " @databasename='" + databasename + "',"
                           + " @schemaname='" + schemaname + "',"
                           + " @tablename='" + tablename + "',"
                           + " @targetdirectory='" + targetdirectory + "',"
                           + " @filename='" + filename + "',"
                           + " @status='" + status + "',"
                           + " @message='" + message + "'"
                           )

        except ConnectionError:
            logging.warning(f"Row not logged, ConnectionError")
