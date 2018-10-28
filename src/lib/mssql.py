import pymssql
import logging
import sys
from src.settings import MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASS, MSSQL_DB


class General:
    @staticmethod
    def init():
        try:
            conn = pymssql.connect(server=MSSQL_HOST,
                                   port=MSSQL_PORT,
                                   user=MSSQL_USER,
                                   password=MSSQL_PASS,
                                   database=MSSQL_DB,
                                   autocommit=True)
            return conn
            logging.info(f'SQL Server connection initiated')
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
