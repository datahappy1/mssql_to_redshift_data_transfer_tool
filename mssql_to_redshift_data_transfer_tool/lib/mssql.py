""" MSSQL module """
import pyodbc

from mssql_to_redshift_data_transfer_tool.lib import ODBC_MSSQL_DSN_NAME, \
    ODBC_MSSQL_UID, ODBC_MSSQL_PWD
from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException


def _connect():
    try:
        return pyodbc.connect(f'DSN={ODBC_MSSQL_DSN_NAME};'
                              f'UID={ODBC_MSSQL_UID};PWD={ODBC_MSSQL_PWD}')
    except pyodbc.InterfaceError as i_err:
        raise MsSqlToRedshiftBaseException(i_err)
    except pyodbc.DatabaseError as db_err:
        raise MsSqlToRedshiftBaseException(db_err)


class MsSql:
    def __init__(self):
        self.conn = _connect()

    def __repr__(self):
        return self.conn

    def disconnect(self):
        self.conn.close()

    def run_extract_filter_bcp_stored_procedure(self, database_name, schema_name,
                                                target_directory, dry_run):
        try:
            cursor = self.conn.cursor()

            sql_cmd = """\
            EXEC [MSSQL_to_Redshift].[mngmt].[Extract_Filter_BCP] 
            @DatabaseName=?, @SchemaName=?, @TargetDirectory=?, @DryRun=?
            """
            params = (database_name, schema_name, target_directory, dry_run)
            cursor.execute(sql_cmd, params)

            cursor_fetched_data=cursor.fetchall()
            cursor.close()

            return cursor_fetched_data
        except pyodbc.Error as pyodbc_err:
            raise MsSqlToRedshiftBaseException(pyodbc_err)
