""" MSSQL module """
import pyodbc
from os import getenv

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException


def _connect(dsn_name, uid='', pwd=''):
    try:
        return pyodbc.connect(f'DSN={dsn_name};UID={uid};PWD={pwd}')
    except pyodbc.InterfaceError as i_err:
        raise MsSqlToRedshiftBaseException(i_err)
    except pyodbc.DatabaseError as db_err:
        raise MsSqlToRedshiftBaseException(db_err)


class MsSql:
    def __init__(self):
        self.conn = _connect(dsn_name='SQL Server',
                             uid=getenv('UID'),
                             pwd=getenv('PWD'))

    def disconnect(self):
        self.conn.disconnect()

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

            return cursor.fetchall()
        except pyodbc.Error as pyodbc_err:
            raise MsSqlToRedshiftBaseException(pyodbc_err)
