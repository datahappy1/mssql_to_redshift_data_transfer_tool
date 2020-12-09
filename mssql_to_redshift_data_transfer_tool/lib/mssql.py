""" MSSQL module """
import pyodbc

from mssql_to_redshift_data_transfer_tool.lib import ODBC_MSSQL_DSN_NAME, \
    ODBC_MSSQL_UID, ODBC_MSSQL_PWD
from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException


def _connect():
    try:
        return pyodbc.connect(f'DSN={ODBC_MSSQL_DSN_NAME};'
                              f'UID={ODBC_MSSQL_UID};PWD={ODBC_MSSQL_PWD}')
    except pyodbc.Error as err:
        raise MsSqlToRedshiftBaseException(err)


class MsSql:
    """
    MsSql base class
    """

    def __init__(self):
        self.conn = _connect()

    def __repr__(self):
        return self.conn

    def disconnect(self):
        """
        method for closing the connection
        :return:
        """
        self.conn.close()

    def run_stored_procedure(self, sql_cmd, params):
        """
        method for running the target Stored Procedure
        :param sql_cmd:
        :param params:
        :return:
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_cmd, params)
            cursor_fetched_data = cursor.fetchall()
            cursor.close()

            return cursor_fetched_data
        except pyodbc.Error as pyodbc_err:
            raise MsSqlToRedshiftBaseException(pyodbc_err)
