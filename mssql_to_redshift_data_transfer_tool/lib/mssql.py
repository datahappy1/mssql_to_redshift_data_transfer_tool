""" MSSQL Library """

import logging
import pyodbc

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException
from mssql_to_redshift_data_transfer_tool.settings import MS_SQL_DB
from mssql_to_redshift_data_transfer_tool.lib.utils import get_env_var_value

logging.getLogger().setLevel(logging.INFO)


def init():
    """
    Initiate MSSQL Server
    :return: conn_mssql on success, sys.exit on error
    """
    mssql_host = get_env_var_value("mssql_host")
    mssql_port = get_env_var_value("mssql_port")
    mssql_user = get_env_var_value("mssql_user")
    mssql_pass = get_env_var_value("mssql_pass")

    try:
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux

        db = 'MSSQL_to_Redshift'
        mssql_host = 'docker@localhost'
        # conn_mssql = pyodbc.connect(server=mssql_host,
        #                             port=mssql_port,
        #                             user=mssql_user,
        #                             password=mssql_pass,
        #                             database=MS_SQL_DB,
        #                             autocommit=True)
        # conn_mssql = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+mssql_host+';DATABASE='+db+';UID='+mssql_user+';PWD='+ mssql_pass)
        conn_mssql = pyodbc.connect(f'DSN=MSSQLServerDatabase;UID={mssql_user};PWD={mssql_pass}')

        logging.info('SQL Server connection initiated')
        return conn_mssql
    except pyodbc.InterfaceError as if_err:
        logging.error(f"SQL Server connection failed."
                      f"A MSSQLDriverException has been caught: {if_err}")
        raise MsSqlToRedshiftBaseException(if_err)
    except pyodbc.DatabaseError as db_err:
        logging.error(f"SQL Server connection failed."
                      f"A MSSQLDatabaseException has been caught: {db_err}")
        raise MsSqlToRedshiftBaseException(db_err)


def close(conn_mssql):
    """
    Close MSSQL Connection
    :param: conn_mssql
    :return: 0
    """
    conn_mssql.close()
    logging.info('SQL Server connection closed')
    return 0


def run_extract_filter_bcp(conn_mssql, database_name, schema_name, target_directory, dry_run):
    """
    Fire the extract_filter_bcp stored procedure
    :param conn_mssql:
    :param database_name:
    :param schema_name:
    :param target_directory:
    :param dry_run:
    :return: ret on success, sys.exit on error
    """
    try:
        cursor = conn_mssql.cursor()
        cursor.execute("EXEC [mngmt].[Extract_Filter_BCP] '"
                       + database_name + "','"
                       + schema_name + "','"
                       + target_directory + "','"
                       + str(dry_run) + "'")

        # sql injection safe code:
        # cursor.callproc('[mngmt].[Extract_Filter_BCP]', (database_name,schema_name,
        #                                                  target_directory,str(dry_run),))

        ret = cursor.fetchone()[0]
        return ret
    except pyodbc.Error as pyodbc_err:
        logging.error("Stored Procedure Extract_Filter_BCP failed, pymssql Error")
        raise MsSqlToRedshiftBaseException(pyodbc_err)
