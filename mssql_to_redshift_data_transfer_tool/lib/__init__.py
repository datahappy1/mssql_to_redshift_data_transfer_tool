"""__init__.py"""
import os

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException

try:
    ODBC_MSSQL_DSN_NAME = os.environ["odbc_mssql_dsn_name"]
    AWS_ACCESS_KEY_ID = os.environ["aws_access_key_id"]
    AWS_SECRET_ACCESS_KEY = os.environ["aws_secret_access_key"]
    REDSHIFT_HOST = os.environ["redshift_host"]
    REDSHIFT_PORT = os.environ["redshift_port"]
    REDSHIFT_USER = os.environ["redshift_user"]
    REDSHIFT_PASS = os.environ["redshift_pass"]
except KeyError as key_err:
    raise MsSqlToRedshiftBaseException(f"Missing environment variable {key_err}")

ODBC_MSSQL_UID = os.getenv("odbc_mssql_uid")
ODBC_MSSQL_PWD = os.getenv("odbc_mssql_pwd")
