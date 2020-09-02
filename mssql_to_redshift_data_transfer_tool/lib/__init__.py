"""__init__.py"""
from os import getenv

ODBC_MSSQL_DSN_NAME = getenv('odbc_mssql_dsn_name')
ODBC_MSSQL_UID = getenv('odbc_mssql_uid')
ODBC_MSSQL_PWD = getenv('odbc_mssql_pwd')

AWS_ACCESS_KEY_ID = getenv('aws_access_key_id')
AWS_SECRET_ACCESS_KEY = getenv('aws_secret_access_key')

REDSHIFT_HOST = getenv("redshift_host")
REDSHIFT_PORT = getenv("redshift_port")
REDSHIFT_USER = getenv("redshift_user")
REDSHIFT_PASS = getenv("redshift_pass")
