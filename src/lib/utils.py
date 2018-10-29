import os


def str_split(ret):
    return str(ret).strip('()').split(',')


def env_vars():
    env_variables = os.getenv('mssql_to_redshift_data_transfer_tool')
    return env_variables
