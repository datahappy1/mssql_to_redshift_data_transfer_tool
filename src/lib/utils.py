import os
import sys
import logging


def str_split(ret):
    try:
        ret = str(ret).strip('()').split(',')
        return ret
    except:
        logging.error(f"Could not strip or split the source variable {ret}")
        sys.exit(1)


def env_vars():
    try:
        env_variables = os.getenv('mssql_to_redshift_data_transfer_tool')
        return env_variables
    except:
        logging.error(f"Could not retrieve environment variables")
        sys.exit(1)

