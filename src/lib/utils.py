import os
import sys
import logging
import json
import base64
import urllib.parse


def str_split(ret):
    try:
        ret = str(ret).strip('()').split(',')
        return ret
    except Exception as ex:
        logging.error(f"Could not strip or split the source variable {ret}")
        logging.error(ex)
        sys.exit(1)


def env_vars():
    try:
        env_variables = os.getenv('mssql_to_redshift_data_transfer_tool')
        return env_variables
    except Exception as ex:
        logging.error(f"Could not retrieve environment variable mssql_to_redshift_data_transfer_tool")
        logging.error(ex)
        sys.exit(1)


def decode_env_vars(key):
    try:
        env_var_json = json.loads(env_vars())

        value = env_var_json[key]
        value = base64.b16decode(value)
        value = str(value).lstrip("b'").rstrip("'")
        value = urllib.parse.unquote_plus(value)
        return value
    except Exception as ex:
        logging.error(f"Could not decode environment variable configuration value {key} ")
        logging.error(ex)
        sys.exit(1)
