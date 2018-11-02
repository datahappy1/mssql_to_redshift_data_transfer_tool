import os
import sys
import logging
import json
import base64


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


def decode_env_vars(key):
    try:
        env_var_json = json.loads(env_vars())

        value = env_var_json['"' + key + '"']
        value = base64.b64decode(value)
        value = str(value).strip("b'")
        return value
    except:
        logging.error(f"Could not decode environment variable configuration value {key} ")
        sys.exit(1)
