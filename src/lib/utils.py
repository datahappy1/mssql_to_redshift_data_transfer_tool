""" common utility functions """

import os
import sys
import logging
import json
import base64
import urllib.parse


def str_split(ret):
    """
    String splitter
    :param ret:
    :return: stripped and splitted ret str
    """
    ret = str(ret).strip('()').split(',')
    return ret

def env_vars():
    """
    Get environment variable value
    :return: environment variable
    """
    try:
        env_variables = os.getenv('mssql_to_redshift_data_transfer_tool')
        return env_variables
    except Exception as ex:
        logging.error("Could not retrieve environment variable "
                      "mssql_to_redshift_data_transfer_tool")
        logging.error(ex)
        sys.exit(1)


def decode_env_vars(key):
    """
    Decode from binary 16 environment variable
    :param key:
    :return: decoded key
    """
    try:
        env_var_json = json.loads(env_vars())

        value = env_var_json[key]
        value = base64.b16decode(value)
        value = str(value).lstrip("b'").rstrip("'")
        value = urllib.parse.unquote_plus(value)
        return value
    except Exception as ex:
        logging.error("Could not decode environment variable configuration value %s", key)
        logging.error(ex)
        sys.exit(1)
