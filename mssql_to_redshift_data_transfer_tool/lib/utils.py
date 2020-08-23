""" common utility functions """

import os
import logging
import json

from mssql_to_redshift_data_transfer_tool.exceptions import MsSqlToRedshiftBaseException


def str_splitter(ret):
    """
    String splitter
    :param ret:
    :return: stripped and splitted ret str
    """
    ret = str(ret).strip('()').split(',')
    return ret


def get_env_var_value(key):
    """
    Read environment variable
    :param key:
    :return: decoded key
    """
    try:
        env_vars_json = json.loads(os.getenv('mssql_to_redshift_data_transfer_tool'))
        value = env_vars_json[key]
        return value

    except KeyError:
        logging.error("Could not read environment variable configuration value %s", key)
        raise MsSqlToRedshiftBaseException(KeyError)
