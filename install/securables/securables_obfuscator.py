""" Installation securables obfuscator """

import base64
import urllib.parse

CONFIG_VALUES_IN = ["mssql_host", "mssql_port", "mssql_user", "mssql_pass",
                    "aws_access_key_id", "aws_secret_access_key",
                    "redshift_host", "redshift_port", "redshift_user", "redshift_pass"]

ENV_VAR_OUT = {}

for i in CONFIG_VALUES_IN:
    CONFIG_VALUES_IN = input(i + ":")
    CONFIG_VALUES_IN = urllib.parse.quote_plus(CONFIG_VALUES_IN)
    CONFIG_VALUES_IN = bytes(CONFIG_VALUES_IN, encoding='utf-8')

    encoded = base64.b16encode(CONFIG_VALUES_IN)
    encoded = str(encoded).rstrip("'").lstrip("b'")
    ENV_VAR_OUT[i] = encoded

ENV_VAR_OUT = str(ENV_VAR_OUT).replace('\'', '"')

# now set the environment variable for your user
print("Environment variable name: mssql_to_redshift_data_transfer_tool")
print("Environment variable value: %s", ENV_VAR_OUT)
