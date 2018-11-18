import base64
import urllib.parse

config_values_in = ["mssql_host", "mssql_port", "mssql_user", "mssql_pass",
                    "aws_access_key_id", "aws_secret_access_key",
                    "redshift_host", "redshift_port", "redshift_user", "redshift_pass"]

env_var_out = {}

for i in config_values_in:
    config_values_in = input(i + ":")
    config_values_in = urllib.parse.quote_plus(config_values_in)
    config_values_in = bytes(config_values_in, encoding='utf-8')

    encoded = base64.b16encode(config_values_in)
    encoded = str(encoded).rstrip("'").lstrip("b'")
    env_var_out[i] = encoded

env_var_out = str(env_var_out).replace('\'', '"')

# now set the environment variable for your user
print("Environment variable name: mssql_to_redshift_data_transfer_tool")
print("Environment variable value: %s", env_var_out)

