from setuptools import setup

setup(
    name='mssql_to_redshift_data_transfer_tool',
    version='1.0',
    packages=[''],
    url='https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/',
    license='MIT',
    author='datahappy1',
    author_email='',
    description='Python 3.7 tool to extract data out of a MSSQL database and load it into an AWS Redshift Cluster',
    install_requires=['boto3', 'psycopg2', 'pymssql']
)
