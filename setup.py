<<<<<<< HEAD
""" Setup.py """
=======
""" MSSQL to Redshift data transfer tool setup """

>>>>>>> 4753e0b43727d92c31e78d62d8cba21eea236d00
from setuptools import setup

setup(
    name='mssql_to_redshift_data_transfer_tool',
    version='1.0',
    packages=[''],
    url='https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/',
    license='MIT',
    author='datahappy1',
    author_email='',
    description='Python 3.7 tool to extract data out of a MSSQL database '
                'and load it into an AWS Redshift Cluster',
    install_requires=['boto3', 'psycopg2', 'pymssql']
)
