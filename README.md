# mssql_to_redshift_data_transfer_tool
MSSQL to AWS Redshift data transfer tool written in Python 3.7

![](https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/docs/img/rating.svg)

- [Introduction](#introduction)
- [How this tool works](#how-this-tool-works)
- [How to install and run the tool](#how-to-install-and-run-the-tool)
- [How to setup a new MSSQL to Redshift data migration project](#how-to-setup-a-new-MSSQL-to-Redshift-data-migration-project)


## Introduction
This tool is able to migrate data from your MSSQL Database to AWS Redshift.
It consumes arguments defining: 
- `--databasename` or `-dn` ( the name of the database with the tables in MSSQL you wish to migrate over, this argument needs to be aligned with the values in the column DatabaseName inside the configuration table MSSQL_to_Redshift.mngmt.ControlTable )
- `--schemaname` or `-sn` ( the name of the database schema with the tables in MSSQL you wish to migrate over, this argument needs to be aligned with the values in the column SchemaName inside the configuration table MSSQL_to_Redshift.mngmt.ControlTable )
- `--generated_csv_files_target_directory` or `-td` ( the local folder where you wish to store your .csv files, if the folder not exists, it will be created for you during the runtime)
- `--dryrun` or `-dr` ( `True` | `False` allowed ,this argument let's you run a dry run for testing purposes, filtering SQL Server data to 0 rows)


## How this tool works
![alt text][diagram]

[diagram]: https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/docs/img/diagram.png "How this tool works"


## How to install and run the tool
To install:
- `git clone https://www.github.com/datahappy1/mssql_to_redshift_data_transfer_tool mssql_to_redshift_data_transfer_tool`
- `cd c:\mssql_to_redshift_data_transfer_tool`
- create and activate a virtual environment
- download Windows Postgres Driver from https://www.enterprisedb.com/downloads/postgres-postgresql-downloads
- `pip3 install -r requirements.txt` 
- run the database build script using `mssql_database_buildscript.sql` located [here](https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/install/mssql_database_buildscript.sql)

Setup these environment variables:
- `odbc_mssql_dsn_name` - **mandatory** MSSQL DSN name for PyODBC
- `odbc_mssql_uid` - **optional** MSSQL DSN user name, not needed if you set the user in Windows ODBC connection
- `odbc_mssql_pwd` - **optional** MSSQL DSN password, not needed if you set the password in Windows ODBC connection
- `aws_access_key_id` -**mandatory** AWS Access key ID 
- `aws_secret_access_key` -**mandatory** AWS Secret access key
- `redshift_host` -**mandatory** AWS Redshift host name
- `redshift_port` -**mandatory** AWS Redshift port ( integer )
- `redshift_user` -**mandatory** AWS Redshift user name
- `redshift_pass` -**mandatory** AWS Redshift password


### First run on Windows:

You need to set the PYTHONPATH like this:
`set PYTHONPATH=%PYTHONPATH%;C:\mssql_to_redshift_data_transfer_tool\`

A permanent solution is to:
1) Go to the Windows menu, right-click on “Computer” and select “Properties”
2) From the computer properties dialog, select “Advanced system settings” on the left
3) From the advanced system settings dialog, choose the “Environment variables” button
4) In the Environment variables dialog, click the “New” button in the top half of the dialog, to make a new user variable
5) Give the variable name as PYTHONPATH and the value is the path to the code directory. Choose OK and OK again to save this variable
6) Now open a cmd Window (Windows key, then type cmd and press Return). Type: `echo %PYTHONPATH%` to confirm the environment variable is correctly set
> https://bic-berkeley.github.io/psych-214-fall-2016/using_pythonpath.html 

### First run on Linux:
You need to set the PYTHONPATH like this:
`export PYTHONPATH=$PYTHONPATH:/home/your_user/your_git_projects/mssql_to_redshift_data_transfer_tool/`

A permanent solution is to:
1) Open your favorite terminal program
2) Open the file ~/.bashrc in your text editor – e.g. atom ~/.bashrc
3) Add the following line to the end:
`export PYTHONPATH=/home/my_user/code`
4) Save the file
5) Close your terminal application
6) Start your terminal application again, to read in the new settings, and type this:
`echo $PYTHONPATH` , you should see something like /home/my_user/code

### To run:
You need to set the required arguments :

- `--databasename` or `-dn` 
- `--schemaname` or `-sn` 
- `--generated_csv_files_target_directory` or `td`
- `--dryrun` or `-dr`

Run these commands to execute:
1) `cd mssql_to_redshift_data_transfer_tool`
2) `python mssql_to_redshift_data_transfer_tool.py -dn AdventureWorksDW2016 -sn dbo -td C:\mssql_to_redshift_data_transfer_tool\files -dr False`


## How to setup a new MSSQL to Redshift data migration project
1) Setup the database tables with their columns that you need to transfer over to AWS Redshift in the MSSQL Configuration table `MSSQL_to_Redshift.mngmt.ControlTable`
> Note that this tool's internal database MSSQL_to_Redshift has to be installed at the same host where your source MSSQL databases are located.
> Another option is to use Linked Servers
2) Don't forget to setup the project-scoped settings like the AWS S3 bucket name, the maximum csv filesize in MB, database names and others using [settings.py](https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/mssql_to_redshift_data_transfer_tool/settings.py)
3) Make sure you've got your AWS Redshift tables ready ( the Redshift tables need to be named exactly like the tables configured in `MSSQL_to_Redshift.mngmt.ControlTable` MSSQL table )
4) Set the Pythonpath env.variable
5) Try running this tool with the `--Dryrun` argument first set to `true`
6) Now you can go and configure the databases, schemas and table names that will be transferred over to AWS Redshift in the `MSSQL_to_Redshift.mngmt.ControlTable` MSSQL table