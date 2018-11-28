# mssql_to_redshift_data_transfer_tool
MSSQL to AWS Redshift data transfer tool written in Python 3.7

![](https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/rating.svg)

This tool is able to migrate data from your MSSQL Database to AWS Redshift.
It consumes arguments defining: 
- databasename (the name of the database with the tables in MSSQL you wish to migrate over, this argument needs to be aligned with the values in the column DatabaseName inside the configuration table MSSQL_to_Redshift.mngmt.ControlTable )
- schemaname (the name of the database schema with the tables in MSSQL you wish to migrate over, this argument needs to be aligned with the values in the column SchemaName inside the configuration table MSSQL_to_Redshift.mngmt.ControlTable )
- targetdirectory (the local folder where you wish to store your .csv files, if the folder not exists, it will be created for you during the runtime)
- dryrun (True | False,this argument let's you run a dryrun for testing of the redshift copy commands)


[How this tool works](#how-this-tool-works)

[How to install and run the program](#how-to-install-and-run-the-program)

[How to setup a new MSSQL to Redshift data migration project](#How-to-setup-a-new-MSSQL-to-Redshift-data-migration-project)

[Important notes](#important-notes)


# How this tool works
![alt text][diagram]

[diagram]: https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/docs/img/diagram.png "How this tool works"


# How to install and run the program
### To install:
`git clone https://www.github.com/datahappy1/mssql_to_redshift_data_transfer_tool mssql_to_redshift_data_transfer_tool` <br />
<br />
#### Securables ( hostnames, usernames, passwords etc. ) :
Follow the instructions in notes.txt file from:<br />
https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/tree/master/install/securables

#### MSSQL:
Run the database build using buildscript.sql and then follow the instructions in notes.txt file from:<br />
https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/tree/master/install/mssql

#### AWS:
Follow the instructions in notes.txt file from:<br /> 
https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/tree/master/install/aws
<br />

### First run on Windows:<br />

You need to set the PYTHONPATH like this:
`set PYTHONPATH=%PYTHONPATH%;C:\mssql_to_redshift_data_transfer_tool\`<br />
<br />
A permanent solution is to:<br />
-Go to the Windows menu, right-click on “Computer” and select “Properties”:<br />
-From the computer properties dialog, select “Advanced system settings” on the left:<br />
-From the advanced system settings dialog, choose the “Environment variables” button:<br />
-In the Environment variables dialog, click the “New” button in the top half of the dialog, to make a new user variable:<br />
-Give the variable name as PYTHONPATH and the value is the path to the code directory. Choose OK and OK again to save this variable.<br />
-Now open a cmd Window (Windows key, then type cmd and press Return). Type: `echo %PYTHONPATH%` to confirm the environment variable is correctly set.<br />

*This Pythonpath setup tutorial was taken from https://bic-berkeley.github.io/psych-214-fall-2016/using_pythonpath.html
<br />
<br />

### First run on Linux:<br />
You need to set the PYTHONPATH like this:
`export PYTHONPATH=$PYTHONPATH:/home/pavelp/GIT_PROJECTS/mssql_to_redshift_data_transfer_tool/`<br />
<br />
A permanent solution is to:<br />
TODO


#### Pytest testing:<br />
Pytest (version 3.9.1) integration and connectivity tests are also a part of this tool.
To make sure you're working with a healthy version, install Pytest running `pip install pytest`<br /> 
( Alternatively you can run the command `pip install -r requirements.txt` to install Pytest )<br />
<br />

To run the tests, just `cd tests` and run the command `pytest`

### To run:<br />
You need to set the required arguments :

databasename -pn <br />
schemaname -fn <br />
targetdirectory -td <br />
dryrun -dr <br />

Run this command to execute:<br />
`cd src`<br />
`python mssql_to_redshift_data_transfer_tool.py -dn AdventureWorksDW2016 -sn dbo -td C:\mssql_to_redshift_data_transfer_tool\files -dr False`<br />


# How to setup a new MSSQL to Redshift data migration project
-Install everything needed described under this project's /install/ folder<br />
-Setup the database tables with their columns that you need to transfer over to AWS Redshift in the MSSQL Configuration table mngmt.ControlTable<br />
*Note that this tool's internal database MSSQL_to_Redshift has to be installed at the same host where your source MSSQL databases are located   <br />
-Don't forget to setup the project-scoped settings like the AWS S3 bucket name, the maximum csv filesize, database names and others under /src/settings.py <br />
-Make sure you've got your AWS Redshift tables ready <br />
-Set the Pythonpath env.variable <br />
-Run Pytest to make sure everything's working correctly <br />
-Try running this tool with the Dryrun argument set to true <br />
-If the AWS Redshift commands are correct, re-run with the Dryrun argument set to false <br />


# Important Notes
- Feel free to contribute to this project
- In the future, I'd like to make this tool run also on watermarks, so you transfer to Redshift only the data increments
