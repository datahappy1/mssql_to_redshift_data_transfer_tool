# mssql_to_redshift_data_transfer_tool
MSSQL to AWS Redshift data transfer tool written in Python 3.7

This tool is able to generate dummy csv or flat txt files based on the configuration settings you setup for your project(s).
It consumes arguments defining: 
- projectname ( *based on the projectname, the correct settings from config.json file are loaded ),* 
- filename defining the output file name
- filesize (optional argument) defining the desired size (in kBs) of the output file 
- rowcount (optional argument) defining the desired row count of the output file
- generated files location (optional argument) defining the output files location in case it's different then the default location in /generated_files/..

[How this tool works](#how-this-tool-works)

[How to install and run the program](#how-to-install-and-run-the-program)

[How to setup a new dummy file generator project](#how-to-setup-a-new-dummy-file-generator-project)

[Important notes](#important-notes)


# How this tool works
![alt text][diagram]

[diagram]: https://github.com/datahappy1/mssql_to_redshift_data_transfer_tool/blob/master/docs/img/diagram.png "How this tool works"


# How to install and run the program
### To install:
`git clone https://www.github.com/datahappy1/mssql_to_redshift_data_transfer_tool mssql_to_redshift_data_transfer_tool` <br />
<br />
#### On Windows:<br />

You need to set the PYTHONPATH like this:
`set PYTHONPATH=%PYTHONPATH%;C:\dummy_file_generator\`<br />
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


#### Pytest testing:<br />
Pytest (version 3.9.1) unit, integration and performance tests are also a part of this tool.
To make sure you're working with a healthy version, install Pytest running `pip install pytest`<br /> 
( Alternatively you can run the command `pip install -r requirements.txt` to install Pytest )<br />
<br />

To run the tests, just `cd tests` and run the command `pytest`

### To run:<br />
You need to set the required arguments :

projectname -pn <br />
filename -fn <br />

The optional arguments are :

filesize -fs (in kB) <br />
rowcount -rc <br />
generated_files_location -gf <br />

*Note if you do NOT specify the filesize and do NOT specify the rowcount, the default row_count value ( set to 100 ) from
settings.py will be used

Run this command to execute:<br />
- with the -fs argument to set the desired filesize of 256 kB :<br />
`cd src`<br />
`python dummy_file_generator.py -pn dummy1 -fn dummy1file -fs 256`<br />
- with the -rc argument to set the desired rowcount of 1000 rows :<br />
`cd src`<br />
`python dummy_file_generator.py -pn dummy1 -fn dummy1file -rc 1000`<br />


# How to setup a new MSSQL to Redshift data migration project


# Important Notes
- To preserve the existing pytest integration and performance tests, do not remove: 
    - test_csv and test_flatfile projects configurations from config.json
    - test.txt file in the data_files folder
    - referential_result_integration_test_csv.csv and referential_result_integration_test_flatfile.txt from generated_files/tests folder

- Whenever you need to add a new source file in the data_file folder, just follow the logic of handling these files in data_files_handler.py and add there your new source file accordingly
- Feel free to contribute to this project
