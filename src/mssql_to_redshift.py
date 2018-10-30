import argparse
import logging
import os
import sys
import pymssql
from src import settings
from src.lib import mssql
from src.lib import aws
from src.lib import utils
from time import sleep


def prepare_args():
    ###########################################################################
    # 0: parsing and storing arguments
    ###########################################################################

    parser = argparse.ArgumentParser()
    parser.add_argument('-dn', '--databasename', type=str, required=True)
    parser.add_argument('-sn', '--schemaname', type=str, required=True)
    parser.add_argument('-td', '--targetdirectory', type=str, required=True)
    parser.add_argument('-dr', '--dryrun', type=str, required=True)
    parsed = parser.parse_args()

    databasename = parsed.databasename
    schemaname = parsed.schemaname

    targetdirectory = parsed.targetdirectory
    targetdirectory = targetdirectory.replace('\f', '\\f')

    dryrun = parsed.dryrun
    # argparse bool datatype known bug workaround
    if dryrun.lower() in ('yes', 'true', 't', 'y', '1'):
        dryrun = True
    elif dryrun.lower() in ('no', 'false', 'f', 'n', '0'):
        dryrun = False
    else:
        logging.error(f'Argument dryrun needs to be convertible to boolean')
        sys.exit(1)

    main(databasename, schemaname, targetdirectory, dryrun)


def main(databasename, schemaname, targetdirectory, dryrun):
    # set logging levels for console
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    # additional variables preparations

    ###########################################################################
    # 1: init MSSQL
    ###########################################################################

    mssql.General.init()
    logging.info(f'SQL Server connection initiated')

    ###########################################################################
    # 2: create a working folder for the .csv files
    ###########################################################################

    try:
        if not os.path.exists(targetdirectory):
            os.makedirs(targetdirectory)
            logging.info(f'{targetdirectory} folder created')

        else:
            logging.info(f'{targetdirectory} folder already exists')

    except OSError:
        logging.error(f'Could not create the {targetdirectory} folder, OSError')
        sys.exit(1)

    ###########################################################################
    # 3: execute the mngmt.Extract_Filter_BCP MSSQL Stored Procedure with pymssql
    ###########################################################################

    logging.info(f'Generating .csv files started')
    ret = mssql.StoredProc.run_extract_filter_bcp(databasename, schemaname, targetdirectory, dryrun)

    if "MSSQL error, details:" in ret:
        logging.error(f'SQL code error in the stored procedure')
        logging.error(f'{ret}')
        sys.exit(1)

    elif "No .csv files generated" in ret:
        logging.error(f'No .csv files generated')
        sys.exit(1)

    else:
        logging.info(f'Generating .csv files finished')

    ###########################################################################
    # 4: check the .csv filesize , init S3 and copy the generated files to S3 and write log rows
    ###########################################################################

    files = utils.str_split(ret)

    for filename in files:
        filename = filename.strip("'")
        fs = os.path.getsize(filename)
        # check that the filesize in MB is not greater than settings.csv_max_filesize
        if fs / 1048576 > settings.csv_max_filesize:
            logging.error(f'The file {filename} has filesize {str(fs)} MB and that is larger than csv_max_filesize'
                          f'set in settings.py')
            sys.exit(1)
        else:
            pass

    aws.s3.init()
    logging.info(f'AWS S3 connection initiated')

    logging.info(f'Upload of the .csv files to the S3 bucket location {settings.s3_bucketname}/{settings.s3_targetdir} '
                 f'started')

    for filename in files:
        fullfilename = filename.strip("'")
        filename = fullfilename.rsplit('\\', 1)[1]

        if bool(dryrun):
            logging.info(f's3 copy dryrun {filename}')
        else:
            aws.S3.upload(fullfilename, filename)
            sleep(0.1)
            aws.RedShift.copy(filename)


        mssql.StoredProc.write_log_row('S3 UPLOAD', databasename, schemaname, '#N/A', settings.s3_bucketname + '/'
                                       + settings.s3_targetdir, filename, 'S', 'file ' + filename
                                       + ' copied to S3 bucket')

    logging.info(f'Upload of the .csv files to the S3 bucket location {settings.s3_bucketname}/{settings.s3_targetdir} '
                 f'finished')

    ###########################################################################
    # 5: init connect to AWS Redshift
    ###########################################################################

    ###########################################################################
    # 6: run Redshift COPY commands and write log rows
    ###########################################################################

    # https://stackoverflow.com/questions/51130199/redshift-copy-csv-in-s3-using-python
    # files = utils.str_split(ret)
    # mssql.StoredProc.write_log_row('RS COPY', databasename, schemaname, '#N/A', settings.s3_bucketname + '/'
    #                                + settings.s3_targetdir, filename.strip("'"), 'S', 'file '
    #                                + filename.strip("'") + ' copied to Redshift')

    ###########################################################################
    # 7: close connections and cleanup files, query and list the log values
    ###########################################################################

    mssql.General.init().close()
    logging.info(f'MSSQL Connection closed')


if __name__ == "__main__":
    prepare_args()
