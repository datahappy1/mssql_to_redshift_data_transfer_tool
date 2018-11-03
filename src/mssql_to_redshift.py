import argparse
import logging
import os
import sys
import pymssql
from src import settings
from src.lib import mssql
from src.lib import aws
from src.lib import utils


def prepare_args():

    ####################################################################################################################
    # 0: parsing and storing arguments
    ####################################################################################################################

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
    # set logging levels for main function console output
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.WARNING)

    # variables preparation
    if bool(dryrun):
        dryrunloggingstringprefix = 'Dryrun '
    else:
        dryrunloggingstringprefix = ''

    ####################################################################################################################
    # 1: init MSSQL
    ####################################################################################################################

    mssql.General.init()

    ####################################################################################################################
    # 2: create a working folder for the .csv files
    ####################################################################################################################

    try:
        if not os.path.exists(targetdirectory):
            os.makedirs(targetdirectory)
            logging.info(f'{targetdirectory} folder created')

        else:
            logging.info(f'{targetdirectory} folder already exists')

    except OSError:
        logging.error(f'Could not create the {targetdirectory} folder, OSError')
        sys.exit(1)

    ####################################################################################################################
    # 3: execute the mngmt.Extract_Filter_BCP MSSQL Stored Procedure with pymssql
    ####################################################################################################################

    logging.info(f'{dryrunloggingstringprefix}Generating .csv files started')
    ret = mssql.StoredProc.run_extract_filter_bcp(databasename, schemaname, targetdirectory, dryrun)

    if "MSSQL error, details:" in ret:
        logging.error(f'SQL code error in the stored procedure')
        logging.error(f'{ret}')
        sys.exit(1)

    elif "No .csv files generated" in ret:
        logging.error(f'{dryrunloggingstringprefix}No .csv files generated')
        sys.exit(1)

    else:
        logging.info(f'{dryrunloggingstringprefix}Generating .csv files finished')

    ####################################################################################################################
    # 4: check the .csv filesize
    ####################################################################################################################

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

    ####################################################################################################################
    # 5: init AWS S3
    ####################################################################################################################

    aws.S3.init()

    ####################################################################################################################
    # 6: upload csv files to S3
    ####################################################################################################################

    logging.info(f'{dryrunloggingstringprefix}Upload of the .csv files to the S3 bucket location '
                 f'{settings.s3_bucketname}/{settings.s3_targetdir} started')

    for filename in files:
        fullfilename = filename.strip("'")
        filename = fullfilename.rsplit('\\', 1)[1]

        if bool(dryrun):
            logging.info(f'{dryrunloggingstringprefix}S3 copy {filename}')
        else:
            aws.S3.upload(fullfilename, filename)
            mssql.StoredProc.write_log_row('S3 UPLOAD', databasename, schemaname, '#N/A', settings.s3_bucketname + '/'
                                           + settings.s3_targetdir, filename, 'S', 'file ' + filename
                                           + ' copied to S3 bucket')

    logging.info(f'{dryrunloggingstringprefix}Upload of the .csv files to the S3 bucket location '
                 f'{settings.s3_bucketname}/{settings.s3_targetdir} finished')

    ####################################################################################################################
    # 7: init AWS Redshift
    ####################################################################################################################

    aws.RedShift.init()

    ####################################################################################################################
    # 8: run Redshift COPY commands
    ####################################################################################################################

    logging.info(f'{dryrunloggingstringprefix}Copy of the .csv files to the AWS Redshift cluster started')

    for filename in files:
        fullfilename = filename.strip("'")
        filename = fullfilename.rsplit('\\', 1)[1]

        # TODO AWS Redshift tablename set as the filename without the .csv extension and the timestamp
        # TODO add settings.py value for AWS vacuum tablename command possibility
        tablename = filename[0:(len(filename)-24)]

        if bool(dryrun):
            logging.info(f'{dryrunloggingstringprefix} copy {tablename} {filename} from s3://{settings.s3_bucketname}')
        else:
            aws.RedShift.copy(tablename, filename)
            mssql.StoredProc.write_log_row('RS UPLOAD', databasename, schemaname, tablename, settings.s3_bucketname + '/'
                                           + settings.s3_targetdir, filename, 'S', 'file ' + filename
                                           + ' copied to a Redshift table ' + tablename + ' from the S3 bucket')

    logging.info(f'{dryrunloggingstringprefix}Copy of the .csv files to the AWS Redshift cluster finished')

    ####################################################################################################################
    # 9: close connections
    ####################################################################################################################

    mssql.General.init().close()
    logging.info(f'MSSQL connection closed')

    aws.RedShift.init().close()
    logging.info(f'AWS Redshift connection closed')

    logging.info(f'Program ran successfully!')


if __name__ == "__main__":
    prepare_args()
