# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import logging
import argparse
import datetime
import subprocess
import boto3
import botocore

logger = logging.getLogger('dumpfreeze')


def glacier_upload(backup_file, vault):
    """ Upload db dump to Amazon Glaier
    Args:
        backup_file: Path to backup file
        vault: Vault to upload to
    Returns:
        Returns response from AWS
    """

    # Create boto aws client
    client = boto3.client('glacier')

    # Open db dump
    try:
        with open(backup_file, 'rb') as dump:
            # Upload dump
            try:
                response = client.upload_archive(vaultName=vault, body=dump)
            except botocore.exceptions.NoCredentialsError:
                logger.error('Credentials Not Found')
                raise
            except client.exceptions.ResourceNotFoundException:
                logger.error('Vault not found')
                raise
            except botocore.exceptions.ClientError as e:
                logger.error(e)
                raise e
    except OSError as e:
        logger.exception('Failed to open db dump %s for read', backup_file)
        raise e

    return(response)


def db_dump(db_name, db_user):
    """ Generate mysqldump file for db_name
    Args:
        db_name: Name of database to backup
        db_user: Username to connect to mysql with
    Returns:
        Returns the database backup file name
    """

    # Generate ISO 8601 date
    date = datetime.date.isoformat(datetime.datetime.today())
    # Set backup name
    backup_name = db_name + '-backup-' + date + '.sql'

    # Open backup file for write
    try:
        with open(backup_name, 'w') as backup_file:
            # mysqldump command
            dump_args = ['mysqldump', '--user=' + db_user, db_name]

            # Run mysqldump command in subprocess
            try:
                subprocess.run(args=dump_args,
                               stdout=backup_file,
                               stderr=subprocess.PIPE,
                               universal_newlines=True,
                               check=True)
            except subprocess.CalledProcessError as e:
                logger.exception(e.stderr)
                raise SystemExit(1)
    except OSError:
        logger.exception('Failed to open file for write')
        raise SystemExit(1)

    return(backup_name)


if __name__ == '__main__':
    """ Main Program """
    # Setup argument parser
    parser = argparse.ArgumentParser(description='Create MySQl dumps '
                                     'and backup to Amazon Glacier')
    parser.add_argument('--database',
                        '-d',
                        help='Database to backup',
                        required=True)
    parser.add_argument('--user',
                        '-u',
                        help='User to connect to mysql with',
                        default='root')

    parser.add_argument('--vault',
                        help='Glacier vault to upload to',
                        required=True)

    parser.add_argument('--verbose',
                        '-v',
                        action='count',
                        help='Verbosity -vvv for full debug',
                        default=0)

    cmd_args = parser.parse_args()

    # Set logger verbosity
    if cmd_args.verbose == 1:
        logging.basicConfig(level=logging.ERROR)
    elif cmd_args.verbose == 2:
        logging.basicConfig(level=logging.INFO)
    elif cmd_args.verbose == 3:
        logging.basicConfig(level=logging.DEBUG)

    # Create db dump
    backup_file = db_dump(cmd_args.database, cmd_args.user)
    logger.info('Uploaded %s to AWS Glacier', backup_file)

    # Upload dump to Glacier
    upload_response = glacier_upload(backup_file, cmd_args.vault)
    logger.info('Created db dump at %s', backup_file)
