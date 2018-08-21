# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import os
import logging
import argparse
import datetime
import subprocess
import boto3
import botocore
from sys import argv

logger = logging.getLogger('dumpfreeze')


def glacier_upload(backup_path, vault):
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
        with open(backup_path, 'rb') as dump:
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
                raise
    except OSError:
        logger.error('Failed to open db dump %s for read', backup_path)
        raise

    return(response)


def db_dump(db_name, db_user, backup_dir):
    """ Generate mysqldump file for db_name
    Args:
        db_name: Name of database to backup
        db_user: Username to connect to mysql with
    Returns:
        Returns the database backup full path
    """

    # Generate ISO 8601 date
    date = datetime.date.isoformat(datetime.datetime.today())
    # Set backup name
    backup_name = db_name + '-backup-' + date + '.sql'
    backup_path = os.path.join(backup_dir, backup_name)

    # Open backup file for write
    try:
        with open(backup_path, 'w') as backup_file:
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
                logger.error(e.stderr)
                raise
    except FileNotFoundError:
        logger.error('Invalid path for %s', backup_path)
        raise
    except PermissionError:
        logger.error('Invalid permission to write to %s', backup_path)
        raise
    except OSError:
        logger.error('Failed to open file %s for write', backup_path)
        raise

    return(backup_path)


def main():
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
                        required='--backup-only' not in argv)
    parser.add_argument('--verbose',
                        '-v',
                        action='count',
                        help='Verbosity -vvv for full debug',
                        default=0)
    parser.add_argument('--backup-only',
                        action='store_true',
                        help='Local backup only')
    parser.add_argument('--backup-dir',
                        help='Path to backup directory',
                        default=os.getcwd())
    cmd_args = parser.parse_args()

    # Set logger verbosity
    if cmd_args.verbose == 1:
        logging.basicConfig(level=logging.ERROR)
    elif cmd_args.verbose == 2:
        logging.basicConfig(level=logging.INFO)
    elif cmd_args.verbose == 3:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.CRITICAL)

    # Create db dump
    try:
        backup_path = db_dump(cmd_args.database,
                              cmd_args.user,
                              cmd_args.backup_dir)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)
    logger.info('Created db dump at %s', backup_path)

    # Exit if local backup only
    if cmd_args.backup_only:
        return

    # Upload dump to Glacier
    try:
        glacier_upload(backup_path, cmd_args.vault)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)
    logger.info('Uploaded %s to AWS Glacier', backup_path)


if __name__ == '__main__':
    main()
