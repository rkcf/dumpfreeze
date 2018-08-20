# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import argparse
import datetime
from subprocess import Popen
import boto3


def glacier_upload(backup_file, vault):
    """ Upload db dump to Amazon Glaier
    Args:
        backup_file: Path to backup file
        vault: Vault to upload to
    Returns:
        Returns response from AWS
    """

    # Create boto aws client
    aws_client = boto3.client('glacier')

    # Open db dump
    with open(backup_file, 'rb') as dump:
        # Upload dump
        response = aws_client.upload_archive(vaultName=vault, body=dump)

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
    with open(backup_name, 'w') as backup_file:
        # mysqldump command
        dump_args = ['mysqldump', '--user=' + db_user, db_name]
        # Run mysqldump command in subprocess
        Popen(dump_args, stdout=backup_file)

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
    cmd_args = parser.parse_args()

    # Create db dump
    backup_file = db_dump(cmd_args.database, cmd_args.user)

    # Upload dump to Glacier
    upload_response = glacier_upload(backup_file, cmd_args.vault)
