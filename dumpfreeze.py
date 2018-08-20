# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import argparse
import datetime
from subprocess import Popen


def db_dump(db_name, db_user):
    """ Generate mysqldump file for db_name """

    # Generate ISO 8601 date
    date = datetime.date.isoformat(datetime.datetime.today())
    # Set backup name
    backup_name = db_name + '-backup-' + date + '.sql'
    # Open backup file for write
    backup_file = open(backup_name, 'w')
    # mysqldump command
    dump_args = ['mysqldump', '--user=' + db_user, db_name]
    # Run mysqldump command in subprocess
    Popen(dump_args, stdout=backup_file)


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

    cmd_args = parser.parse_args()

    # Create db dump
    db_dump(cmd_args.database, cmd_args.user)
