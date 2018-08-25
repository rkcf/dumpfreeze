# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import os
import logging
import datetime
import sys
import argparse
import dumpfreeze

logger = logging.getLogger(__name__)


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
                        required='--backup-only' not in sys.argv)
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
        backup_path = dumpfreeze.backup.create_dump(cmd_args.database,
                                                    cmd_args.user,
                                                    cmd_args.backup_dir)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    # Exit if local backup only
    if cmd_args.backup_only:
        return

    # Upload dump to Glacier
    try:
        upload_response = dumpfreeze.aws.glacier_upload(backup_path,
                                                        cmd_args.vault)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    # Insert archive info into local inventory db
    archive_info = (upload_response['archiveId'],
                    cmd_args.vault,
                    cmd_args.database,
                    upload_response['location'],
                    datetime.date.isoformat(datetime.datetime.today()))
    with dumpfreeze.inventorydb.InventoryDb() as local_db:
        local_db.insert_archive(archive_info)
        local_db.list_inventory()


if __name__ == '__main__':
    main()
