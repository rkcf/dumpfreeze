# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import os
import logging
import datetime
import click
import uuid
from dumpfreeze import backup as bak
from dumpfreeze import aws
from dumpfreeze.inventorydb import InventoryDb
from dumpfreeze import __version__

logger = logging.getLogger(__name__)


def abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()


@click.group()
@click.option('-v', '--verbose', count=True)
@click.version_option(__version__, prog_name='dumpfreeze')
def main(verbose):
    """ Create and manage MySQL dumps locally and on AWS Glacier """
    # Set logger verbosity
    if verbose == 1:
        logging.basicConfig(level=logging.ERROR)
    elif verbose == 2:
        logging.basicConfig(level=logging.INFO)
    elif verbose == 3:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.CRITICAL)


# Backup operations
@click.group()
def backup():
    """ Operations on local backups """
    pass


@backup.command('create')
@click.option('--user', default='root', help='Database user')
@click.option('--backup-dir',
              default=os.getcwd(),
              help='Backup storage directory')
@click.argument('database')
def create_backup(database, user, backup_dir):
    """ Create a mysqldump backup"""
    backup_uuid = uuid.uuid4().hex
    try:
        bak.create_dump(database, user, backup_dir, backup_uuid)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    # Insert backup info into backup inventory db
    backup_info = (backup_uuid,
                   database,
                   backup_dir,
                   datetime.date.isoformat(datetime.datetime.today()))
    with InventoryDb() as local_db:
        local_db.insert_backup(backup_info)

    click.echo(backup_info[0])


@backup.command('upload')
@click.option('--vault', required=True, help='Vault to upload to')
@click.argument('backup_uuid', metavar='UUID')
def upload_backup(vault, backup_uuid):
    """ Upload a local backup dump to AWS Glacier """
    # Get backup info
    with InventoryDb() as local_db:
        backup_info = local_db.get_backup(backup_uuid)

    # Construct backup path
    backup_file = backup_info[0] + '.sql'
    backup_path = os.path.join(backup_info[2], backup_file)

    # Upload backup_file to Glacier
    try:
        upload_response = aws.glacier_upload(backup_path, vault)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    # Insert archive info into archive inventory db
    archive_info = (uuid.uuid4().hex,
                    upload_response['archiveId'],
                    upload_response['location'],
                    vault,
                    backup_info[1],
                    backup_info[3])

    with InventoryDb() as local_db:
        local_db.insert_archive(archive_info)

    click.echo(archive_info[0])


@backup.command('delete')
@click.argument('backup_uuid', metavar='UUID')
@click.option('--yes',
              '-y',
              is_flag=True,
              callback=abort_if_false,
              expose_value=False,
              prompt='Delete backup?')
def delete_backup(backup_uuid):
    """ Delete a local dump backup """
    # Get backup info
    with InventoryDb() as local_db:
        backup_info = local_db.get_backup(backup_uuid)

    # Construct backup path
    backup_file = backup_info[0] + '.sql'
    backup_path = os.path.join(backup_info[2], backup_file)

    # Delete file
    os.remove(backup_path)

    # Remove from db
    with InventoryDb() as local_db:
        local_db.remove_backup(backup_uuid)

    click.echo(backup_info[0])


@backup.command('list')
def list_backup():
    """ Return a list of all local backups """
    # Get Inventory
    with InventoryDb() as local_db:
        backups = local_db.list_backups()

    # Add header
    backups.insert(0, ('UUID', 'DATABASE', 'LOCATION', 'DATE'))

    # Calculate widths
    widths = [max(map(len, column)) for column in zip(*backups)]

    # Print inventory
    for backup in backups:
        print("  ".join((val.ljust(width)
              for val, width in zip(backup, widths))))


# Archive operations
@click.group()
def archive():
    """ Operations on AWS Glacier Archives """
    pass


@archive.command('delete')
@click.argument('archive_uuid', metavar='UUID')
@click.option('--yes',
              '-y',
              is_flag=True,
              callback=abort_if_false,
              expose_value=False,
              prompt='Delete archive?')
def delete_archive(archive_uuid):
    """ Delete an archive on AWS Glacier """
    # Get archive info
    with InventoryDb() as local_db:
        archive_info = local_db.get_archive(archive_uuid)

    # Send delete job to AWS
    aws.delete_archive(archive_info)

    # Remove from db
    with InventoryDb() as local_db:
        local_db.remove_archive(archive_uuid)

    click.echo(archive_uuid)


@archive.command('retrieve')
def retrieve_archive():
    """ Not Implemented: Initiate an archive retrieval from AWS Glacier """
    pass


@archive.command('list')
def list_archive():
    """ Return a list of uploaded archives """
    # Get inventory
    with InventoryDb() as local_db:
        archives = local_db.list_archives()

    # Strip info
    stripped = []
    for archive in archives:
        stripped.append((archive[0], archive[3], archive[4], archive[5]))

    # Add header
    stripped.insert(0, ('UUID', 'VAULT', 'DATABASE', 'DATE'))

    # Calculate widths
    widths = [max(map(len, column)) for column in zip(*stripped)]

    # Print inventory
    for archive in stripped:
        print("  ".join((val.ljust(width)
              for val, width in zip(archive, widths))))


main.add_command(backup)
main.add_command(archive)
main()
