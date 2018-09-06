# dumpfreeze
# Create MySQL dumps and backup to Amazon Glacier

import os
import logging
import datetime
import click
import uuid
import sqlalchemy as sa
from dumpfreeze import backup as bak
from dumpfreeze import aws
from dumpfreeze import inventorydb
from dumpfreeze import __version__

logger = logging.getLogger(__name__)


def abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()


@click.group()
@click.option('-v', '--verbose', count=True)
@click.option('--local-db', default='~/.dumpfreeze/inventory.db')
@click.version_option(__version__, prog_name='dumpfreeze')
@click.pass_context
def main(ctx, verbose, local_db):
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

    # Check if db exists, if not create it
    expanded_db_path = os.path.expanduser(local_db)
    if not os.path.isfile(expanded_db_path):
        inventorydb.setup_db(expanded_db_path)

    # Create db session
    db_engine = sa.create_engine('sqlite:///' + expanded_db_path)
    Session = sa.orm.sessionmaker(bind=db_engine)
    ctx.obj['session_maker'] = Session
    return


# Backup operations
@click.group()
@click.pass_context
def backup(ctx):
    """ Operations on local backups """
    pass


@backup.command('create')
@click.option('--user', default='root', help='Database user')
@click.option('--backup-dir',
              default=os.getcwd(),
              help='Backup storage directory')
@click.argument('database')
@click.pass_context
def create_backup(ctx, database, user, backup_dir):
    """ Create a mysqldump backup"""
    backup_uuid = uuid.uuid4().hex
    try:
        bak.create_dump(database, user, backup_dir, backup_uuid)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    today = datetime.date.isoformat(datetime.datetime.today())

    # Insert backup info into backup inventory db
    backup_info = inventorydb.Backup(id=backup_uuid,
                                     database_name=database,
                                     backup_dir=backup_dir,
                                     date=today)
    local_db = ctx.obj['session_maker']()
    backup_info.store(local_db)

    click.echo(backup_uuid)


@backup.command('upload')
@click.option('--vault', required=True, help='Vault to upload to')
@click.argument('backup_uuid', metavar='UUID')
@click.pass_context
def upload_backup(ctx, vault, backup_uuid):
    """ Upload a local backup dump to AWS Glacier """
    # Get backup info
    local_db = ctx.obj['session_maker']()
    try:
        query = local_db.query(inventorydb.Backup)
        backup_info = query.filter_by(id=backup_uuid).one()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Construct backup path
    backup_file = backup_info.id + '.sql'
    backup_path = os.path.join(backup_info.backup_dir, backup_file)

    # Upload backup_file to Glacier
    try:
        upload_response = aws.glacier_upload(backup_path, vault)
    except Exception as e:
        logger.critical(e)
        raise SystemExit(1)

    archive_uuid = uuid.uuid4().hex
    # Insert archive info into archive inventory db
    archive_info = inventorydb.Archive(id=archive_uuid,
                                       aws_id=upload_response['archiveId'],
                                       location=upload_response['location'],
                                       vault_name=vault,
                                       database_name=backup_info.database_name,
                                       date=backup_info.date)
    local_db = ctx.obj['session_maker']()
    archive_info.store(local_db)

    click.echo(archive_uuid)


@backup.command('restore')
@click.option('--user', default='root', help='Database user')
@click.argument('backup_uuid', metavar='UUID')
@click.pass_context
def restore_backup(ctx, user, backup_uuid):
    """ Restore a backup to the database """
    # Get backup info
    local_db = ctx.obj['session_maker']()
    try:
        query = local_db.query(inventorydb.Backup)
        backup_info = query.filter_by(id=backup_uuid).one()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Restore backup to database
    bak.restore_dump(backup_info.database_name,
                     user,
                     backup_info.backup_dir,
                     backup_info.id)


@backup.command('delete')
@click.argument('backup_uuid', metavar='UUID')
@click.option('--yes',
              '-y',
              is_flag=True,
              callback=abort_if_false,
              expose_value=False,
              prompt='Delete backup?')
@click.pass_context
def delete_backup(ctx, backup_uuid):
    """ Delete a local dump backup """
    # Get backup info
    local_db = ctx.obj['session_maker']()
    try:
        query = local_db.query(inventorydb.Backup)
        backup_info = query.filter_by(id=backup_uuid).one()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Construct backup path
    backup_file = backup_info.id + '.sql'
    backup_path = os.path.join(backup_info.backup_dir, backup_file)

    # Delete file
    os.remove(backup_path)

    # Remove from db
    local_db = ctx.obj['session_maker']()
    backup_info.delete(local_db)

    click.echo(backup_info.id)


@backup.command('list')
@click.pass_context
def list_backup(ctx):
    """ Return a list of all local backups """
    # Get Inventory
    local_db = ctx.obj['session_maker']()
    try:
        backups = local_db.query(inventorydb.Backup).all()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # do some formatting for printing
    formatted = []
    for backup in backups:
        formatted.append([backup.id,
                          backup.database_name,
                          backup.backup_dir,
                          backup.date])

    # Add header
    formatted.insert(0, ['UUID', 'DATABASE', 'LOCATION', 'DATE'])

    # Calculate widths
    widths = [max(map(len, column)) for column in zip(*formatted)]

    # Print inventory
    for row in formatted:
        print("  ".join((val.ljust(width)
              for val, width in zip(row, widths))))


# Archive operations
@click.group()
@click.pass_context
def archive(ctx):
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
@click.pass_context
def delete_archive(ctx, archive_uuid):
    """ Delete an archive on AWS Glacier """
    # Get archive info
    local_db = ctx.obj['session_maker']()
    try:
        query = local_db.query(inventorydb.Archive)
        archive_info = query.filter_by(id=archive_uuid).one()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Send delete job to AWS
    aws.delete_archive(archive_info)

    # Remove from db
    local_db = ctx.obj['session_maker']()
    archive_info.delete(local_db)

    click.echo(archive_uuid)


@archive.command('retrieve')
@click.argument('archive_uuid', metavar='UUID')
@click.pass_context
def retrieve_archive(ctx, archive_uuid):
    """ Initiate an archive retrieval from AWS Glacier """
    # Get archive info
    local_db = ctx.obj['session_maker']()
    try:
        query = local_db.query(inventorydb.Archive)
        archive_info = query.filter_by(id=archive_uuid).one()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Initiate archive retrieval job
    job_response = aws.retrieve_archive(archive_info)

    # Insert backup info into backup inventory db
    job_info = inventorydb.Job(account_id=job_response[0],
                               vault_name=job_response[1],
                               id=job_response[2])

    local_db = ctx.obj['session_maker']()
    job_info.store(local_db)


@archive.command('list')
@click.pass_context
def list_archive(ctx):
    """ Return a list of uploaded archives """
    # Get inventory
    local_db = ctx.obj['session_maker']()
    try:
        archives = local_db.query(inventorydb.Archive).all()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # do some formatting for printing
    formatted = []
    for archive in archives:
        formatted.append([archive.id,
                          archive.vault_name,
                          archive.database_name,
                          archive.date])

    # Add header
    formatted.insert(0, ('UUID', 'VAULT', 'DATABASE', 'DATE'))

    # Calculate widths
    widths = [max(map(len, column)) for column in zip(*formatted)]

    # Print inventory
    for row in formatted:
        print("  ".join((val.ljust(width)
              for val, width in zip(row, widths))))


@click.command('poll-jobs')
@click.pass_context
def poll_jobs(ctx):
    """ Check each job in job list, check for completion,
    and download job data
    """
    # Get job list
    local_db = ctx.obj['session_maker']()
    try:
        job_list = local_db.query(inventorydb.Job).all()
    except Exception as e:
        logger.critical(e)
        local_db.rollback()
        raise SystemExit(1)
    finally:
        local_db.close()

    # Check for job completion
    for job in job_list:
        logger.info('Checking job %s for completion', job.id)
        if aws.check_job(job):
            logger.info('Job %s complete, getting data', job.id)
            # Pull archive data
            backup_data = aws.get_archive_data(job)

            # Store backup data as new file
            backup_dir = os.getcwd()
            backup_uuid = uuid.uuid4().hex
            backup_file = backup_uuid + '.sql'
            backup_path = os.path.join(backup_dir, backup_file)

            with open(backup_path, 'w') as f:
                f.write(backup_data)

            # Get corrosponding archive data
            archive_id = aws.get_job_archive(job)
            local_db = ctx.obj['session_maker']()
            try:
                query = local_db.query(inventorydb.Archive)
                archive_info = query.filter_by(aws_id=archive_id).one()
            except Exception as e:
                logger.critical(e)
                local_db.rollback()
                raise SystemExit(1)
            finally:
                local_db.close()

            database_name = archive_info.database_name
            backup_date = archive_info.date

            # Insert backup info into backup inventory db
            backup_info = inventorydb.Backup(id=backup_uuid,
                                             database_name=database_name,
                                             backup_dir=backup_dir,
                                             date=backup_date)
            local_db = ctx.obj['session_maker']()
            backup_info.store(local_db)

            # Delete job from db
            local_db = ctx.obj['session_maker']()
            job.delete(local_db)

        click.echo(backup_uuid)


main.add_command(backup)
main.add_command(archive)
main.add_command(poll_jobs, name='poll-jobs')
main(obj={})
