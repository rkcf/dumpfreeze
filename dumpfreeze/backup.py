# Operations on database backups

import subprocess
import datetime
import os
from logging import getLogger

logger = getLogger(__name__)


def create_dump(db_name, db_user, backup_dir):
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

    logger.info('Created db dump at %s', backup_path)

    return backup_path
