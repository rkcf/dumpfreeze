# Operations on local database for storage of archive inventory and job list
import sqlite3
import os
from logging import getLogger

logger = getLogger(__name__)


class InventoryDb:
    """ Local Inventory DB class """

    def __init__(self):
        # Check if inventory.db exists, if not create a new one
        data_dir = os.path.join(os.environ.get('HOME'), '.dumpfreeze')
        self.dbfile = os.path.join(data_dir, 'inventory.db')

        if os.path.isfile(self.dbfile):
            self.conn = sqlite3.connect(self.dbfile)
        else:
            self.conn = sqlite3.connect(self.dbfile)
            self.setup_db()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()

    def setup_db(self):
        """ Create a new database """
        c = self.conn.cursor()

        c.execute('''CREATE TABLE backup (uuid text,
                                          database_name text,
                                          backup_dir text,
                                          date text
                                          )''')
        c.execute('''CREATE TABLE archive (uuid text,
                                           archive_id text,
                                           location text,
                                           vault_name text,
                                           database_name text,
                                           date text
                                           )''')
        c.execute('''CREATE TABLE job (account_id text,
                                       vault_name text,
                                       job_id text
                                       )''')

        self.conn.commit()
        logger.info('Created new local database')
        c.close()

    def insert_archive(self, archive):
        """ Insert archive into database
        Args:
            archive: archive metadata
        """
        c = self.conn.cursor()

        c.execute('INSERT INTO archive VALUES (?,?,?,?,?,?)', archive)

        self.conn.commit()
        logger.info('Inserted archive of %s into local inventory', archive[2])
        c.close()

    def get_archive(self, archive_uuid):
        """ Return backup info
        Args:
            backup_uuid: UUID of backup
        """
        c = self.conn.cursor()

        uuid = (archive_uuid, )
        c.execute('SELECT * from archive WHERE uuid=?', uuid)
        archive_info = c.fetchone()

        c.close()
        logger.info('Fetched archive info for %s from inventory', archive_uuid)
        return archive_info

    def remove_archive(self, archive_uuid):
        """ Remove archive info
        Args:
            archive_uuid: UUID of archive
        """
        c = self.conn.cursor()

        uuid = (archive_uuid, )
        c.execute('DELETE from archive WHERE uuid=?', uuid)

        self.conn.commit()
        logger.info('Removed archive %s info from inventory', archive_uuid)
        c.close()

    def list_archives(self):
        """ Return a list of archive info """
        c = self.conn.cursor()

        c.execute('SELECT * from archive')
        archives = c.fetchall()

        c.close
        logger.info('Fetched list of archives from inventory')
        return archives

    def insert_backup(self, backup):
        """ Insert local backup info into database
        Args:
            backup: backup metadata
        """
        c = self.conn.cursor()

        c.execute('INSERT INTO backup VALUES (?,?,?,?)', backup)

        self.conn.commit()
        logger.info('Inserted backup of %s into local inventory', backup[1])
        c.close()

    def get_backup(self, backup_uuid):
        """ Return backup info
        Args:
            backup_uuid: UUID of backup
        """
        c = self.conn.cursor()

        uuid = (backup_uuid, )
        c.execute('SELECT * from backup WHERE uuid=?', uuid)
        backup_info = c.fetchone()

        c.close()
        logger.info('Fetched backup info for %s from inventory', backup_uuid)
        return backup_info

    def remove_backup(self, backup_uuid):
        """ Remove backup info
        Args:
            backup_uuid: UUID of backup
        """
        c = self.conn.cursor()

        uuid = (backup_uuid, )
        c.execute('DELETE from backup WHERE uuid=?', uuid)

        self.conn.commit()
        c.close()
        logger.info('Removed backup %s from inventory', backup_uuid)

    def list_backups(self):
        """ Return list of local backups """
        c = self.conn.cursor()

        c.execute('SELECT * from backup')
        backups = c.fetchall()

        c.close()
        logger.info('Fetched list of backups from inventory')
        return backups

    def insert_job(self, job):
        """ Insert AWS Glacier job info into database """
        c = self.conn.cursor()

        account_id = job.vault_arn.split(':')[4]
        vault_name = job.vault_arn.split(':')[5].split('/')[1]

        c.execute("Insert INTO job VALUES (?,?,?)",
                  account_id,
                  vault_name,
                  job.job_id)

        self.conn.commit()
        logger.info('Inserted job into local job list')
        c.close()
