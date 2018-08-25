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

        c.execute('''CREATE TABLE archive (archive_id text,
                                           vault_name text,
                                           database_name text,
                                           location text,
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

        c.execute('INSERT INTO archive VALUES (?,?,?,?,?)', archive)

        self.conn.commit()
        logger.info('Inserted archive of %s into local inventory', archive[2])
        c.close()

    def list_inventory(self):
        c = self.conn.cursor()
        inventory = c.execute('SELECT * from archive')
        print('database date')
        for row in inventory:
            print(row[2], row[4])
        c.close
