# Operations on local database for storage of archive inventory and job list
import sqlalchemy as sa
import sqlalchemy.ext.declarative
from logging import getLogger

logger = getLogger(__name__)

base = sqlalchemy.ext.declarative.declarative_base()


class Archive(base):
    """ AWS Glacier archive object """
    __tablename__ = 'archive'
    id = sa.Column(sa.String, primary_key=True)
    aws_id = sa.Column(sa.String)
    location = sa.Column(sa.String)
    vault_name = sa.Column(sa.String)
    database_name = sa.Column(sa.String)
    date = sa.Column(sa.String)

    def store(self, session):
        """ store object in db
        Args:
            session: sqlalchemy session
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()

    def delete(self, session):
        """ delete object from db
        Args:
            session: sqlalchemy session
        """
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()


class Backup(base):
    """ Local backup object """
    __tablename__ = 'backup'
    id = sa.Column(sa.String, primary_key=True)
    database_name = sa.Column(sa.String)
    backup_dir = sa.Column(sa.String)
    date = sa.Column(sa.String)

    def store(self, session):
        """ store object in db
        Args:
            session: sqlalchemy session
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()

    def delete(self, session):
        """ delete object from db
        Args:
            session: sqlalchemy session
        """
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()


class Job(base):
    """ AWS Glacier job object """
    __tablename__ = 'job'
    account_id = sa.Column(sa.String)
    vault_name = sa.Column(sa.String)
    id = sa.Column(sa.String, primary_key=True)

    def store(self, session):
        """ store object in db
        Args:
            session: sqlalchemy session
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()

    def delete(self, session):
        """ delete object from db
        Args:
            session: sqlalchemy session
        """
        try:
            session.delete(self)
            session.commit()
        except Exception as e:
            logger.critical(e)
            session.rollback()
            raise SystemExit(1)
        finally:
            session.close()


def setup_db(local_db):
    """ Initialize database
    Args:
        local_db: path to local database file
    """
    engine = sa.create_engine('sqlite:///' + local_db)
    base.metadata.create_all(engine)
