"""
Module contining utility functions.

@author davidrpugh

"""
import sqlalchemy


def create_engine(user_name, password, host, port, db):
    """Create a PostgreSQL engine."""
    url = "postgresql://{}:{}@{}:{}/{}".format(user_name, password, host, port, db)
    engine = sqlalchemy.create_engine(url)
    return engine
