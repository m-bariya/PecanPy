import sqlalchemy


def create_local_engine(user_name, password):
    """Create an SQL engine given KAPSARC credentials."""
    host = "helium-63.test.kapsarc"
    port = 5432
    db = "pecan_raw"
    return _create_engine(user_name, password, host, port, db)


def create_remote_engine(user_name, password):
    """Create a PostgreSQL engine given Pecan Street Dataport credentials."""
    host = "dataport.cloud"
    port = 5434
    db = "postgres"
    return _create_engine(user_name, password, host, port, db)


def _create_engine(user_name, password, host, port, db):
    """Create an PostgreSQL engine."""
    url = "postgresql://{}:{}@{}:{}/{}".format(user_name, password, host, port, db)
    engine = sqlalchemy.create_engine(url)
    return engine
