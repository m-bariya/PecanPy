"""
Module contining utility functions.

@author davidrpugh

"""
from typing import Generator, List, Union

import pandas as pd
import sqlalchemy


def read_sql_query(con: sqlalchemy.engine.Connectable,
                   sql_str: Union[str, None] = None,
                   sql_file: Union[str, None] = None,
                   index_col = None,
                   parse_dates = None,
                   params = None,
                   chunksize: Union[int, None] = None) -> Union[pd.DataFrame, Generator]:
    """
    Execute arbitrary SQL Select query against a database, returning results
    in a pandas DataFrame. No data manipulation or munging is performed. Either
    SQLstr or SQLfile should be not None. Either way, the first word in the SQL
    query should be "SELECT". If chunksize is used, this function returns a
    generator.

    Parameters:
    -----------
    con = `sqlalchemy.engine.Connectable`
        An object which supports execution of SQL constructs. Currently there
        are two implementations: `sqlalchemy.engine.Connection` and
        `sqlalchemy.engine.Engine`.
    SQLstr = `str`
        string holding the select query to execute
    SQLfile = `str`
        full path to text file holding the select query to execute
    index_col = `string or list of strings`, optional
        see pandas.read_sql_query()
    parse_dates = `list or dict`, optional
        see pandas.read_sql_query()
    params = `list, tuple or dict`, optional
        see pandas.read_sql_query()
    chunksize: `int`, optional
        see pandas.read_sql_query()

    Returns:
    --------
    pandas dataframe holding query results, or a generator if chunksize is used
    """

    # input is either through a SQL string or a file with SQL code
    if (SQLstr is None) and (SQLfile is None):
      raise TypeError('Please pass either the SQLstr or the SQLfile argument!')
    elif (SQLstr is not None):
      SQL = SQLstr
    elif (SQLfile is not None):
      with open(SQLfile,'rt') as f:
        SQL = f.read()
    else:
      return None # should never happen

    # enforce that this must be a select query
    try:
      SQL.upper().index('SELECT',0)
    except ValueError as e:
      raise ValueError('SQL statement must start with "SELECT"!')

    return pd.read_sql_query(SQL, con, index_col = index_col, parse_dates = parse_dates,
                             params = params, chunksize = chunksize)


def create_engine(user_name: str,
                  password: str,
                  host: str,
                  port: int,
                  db: str) -> sqlalchemy.engine.Engine:
    """Create a PostgreSQL engine."""
    url = "postgresql://{}:{}@{}:{}/{}".format(user_name, password, host, port, db)
    engine = sqlalchemy.create_engine(url)
    return engine


def read_metadata_table(con: sqlalchemy.engine.Connectable,
                        schema: str,
                        tz: str = "US/Central") -> pd.DataFrame:
    """
    Read metadata table from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
        An object which supports execution of SQL constructs. Currently there
        are two implementations: `sqlalchemy.engine.Connection` and
        `sqlalchemy.engine.Engine`.
    schema : `str`
        Name of a schema containing the `metadata` table/view.
    tz : `str`, default: "US/Central"

    Returns
    -------
    df: `pandas.DataFrame`

        Metadata table.

    """
    df = pd.read_sql_table("metadata", con, schema, index_col="dataid")

    # Columns with only "yes" and `None` or " " should have type `bool`
    for column in df:
        unique_values = set(df[column].unique())
        if unique_values == {"yes", None} or unique_values == {"yes", " "}:
            df[column] = df[column] == "yes"

    # Columns that contain timestamps need to be made time-zone aware.
    datetime_columns = ["indoor_temp_min_time", "indoor_temp_max_time",
                        "gas_ert_min_time", "gas_ert_max_time",
                        "water_ert_min_time", "water_ert_max_time",
                        "egauge_min_time", "egauge_max_time"]
    for column in datetime_columns:
        try:
            df[column] = df[column].dt.tz_localize("UTC").dt.tz_convert(tz)
        except TypeError:
            df[column] = df[column].dt.tz_convert(tz)

    return df
