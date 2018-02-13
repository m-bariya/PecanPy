"""
Functions for reading electricity egauge tables and queries from the Pecan
Street Dataport and returning the results as properly formatted Pandas
DataFrame instances.

@author : davidrpugh

"""
from typing import Generator, List, Union

import numpy as np
import pandas as pd
from pandas.api import types
import sqlalchemy


def read_electricity_egauge_query(con: sqlalchemy.engine.Connectable,
                                  schema: str,
                                  dataid: int,
                                  start_time: Union[pd.Timestamp, str],
                                  end_time: Union[pd.Timestamp, str],
                                  columns: Union[List[str], str] = "all",
                                  freq: str = 'T',
                                  tz: str = "US/Central",
                                  chunksize: Union[int, None] = None) -> Union[pd.DataFrame, Generator]:
    """
    Read electricity egauge data from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
        An object which supports execution of SQL constructs. Currently there
        are two implementations: `sqlalchemy.engine.Connection` and
        `sqlalchemy.engine.Engine`.
    schema : `str`
        Name of a schema containing the "electricity_egauge_minutes",
        "electricity_egauge_15min" and "electricity_egauge_hours" tables/views.
    dataid : `int`
        The unique identifier for a particular household.
    start_time : `Union[pd.Timestamp, str]`
    end_time : `Union[pd.Timestamp, str]`
    columns : `Union[List[str], str]`, default: "all"
    freq : `str`, default: 'T'
        The desired sampling frequency for the returned electricity egauge data.
        Must be one of 'T' (minutes), "15T" (15-minute), or 'H' (hourly).
    tz : `str`, default: "US/Central"
    chunksize : `Union[int, None]`, default: `None`.
        If specified, return an iterator where chunksize is the number of rows
        to include in each chunk.

    Returns
    -------
    results_df: `pandas.DataFrame`

        Electricity egauge data for a particular household.

    Raises
    ------
    ValueError
        If `freq` is not one of 'T', "15T", or 'H'.

    """
    kwargs = {"con": con, "schema": schema, "dataid": dataid,
              "start_time": start_time, "end_time": end_time,
              "columns": columns, "tz": tz, "chunksize": chunksize}
    if freq == 'T':
        minutes_kwargs = {"table": "electricity_egauge_minutes",
                          "local_minute": "localminute"}
        kwargs.update(minutes_kwargs)
    elif freq == "15T":
        qtr_hour_kwargs = {"table": "electricity_egauge_15min",
                           "local_minute": "local_15min"}
        kwargs.update(qtr_hour_kwargs)
    elif freq == 'H':
        hour_kwargs = {"table": "electricity_egauge_hours",
                       "local_minute": "localhour"}
        kwargs.update(hour_kwargs)
    else:
        msg = """The 'freq' keyword argument must be one of 'T' (minutes),
                 '15T' (15 minutes), or 'H' (hourly)."""
        raise ValueError(msg)
    results_df = _read_electricity_egauge_query(**kwargs)
    return results_df


def _read_electricity_egauge_query(con: sqlalchemy.engine.Connectable,
                                   schema: str,
                                   table: str,
                                   local_minute: str,
                                   dataid: int,
                                   start_time: Union[pd.Timestamp, str],
                                   end_time: Union[pd.Timestamp, str],
                                   columns: Union[List[str], str],
                                   tz: str,
                                   chunksize: Union[int, None]) -> Union[pd.DataFrame, Generator]:
    """
    Read electricity egauge data from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
        An object which supports execution of SQL constructs. Currently there
        are two implementations: `sqlalchemy.engine.Connection` and
        `sqlalchemy.engine.Engine`.
    schema : `str`
        Name of a schema containing the "electricity_egauge_minutes",
        "electricity_egauge_15min" and "electricity_egauge_hours" tables/views.
    table : `str`
        Table/view containing the electricity egauge data. Must be one of
         "electricity_egauge_minutes", "electricity_egauge_15min", or
         "electricity_egauge_hours".
    local_minute : `str`
        Name of the datetime column. Varies depending on `table` name.
    dataid : `int`
        The unique identifier for a particular household.
    start_time : `Union[pd.Timestamp, str]`
    end_time : `Union[pd.Timestamp, str]`
    columns : `Union[List[str], str]`
    tz : `str`, default: "US/Central"
    chunksize : `Union[int, None]`, default: `None`.
        If specified, return an iterator where chunksize is the number of rows
        to include in each chunk.

    Returns
    -------
    results_df: `pandas.DataFrame`

        Electricity egauge data for a particular household.

    """
    template = """SELECT {columns} FROM {schema}.{table}
                  WHERE dataid={dataid} AND
                    {local_minute} >= '{start_time}' AND
                    {local_minute} < '{end_time}'
                  ORDER BY {local_minute} ASC;"""
    kwargs = {"columns": '*' if columns == "all" else ", ".join([local_minute] + columns),
              "schema": schema,
              "table": table,
              "local_minute": local_minute,
              "dataid": dataid,
              "start_time": start_time,
              "end_time": end_time}
    query = template.format(**kwargs)
    parse_dates= {local_minute: {"utc": True}}
    df = pd.read_sql_query(query, con=con, parse_dates=parse_dates,
                           chunksize=chunksize)
    df[local_minute] = df[local_minute].dt.tz_convert(tz)
    df.set_index(local_minute, inplace=True)
    return df
