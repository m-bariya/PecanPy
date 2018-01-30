"""
Functions for reading electricity egauge tables and queries from the Pecan
Street Dataport and returning the results as properly formatted Pandas
DataFrame instances.

@author : davidrpugh

"""
from typing import List, Union

import numpy as np
import pandas as pd
from pandas.api import types
import sqlalchemy


def _read_electricity_egauge_query(con: sqlalchemy.engine.Connectable,
                                   schema: str,
                                   table: str,
                                   local_minute: str,
                                   dataid: int,
                                   start_time: Union[pd.Timestamp, str],
                                   end_time: Union[pd.Timestamp, str],
                                   columns: Union[List[str], str],
                                   tz: str) -> pd.DataFrame:
    """
    Read electricity egauge data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

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
    df = pd.read_sql_query(query, con=con, parse_dates=parse_dates)
    df[local_minute] = df[local_minute].dt.tz_convert(tz)
    df.set_index(local_minute, inplace=True)
    return df


def read_electricity_egauge_minutes_query(con: sqlalchemy.engine.Connectable,
                                          schema: str,
                                          dataid: int,
                                          start_time: Union[pd.Timestamp, str],
                                          end_time: Union[pd.Timestamp, str],
                                          columns: Union[List[str], str] = "all",
                                          tz: str = "US/Central") -> pd.DataFrame:
    """
    Read electricity egauge minutes data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    results_df = _read_electricity_egauge_query(con,
                                                schema,
                                                "electricity_egauge_minutes",
                                                "localminute",
                                                dataid,
                                                start_time,
                                                end_time,
                                                columns,
                                                tz)
    return results_df


def read_electricity_egauge_15min_query(con: sqlalchemy.engine.Connectable,
                                        schema: str,
                                        dataid: int,
                                        start_time: Union[pd.Timestamp, str],
                                        end_time: Union[pd.Timestamp, str],
                                        columns: Union[List[str], str] = "all",
                                        tz: str = "US/Central") -> pd.DataFrame:
    """
    Read 15-minute electricity egauge data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    results_df = _read_electricity_egauge_query(con,
                                                schema,
                                                "electricity_egauge_15min",
                                                "local_15min",
                                                dataid,
                                                start_time,
                                                end_time,
                                                columns,
                                                tz)
    return results_df


def read_electricity_egauge_hours_query(con: sqlalchemy.engine.Connectable,
                                        schema: str,
                                        dataid: int,
                                        start_time: Union[pd.Timestamp, str],
                                        end_time: Union[pd.Timestamp, str],
                                        columns: Union[List[str], str] = "all",
                                        tz: str = "US/Central") -> pd.DataFrame:
    """
    Read one hour electricity egauge data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    results_df = _read_electricity_egauge_query(con,
                                                schema,
                                                "electricity_egauge_hours",
                                                "localhour",
                                                dataid,
                                                start_time,
                                                end_time,
                                                columns,
                                                tz)
    return results_df
