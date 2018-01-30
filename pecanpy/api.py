"""
Functions for reading tables and queries from the Pecan Street Dataport and
returning the results as properly formatted Pandas DataFrame instances.

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


def read_gas_ert_query(con: sqlalchemy.engine.Connectable,
                       schema: str,
                       dataid: int,
                       start_time: Union[pd.Timestamp, str],
                       end_time: Union[pd.Timestamp, str],
                       tz: str = "US/Central")-> pd.DataFrame:
    template = """SELECT readtime, meter_value FROM {schema}.gas_ert
                  WHERE dataid={dataid} AND
                    readtime >= '{start_time}' AND
                    readtime < '{end_time}'
                  ORDER BY readtime ASC;"""
    kwargs = {"schema": schema,
              "dataid": dataid,
              "start_time": start_time,
              "end_time": ert_end_time}
    query = template.format(kwargs)
    df = pd.read_sql_query(query, con=con, parse_dates=["readtime"])
    df.readtime = df.readtime.dt.tz_convert(tz)
    df.set_index("readtime", inplace=True)

    return df


def read_electric_vehicles_table(con: sqlalchemy.engine.Connectable,
                                 schema: str) -> pd.DataFrame:
    datetime_columns = ["delivery_date", "lease_end_date"]
    df = pd.read_sql_table("electric_vehicles", con, schema, index_col="dataid",
                           parse_dates=datetime_columns)
    return df


def read_metadata_table(con: sqlalchemy.engine.Connectable,
                        schema: str,
                        tz: str = "US/Central") -> pd.DataFrame:
    """Read PostgreSQL metadata table into a pandas DataFrame."""
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


def read_water_ert_query(con: sqlalchemy.engine.Connectable,
                         schema: str,
                         dataid: int,
                         start_time: Union[pd.Timestamp, str],
                         end_time: Union[pd.Timestamp, str],
                         tz: str = "US/Central")-> pd.DataFrame:
    template = """SELECT readtime, meter_value FROM {schema}.water_ert
                  WHERE dataid={dataid} AND
                    readtime >= '{start_time}' AND
                    readtime < '{end_time}'
                  ORDER BY readtime ASC;"""
    kwargs = {"schema": schema,
              "dataid": dataid,
              "start_time": start_time,
              "end_time": ert_end_time}
    query = template.format(kwargs)
    df = pd.read_sql_query(query, con=con, parse_dates=["readtime"])
    df.readtime = df.readtime.dt.tz_convert(tz)
    df.set_index("readtime", inplace=True)

    return df


def read_water_ert_capstone_query(con: sqlalchemy.engine.Connectable,
                                  schema: str,
                                  dataid: int,
                                  start_time: Union[pd.Timestamp, str],
                                  end_time: Union[pd.Timestamp, str],
                                  tz: str = "US/Central")-> pd.DataFrame:
    template = """SELECT localminute, consumption FROM {schema}.water_ert_capstone
                  WHERE dataid={dataid} AND
                    localminute >= '{start_time}' AND
                    localminute < '{end_time}'
                  ORDER BY localminute ASC;"""
    kwargs = {"schema": schema,
              "dataid": dataid,
              "start_time": start_time,
              "end_time": ert_end_time}
    query = template.format(kwargs)
    df = pd.read_sql_query(query, con=con, parse_dates=["localminute"])
    df.localminute = df.localminute.dt.tz_convert(tz)
    df.set_index("localminute", inplace=True)

    return df
