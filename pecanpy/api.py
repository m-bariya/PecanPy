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


def read_gas_ert_query(con: sqlalchemy.engine.Connectable,
                       schema: str,
                       dataid: int,
                       start_time: Union[pd.Timestamp, str],
                       end_time: Union[pd.Timestamp, str],
                       tz: str = "US/Central")-> pd.DataFrame:
    """
    Read gas ERT data from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
    schema : `str`
        Name of a schema containing the `gas_ert` table/view.
    dataid : `int`
        The unique identifier for a particular household.
    start_time : `Union[pd.Timestamp, str]`
    end_time : `Union[pd.Timestamp, str]`
    tz : `str`, default: "US/Central"

    Returns
    -------
    df: `pandas.DataFrame`

        Gas ERT data for a particular household.

    """
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
    """
    Read electric vehicles table from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
    schema : `str`
        Name of a schema containing the `electric_vechicles` table/view.

    Returns
    -------
    df: `pandas.DataFrame`

        Electric vehicles data.

    """
    datetime_columns = ["delivery_date", "lease_end_date"]
    df = pd.read_sql_table("electric_vehicles", con, schema, index_col="dataid",
                           parse_dates=datetime_columns)
    return df


def read_metadata_table(con: sqlalchemy.engine.Connectable,
                        schema: str,
                        tz: str = "US/Central") -> pd.DataFrame:
    """
    Read metadata table from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
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


def read_water_ert_query(con: sqlalchemy.engine.Connectable,
                         schema: str,
                         dataid: int,
                         start_time: Union[pd.Timestamp, str],
                         end_time: Union[pd.Timestamp, str],
                         tz: str = "US/Central")-> pd.DataFrame:
    """
    Read water ERT data from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
    schema : `str`
        Name of a schema containing the `water_ert` table/view.
    dataid : `int`
        The unique identifier for a particular household.
    start_time : `Union[pd.Timestamp, str]`
    end_time : `Union[pd.Timestamp, str]`
    tz : `str`, default: "US/Central"

    Returns
    -------
    df: `pandas.DataFrame`

        Water ERT data for a particular household.

    """
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


def read_water_capstone_query(con: sqlalchemy.engine.Connectable,
                              schema: str,
                              dataid: int,
                              start_time: Union[pd.Timestamp, str],
                              end_time: Union[pd.Timestamp, str],
                              tz: str = "US/Central")-> pd.DataFrame:
    """
    Read water capstone data from a database into a `pandas.DataFrame`.

    Parameters
    ----------
    con : `sqlalchemy.engine.Connectable`
    schema : `str`
        Name of a schema containing the `water_capstone` table/view.
    dataid : `int`
        The unique identifier for a particular household.
    start_time : `Union[pd.Timestamp, str]`
    end_time : `Union[pd.Timestamp, str]`
    tz : `str`, default: "US/Central"

    Returns
    -------
    df: `pandas.DataFrame`

        Water capstone data for a particular household.

    """
    template = """SELECT localminute, consumption FROM {schema}.water_capstone
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
