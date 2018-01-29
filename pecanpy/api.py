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


def read_electricity_egauge_5min_query(con: sqlalchemy.engine.Connectable,
                                       schema: str,
                                       dataid: int,
                                       start_time: Union[pd.Timestamp, str],
                                       end_time: Union[pd.Timestamp, str],
                                       columns: Union[List[str], str] = "all",
                                       tz: str = "US/Central") -> pd.DataFrame:
    """
    Read 5-minute electricity egauge data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    results_df = _read_electricity_egauge_query(con,
                                                schema,
                                                "electricity_egauge_5min",
                                                "local_5min",
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


def read_electricity_egauge_30min_query(con: sqlalchemy.engine.Connectable,
                                        schema: str,
                                        dataid: int,
                                        start_time: Union[pd.Timestamp, str],
                                        end_time: Union[pd.Timestamp, str],
                                        columns: Union[List[str], str] = "all",
                                        tz: str = "US/Central") -> pd.DataFrame:
    """
    Read 30-minute electricity egauge data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    results_df = _read_electricity_egauge_query(con,
                                                schema,
                                                "electricity_egauge_30min",
                                                "local_30min",
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

def read_survey_2011_all_participants_table(con: sqlalchemy.engine.Connectable,
                                            schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2011_all_participants", con, schema)
    return df


def read_survey_2012_all_participants_table(con: sqlalchemy.engine.Connectable,
                                            schema: str) -> pd.DataFrame:
    datetime_columns = ["start_time", "date_submitted"]
    df = pd.read_sql_table("survey_2012_all_participants", con, schema,
                           index_col=["response_id"], parse_dates=datetime_columns)
    return df


def read_survey_2012_field_descriptions_table(con: sqlalchemy.engine.Connectable,
                                              schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2012_field_descriptions", con, schema,
                           index_col=["column_name"])
    return df


def read_survey_2013_all_participants_table(con: sqlalchemy.engine.Connectable,
                                            schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2013_all_participants", con, schema)

    # these variables are encoded as True/False but could also be 1/0.
    # merge the two foundation columns into single categorical columns
    dtype = types.CategoricalDtype(categories=["Pier and beam", "Slab", "Both"],
                                   ordered=False)
    df["foundation"] = (df.apply(_merge_foundation_columns, axis=1)
                          .astype(dtype))
    df.drop(["foundation_pier_beam", "foundation_slab"], axis=1, inplace=True)

    for column in df:
        if column == "primary_residence":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "number_floors":
            dtype = types.CategoricalDtype(categories=[1,2,3,4], ordered=True)
            strs_to_ints = {"One": 1, "Two": 2, "Three": 3, "Four": 4}
            df[column] = (df[column].replace(strs_to_ints)
                                    .astype(dtype))
        elif column == "year_moved_into_house":
            categories = range(1958, 2018)
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].apply(lambda v: int(v) if v is not None else v)
                                    .astype(dtype))
        elif column == "month_moved_into_house":
            categories = ["January", "February", "March", "April", "May", "June",
                          "July", "August", "September", "October", "November",
                          "December"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "year_house_constructed":
            categories = range(1930, 2018)
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({"1930 or earlier": "1930"})
                                    .apply(lambda v: int(v) if v is not None else v)
                                    .astype(dtype))
        elif column == "house_num_rooms":
            categories = range(1, 17)
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].apply(lambda v: int(v) if v is not None else v)
                                    .astype(dtype))
        elif column == "house_ceiling_height":
            df.drop(column, axis=1, inplace=True)  # needs substantial cleaning!
        elif column == "house_square_feet":
            df[column] = df[column].astype("float64")
        elif column.startswith("spend_time_at_home_"):
            df[column] = df[column].notnull()
        elif column.startswith("ethnicity_"):
            df[column] = df[column].notnull()
        elif column.startswith("sex_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {None: 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column.startswith("residents_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {None: 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column == "education_level":
            categories = ["High School graduate",
                          "Some college/trade/vocational school",
                          "College graduate",
                          "Postgraduate degree"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "total_annual_income":
            categories = ["Less than $10,000", "$10,000 - $19,999",
                          "$20,000 - $34,999", "$35,000 - $49,999",
                          "$50,000 - $74,999", "$75,000 - $99,999",
                          "$100,000 - $149,999", "$150,000 - $299,000",
                          "$300,000 - $1,000,000", "more than $1,000,000"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].str
                                    .replace('-', ',')
                                    .str
                                    .replace(" , ", " - ")
                                    .astype(dtype))
        elif column == "smartphone_own":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "tablet_own":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "pv_system_own":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "pv_system_size":
            specific_replacements = {"5kw I think": 5.0,
                                     "8060 Watts": 8.060,
                                     "20W (solar powered attic fan)": 0.020}
            str_to_numeric =  pd.to_numeric(df.pv_system_size
                                              .str
                                              .replace(' ', '')
                                              .str
                                              .replace("kw", '', case=False)
                                              .replace(specific_replacements),
                                              errors="coerce")
            df[column] = str_to_numeric.apply(lambda v: v / 1e3 if v > 1e3 else v)
        elif column == "electricity_used_monthly":
            pass  # TODO column needs significant cleaning!
        elif column == "gas_used_monthly":
            pass # TODO column needs significant cleaning!
        elif column == "retrofits":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "retrofits_detail":
            df.drop(column, axis=1, inplace=True)
        elif column == "retrofits_reason":
            df[column] = df[column].replace({"Yes": True, "No": False, "N/A": None})
        elif column.startswith("appliance_"):
            categories = ['rarely', 'once or twice a month',
                          'once or twice a week', 'several times a week',
                          'daily basis']
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({"N/A": None})
                                    .astype(dtype))
        elif column == "irrigation_system":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "cooking_weekdays_times":
            categories = ["None", "Less than 1 hour", "An hour or more"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "cooking_weekends_times":
            categories = ["None", "Less than 1 hour", "An hour or more"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column.startswith("cooking_"):
            df[column] = df[column].notnull()
        elif column.startswith("blinds_"):
            categories = ['Rarely or never', 'Some days', 'Most days']
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "thermostat_settings":
            categories = ["We are generally in agreement",
                          "Our preferences vary 1-2 degrees Fahrenheit",
                          "Our preferences vary 3-5 degrees Fahrenheit",
                          "Our preferences vary more than 5 degrees Fahrenheit"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "tv_hours":
            categories = range(0, 12)
            dtype = types.CategoricalDtype(categories, ordered=True)
            strs_with_ints = {None: 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5,
                              '6': 6, '7': 7, '8': 8, '9': 9, "10": 10,
                              "10 or more": 11}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column == "care_energy_cost":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "reduce_energy_cost":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "reduce_energy_yes":
            df.drop(column, axis=1, inplace=True)  # TODO substantial work required to make this useful!
        elif column == "modify_routines":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column.endswith("_brand") or column.endswith("_models"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("hvac_"):
            df[column] = df[column].notnull()
        elif column.startswith("compressor1_"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("compressor2_"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("compressor3_"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("air_handler1_"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("air_handler2_"):
            df.drop(column, axis=1, inplace=True)
        elif column.startswith("heating_"):
            df[column] = df[column].notnull()
        elif column.startswith("temp_"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
        elif column == "pets":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column == "programmable_thermostat_currently_programmed":
            df[column] = df[column].replace({"Yes": True, "No": False, "I dont know": None})
        elif column == "programmable_thermostat_difficultly": # typo in name!
            categories = ["Havent tried", "Easy", "Moderately difficult", "Very difficult"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df["programmable_thermostat_difficulty"] = df[column].astype(dtype)
            df.drop(column, axis=1, inplace=True)
        elif column == "ac_comfortability":
            dtype = types.CategoricalDtype([1,2,3,4,5], ordered=True)
            strs_with_ints = {'1': 1, '2': 2, '3': 3, '4': 4, '5': 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column == "house_drafty":
            categories = ["No", "Somewhat", "Yes"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({"I dont know/Havent noticed": None})
                                    .astype(dtype))
        elif column.startswith("ac_cooling_"):
            df[column] = df[column].notnull()
        elif column == "change_ac_filters":
            categories = ["Every year or greater", "Every 6-12 months",
                          "Every 4-6 months", "Every 2-3 months",
                          "At least once every month"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({"There is an HVAC filter!?": None})
                                    .astype(dtype))
        elif column == "ac_service_package":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column.startswith("ac_service_package_cost"):
            df.drop(column, axis=1, inplace=True)
        elif column == "ac_service_date":
            categories = ["Less than a year ago",  "1-2 years ago",
                          "2-3 years ago", "3-5 years ago", "Never"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({"I dont know": None})
                                    .astype(dtype))
        elif column == "water_heater_tankless":
            df[column] = df[column].replace({"Yes": True, "No": False})
        elif column.startswith("light_bulbs_"):
            df[column] = df[column].notnull()
        elif column.startswith("electronic_devices_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {None: 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column.endswith("_number"):
            df.drop(column, axis=1, inplace=True)
        elif column == "recessed_lights_location":
            df.drop(column, axis=1, inplace=True)
        elif column == "track_lights_location":
            df.drop(column, axis=1, inplace=True)
        else:
            pass

    return df


def read_survey_2013_field_descriptions_table(con: sqlalchemy.engine.Connectable,
                                              schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2013_field_descriptions", con, schema,
                           index_col=["column_name"])
    return df


def read_survey_2014_all_participants_table(con: sqlalchemy.engine.Connectable,
                                            schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2014_all_participants", con, schema)

    # merge the two foundation columns into single categorical columns
    dtype = types.CategoricalDtype(categories=["Pier and beam", "Slab", "Both"],
                                   ordered=False)
    df["foundation"] = (df.apply(_merge_foundation_columns, axis=1)
                          .astype(dtype))
    df.drop(["foundation_pier_beam", "foundation_slab"], axis=1, inplace=True)

    for column in df:
        if column == "status":
            dtype = types.CategoricalDtype(categories=["Complete", "Partial"],
                                           ordered=False)
            df[column] = df[column].astype(dtype)
        elif column.startswith("spend_time_at_home_"):
            df[column] = (df[column].replace({'': None})
                                    .notnull())
        elif column.startswith("ethnicity_"):
            df[column] = (df[column].replace({'': None})
                                    .notnull())
        elif column.startswith("hvac_"):
            df[column] = (df[column].replace({'': None})
                                    .notnull())
        elif column.startswith("residents_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {'': 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        elif column == "education_level":
            categories = ["High School graduate",
                          "Some college/trade/vocational school",
                          "College graduate",
                          "Postgraduate degree"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = df[column].astype(dtype)
        elif column == "total_annual_income":
            categories = ["Less than $10,000", "$10,000 - $19,999",
                          "$20,000 - $34,999", "$35,000 - $49,999",
                          "$50,000 - $74,999", "$75,000 - $99,999",
                          "$100,000 - $149,999", "$150,000 - $299,000",
                          "$300,000 - $1,000,000", "more than $1,000,000"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].str
                                    .replace('"""', '')
                                    .replace({'': None})
                                    .astype(dtype))
        elif column == "pv_system_own":
            df[column] = df[column] == "Yes"
        elif column == "pv_system_size":
            str_to_numeric =  pd.to_numeric(df.pv_system_size
                                              .str
                                              .replace("kw", '', case=False)
                                              .str
                                              .replace('"', '')
                                              .str
                                              .replace(',', '')
                                              .str
                                              .replace(' ', '')
                                              .str
                                              .replace('DC', '')
                                              .replace({'': None, 'NA': None, 'n/a': None}),
                                              errors="coerce")
            df[column] = str_to_numeric.apply(lambda v: v / 1e3 if v > 1e3 else v)
        elif column == "pv_system_reason":
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column == "pv_system_satisfied":
            categories = ["Very dissatisfied", "Somewhat dissatisfied",
                          "Neutral", "Somewhat satisfied", "Very"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].replace({'': None})
                                    .astype(dtype))
        elif column.startswith("pv_system_features_"):
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("pv_system_common_"):
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("pv_neg_factors_"):
            df[column] = (df[column].replace({'': None})
                                    .notnull())
        elif column.startswith("pv_pos_"):
            df[column] = (df[column].replace({'': None})
                                    .notnull())
        elif column == "pv_owner_response":
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column == "retrofits":
            df[column] = df[column].replace({'': None, "Yes": True, "No": False})
        elif column == "retrofits_detail":
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column == "irrigation_system":
            df[column] = df[column].replace({'': None, "Yes": True, "No": False})
        elif column == "ceiling_fans_count":
            df[column] = (df.ceiling_fans_count
                            .replace({'': '0'})
                            .astype("int64"))
        elif column == "compressors_count":
            df[column] = (df.compressors_count
                            .replace({'': '0'})
                            .astype("int64"))
        elif column.startswith("compressor1_"):
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("compressor2_"):
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("compressor3_"):
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("temp_summer_"):
            df[column] = (df[column].replace({'': None})
                                    .astype("float64"))
        elif column.startswith("temp_winter_"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
        elif column == "thermostats_brand":
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column == "programmable_thermostat_currently_programmed":
            df[column] = df[column].replace({'': None, "Yes": True, "No": False, "\"\"\"I don\'t know\"\"\"": None})
        elif column == "programmable_thermostat_difficulty":
            categories = ["Easy", "Moderately difficult", "Very difficult"]
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df.programmable_thermostat_difficulty
                            .replace({'': None, "\"\"\"Haven't tried\"\"\"": None})
                            .astype(dtype))
        elif column == "ac_service_package":
            df[column] = df[column].replace({'': None, "Yes": True, "No": False})
        elif column == "electronic_devices_on_other":
            df.drop(column, axis=1, inplace=True)  # significant cleaning required!
        elif column.startswith("electronic_devices_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {'': 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        else:
            pass

    return df


def read_survey_2014_field_descriptions_table(con: sqlalchemy.engine.Connectable,
                                              schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2014_field_descriptions", con, schema,
                           index_col=["column_name"])
    df.drop("id", axis=1, inplace=True)
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


def _merge_foundation_columns(row):
    if row.foundation_pier_beam == '' or row.foundation_pier_beam is None:
        value = "Slab" if row.foundation_slab == "Slab" else None
    elif row.foundation_pier_beam == "Pier and beam" and (row.foundation_slab == '' or row.foundation_slab is None):
        value = "Pier and beam"
    else:
        value = "Both"
    return value
