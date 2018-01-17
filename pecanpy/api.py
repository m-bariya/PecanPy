"""
Functions for reading tables and queries from the Pecan Street Dataport and
returning the results as properly formatted Pandas DataFrame instances.

@author : davidrpugh
@date : 2018-01-02

"""
from typing import List, Union

import numpy as np
import pandas as pd
from pandas.api import types
import sqlalchemy


def read_electricity_egauge_minutes_query(con: sqlalchemy.engine.Connectable,
                                          schema: str,
                                          columns: Union[List[str], str],
                                          dataid: int,
                                          start_time: Union[pd.Timestamp, str],
                                          end_time: Union[pd.Timestamp, str],
                                          tz: str = "US/Central") -> pd.DataFrame:
    """
    Read electritity eguage data from a database into a `DataFrame`.

    Parameters:
    -----------

    Returns:
    --------

    """
    template = """SELECT {columns} FROM {schema}.electricity_egauge_minutes
                  WHERE dataid={dataid} AND
                    localminute >= '{start_time}' AND
                    localminute < '{end_time}'
                  ORDER BY localminute ASC;"""
    kwargs = {"columns": '*' if columns == "all" else ", ".join(["localminute"] + columns),
              "schema": schema,
              "dataid": dataid,
              "start_time": start_time,
              "end_time": end_time}
    query = template.format(**kwargs)
    parse_dates= {"localminute": {"utc": True}}
    df = pd.read_sql_query(query, con=con, parse_dates=parse_dates)
    df.localminute = df.localminute.dt.tz_convert(tz)
    df.set_index("localminute", inplace=True)

    return df


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
    df.primary_residence = df.primary_residence == "Yes"
    df.foundation_pier_beam = df.foundation_pier_beam == "Pier and beam"
    df.foundation_slab = df.foundation_slab == "Slab"
    df.smartphone_own = df.smartphone_own == "Yes"
    df.tablet_own = df.tablet_own == "Yes"
    df.pv_system_own = df.pv_system_own == "Yes"
    df.retrofits = df.retrofits == "Yes"
    df.care_energy_cost = df.care_energy_cost == "Yes"
    df.reduce_energy_cost = df.reduce_energy_cost == "Yes"
    df.modify_routines = df.modify_routines == "Yes"
    df.pets = df.pets == "Yes"
    df.irrigation_system = df.irrigation_system == "Yes"
    df.ac_service_package = df.ac_service_package == "Yes"
    df.light_bulbs_cfl = df.light_bulbs_cfl == "CFL"
    df.light_bulbs_fluorescent = df.light_bulbs_fluorescent == "Fluorescent"
    df.light_bulbs_halogen = df.light_bulbs_halogen == "Halogen"
    df.light_bulbs_incandescent = df.light_bulbs_incandescent == "Incandescent"
    df.light_bulbs_led = df.light_bulbs_led == "LED"
    df.house_num_rooms = df.house_num_rooms.astype("float64")
    df.house_square_feet = df.house_square_feet.astype("float64")

    for column in df:
        if column.startswith("spend_time_at_home_"):
            df[column] = df[column].notnull()
        if column.startswith("ethnicity_"):
            df[column] = df[column].notnull()
        if column.startswith("hvac_"):
            df[column] = df[column].notnull()
        if column.startswith("heating_"):
            df[column] = df[column].notnull()
        if column.startswith("ac_cooling_"):
            df[column] = df[column].notnull()
        if column.startswith("appliance_"):
            df[column] = (df[column].replace({"N/A": np.nan})
                                    .astype("category"))
        if column.startswith("electronic_devices_"):
            df[column] = (df[column].replace({"None": '0', '': '0'})
                                    .astype("category"))
        if column.startswith("sex_"):
            df[column] = (df[column].replace({"None": '0'})
                                    .astype("category"))
        if column.startswith("cooking_"):
            if column.endswith("_times"):
                df[column] = (df[column].replace({"None": np.nan})
                                        .astype("category"))
            else:
                df[column] = df[column].notnull()
        if column.endswith("_count"):
            df[column] = (df[column].replace({None: '0', "N/A": '0'})
                                    .astype("int64"))
        if column.startswith("residents_"):
            dtype = types.CategoricalDtype(categories=[0, 1, 2, 3, 4, 5],
                                           ordered=True)
            strs_with_ints = {None: 0, '1': 1, '2': 2, '3': 3, '4': 4, "5 or more": 5}
            df[column] = (df[column].replace(strs_with_ints)
                                    .astype(dtype))
        # TODO these columns need to be parsed using regex in order to be useful!
        if column.endswith("_brand") or column.endswith("_models"):
            df[column] = df[column].astype("category")
        if column.startswith("temp_"):
            df[column] = pd.to_numeric(df[column], errors="coerce")

    # create categorical variables
    df.year_house_constructed = df.year_house_constructed.astype("category")
    df.year_moved_into_house = df.year_moved_into_house.astype("category")
    df.number_floors = df.number_floors.astype("category")
    df.month_moved_into_house = df.month_moved_into_house.astype("category")
    df.education_level = df.education_level.astype("category")
    df.total_annual_income = df.total_annual_income.astype("category")
    df.blinds_summer = df.blinds_summer.astype("category")
    df.blinds_winter = df.blinds_winter.astype("category")
    df.thermostat_settings = df.thermostat_settings.astype("category")
    df.tv_hours = df.tv_hours.astype("category")
    df.typical_bulb_fans = df.typical_bulb_fans.astype("category")
    df.typical_bulb_lamps = df.typical_bulb_lamps.astype("category")
    df.house_drafty = df.house_drafty.astype("category")
    df.ac_comfortability = df.ac_comfortability.astype("category")
    df.ac_service_date = df.ac_service_date.astype("category")
    df.programmable_thermostat_difficultly = df.programmable_thermostat_difficultly.astype("category")
    df.change_ac_filters = df.change_ac_filters.astype("category")
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
    def _merge_foundation_columns(row):
        if row.foundation_pier_beam == '':
            value = "Slab" if row.foundation_slab == "Slab" else None
        elif row.foundation_pier_beam == "Pier and beam" and row.foundation_slab == '':
            value = "Pier and beam"
        else:
            value = "Both"
        return value

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
            categories = ['College graduate',
                          'Postgraduate degree',
                          'Some college/trade/vocational school',
                          'High School graduate']
            dtype = types.CategoricalDtype(categories, ordered=False)
            df[column] = df[column].astype(dtype)
        elif column == "total_annual_income":
            categories = ['"Less than $10,000"', '"$10,000 - $19,999"',
                          '"$20,000 - $34,999"', '"$35,000 - $49,999"',
                          '"$50,000 - $74,999"', '"$75,000 - $99,999"',
                          '"$100,000 - $149,999"', '"$150,000 - $299,000"',
                          '"$300,000 - $1,000,000"', '"more than $1,000,000"']
            dtype = types.CategoricalDtype(categories, ordered=True)
            df[column] = (df[column].str
                                    .replace('"""', '"')
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
