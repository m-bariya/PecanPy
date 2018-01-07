"""
Functions for reading tables and queries from the Pecan Street Dataport and
returning the results as properly formatted Pandas DataFrame instances.

@author : davidrpugh
@date : 2018-01-02

"""
from typing import List, Union

import pandas as pd
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
    df.spend_time_at_home_monday = df.spend_time_at_home_monday == "Monday"
    df.spend_time_at_home_tuesday = df.spend_time_at_home_tuesday == "Tuesday"
    df.spend_time_at_home_wednesday = df.spend_time_at_home_wednesday == "Wednesday"
    df.spend_time_at_home_thursday = df.spend_time_at_home_thursday == "Thursday"
    df.spend_time_at_home_friday = df.spend_time_at_home_friday == "Friday"
    df.spend_time_at_home_none = df.spend_time_at_home_none == "none"
    df.ethnicity_asian_pacific_islander = df.ethnicity_asian_pacific_islander == "Asian/Pacific Islander"
    df.ethnicity_black_african_american = df.ethnicity_black_african_american == "Black/African-American"
    df.ethnicity_caucasian_other_than_hispanic_or_latino = df.ethnicity_caucasian_other_than_hispanic_or_latino == "Caucasian"
    df.ethnicity_decline = df.ethnicity_decline == "Decline to Respond"
    df.ethnicity_hispanic_or_latino = df.ethnicity_hispanic_or_latino == "Hispanic"
    df.ethnicity_native_american_alaska_native = df.ethnicity_native_american_alaska_native == "American/Alaska Native"
    df.ethnicity_other = df.ethnicity_other == "Other"
    df.smart_phone_own = df.smart_phone_own == "Yes"
    df.tablet_own = df.tablet_own == "Yes"
    df.pv_system_own = df.pv_system_own == "Yes"
    df.light_bulbs_cfl = df.light_bulbs_cfl == "CFL"
    df.light_bulbs_fluorescent = df.light_bulbs_fluorescent == "Fluorescent"
    df.light_bulbs_halogen = df.light_bulbs_halogen == "Halogen"
    df.light_bulbs_incandescent = df.light_bulbs_incandescent == "Incandescent"
    df.light_bulbs_led = df.light_bulbs_led == "LED"

    # create categorical variables
    df.month_moved_into_house = df.month_moved_into_house.astype("category")
    df.education_level = df.education_level.astype("category")
    df.total_annual_income = df.total_annual_income.astype("category")
    df.typical_bulb_fans = df.typical_bulb_fans.astype("category")
    df.typical_bulb_lamps = df.typical_bulb_lamps.astype("category")
    df.electronic_devices_dvr_number = (df.electronic_devices_dvr_number
                                          .replace({"None": 0})
                                          .astype("category"))
    df.electronic_devices_cable_box_number = (df.electronic_devices_cable_box_number
                                                .replace({"None": 0, '': 0})
                                                .astype("category"))
    df.electronic_devices_game_system_number = (df.electronic_devices_game_system_number
                                                  .replace({"None": 0, '': 0})
                                                  .astype("category"))
    df.electronic_devices_wifi_number = (df.electronic_devices_wifi_number
                                           .replace({"None": 0, '': 0})
                                           .astype("category"))

    return df


def read_survey_2013_field_descriptions_table(con: sqlalchemy.engine.Connectable,
                                              schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2013_field_descriptions", con, schema,
                           index_col=["column_name"])
    return df


def read_survey_2014_all_participants_table(con: sqlalchemy.engine.Connectable,
                                            schema: str) -> pd.DataFrame:
    df = pd.read_sql_table("survey_2014_all_participants", con, schema)

    # these variables are encoded as True/False but could also be 1/0.
    df.spend_time_at_home_monday = df.spend_time_at_home_monday == "Monday"
    df.spend_time_at_home_tuesday = df.spend_time_at_home_tuesday == "Tuesday"
    df.spend_time_at_home_wednesday = df.spend_time_at_home_wednesday == "Wednesday"
    df.spend_time_at_home_thursday = df.spend_time_at_home_thursday == "Thursday"
    df.spend_time_at_home_friday = df.spend_time_at_home_friday == "Friday"
    df.spend_time_at_home_none = df.spend_time_at_home_none == "none"

    # create the relevant categorical variables
    df.programmable_thermostat_difficulty = (df.programmable_thermostat_difficulty
                                               .replace({'': np.nan, "\"\"\"Haven't tried\"\"\"": np.nan})
                                               .astype("category"))
    df.electronic_devices_cable_box_number = (df.electronic_devices_cable_box_number
                                                .replace({"None": 0, '': 0})
                                                .astype("category"))
    df.electronic_devices_game_system_number = (df.electronic_devices_game_system_number
                                                  .replace({"None": 0, '': 0})
                                                  .astype("category"))
    df.electronic_devices_wifi_number = (df.electronic_devices_wifi_number
                                           .replace({"None": 0, '': 0})
                                           .astype("category"))

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
