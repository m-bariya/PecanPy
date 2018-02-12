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
