from datetime import date, datetime

import pandas as pd


def filter_df_by_timelike(
    df: pd.DataFrame,
    column: str,
    col_format: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Inclusively filters a DataFrame by a column that is a datetime-like type. This is done by converting the DF column
    to a string and having it match the format of the start and end dates.
    """

    start_format = start.strftime(col_format)
    end_format = end.strftime(col_format)

    df_time = df[column]
    if len(df_time) == 0:
        return df
    elif isinstance(df_time[0], (datetime, date)):
        df_time = df_time.apply(lambda x: x.strftime(col_format))  # type: ignore
    elif not pd.api.types.is_string_dtype(df_time.dtype):  # type: ignore
        df_time = df_time.dt.strftime(col_format)
    return df[df_time.between(start_format, end_format)]
