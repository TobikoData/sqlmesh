from __future__ import annotations

import typing as t
from datetime import date, datetime

import numpy as np
import pandas as pd
from sqlglot import exp

PANDAS_TYPE_MAPPINGS = {
    np.dtype("int8"): exp.DataType.build("tinyint"),
    np.dtype("int16"): exp.DataType.build("smallint"),
    np.dtype("int32"): exp.DataType.build("int"),
    np.dtype("int64"): exp.DataType.build("bigint"),
    np.dtype("float16"): exp.DataType.build("float"),
    np.dtype("float32"): exp.DataType.build("float"),
    np.dtype("float64"): exp.DataType.build("double"),
    np.dtype("O"): exp.DataType.build("varchar"),
    np.dtype("bool"): exp.DataType.build("boolean"),
    pd.Int8Dtype(): exp.DataType.build("tinyint"),
    pd.Int16Dtype(): exp.DataType.build("smallint"),
    pd.Int32Dtype(): exp.DataType.build("int"),
    pd.Int64Dtype(): exp.DataType.build("bigint"),
    pd.Float32Dtype(): exp.DataType.build("float"),
    pd.Float64Dtype(): exp.DataType.build("double"),
    pd.StringDtype(): exp.DataType.build("varchar"),  # type: ignore
    pd.BooleanDtype(): exp.DataType.build("boolean"),
}


def columns_to_types_from_df(df: pd.DataFrame) -> t.Dict[str, exp.DataType]:
    result = {}
    for column_name, column_type in df.dtypes.items():
        exp_type = PANDAS_TYPE_MAPPINGS.get(column_type)
        if not exp_type:
            raise ValueError(f"Unsupported pandas type '{column_type}'")
        result[str(column_name)] = exp_type
    return result


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
