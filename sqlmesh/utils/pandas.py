from __future__ import annotations

import sys
import typing as t

import numpy as np
import pandas as pd
from sqlglot import exp

if t.TYPE_CHECKING:
    # https://github.com/python/mypy/issues/1153
    if sys.version_info >= (3, 9):
        try:
            from pandas.core.frame import _PandasNamedTuple as PandasNamedTuple
        except ImportError:
            PandasNamedTuple = t.Tuple[t.Any, ...]  # type: ignore
    else:
        PandasNamedTuple = t.Tuple[t.Any, ...]


PANDAS_TYPE_MAPPINGS = {
    np.dtype("int8"): exp.DataType.build("tinyint"),
    np.dtype("int16"): exp.DataType.build("smallint"),
    np.dtype("int32"): exp.DataType.build("int"),
    np.dtype("int64"): exp.DataType.build("bigint"),
    np.dtype("float16"): exp.DataType.build("float"),
    np.dtype("float32"): exp.DataType.build("float"),
    np.dtype("float64"): exp.DataType.build("double"),
    np.dtype("O"): exp.DataType.build("text"),
    np.dtype("bool"): exp.DataType.build("boolean"),
    np.dtype("datetime64"): exp.DataType.build("timestamp"),
    np.dtype("datetime64[ns]"): exp.DataType.build("timestamp"),
    np.dtype("datetime64[us]"): exp.DataType.build("timestamp"),
    pd.Int8Dtype(): exp.DataType.build("tinyint"),
    pd.Int16Dtype(): exp.DataType.build("smallint"),
    pd.Int32Dtype(): exp.DataType.build("int"),
    pd.Int64Dtype(): exp.DataType.build("bigint"),
    pd.Float32Dtype(): exp.DataType.build("float"),
    pd.Float64Dtype(): exp.DataType.build("double"),
    pd.StringDtype(): exp.DataType.build("text"),  # type: ignore
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
