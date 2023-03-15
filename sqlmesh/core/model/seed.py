from __future__ import annotations

import typing as t
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from sqlglot import exp

from sqlmesh.utils.pydantic import PydanticModel

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
}


class Seed(PydanticModel):
    """Represents content of a seed.

    Presently only CSV format is supported.
    """

    content: str
    _df: t.Optional[pd.DataFrame] = None

    @property
    def columns_to_types(self) -> t.Dict[str, exp.DataType]:
        result = {}
        for column_name, column_type in self._get_df().dtypes.items():
            exp_type = PANDAS_TYPE_MAPPINGS.get(column_type)
            if not exp_type:
                raise ValueError(f"Unsupported pandas type '{column_type}'")
            result[str(column_name)] = exp_type
        return result

    def read(self, batch_size: t.Optional[int] = None) -> t.Generator[pd.DataFrame, None, None]:
        df = self._get_df()

        batch_size = batch_size or df.size
        batch_start = 0
        while batch_start < df.shape[0]:
            yield df.iloc[batch_start : batch_start + batch_size, :]
            batch_start += batch_size

    def _get_df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_csv(
                StringIO(self.content),
                index_col=False,
                on_bad_lines="error",
            )
        return self._df


def create_seed(path: str | Path) -> Seed:
    with open(path, "r") as fd:
        return Seed(content=fd.read())
