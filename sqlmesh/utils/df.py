import typing as t

import pandas as pd
from sqlglot import exp

from sqlmesh.core.dialect import select_from_values


def pandas_to_sql(
    df: pd.DataFrame,
    columns_to_types: t.Dict[str, exp.DataType],
    batch_size: int = 0,
    alias: str = "t",
) -> t.Generator[exp.Select, None, None]:
    """Convert a pandas dataframe into a VALUES sql statement.

    Args:
        df: A pandas dataframe to convert.
        columns_to_types: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batch, if <= 0 then no batching will occur.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    yield from select_from_values(
        values=df.itertuples(index=False, name=None),
        columns_to_types=columns_to_types,
        batch_size=batch_size,
        alias=alias,
    )
