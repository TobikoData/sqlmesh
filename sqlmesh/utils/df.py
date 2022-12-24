import typing as t

import pandas as pd
from sqlglot import exp


def pandas_to_sql(
    df: pd.DataFrame,
    column_mapping: t.Dict[str, exp.DataType],
    batch_size: int = 0,
    alias: str = "t",
) -> t.Generator[exp.Select, None, None]:
    """Convert a pandas dataframe into a VALUES sql statement.

    Args:
        df: A pandas dataframe to convert.
        column_mapping: Mapping of column names to types to assign to the values.
        batch_size: The maximum number of tuples per batch, if <= 0 then no batching will occur.
        alias: The alias to assign to the values expression. If not provided then will default to "t"

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    casted_columns = [
        exp.alias_(exp.Cast(this=exp.to_column(column), to=kind), column)
        for column, kind in column_mapping.items()
    ]
    batch = []
    for row in df.itertuples():
        batch.append(row[1:])
        if batch_size > 0 and len(batch) > batch_size:
            values = exp.values(batch, alias=alias, columns=column_mapping)
            yield exp.select(*casted_columns).from_(values)
            batch.clear()
    if batch:
        values = exp.values(batch, alias=alias, columns=column_mapping)
        yield exp.select(*casted_columns).from_(values)
