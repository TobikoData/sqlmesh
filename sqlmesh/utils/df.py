import typing as t

import pandas as pd
from sqlglot import exp


def pandas_to_sql(
    df: pd.DataFrame,
    batch_size: int = 0,
    alias: str = "t",
    columns: t.Optional[t.Iterable[str]] = None,
) -> t.Generator[exp.Values, None, None]:
    """Convert a pandas dataframe into a VALUES sql statement.

    Args:
        df: A pandas dataframe to convert.
        batch_size: The maximum number of tuples per batch, if <= 0 then no batching will occur.

    Returns:
        This method operates as a generator and yields a VALUES expression.
    """
    batch = []
    for row in df.itertuples():
        batch.append(row[1:])

        if batch_size > 0 and len(batch) > batch_size:
            yield exp.values(batch, alias=alias, columns=columns)
            batch.clear()
    if batch:
        yield exp.values(batch)
