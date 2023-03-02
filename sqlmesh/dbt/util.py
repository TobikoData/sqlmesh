import agate
import pandas as pd


def pandas_to_agate(df: pd.DataFrame) -> agate.Table:
    """
    Converts a Pandas DataFrame to an Agate Table
    """
    from dbt.clients.agate_helper import table_from_data

    return table_from_data(df.to_dict(orient="records"), df.columns.tolist())
