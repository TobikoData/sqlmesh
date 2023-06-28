import typing as t

import agate
import pandas as pd
from dbt.version import get_installed_version


def pandas_to_agate(df: pd.DataFrame) -> agate.Table:
    """
    Converts a Pandas DataFrame to an Agate Table
    """
    from dbt.clients.agate_helper import table_from_data

    return table_from_data(df.to_dict(orient="records"), df.columns.tolist())


def _get_dbt_version() -> t.Tuple[int, int]:
    dbt_version = get_installed_version()
    return (int(dbt_version.major or "0"), int(dbt_version.minor or "0"))


DBT_VERSION = _get_dbt_version()
