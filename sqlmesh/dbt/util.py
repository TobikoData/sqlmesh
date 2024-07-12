import typing as t

import agate
import pandas as pd
from dbt.version import get_installed_version


def _get_dbt_version() -> t.Tuple[int, int]:
    dbt_version = get_installed_version()
    return (int(dbt_version.major or "0"), int(dbt_version.minor or "0"))


DBT_VERSION = _get_dbt_version()

if DBT_VERSION < (1, 8):
    from dbt.clients.agate_helper import table_from_data_flat, empty_table, as_matrix  # type: ignore  # noqa: F401
else:
    from dbt_common.clients.agate_helper import table_from_data_flat, empty_table, as_matrix  # type: ignore  # noqa: F401


def pandas_to_agate(df: pd.DataFrame) -> agate.Table:
    """
    Converts a Pandas DataFrame to an Agate Table
    """

    return table_from_data_flat(df.to_dict(orient="records"), df.columns.tolist())
