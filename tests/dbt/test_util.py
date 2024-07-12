from __future__ import annotations

import pandas as pd
from sqlmesh.dbt.util import pandas_to_agate


def test_pandas_to_agate_type_coercion():
    df = pd.DataFrame({"data": ["_2024_01_01", "_2024_01_02", "_2024_01_03"]})
    agate_rows = pandas_to_agate(df).rows

    values = [v[0] for v in agate_rows.values()]
    assert values == ["_2024_01_01", "_2024_01_02", "_2024_01_03"]
