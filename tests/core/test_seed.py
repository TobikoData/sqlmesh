import pandas as pd
import pytest
from sqlglot import exp

from sqlmesh.core.model.seed import Seed


def test_read():
    content = """key,value,ds
1,one,2022-01-01
2,two,2022-01-02
3,three,2022-01-03
"""
    seed = Seed(content=content)

    assert seed.columns_to_types == {
        "key": exp.DataType.build("bigint"),
        "value": exp.DataType.build("varchar"),
        "ds": exp.DataType.build("varchar"),
    }

    expected_df = pd.DataFrame(
        data={
            "key": [1, 2, 3],
            "value": ["one", "two", "three"],
            "ds": ["2022-01-01", "2022-01-02", "2022-01-03"],
        }
    )

    dfs = seed.read(batch_size=2)
    pd.testing.assert_frame_equal(next(dfs), expected_df.iloc[:2, :])
    pd.testing.assert_frame_equal(next(dfs), expected_df.iloc[2:, :])

    with pytest.raises(StopIteration):
        next(dfs)
