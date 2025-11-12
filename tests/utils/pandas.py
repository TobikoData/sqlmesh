from __future__ import annotations

import typing as t

import numpy as np  # noqa: TID253
import pandas as pd  # noqa: TID253


def create_df(data: t.Sequence[t.Tuple], schema: t.Dict[str, str]) -> pd.DataFrame:
    return pd.DataFrame(
        np.array(
            data,
            [(k, v) for k, v in schema.items()],
        )
    )


def compare_dataframes(
    actual: pd.DataFrame, expected: pd.DataFrame, msg: str = "DataFrame", **kwargs
) -> None:
    actual = actual.sort_values(by=actual.columns.to_list()).reset_index(drop=True)
    expected = expected.sort_values(by=expected.columns.to_list()).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, obj=msg, **kwargs)
