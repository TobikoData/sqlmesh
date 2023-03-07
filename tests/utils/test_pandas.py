from datetime import date, datetime

import numpy as np
import pandas as pd

from sqlmesh.utils.pandas import filter_df_by_timelike


def test_filter_df_by_timelike_py_datetime():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [datetime(2020, 1, 1), datetime(2020, 1, 2), datetime(2020, 1, 3)],
        }
    )
    df = filter_df_by_timelike(df, "b", "%Y-%m-%d", datetime(2020, 1, 2), datetime(2020, 1, 3))
    assert df.shape == (2, 2)
    assert df["a"].tolist() == [2, 3]
    assert df["b"].tolist() == [datetime(2020, 1, 2), datetime(2020, 1, 3)]


def test_filter_df_by_timelike_py_date():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
        }
    )
    df = filter_df_by_timelike(df, "b", "%Y-%m-%d", date(2020, 1, 2), date(2020, 1, 3))
    assert df.shape == (2, 2)
    assert df["a"].tolist() == [2, 3]
    assert df["b"].tolist() == [date(2020, 1, 2), date(2020, 1, 3)]


def test_filter_df_by_timelike_str_datetime():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["2020-01-01", "2020-01-02", "2020-01-03"],
        }
    )
    df = filter_df_by_timelike(df, "b", "%Y-%m-%d", datetime(2020, 1, 2), datetime(2020, 1, 3))
    assert df.shape == (2, 2)
    assert df["a"].tolist() == [2, 3]
    assert df["b"].tolist() == ["2020-01-02", "2020-01-03"]


def test_filter_df_by_timelike_np_datetime():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [
                np.datetime64("2020-01-01"),
                np.datetime64("2020-01-02"),
                np.datetime64("2020-01-03"),
            ],
        }
    )
    df = filter_df_by_timelike(df, "b", "%Y-%m-%d", datetime(2020, 1, 2), datetime(2020, 1, 3))
    assert df.shape == (2, 2)
    assert df["a"].tolist() == [2, 3]
    assert df["b"].tolist() == [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")]


def test_filter_df_by_timelike_pd_timestamp():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [
                pd.Timestamp("2020-01-01"),
                pd.Timestamp("2020-01-02"),
                pd.Timestamp("2020-01-03"),
            ],
        }
    )
    df = filter_df_by_timelike(df, "b", "%Y-%m-%d", datetime(2020, 1, 2), datetime(2020, 1, 3))
    assert df.shape == (2, 2)
    assert df["a"].tolist() == [2, 3]
    assert df["b"].tolist() == [pd.Timestamp("2020-01-02"), pd.Timestamp("2020-01-03")]
