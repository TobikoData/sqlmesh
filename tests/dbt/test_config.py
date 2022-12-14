import typing as t

import pytest

from dbt.config import Materialization, ModelConfig, UpdateStrategy

@pytest.mark.parametrize(
    "current, new, expected",
    [
        ({}, {"identifier": "correct name"}, {"identifier": "correct name"}),
        ({"identifier": "correct name"}, {}, {"identifier": "correct name"}),
        ({"identifier": "wrong name"}, {"identifier": "correct name"}, {"identifier": "correct name"}),
        ({}, {"tags": ["two"]}, {"tags": ["two"]}),
        ({"tags": ["one"]}, {}, {"tags": ["one"]}),
        ({"tags": ["one"]}, {"tags": ["two"]}, {"tags": ["one", "two"]}),
        ({"tags": "one"}, {"tags": "two"}, {"tags": ["one", "two"]})
    ],
)
def test_update(current: t.Dict[str, t.Any], new: t.Dict[str, t.Any], expected: t.Dict[str, t.Any]):
    config = ModelConfig(**current).update_with(new)
    assert {k: v for k, v in config.dict().items() if k in expected} == expected
