import typing as t
from pathlib import Path

import pytest

from sqlmesh.dbt.models import Materialization, ModelConfig
from sqlmesh.dbt.project import ProjectConfig


@pytest.mark.parametrize(
    "current, new, expected",
    [
        ({}, {"identifier": "correct name"}, {"identifier": "correct name"}),
        ({"identifier": "correct name"}, {}, {"identifier": "correct name"}),
        (
            {"identifier": "wrong name"},
            {"identifier": "correct name"},
            {"identifier": "correct name"},
        ),
        ({}, {"tags": ["two"]}, {"tags": ["two"]}),
        ({"tags": ["one"]}, {}, {"tags": ["one"]}),
        ({"tags": ["one"]}, {"tags": ["two"]}, {"tags": ["one", "two"]}),
        ({"tags": "one"}, {"tags": "two"}, {"tags": ["one", "two"]}),
        ({}, {"meta": {"owner": "jen"}}, {"meta": {"owner": "jen"}}),
        ({"meta": {"owner": "jen"}}, {}, {"meta": {"owner": "jen"}}),
        (
            {"meta": {"owner": "bob"}},
            {"meta": {"owner": "jen"}},
            {"meta": {"owner": "jen"}},
        ),
        ({}, {"grants": {"select": ["bob"]}}, {"grants": {"select": ["bob"]}}),
        ({"grants": {"select": ["bob"]}}, {}, {"grants": {"select": ["bob"]}}),
        (
            {"grants": {"select": ["bob"]}},
            {"grants": {"select": ["jen"]}},
            {"grants": {"select": ["bob", "jen"]}},
        ),
    ],
)
def test_update(
    current: t.Dict[str, t.Any], new: t.Dict[str, t.Any], expected: t.Dict[str, t.Any]
):
    config = ModelConfig(**current).update_with(new)
    assert {k: v for k, v in config.dict().items() if k in expected} == expected


def test_model_config():
    expected_models = {
        "items",
        "customers",
        "orders",
        "order_items",
        "waiters",
        "top_waiters",
        "customer_revenue_by_day",
        "waiter_revenue_by_day",
    }

    model_configs = ProjectConfig.load(Path("tests/projects/sushi_dbt")).models
    assert set(model_configs) == expected_models

    expected_config = {
        "materialized": Materialization.INCREMENTAL,
        "incremental_strategy": "delete+insert",
        "cluster_by": ["ds"],
        "schema": "sushi",
    }
    actual_config = {
        k: v
        for k, v in model_configs["customer_revenue_by_day"].dict().items()
        if k in expected_config
    }
    assert actual_config == expected_config
