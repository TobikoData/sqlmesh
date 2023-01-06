import typing as t
from pathlib import Path

import pytest

from sqlmesh.dbt.models import Materialization, ModelConfig
from sqlmesh.dbt.project import ProjectConfig


@pytest.mark.parametrize(
    "current, new, expected",
    [
        ({}, {"alias": "correct name"}, {"alias": "correct name"}),
        ({"alias": "correct name"}, {}, {"alias": "correct name"}),
        (
            {"alias": "wrong name"},
            {"alias": "correct name"},
            {"alias": "correct name"},
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
        ({"uknown": "field"}, {"uknown": "value"}, {"uknown": "value"}),
        ({"uknown": "field"}, {}, {"uknown": "field"}),
        ({}, {"uknown": "value"}, {"uknown": "value"}),
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
        "schema": "db",
    }
    actual_config = {
        k: v
        for k, v in model_configs["customer_revenue_by_day"].dict().items()
        if k in expected_config
    }
    assert actual_config == expected_config


def test_source_config():
    expected_sources = {
        "orders",
        "order_items",
    }

    source_configs = ProjectConfig.load(
        Path("tests/projects/sushi_dbt")
    ).sources
    assert set(source_configs) == expected_sources

    expected_config = {
        "schema": "raw",
        "identifier": "order_items",
    }
    actual_config = {
        k: v for k, v in source_configs["order_items"].dict().items() if k in expected_config
    }
    assert actual_config == expected_config
