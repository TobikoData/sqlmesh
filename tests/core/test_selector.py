from __future__ import annotations

import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import dialect as d
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, SqlModel
from sqlmesh.core.selector import Selector
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.utils.errors import SQLMeshError


def test_select_models(mocker: MockerFixture, make_snapshot):
    added_model = SqlModel(name="added_model", query=d.parse_one("SELECT 1 AS a"))
    modified_model_v1 = SqlModel(
        name="modified_model", query=d.parse_one("SELECT a + 1 FROM added_model")
    )
    modified_model_v2 = SqlModel(
        name="modified_model", query=d.parse_one("SELECT a + 2 FROM added_model")
    )
    removed_model = SqlModel(name="removed_model", query=d.parse_one("SELECT a FROM added_model"))

    modified_model_v1_snapshot = make_snapshot(modified_model_v1)
    modified_model_v1_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    removed_model_snapshot = make_snapshot(removed_model)
    removed_model_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    env_name = "test_env"

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_environment.return_value = Environment(
        name=env_name,
        snapshots=[s.table_info for s in (modified_model_v1_snapshot, removed_model_snapshot)],
        start_at="2023-01-01",
        end_at="2023-02-01",
        plan_id="test_plan_id",
    )
    state_reader_mock.get_snapshots.return_value = {
        modified_model_v1_snapshot.snapshot_id: modified_model_v1_snapshot,
        removed_model_snapshot.snapshot_id: removed_model_snapshot,
    }

    added_model_schema = {"added_model": {"a": "INT"}}

    local_models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    local_models[added_model.name] = added_model
    local_models[modified_model_v2.name] = modified_model_v2.copy(
        update={"mapping_schema": added_model_schema}
    )

    selector = Selector(state_reader_mock, local_models, {})

    _assert_models_equal(
        selector.select_models(["added_model"], env_name),
        {
            added_model.name: added_model,
            modified_model_v1.name: modified_model_v1.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.name: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["modified_model"], "missing_env", fallback_env_name=env_name),
        {
            modified_model_v2.name: modified_model_v2,
            removed_model.name: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["removed_model"], env_name),
        {
            modified_model_v1.name: modified_model_v1,
        },
    )
    _assert_models_equal(
        selector.select_models(
            ["added_model", "modified_model"], "missing_env", fallback_env_name=env_name
        ),
        {
            added_model.name: added_model,
            modified_model_v2.name: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.name: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["+modified_model"], env_name),
        {
            added_model.name: added_model,
            modified_model_v2.name: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.name: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["added_model+"], env_name),
        {
            added_model.name: added_model,
            modified_model_v2.name: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.name: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["added_model", "modified_model", "removed_model"], env_name),
        local_models,
    )
    _assert_models_equal(
        selector.select_models(["*_model", "removed_model"], env_name),
        local_models,
    )


def test_select_models_missing_env(mocker: MockerFixture, make_snapshot):
    model = SqlModel(name="test_model", query=d.parse_one("SELECT 1 AS a"))

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_environment.return_value = None

    local_models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    local_models[model.name] = model

    selector = Selector(state_reader_mock, local_models, {})

    with pytest.raises(SQLMeshError):
        selector.select_models([model.name], "missing_env")

    with pytest.raises(SQLMeshError):
        selector.select_models([model.name], "missing_env", fallback_env_name="another_missing_env")

    state_reader_mock.get_environment.assert_has_calls(
        [
            call("missing_env"),
            call("missing_env"),
            call("another_missing_env"),
        ]
    )


def _assert_models_equal(actual: t.Dict[str, Model], expected: t.Dict[str, Model]) -> None:
    assert set(actual) == set(expected)
    for name, model in actual.items():
        # Use dict() to make Pydantic V2 happy.
        assert model.dict() == expected[name].dict()
