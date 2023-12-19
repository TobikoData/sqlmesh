from __future__ import annotations

import typing as t
from unittest.mock import call

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import dialect as d
from sqlmesh.core.audit import StandaloneAudit
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, SqlModel
from sqlmesh.core.selector import Selector
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils import UniqueKeyDict


@pytest.mark.parametrize(
    "default_catalog",
    [
        None,
        "test_catalog",
    ],
)
def test_select_models(mocker: MockerFixture, make_snapshot, default_catalog: t.Optional[str]):
    added_model = SqlModel(
        name="db.added_model", query=d.parse_one("SELECT 1 AS a"), default_catalog=default_catalog
    )
    modified_model_v1 = SqlModel(
        name="db.modified_model",
        query=d.parse_one("SELECT a + 1 FROM db.added_model"),
        default_catalog=default_catalog,
    )
    modified_model_v2 = SqlModel(
        name="db.modified_model",
        query=d.parse_one("SELECT a + 2 FROM db.added_model"),
        default_catalog=default_catalog,
    )
    removed_model = SqlModel(
        name="db.removed_model",
        query=d.parse_one("SELECT a FROM db.added_model"),
        default_catalog=default_catalog,
    )
    standalone_audit = StandaloneAudit(
        name="test_audit", query=d.parse_one(f"SELECT * FROM added_model WHERE a IS NULL")
    )

    modified_model_v1_snapshot = make_snapshot(modified_model_v1)
    modified_model_v1_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    removed_model_snapshot = make_snapshot(removed_model)
    removed_model_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    standalone_audit_snapshot = make_snapshot(standalone_audit)
    standalone_audit_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    env_name = "test_env"

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_environment.return_value = Environment(
        name=env_name,
        snapshots=[
            s.table_info
            for s in (modified_model_v1_snapshot, removed_model_snapshot, standalone_audit_snapshot)
        ],
        start_at="2023-01-01",
        end_at="2023-02-01",
        plan_id="test_plan_id",
    )
    state_reader_mock.get_snapshots.return_value = {
        modified_model_v1_snapshot.snapshot_id: modified_model_v1_snapshot,
        removed_model_snapshot.snapshot_id: removed_model_snapshot,
        standalone_audit_snapshot.snapshot_id: standalone_audit_snapshot,
    }

    added_model_schema = {'"db"': {'"added_model"': {"a": "INT"}}}
    if default_catalog:
        added_model_schema = {f'"{default_catalog}"': added_model_schema}  # type: ignore

    local_models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    local_models[added_model.fqn] = added_model
    local_models[modified_model_v2.fqn] = modified_model_v2.copy(
        update={"mapping_schema": added_model_schema}
    )
    selector = Selector(state_reader_mock, local_models, default_catalog=default_catalog)

    _assert_models_equal(
        selector.select_models(["db.added_model"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v1.fqn: modified_model_v1.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["db.modified_model"], "missing_env", fallback_env_name=env_name),
        {
            modified_model_v2.fqn: modified_model_v2,
            removed_model.fqn: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["db.removed_model"], env_name),
        {
            modified_model_v1.fqn: modified_model_v1,
        },
    )
    _assert_models_equal(
        selector.select_models(
            ["db.added_model", "db.modified_model"], "missing_env", fallback_env_name=env_name
        ),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["+db.modified_model"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(["db.added_model+"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model.copy(update={"mapping_schema": added_model_schema}),
        },
    )
    _assert_models_equal(
        selector.select_models(
            ["db.added_model", "db.modified_model", "db.removed_model"], env_name
        ),
        local_models,
    )
    _assert_models_equal(
        selector.select_models(["*_model", "db.removed_model"], env_name),
        local_models,
    )


def test_select_models_missing_env(mocker: MockerFixture, make_snapshot):
    model = SqlModel(name="test_model", query=d.parse_one("SELECT 1 AS a"))

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_environment.return_value = None

    local_models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    local_models[model.fqn] = model

    selector = Selector(state_reader_mock, local_models)

    assert selector.select_models([model.name], "missing_env").keys() == {model.fqn}
    assert not selector.select_models(["missing"], "missing_env")

    assert selector.select_models(
        [model.name], "missing_env", fallback_env_name="another_missing_env"
    ).keys() == {model.fqn}

    state_reader_mock.get_environment.assert_has_calls(
        [
            call("missing_env"),
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
