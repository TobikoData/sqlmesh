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
        name="db.added_model",
        query=d.parse_one("SELECT 1 AS a"),
        default_catalog=default_catalog,
        tags=["tag1"],
    )
    modified_model_v1 = SqlModel(
        name="db.modified_model",
        query=d.parse_one("SELECT a + 1 FROM db.added_model"),
        default_catalog=default_catalog,
        tags=["tag2"],
    )
    modified_model_v2 = SqlModel(
        name="db.modified_model",
        query=d.parse_one("SELECT a + 2 FROM db.added_model"),
        default_catalog=default_catalog,
        tags=["tag2"],
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
            removed_model.fqn: removed_model,
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
            removed_model.fqn: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["+db.modified_model"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["db.added_model+"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model,
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
    _assert_models_equal(
        selector.select_models(["tag:tag1", "tag:tag2"], "missing_env", fallback_env_name=env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["tag:tag*"], "missing_env", fallback_env_name=env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model,
        },
    )
    _assert_models_equal(
        selector.select_models(["tag:+tag2"], env_name),
        {
            added_model.fqn: added_model,
            modified_model_v2.fqn: modified_model_v2.copy(
                update={"mapping_schema": added_model_schema}
            ),
            removed_model.fqn: removed_model,
        },
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


@pytest.mark.parametrize(
    "tags, tag_selections, output",
    [
        # Direct matching only
        (
            [("model1", "tag1", None), ("model2", "tag2", None), ("model3", "tag3", None)],
            ["tag:tag1", "tag:tag3"],
            {'"model1"', '"model3"'},
        ),
        # Wildcard works
        (
            [("model1", "tag1", None), ("model2", "tag2", None), ("model3", "tag3", None)],
            ["tag:tag*"],
            {'"model1"', '"model2"', '"model3"'},
        ),
        # Downstream models are included
        (
            [("model1", "tag1", None), ("model2", "tag2", {"model1"}), ("model3", "tag3", None)],
            ["tag:tag1+"],
            {'"model1"', '"model2"'},
        ),
        # Upstream models are included
        (
            [("model1", "tag1", None), ("model2", "tag2", None), ("model3", "tag3", {"model2"})],
            ["tag:+tag3"],
            {'"model2"', '"model3"'},
        ),
        # Upstream and downstream models are included
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", {"model1"}),
                ("model3", "tag3", {"model2"}),
            ],
            ["tag:+tag2+"],
            {'"model1"', '"model2"', '"model3"'},
        ),
        # Wildcard works with upstream and downstream models
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", {"model1"}),
                ("model3", "tag3", {"model2"}),
                ("model4", "blah", {"model3"}),
                ("model5", "tag4", None),
                # Only excluded model since it doesn't match wildcard nor upstream/downstream
                ("model6", "blah", None),
            ],
            ["tag:+tag*+"],
            {'"model1"', '"model2"', '"model3"', '"model4"', '"model5"'},
        ),
        # Multiple tags work
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
                ("model3", "tag3", None),
                ("model4", "tag4", None),
            ],
            ["tag:tag1", "tag:tag3"],
            {'"model1"', '"model3"'},
        ),
        # Multiple tags work with upstream and downstream models
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", {"model1"}),
                ("model3", "tag3", {"model2"}),
                ("model4", "tag4", None),
                ("model5", "tag5", {"model4"}),
                ("model6", "tag6", {"model5"}),
            ],
            ["tag:+tag3", "tag:tag5"],
            {'"model1"', '"model2"', '"model3"', '"model5"'},
        ),
        # Case-insensitive matching
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
                ("model3", "tag3", None),
            ],
            ["tag:TAG*"],
            {'"model1"', '"model2"', '"model3"'},
        ),
        # Wildcard returns everything
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
                ("model3", "tag3", None),
            ],
            ["tag:*"],
            {'"model1"', '"model2"', '"model3"'},
        ),
        # Upstream that don't exist is fine
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
            ],
            ["tag:+tag2"],
            {'"model2"'},
        ),
        # No matches returns empty set
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
            ],
            ["tag:+tag3*+", "tag:+tag3+"],
            set(),
        ),
        # Mix of models and tags
        (
            [
                ("model1", "tag1", None),
                ("model2", "tag2", None),
                ("model3", "tag3", None),
            ],
            ["tag:tag1", "model2"],
            {'"model1"', '"model2"'},
        ),
    ],
)
def test_expand_model_tags(mocker: MockerFixture, make_snapshot, tags, tag_selections, output):
    models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    for (model_name, tag, depends_on) in tags:
        model = SqlModel(
            name=model_name, query=d.parse_one("SELECT 1 AS a"), depends_on=depends_on, tags=[tag]
        )
        models[model.fqn] = model

    selector = Selector(mocker.Mock(), models)
    assert selector.expand_model_selections(tag_selections) == output


def _assert_models_equal(actual: t.Dict[str, Model], expected: t.Dict[str, Model]) -> None:
    assert set(actual) == set(expected)
    for name, model in actual.items():
        # Use dict() to make Pydantic V2 happy.
        assert model.dict() == expected[name].dict()
