from __future__ import annotations

from pytest_mock.plugin import MockerFixture

from sqlmesh.core import dialect as d
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, SqlModel
from sqlmesh.core.selector import ModelSelector
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils import UniqueKeyDict


def test_model_selection(mocker: MockerFixture, make_snapshot):
    added_model = SqlModel(name="added_model", query=d.parse_one("SELECT 1"))
    modified_model_v1 = SqlModel(name="modified_model", query=d.parse_one("SELECT 1"))
    modified_model_v2 = SqlModel(name="modified_model", query=d.parse_one("SELECT 2"))
    removed_model = SqlModel(name="removed_model", query=d.parse_one("SELECT 1"))

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

    local_models: UniqueKeyDict[str, Model] = UniqueKeyDict("models")
    local_models[added_model.name] = added_model
    local_models[modified_model_v2.name] = modified_model_v2

    selector = ModelSelector(state_reader_mock, local_models)

    assert selector.select(["added_model"], env_name) == {
        added_model.name: added_model,
        modified_model_v1.name: modified_model_v1,
        removed_model.name: removed_model,
    }
    assert selector.select(["modified_model"], env_name) == {
        modified_model_v2.name: modified_model_v2,
        removed_model.name: removed_model,
    }
    assert selector.select(["removed_model"], env_name) == {
        modified_model_v1.name: modified_model_v1,
    }
    assert selector.select(["added_model", "modified_model"], env_name) == {
        added_model.name: added_model,
        modified_model_v2.name: modified_model_v2,
        removed_model.name: removed_model,
    }
    assert (
        selector.select(["added_model", "modified_model", "removed_model"], env_name)
        == local_models
    )
