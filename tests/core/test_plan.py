import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.model import Model
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot import SnapshotChangeCategory, SnapshotDataVersion
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.errors import PlanError


def test_forward_only_plan_sets_version(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(Model(name="a", query=parse_one("select 1, ds")))
    snapshot_a.set_version()

    snapshot_b = make_snapshot(Model(name="b", query=parse_one("select 2, ds")))
    snapshot_b.previous_versions = (
        SnapshotDataVersion(fingerprint="test_fingerprint", version="test_version"),
    )
    assert not snapshot_b.version

    dag = DAG[str]({"b": {"a"}})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {"b", (snapshot_b, snapshot_b)}

    state_reader_mock = mocker.Mock()

    plan = Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)

    assert snapshot_b.version == "test_version"

    # Make sure that the choice can't be set manually.
    with pytest.raises(
        PlanError, match="Choice setting is not supported by a forward-only plan."
    ):
        plan.set_choice(snapshot_b, SnapshotChangeCategory.BREAKING)


def test_forward_only_plan_new_models_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(Model(name="a", query=parse_one("select 1, ds")))
    snapshot_a.set_version()

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {"a"}
    context_diff_mock.modified_snapshots = {}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError, match="New models can't be added as part of the forward-only plan."
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)


def test_paused_forward_only_parent(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(Model(name="a", query=parse_one("select 1, ds")))
    snapshot_a.previous_versions = (
        SnapshotDataVersion(
            fingerprint="test_fingerprint", version="test_version", change_category=None
        ),
    )
    snapshot_a.set_version(snapshot_a.previous_version)

    snapshot_b = make_snapshot(Model(name="b", query=parse_one("select 2, ds")))
    assert not snapshot_b.version

    dag = DAG[str]({"b": {"a"}})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {"b", (snapshot_b, snapshot_b)}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match=r"Modified model 'b' depends on a paused version of model 'a'.*",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=False)
