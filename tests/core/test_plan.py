import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.model import SeedKind, SeedModel, SqlModel
from sqlmesh.core.model.seed import Seed
from sqlmesh.core.plan import Plan
from sqlmesh.core.snapshot import (
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotFingerprint,
)
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import PlanError


def test_forward_only_plan_sets_version(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.set_version()

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    snapshot_b.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
        ),
    )
    assert not snapshot_b.version

    dag = DAG[str]({"b": {"a"}})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {"b": (snapshot_b, snapshot_b)}
    context_diff_mock.new_snapshots = {snapshot_b.snapshot_id: snapshot_b}

    state_reader_mock = mocker.Mock()

    plan = Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)

    assert snapshot_b.version == "test_version"

    # Make sure that the choice can't be set manually.
    with pytest.raises(PlanError, match="Choice setting is not supported by a forward-only plan."):
        plan.set_choice(snapshot_b, SnapshotChangeCategory.BREAKING)


def test_forward_only_dev(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    expected_start = "2022-01-01"
    expected_end = to_datetime("2022-01-02")

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    state_reader_mock = mocker.Mock()

    yesterday_ds_mock = mocker.patch("sqlmesh.core.plan.definition.yesterday_ds")
    yesterday_ds_mock.return_value = expected_start

    now_ds_mock = mocker.patch("sqlmesh.core.plan.definition.now")
    now_ds_mock.return_value = expected_end

    plan = Plan(context_diff_mock, dag, state_reader_mock, forward_only=True, is_dev=True)

    assert plan.restatements == {"a"}
    assert plan.start == expected_start
    assert plan.end == expected_end

    yesterday_ds_mock.assert_called_once()
    now_ds_mock.assert_called_once()


def test_forward_only_plan_new_models_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.set_version()

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {"a"}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError, match="New models can't be added as part of the forward-only plan."
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)


def test_paused_forward_only_parent(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot_a.previous_versions = (
        SnapshotDataVersion(
            fingerprint=SnapshotFingerprint(
                data_hash="test_data_hash",
                metadata_hash="test_metadata_hash",
            ),
            version="test_version",
            change_category=None,
        ),
    )
    snapshot_a.set_version(snapshot_a.previous_version)

    snapshot_b = make_snapshot(SqlModel(name="b", query=parse_one("select 2, ds")))
    assert not snapshot_b.version

    dag = DAG[str]({"b": {"a"}})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a, "b": snapshot_b}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {"b": (snapshot_b, snapshot_b)}
    context_diff_mock.new_snapshots = {snapshot_b.snapshot_id: snapshot_b}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match=r"Model 'b' depends on a paused version of model 'a'.*",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=False)


def test_restate_models(sushi_context_pre_scheduling: Context):
    plan = sushi_context_pre_scheduling.plan(
        restate_models=["sushi.waiter_revenue_by_day"], no_prompts=True
    )
    assert plan.restatements == {"sushi.waiter_revenue_by_day"}
    assert plan.requires_backfill

    with pytest.raises(PlanError, match=r"Cannot restate from 'unknown_model'.*"):
        sushi_context_pre_scheduling.plan(restate_models=["unknown_model"], no_prompts=True)


def test_restate_model_with_merge_strategy(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SqlModel(
            name="a",
            query=parse_one("select 1, key"),
            kind="VIEW",
        )
    )

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match=r"Cannot restate from 'a'. Either such model doesn't exist or no other model references it.",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, restate_models=["a"])


def test_new_snapshots_with_restatements(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match=r"Model changes and restatements can't be a part of the same plan.*",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, restate_models=["a"])


def test_end_validation(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    state_reader_mock = mocker.Mock()

    dev_plan = Plan(context_diff_mock, dag, state_reader_mock, end="2022-01-03", is_dev=True)
    assert dev_plan.end == "2022-01-03"
    dev_plan.end = "2022-01-04"
    assert dev_plan.end == "2022-01-04"

    with pytest.raises(
        PlanError,
        match="The end date can't be set for a production plan without restatements.",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, end="2022-01-03")

    prod_plan = Plan(context_diff_mock, dag, state_reader_mock)
    with pytest.raises(
        PlanError,
        match="The end date can't be set for a production plan without restatements.",
    ):
        prod_plan.end = "2022-01-03"

    context_diff_mock.new_snapshots = {}
    restatement_prod_plan = Plan(
        context_diff_mock,
        dag,
        state_reader_mock,
        end="2022-01-03",
        restate_models=["a"],
    )
    assert restatement_prod_plan.end == "2022-01-03"
    restatement_prod_plan.end = "2022-01-04"
    assert restatement_prod_plan.end == "2022-01-04"


def test_forward_only_revert_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 1, ds")))
    snapshot.set_version()
    assert not snapshot.is_forward_only

    forward_only_snapshot = make_snapshot(SqlModel(name="a", query=parse_one("select 2, ds")))
    forward_only_snapshot.set_version(snapshot.version)
    assert forward_only_snapshot.is_forward_only

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot}
    context_diff_mock.added = set()
    context_diff_mock.modified_snapshots = {"a": (snapshot, forward_only_snapshot)}
    context_diff_mock.new_snapshots = {}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match=r"Detected an existing version of model 'a' that has been previously superseded by a forward-only change.*",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)

    # Make sure the plan can be created if a new snapshot version was enforced.
    new_version_snapshot = make_snapshot(
        SqlModel(name="a", query=parse_one("select 1, ds"), stamp="test_stamp")
    )
    new_version_snapshot.set_version()
    context_diff_mock.modified_snapshots = {"a": (new_version_snapshot, forward_only_snapshot)}
    context_diff_mock.new_snapshots = {new_version_snapshot.snapshot_id: new_version_snapshot}
    Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)


def test_forward_only_plan_seed_models_not_allowed(make_snapshot, mocker: MockerFixture):
    snapshot_a = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="content"),
            depends_on=set(),
        )
    )
    snapshot_a.set_version()

    dag = DAG[str]({"a": set()})

    context_diff_mock = mocker.Mock()
    context_diff_mock.snapshots = {"a": snapshot_a}
    context_diff_mock.added = {}
    context_diff_mock.modified_snapshots = {}
    context_diff_mock.new_snapshots = {snapshot_a.snapshot_id: snapshot_a}

    state_reader_mock = mocker.Mock()

    with pytest.raises(
        PlanError,
        match="Seed model 'a' can't be updated as part of the forward-only plan.",
    ):
        Plan(context_diff_mock, dag, state_reader_mock, forward_only=True)
