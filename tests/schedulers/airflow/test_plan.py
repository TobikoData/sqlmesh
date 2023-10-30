import typing as t
from datetime import datetime
from unittest import mock

import pytest
from _pytest.monkeypatch import MonkeyPatch
from pytest_lazyfixture import lazy_fixture
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.context import Context
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelKindName,
    create_sql_model,
)
from sqlmesh.core.node import NodeType
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotFingerprint,
    SnapshotTableInfo,
)
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.plan import PlanDagState, create_plan_dag_spec
from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def snapshot(make_snapshot, random_name) -> Snapshot:
    result = make_snapshot(
        create_sql_model(
            random_name(),
            parse_one("SELECT 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            start="2022-01-01",
        ),
    )
    result.categorize_as(SnapshotChangeCategory.BREAKING)
    return result


@pytest.fixture
def depends_on_past_snapshot(make_snapshot, random_name) -> Snapshot:
    name = random_name()
    result = make_snapshot(
        create_sql_model(
            name,
            parse_one(f"SELECT 1, ds FROM {name}"),
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=1),
            start="2022-01-01",
        ),
    )
    result.categorize_as(SnapshotChangeCategory.BREAKING)
    return result


@pytest.mark.airflow
@pytest.mark.parametrize(
    "the_snapshot, expected_intervals, paused_forward_only",
    [
        (lazy_fixture("snapshot"), [(to_datetime("2022-01-01"), to_datetime("2022-01-05"))], False),
        (lazy_fixture("snapshot"), [(to_datetime("2022-01-01"), to_datetime("2022-01-05"))], True),
        (
            lazy_fixture("depends_on_past_snapshot"),
            [
                (to_datetime("2022-01-01"), to_datetime("2022-01-02")),
                (to_datetime("2022-01-02"), to_datetime("2022-01-03")),
                (to_datetime("2022-01-03"), to_datetime("2022-01-04")),
                (to_datetime("2022-01-04"), to_datetime("2022-01-05")),
            ],
            False,
        ),
    ],
)
def test_create_plan_dag_spec(
    mocker: MockerFixture,
    the_snapshot: Snapshot,
    expected_intervals: t.List[t.Tuple[datetime, datetime]],
    paused_forward_only: bool,
    random_name,
):
    the_snapshot.categorize_as(
        SnapshotChangeCategory.FORWARD_ONLY
        if paused_forward_only
        else SnapshotChangeCategory.BREAKING
    )

    environment_name = random_name()
    new_environment = Environment(
        name=environment_name,
        snapshots=[the_snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-04",
        plan_id="test_plan_id",
        suffix_target=EnvironmentSuffixTarget.TABLE,
    )

    plan_request = common.PlanApplicationRequest(
        request_id="test_request_id",
        new_snapshots=[the_snapshot],
        environment=new_environment,
        no_gaps=True,
        skip_backfill=False,
        restatements={},
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=True,
    )

    deleted_snapshot = SnapshotTableInfo(
        name="test_schema.deleted_model",
        fingerprint=SnapshotFingerprint(data_hash="1", metadata_hash="1"),
        version="test_version",
        physical_schema="test_physical_schema",
        parents=[],
        change_category=SnapshotChangeCategory.BREAKING,
        kind_name=ModelKindName.FULL,
        node_type=NodeType.MODEL,
    )
    old_environment = Environment(
        name=environment_name,
        snapshots=[deleted_snapshot],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        suffix_target=EnvironmentSuffixTarget.SCHEMA,
    )

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {}
    state_sync_mock.get_environment.return_value = old_environment
    state_sync_mock.get_snapshot_intervals.return_value = []
    state_sync_mock.refresh_snapshot_intervals.return_value = []

    with mock.patch(
        "sqlmesh.schedulers.airflow.plan.now_timestamp",
        side_effect=lambda: to_timestamp("2023-01-01"),
    ):
        plan_spec = create_plan_dag_spec(plan_request, state_sync_mock)
    assert plan_spec == common.PlanDagSpec(
        request_id="test_request_id",
        environment_naming_info=EnvironmentNamingInfo(
            name=environment_name, suffix_target=EnvironmentSuffixTarget.TABLE
        ),
        new_snapshots=[the_snapshot],
        backfill_intervals_per_snapshot=[
            common.BackfillIntervalsPerSnapshot(
                snapshot_id=the_snapshot.snapshot_id,
                intervals=expected_intervals,
                before_promote=not paused_forward_only,
            )
        ],
        promoted_snapshots=[the_snapshot.table_info],
        demoted_snapshots=[deleted_snapshot],
        start="2022-01-01",
        end="2022-01-04",
        unpaused_dt=None,
        no_gaps=True,
        plan_id="test_plan_id",
        previous_plan_id=None,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=True,
        dag_start_ts=to_timestamp("2023-01-01"),
        deployability_index=DeployabilityIndex.all_deployable(),
    )

    state_sync_mock.get_snapshots.assert_called_once()
    state_sync_mock.get_environment.assert_called_once()
    state_sync_mock.refresh_snapshot_intervals.assert_called_once()
    list(state_sync_mock.refresh_snapshot_intervals.call_args_list[0][0][0]) == [the_snapshot]


@pytest.mark.airflow
@pytest.mark.parametrize(
    "the_snapshot, expected_intervals",
    [
        (
            lazy_fixture("snapshot"),
            [(to_datetime("2022-01-02"), to_datetime("2022-01-04"))],
        ),
        (
            lazy_fixture("depends_on_past_snapshot"),
            [
                (to_datetime("2022-01-02"), to_datetime("2022-01-03")),
                (to_datetime("2022-01-03"), to_datetime("2022-01-04")),
            ],
        ),
    ],
)
def test_restatement(
    mocker: MockerFixture,
    monkeypatch: MonkeyPatch,
    the_snapshot: Snapshot,
    expected_intervals: t.List[t.Tuple[datetime, datetime]],
    random_name,
):
    environment_name = random_name()
    new_environment = Environment(
        name=environment_name,
        snapshots=[the_snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-07",
        plan_id="test_plan_id",
    )

    the_snapshot.add_interval("2022-01-01", "2022-01-07")

    plan_request = common.PlanApplicationRequest(
        request_id="test_request_id",
        new_snapshots=[],
        environment=new_environment,
        no_gaps=True,
        skip_backfill=False,
        restatements={
            the_snapshot.name: (
                to_timestamp("2022-01-02"),
                to_timestamp("2022-01-04"),
            )
        },
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=True,
    )
    old_environment = Environment(
        name=environment_name,
        snapshots=[the_snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-07",
        plan_id="test_plan_id",
    )

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {the_snapshot.snapshot_id: the_snapshot}
    state_sync_mock.get_environment.return_value = old_environment
    state_sync_mock.refresh_snapshot_intervals.return_value = [the_snapshot]

    now_value = "2022-01-09T23:59:59+00:00"
    with mock.patch(
        "sqlmesh.schedulers.airflow.plan.now", side_effect=lambda: to_datetime(now_value)
    ), mock.patch(
        "sqlmesh.schedulers.airflow.plan.now_timestamp", side_effect=lambda: to_timestamp(now_value)
    ):
        plan_spec = create_plan_dag_spec(plan_request, state_sync_mock)

    assert plan_spec == common.PlanDagSpec(
        request_id="test_request_id",
        environment_naming_info=EnvironmentNamingInfo(
            name=environment_name, suffix_target=EnvironmentSuffixTarget.SCHEMA
        ),
        new_snapshots=[],
        backfill_intervals_per_snapshot=[
            common.BackfillIntervalsPerSnapshot(
                snapshot_id=the_snapshot.snapshot_id,
                intervals=expected_intervals,
            )
        ],
        promoted_snapshots=[the_snapshot.table_info],
        demoted_snapshots=[],
        start="2022-01-01",
        end="2022-01-07",
        unpaused_dt=None,
        no_gaps=True,
        plan_id="test_plan_id",
        previous_plan_id=None,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=True,
        dag_start_ts=to_timestamp(now_value),
        deployability_index=DeployabilityIndex.all_deployable(),
    )

    state_sync_mock.get_snapshots.assert_called_once()
    state_sync_mock.get_environment.assert_called_once()
    state_sync_mock.refresh_snapshot_intervals.assert_called_once()

    state_sync_mock.remove_interval.assert_called_once_with(
        [(the_snapshot, (to_timestamp("2022-01-02"), to_timestamp("2022-01-04")))],
        remove_shared_versions=True,
    )

    assert the_snapshot.intervals == [
        (to_timestamp("2022-01-01"), to_timestamp("2022-01-02")),
        (to_timestamp("2022-01-04"), to_timestamp("2022-01-08")),
    ]


@pytest.mark.airflow
def test_create_plan_dag_spec_duplicated_snapshot(
    mocker: MockerFixture, snapshot: Snapshot, random_name
):
    environment_name = random_name()
    new_environment = Environment(
        name=environment_name,
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
    )

    plan_request = common.PlanApplicationRequest(
        request_id="test_request_id",
        new_snapshots=[snapshot],
        environment=new_environment,
        no_gaps=False,
        skip_backfill=False,
        restatements={},
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=False,
    )

    dag_run_mock = mocker.Mock()
    dag_run_mock.conf = plan_request.dict()

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {snapshot.snapshot_id: snapshot}

    with pytest.raises(SQLMeshError):
        create_plan_dag_spec(plan_request, state_sync_mock)

    state_sync_mock.get_snapshots.assert_called_once()


@pytest.mark.airflow
@pytest.mark.parametrize("unbounded_end", [None, ""])
def test_create_plan_dag_spec_unbounded_end(
    mocker: MockerFixture,
    snapshot: Snapshot,
    make_snapshot,
    random_name,
    unbounded_end: t.Optional[str],
):
    unrelated_snapshot = make_snapshot(create_sql_model(random_name(), parse_one("SELECT 2, ds")))
    unrelated_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    environment_name = random_name()
    new_environment = Environment(
        name=environment_name,
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at=unbounded_end,
        plan_id="test_plan_id",
    )

    plan_request = common.PlanApplicationRequest(
        request_id="test_request_id",
        new_snapshots=[],
        environment=new_environment,
        no_gaps=True,
        skip_backfill=False,
        restatements={},
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=False,
    )

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {
        snapshot.snapshot_id: snapshot,
        unrelated_snapshot.snapshot_id: unrelated_snapshot,
    }
    state_sync_mock.get_environment.return_value = None
    state_sync_mock.get_snapshot_intervals.return_value = []
    state_sync_mock.refresh_snapshot_intervals.return_value = []

    create_plan_dag_spec(plan_request, state_sync_mock)

    state_sync_mock.get_snapshots.assert_called_once()
    state_sync_mock.get_environment.assert_called_once()
    state_sync_mock.refresh_snapshot_intervals.assert_called_once()


def test_plan_dag_state(snapshot: Snapshot, sushi_context: Context, random_name):
    environment_name = random_name()
    plan_dag_spec = common.PlanDagSpec(
        request_id="test_request_id",
        environment_naming_info=EnvironmentNamingInfo(
            name=environment_name, suffix_target=EnvironmentSuffixTarget.SCHEMA
        ),
        new_snapshots=[],
        backfill_intervals_per_snapshot=[],
        promoted_snapshots=[snapshot.table_info],
        demoted_snapshots=[],
        start=to_timestamp("2022-01-02"),
        end=None,
        unpaused_dt=None,
        no_gaps=True,
        plan_id="test_plan_id",
        previous_plan_id=None,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        forward_only=True,
        dag_start_ts=to_timestamp("2023-01-01"),
    )

    plan_dag_state = PlanDagState.from_state_sync(sushi_context.state_sync)

    assert not plan_dag_state.get_dag_specs()

    plan_dag_state.add_dag_spec(plan_dag_spec)
    assert plan_dag_state.get_dag_specs() == [plan_dag_spec]

    plan_dag_state.delete_dag_specs([])
    assert plan_dag_state.get_dag_specs() == [plan_dag_spec]

    plan_dag_state.delete_dag_specs(
        [common.plan_application_dag_id(environment_name, "test_request_id")]
    )
    assert not plan_dag_state.get_dag_specs()
