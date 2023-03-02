import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import IncrementalByTimeRangeKind, create_sql_model
from sqlmesh.core.snapshot import Snapshot, SnapshotFingerprint, SnapshotTableInfo
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.plan import create_plan_dag_spec
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def snapshot(make_snapshot, random_name) -> Snapshot:
    return make_snapshot(
        create_sql_model(
            random_name(),
            parse_one("SELECT 1, ds"),
            kind=IncrementalByTimeRangeKind(time_column="ds"),
        ),
        version="1",
    )


@pytest.mark.airflow
def test_create_plan_dag_spec(mocker: MockerFixture, snapshot: Snapshot, random_name):
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
        no_gaps=True,
        skip_backfill=False,
        restatements={"raw.items"},
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
    )

    deleted_snapshot = SnapshotTableInfo(
        name="test_schema.deleted_model",
        fingerprint=SnapshotFingerprint(data_hash="1", metadata_hash="1"),
        version="test_version",
        physical_schema="test_physical_schema",
        parents=[],
        is_materialized=True,
        is_embedded_kind=False,
    )
    old_environment = Environment(
        name=environment_name,
        snapshots=[deleted_snapshot],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
    )

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {}
    state_sync_mock.get_environment.return_value = old_environment

    plan_spec = create_plan_dag_spec(plan_request, state_sync_mock)
    assert plan_spec == common.PlanDagSpec(
        request_id="test_request_id",
        environment_name=environment_name,
        new_snapshots=[snapshot],
        backfill_intervals_per_snapshot=[
            common.BackfillIntervalsPerSnapshot(
                snapshot_id=snapshot.snapshot_id,
                intervals=[(to_datetime("2022-01-01"), to_datetime("2022-01-02"))],
            )
        ],
        promoted_snapshots=[snapshot.table_info],
        demoted_snapshots=[deleted_snapshot],
        start="2022-01-01",
        end="2022-01-01",
        unpaused_dt=None,
        no_gaps=True,
        plan_id="test_plan_id",
        previous_plan_id=None,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
    )

    state_sync_mock.get_snapshots.assert_called_once()
    state_sync_mock.get_environment.assert_called_once()


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
        restatements=set(),
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
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
    unrelated_snapshot = make_snapshot(
        create_sql_model(random_name(), parse_one("SELECT 2, ds")), version="1"
    )

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
        restatements={"raw.items"},
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
    )

    state_sync_mock = mocker.Mock()
    state_sync_mock.get_snapshots.return_value = {
        snapshot.snapshot_id: snapshot,
        unrelated_snapshot.snapshot_id: unrelated_snapshot,
    }
    state_sync_mock.get_environment.return_value = None

    create_plan_dag_spec(plan_request, state_sync_mock)

    state_sync_mock.get_snapshots.assert_called_once()
    state_sync_mock.get_environment.assert_called_once()
