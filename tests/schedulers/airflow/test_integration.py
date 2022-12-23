import typing as t
from datetime import timedelta

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model
from sqlmesh.core.snapshot import Snapshot, SnapshotTableInfo
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.schedulers.airflow.integration import _plan_receiver_task
from sqlmesh.utils import random_id
from sqlmesh.utils.date import now, to_datetime, yesterday
from sqlmesh.utils.errors import SQLMeshError

DAG_CREATION_WAIT_INTERVAL = 3
DAG_CREATION_RETRY_ATTEMPTS = 5
DAG_RUN_POLL_INTERVAL = 1


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_system_dags(airflow_client: AirflowClient):
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(15), reraise=True)
    def get_system_dags() -> t.List[t.Dict[str, t.Any]]:
        return [
            airflow_client.get_plan_receiver_dag(),
            airflow_client.get_janitor_dag(),
        ]

    system_dags = get_system_dags()
    assert all(d["is_active"] for d in system_dags)


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_apply_plan_create_backfill_promote(
    airflow_client: AirflowClient, make_snapshot, random_name
):
    model_name = random_name()
    snapshot = make_snapshot(_create_model(model_name), version="1")

    environment_name = _random_environment_name()
    environment = _create_environment(snapshot, name=environment_name)
    environment.start = yesterday() - timedelta(days=1)
    environment.end = None

    assert airflow_client.get_environment(environment_name) is None

    _apply_plan_and_block(airflow_client, [snapshot], environment)

    assert airflow_client.get_environment(environment_name).snapshots == [  # type: ignore
        snapshot.table_info
    ]

    # Verify that the incremental DAG for the Snapshot has been created.
    dag = _get_snapshot_dag(airflow_client, model_name, snapshot.version)
    assert dag["is_active"]

    # Make sure that the same Snapshot can't be added again.
    plan_receiver_dag_run_id = airflow_client.apply_plan(
        [snapshot], environment, random_name()
    )
    assert not airflow_client.wait_for_dag_run_completion(
        common.PLAN_RECEIVER_DAG_ID, plan_receiver_dag_run_id, DAG_RUN_POLL_INTERVAL
    )

    # Verify full environment demotion.
    environment.snapshots = []
    environment.previous_plan_id = environment.plan_id
    environment.plan_id = "new_plan_id"
    _apply_plan_and_block(airflow_client, [], environment)
    assert not airflow_client.get_environment(environment_name).snapshots  # type: ignore


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_mult_snapshots_same_version(
    airflow_client: AirflowClient, make_snapshot, random_name
):
    model_name = random_name()

    snapshot = make_snapshot(_create_model(model_name), version="1")
    # Presetting the interval here to avoid backfill.
    snapshot.add_interval("2022-01-01", "2022-01-01")
    snapshot.set_unpaused_ts(now())

    original_fingerprint = snapshot.fingerprint

    environment_name = _random_environment_name()
    environment = _create_environment(snapshot, name=environment_name)
    _apply_plan_and_block(airflow_client, [snapshot], environment)

    dag = _get_snapshot_dag(airflow_client, model_name, snapshot.version)
    assert dag["is_active"]

    snapshot.fingerprint = "test_fingerprint"
    environment.previous_plan_id = environment.plan_id
    environment.plan_id = "new_plan_id"
    _apply_plan_and_block(airflow_client, [snapshot], environment)

    _validate_snapshot_fingerprints_for_version(
        airflow_client, snapshot, [original_fingerprint, "test_fingerprint"]
    )


@pytest.mark.airflow
def test_plan_receiver_task(mocker: MockerFixture, make_snapshot, random_name):
    model_name = random_name()
    snapshot = make_snapshot(_create_model(model_name), version="1")

    environment_name = _random_environment_name()
    new_environment = _create_environment(snapshot, name=environment_name)

    plan_conf = common.PlanReceiverDagConf(
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
    )

    deleted_snapshot = SnapshotTableInfo(
        name="test_schema.deleted_model",
        fingerprint="test_fingerprint",
        version="test_version",
        physical_schema="test_physical_schema",
        parents=[],
    )
    old_environment = Environment(
        name=environment_name,
        snapshots=[deleted_snapshot],
        start="2022-01-01",
        end="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )

    task_instance_mock = mocker.Mock()

    dag_run_mock = mocker.Mock()
    dag_run_mock.conf = plan_conf.dict()

    get_all_snapshots_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_all_snapshots"
    )
    get_all_snapshots_mock.return_value = {}

    get_environment_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_environment"
    )
    get_environment_mock.return_value = old_environment

    _plan_receiver_task(dag_run_mock, task_instance_mock, 1)

    get_all_snapshots_mock.assert_called_once()
    get_environment_mock.assert_called_once()

    task_instance_mock.xcom_push.assert_called_once()
    ((_, xcom_value), _) = task_instance_mock.xcom_push.call_args_list[0]
    assert common.PlanApplicationRequest.parse_raw(
        xcom_value
    ) == common.PlanApplicationRequest(
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
    )


@pytest.mark.airflow
def test_plan_receiver_task_duplicated_snapshot(
    mocker: MockerFixture, make_snapshot, random_name
):
    model_name = random_name()
    snapshot = make_snapshot(_create_model(model_name), version="1")

    environment_name = _random_environment_name()
    new_environment = _create_environment(snapshot, name=environment_name)

    plan_conf = common.PlanReceiverDagConf(
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
    )

    task_instance_mock = mocker.Mock()

    dag_run_mock = mocker.Mock()
    dag_run_mock.conf = plan_conf.dict()

    get_all_snapshots_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_all_snapshots"
    )
    get_all_snapshots_mock.return_value = {snapshot.snapshot_id: snapshot}

    get_environment_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_environment"
    )

    with pytest.raises(SQLMeshError):
        _plan_receiver_task(dag_run_mock, task_instance_mock, 1)

    get_all_snapshots_mock.assert_called_once()


@pytest.mark.airflow
@pytest.mark.parametrize("unbounded_end", [None, ""])
def test_plan_receiver_task_unbounded_end(
    mocker: MockerFixture, make_snapshot, random_name, unbounded_end: t.Optional[str]
):
    snapshot = make_snapshot(_create_model(random_name()), version="1")
    unrelated_snapshot = make_snapshot(_create_model(random_name()), version="1")

    environment_name = _random_environment_name()
    new_environment = _create_environment(snapshot, name=environment_name)
    new_environment.end = unbounded_end

    plan_conf = common.PlanReceiverDagConf(
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
    )

    task_instance_mock = mocker.Mock()

    dag_run_mock = mocker.Mock()
    dag_run_mock.conf = plan_conf.dict()

    get_all_snapshots_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_all_snapshots"
    )
    get_all_snapshots_mock.return_value = {
        snapshot.snapshot_id: snapshot,
        unrelated_snapshot.snapshot_id: unrelated_snapshot,
    }

    get_environment_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.get_environment"
    )

    _plan_receiver_task(dag_run_mock, task_instance_mock, 1)

    get_all_snapshots_mock.assert_called_once()
    get_environment_mock.assert_called_once()

    task_instance_mock.xcom_push.assert_called_once()


def _apply_plan_and_block(
    airflow_client: AirflowClient,
    new_snapshots: t.List[Snapshot],
    environment: Environment,
) -> None:
    plan_request_id = random_id()
    plan_receiver_dag_run_id = airflow_client.apply_plan(
        new_snapshots, environment, plan_request_id
    )
    assert airflow_client.wait_for_dag_run_completion(
        common.PLAN_RECEIVER_DAG_ID, plan_receiver_dag_run_id, DAG_RUN_POLL_INTERVAL
    )

    plan_application_dag_id = common.plan_application_dag_id(
        environment.name, plan_request_id
    )
    plan_application_dag_run_id = airflow_client.wait_for_first_dag_run(
        plan_application_dag_id, DAG_CREATION_WAIT_INTERVAL, DAG_CREATION_RETRY_ATTEMPTS
    )
    assert airflow_client.wait_for_dag_run_completion(
        plan_application_dag_id, plan_application_dag_run_id, DAG_RUN_POLL_INTERVAL
    )


@retry(wait=wait_fixed(3), stop=stop_after_attempt(5), reraise=True)
def _validate_snapshot_fingerprints_for_version(
    airflow_client: AirflowClient,
    snapshot: Snapshot,
    expected_fingerprints: t.List[str],
) -> None:
    assert sorted(
        airflow_client.get_snapshot_fingerprints_for_version(
            snapshot.name, snapshot.version
        )
    ) == sorted(expected_fingerprints)


@retry(wait=wait_fixed(3), stop=stop_after_attempt(5), reraise=True)
def _get_snapshot_dag(
    airflow_client: AirflowClient, model_name: str, version: str
) -> t.Dict[str, t.Any]:
    return airflow_client.get_snapshot_dag(model_name, version)


def _create_model(name: str) -> Model:
    return Model(
        name=name,
        description="Dummy table",
        owner="jen",
        cron="@daily",
        start="2020-01-01",
        batch_size=30,
        partitioned_by=["ds"],
        query=parse_one("SELECT '2022-01-01'::TEXT AS ds, 1::INT AS one"),
    )


def _create_environment(
    snapshot: Snapshot, name: t.Optional[str] = None
) -> Environment:
    return Environment(
        name=name or _random_environment_name(),
        snapshots=[snapshot.table_info],
        start="2022-01-01",
        end="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )


def _random_environment_name() -> str:
    return f"test_environment_{random_id()[-8:]}"
