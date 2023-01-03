import json

import pytest
import requests
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.date import to_datetime


@pytest.fixture
def snapshot() -> Snapshot:
    snapshot = Snapshot.from_model(
        Model(
            name="test_model",
            storage_format="parquet",
            partitioned_by=["a"],
            query=parse_one("SELECT a, ds FROM tbl"),
            expressions=[
                parse_one("@DEF(key, 'value')"),
            ],
        ),
        physical_schema="physical_schema",
        models={},
        ttl="in 1 week",
    )
    snapshot.version = snapshot.fingerprint
    snapshot.updated_ts = 1665014400000
    snapshot.created_ts = 1665014400000
    return snapshot


@pytest.mark.parametrize("dag_run_entries", [0, 1])
def test_apply_plan(mocker: MockerFixture, snapshot: Snapshot, dag_run_entries: int):
    post_dag_run_response_mock = mocker.Mock()
    post_dag_run_response_mock.json.return_value = {"dag_run_id": "test_dag_run_id"}
    post_dag_run_mock = mocker.patch("requests.Session.post")
    post_dag_run_mock.return_value = post_dag_run_response_mock

    get_dag_runs_response_mock = mocker.Mock()
    get_dag_runs_response_mock.json.return_value = {
        "total_entries": dag_run_entries,
        "dag_runs": [] if dag_run_entries == 0 else [{"dag_run_id": "init_run"}],
    }
    get_dag_runs_mock = mocker.patch("requests.Session.get")
    get_dag_runs_mock.return_value = get_dag_runs_response_mock

    timestamp = to_datetime("2022-08-16T02:40:19.000000Z")

    expected_dag_run_id = (
        "init_run" if dag_run_entries == 0 else "2022-08-16T02:40:19.000000Z"
    )

    environment = Environment(
        name="test_env",
        snapshots=[snapshot.table_info],
        start="2022-01-01",
        end="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="previous_plan_id",
    )

    request_id = "test_request_id"

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.apply_plan([snapshot], environment, request_id, timestamp=timestamp)

    assert result == "test_dag_run_id"

    get_dag_runs_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns?limit=1"
    )

    post_dag_run_mock.assert_called_once()
    args, data = post_dag_run_mock.call_args_list[0]

    assert (
        args[0] == "http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns"
    )
    assert json.loads(data["data"]) == {
        "conf": {
            "new_snapshots": [
                {
                    "created_ts": 1665014400000,
                    "ttl": "in 1 week",
                    "fingerprint": snapshot.fingerprint,
                    "indirect_versions": {},
                    "intervals": [],
                    "dev_intervals": [],
                    "model": {
                        "audits": {},
                        "cron": "@daily",
                        "dialect": "",
                        "expressions": ["@DEF(key, " "'value')"],
                        "kind": {
                            "name": "incremental_by_time_range",
                            "time_column": {"column": "ds"},
                        },
                        "name": "test_model",
                        "partitioned_by": ["a"],
                        "query": "SELECT a, ds FROM tbl",
                        "storage_format": "parquet",
                    },
                    "name": "test_model",
                    "parents": [],
                    "previous_versions": [],
                    "physical_schema": "physical_schema",
                    "updated_ts": 1665014400000,
                    "version": snapshot.version,
                }
            ],
            "environment": {
                "name": "test_env",
                "snapshots": [
                    {
                        "fingerprint": snapshot.fingerprint,
                        "name": "test_model",
                        "physical_schema": "physical_schema",
                        "previous_versions": [],
                        "version": snapshot.version,
                        "parents": [],
                        "is_materialized": True,
                    }
                ],
                "start": "2022-01-01",
                "end": "2022-01-01",
                "plan_id": "test_plan_id",
                "previous_plan_id": "previous_plan_id",
            },
            "no_gaps": False,
            "skip_backfill": False,
            "notification_targets": [],
            "request_id": request_id,
            "restatements": [],
            "backfill_concurrent_tasks": 1,
            "ddl_concurrent_tasks": 1,
            "users": [],
            "is_dev": False,
        },
        "dag_run_id": expected_dag_run_id,
        "logical_date": "2022-08-16T02:40:19.000000Z",
    }


def test_get_environment(mocker: MockerFixture, snapshot: Snapshot):
    get_snapshot_response_mock = mocker.Mock()
    environment = Environment(
        name="test",
        snapshots=[snapshot.table_info],
        start="2022-01-01",
        end="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )
    get_snapshot_response_mock.json.return_value = {"value": environment.json()}
    get_snapshot_mock = mocker.patch("requests.Session.get")
    get_snapshot_mock.return_value = get_snapshot_response_mock

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.get_environment("dev")

    assert result == environment

    get_snapshot_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns/init_run/taskInstances/plan_receiver_task/xcomEntries/environment__dev"
    )


def test_get_snapshot(mocker: MockerFixture, snapshot: Snapshot):
    get_snapshot_response_mock = mocker.Mock()
    get_snapshot_response_mock.json.return_value = {"value": snapshot.json()}
    get_snapshot_mock = mocker.patch("requests.Session.get")
    get_snapshot_mock.return_value = get_snapshot_response_mock

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.get_snapshot(snapshot.name, snapshot.fingerprint)

    assert result == snapshot

    get_snapshot_mock.assert_called_once_with(
        f"http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns/init_run/taskInstances/plan_receiver_task/xcomEntries/snapshot_payload__test_model__{snapshot.fingerprint}"
    )


def test_get_snapshot_ids(mocker: MockerFixture):
    get_snapshot_response_mock = mocker.Mock()
    get_snapshot_response_mock.json.return_value = {
        "xcom_entries": [
            {
                "key": f"{common.SNAPSHOT_PAYLOAD_KEY_PREFIX}__test_name__test_fingerprint"
            },
            {
                "key": f"{common.SNAPSHOT_PAYLOAD_KEY_PREFIX}__test_name_2__test_fingerprint_2"
            },
        ]
    }
    get_snapshot_mock = mocker.patch("requests.Session.get")
    get_snapshot_mock.return_value = get_snapshot_response_mock

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.get_snapshot_ids()

    assert result == [
        SnapshotId(name="test_name", fingerprint="test_fingerprint"),
        SnapshotId(name="test_name_2", fingerprint="test_fingerprint_2"),
    ]

    get_snapshot_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns/init_run/taskInstances/plan_receiver_task/xcomEntries?limit=10000000"
    )


def test_get_snapshot_fingerprints_for_version(mocker: MockerFixture):
    expected_fingerprints = [
        "test_fingerprint_1",
        "test_fingerprint_2",
    ]

    get_snapshots_for_version_response_mock = mocker.Mock()
    get_snapshots_for_version_response_mock.json.return_value = {
        "value": json.dumps(expected_fingerprints)
    }
    get_snapshots_for_version_mock = mocker.patch("requests.Session.get")
    get_snapshots_for_version_mock.return_value = (
        get_snapshots_for_version_response_mock
    )

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.get_snapshot_fingerprints_for_version("test_name", "test_version")

    assert result == expected_fingerprints

    get_snapshots_for_version_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/sqlmesh_plan_receiver_dag/dagRuns/init_run/taskInstances/plan_receiver_task/xcomEntries/snapshot_version_index__test_name__test_version"
    )


def test_get_dag_run_state(mocker: MockerFixture):
    get_dag_run_state_mock = mocker.Mock()
    get_dag_run_state_mock.json.return_value = {"state": "failed"}
    get_snapshot_mock = mocker.patch("requests.Session.get")
    get_snapshot_mock.return_value = get_dag_run_state_mock

    client = AirflowClient(
        airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session()
    )
    result = client.get_dag_run_state("test_dag_id", "test_dag_run_id")

    assert result == "failed"

    get_snapshot_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/test_dag_id/dagRuns/test_dag_run_id"
    )
