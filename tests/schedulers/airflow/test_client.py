import json
from urllib.parse import urlencode

import pytest
import requests
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import IncrementalByTimeRangeKind, SqlModel
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.client import AirflowClient, _list_to_json


@pytest.fixture
def snapshot() -> Snapshot:
    snapshot = Snapshot.from_model(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column="ds"),
            storage_format="parquet",
            partitioned_by=["a"],
            query=parse_one("SELECT a, ds FROM tbl"),
            pre_statements=[
                parse_one("@DEF(key, 'value')"),
            ],
        ),
        nodes={},
        ttl="in 1 week",
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot.updated_ts = 1665014400000
    snapshot.created_ts = 1665014400000
    return snapshot


def test_apply_plan(mocker: MockerFixture, snapshot: Snapshot):
    apply_plan_response_mock = mocker.Mock()
    apply_plan_response_mock.json.return_value = {"request_id": "test_request_id"}
    apply_plan_response_mock.status_code = 200
    apply_plan_mock = mocker.patch("requests.Session.post")
    apply_plan_mock.return_value = apply_plan_response_mock

    environment = Environment(
        name="test_env",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id="previous_plan_id",
        promoted_snapshot_ids=[snapshot.snapshot_id],
    )

    request_id = "test_request_id"

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    client.apply_plan([snapshot], environment, request_id)

    apply_plan_mock.assert_called_once()
    args, data = apply_plan_mock.call_args_list[0]

    assert args[0] == "http://localhost:8080/sqlmesh/api/v1/plans"
    assert json.loads(data["data"]) == {
        "new_snapshots": [
            {
                "created_ts": 1665014400000,
                "ttl": "in 1 week",
                "fingerprint": snapshot.fingerprint.dict(),
                "indirect_versions": {},
                "intervals": [],
                "dev_intervals": [],
                "node": {
                    "audits": [],
                    "clustered_by": [],
                    "cron": "@daily",
                    "dialect": "",
                    "pre_statements": ["@DEF(key, " "'value')"],
                    "kind": {
                        "name": "INCREMENTAL_BY_TIME_RANGE",
                        "time_column": {"column": "ds"},
                        "forward_only": False,
                        "disable_restatement": False,
                    },
                    "mapping_schema": {},
                    "name": "test_model",
                    "partitioned_by": ["a"],
                    "query": "SELECT a, ds FROM tbl",
                    "storage_format": "parquet",
                    "jinja_macros": {
                        "global_objs": {},
                        "packages": {},
                        "root_macros": {},
                        "top_level_packages": [],
                    },
                    "source_type": "sql",
                    "tags": [],
                    "grain": [],
                    "hash_raw_query": False,
                },
                "audits": [],
                "name": "test_model",
                "parents": [],
                "previous_versions": [],
                "project": "",
                "updated_ts": 1665014400000,
                "version": snapshot.version,
                "change_category": snapshot.change_category,
            }
        ],
        "environment": {
            "name": "test_env",
            "snapshots": [
                {
                    "fingerprint": snapshot.fingerprint.dict(),
                    "name": "test_model",
                    "previous_versions": [],
                    "version": snapshot.version,
                    "physical_schema": "sqlmesh__default",
                    "change_category": snapshot.change_category,
                    "parents": [],
                    "kind_name": "INCREMENTAL_BY_TIME_RANGE",
                }
            ],
            "start_at": "2022-01-01",
            "end_at": "2022-01-01",
            "plan_id": "test_plan_id",
            "previous_plan_id": "previous_plan_id",
            "promoted_snapshot_ids": [
                {
                    "name": "test_model",
                    "identifier": "3192766394",
                }
            ],
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
        "forward_only": False,
    }


def snapshot_url(snapshot_ids, key="ids") -> str:
    return urlencode({key: _list_to_json(snapshot_ids)})


def test_get_snapshots(mocker: MockerFixture, snapshot: Snapshot):
    snapshots = common.SnapshotsResponse(snapshots=[snapshot])

    get_snapshots_response_mock = mocker.Mock()
    get_snapshots_response_mock.status_code = 200
    get_snapshots_response_mock.json.return_value = snapshots.dict()
    get_snapshots_mock = mocker.patch("requests.Session.get")
    get_snapshots_mock.return_value = get_snapshots_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_snapshots([snapshot.snapshot_id])

    assert result == [snapshot]

    get_snapshots_mock.assert_called_once_with(
        f"http://localhost:8080/sqlmesh/api/v1/snapshots?{snapshot_url([snapshot.snapshot_id])}"
    )


def test_snapshots_exist(mocker: MockerFixture, snapshot: Snapshot):
    snapshot_ids = common.SnapshotIdsResponse(snapshot_ids=[snapshot.snapshot_id])

    snapshots_exist_response_mock = mocker.Mock()
    snapshots_exist_response_mock.status_code = 200
    snapshots_exist_response_mock.json.return_value = snapshot_ids.dict()
    snapshots_exist_mock = mocker.patch("requests.Session.get")
    snapshots_exist_mock.return_value = snapshots_exist_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.snapshots_exist([snapshot.snapshot_id])

    assert result == {snapshot.snapshot_id}

    snapshots_exist_mock.assert_called_once_with(
        f"http://localhost:8080/sqlmesh/api/v1/snapshots?check_existence&{snapshot_url([snapshot.snapshot_id])}"
    )


def test_models_exist(mocker: MockerFixture, snapshot: Snapshot):
    model_names = ["model_a", "model_b"]

    models_exist_response_mock = mocker.Mock()
    models_exist_response_mock.status_code = 200
    models_exist_response_mock.json.return_value = common.ExistingModelsResponse(
        names=model_names
    ).dict()
    models_exist_mock = mocker.patch("requests.Session.get")
    models_exist_mock.return_value = models_exist_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.nodes_exist(model_names, exclude_external=True)

    assert result == set(model_names)

    models_exist_mock.assert_called_once_with(
        "http://localhost:8080/sqlmesh/api/v1/models?exclude_external&names=model_a%2Cmodel_b"
    )


def test_get_environment(mocker: MockerFixture, snapshot: Snapshot):
    environment = Environment(
        name="test",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )

    get_environment_response_mock = mocker.Mock()
    get_environment_response_mock.status_code = 200
    get_environment_response_mock.json.return_value = environment.dict()
    get_environment_mock = mocker.patch("requests.Session.get")
    get_environment_mock.return_value = get_environment_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_environment("dev")

    assert result == environment

    get_environment_mock.assert_called_once_with(
        "http://localhost:8080/sqlmesh/api/v1/environments/dev"
    )


def test_get_environments(mocker: MockerFixture, snapshot: Snapshot):
    environment = Environment(
        name="test",
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )
    environments = common.EnvironmentsResponse(environments=[environment])

    get_environments_response_mock = mocker.Mock()
    get_environments_response_mock.status_code = 200
    get_environments_response_mock.json.return_value = environments.dict()
    get_environments_mock = mocker.patch("requests.Session.get")
    get_environments_mock.return_value = get_environments_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_environments()

    assert result == [environment]

    get_environments_mock.assert_called_once_with(
        "http://localhost:8080/sqlmesh/api/v1/environments"
    )


def test_get_dag_run_state(mocker: MockerFixture):
    get_dag_run_state_mock = mocker.Mock()
    get_dag_run_state_mock.status_code = 200
    get_dag_run_state_mock.json.return_value = {"state": "failed"}
    get_snapshot_mock = mocker.patch("requests.Session.get")
    get_snapshot_mock.return_value = get_dag_run_state_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_dag_run_state("test_dag_id", "test_dag_run_id")

    assert result == "failed"

    get_snapshot_mock.assert_called_once_with(
        "http://localhost:8080/api/v1/dags/test_dag_id/dagRuns/test_dag_run_id"
    )


def test_invalidat_environment(mocker: MockerFixture):
    delete_environment_response_mock = mocker.Mock()
    delete_environment_response_mock.status_code = 200
    delete_environment_response_mock.json.return_value = {"name": "test_environment"}
    delete_environment_mock = mocker.patch("requests.Session.delete")
    delete_environment_mock.return_value = delete_environment_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    client.invalidate_environment("test_environment")

    delete_environment_mock.assert_called_once_with(
        "http://localhost:8080/sqlmesh/api/v1/environments/test_environment"
    )
