import json

import pytest
import requests
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import IncrementalByTimeRangeKind, SqlModel
from sqlmesh.core.node import NodeType
from sqlmesh.core.plan import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.date import to_timestamp

pytestmark = pytest.mark.airflow


@pytest.fixture
def snapshot() -> Snapshot:
    snapshot = Snapshot.from_node(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(time_column="ds", dialect="spark"),
            storage_format="parquet",
            partitioned_by=["a"],
            query=parse_one("SELECT a, ds FROM tbl"),
            pre_statements=[
                parse_one("@DEF(key, 'value')"),
            ],
            dialect="spark",
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

    plan = EvaluatablePlan(
        start=environment.start_at,
        end=environment.end_at,
        new_snapshots=[snapshot],
        environment=environment,
        no_gaps=False,
        skip_backfill=False,
        empty_backfill=False,
        restatements={snapshot.name: (to_timestamp("2024-01-01"), to_timestamp("2024-01-02"))},
        is_dev=False,
        allow_destructive_models=set(),
        forward_only=False,
        end_bounded=False,
        ensure_finalized_snapshots=False,
        directly_modified_snapshots=[snapshot.snapshot_id],
        indirectly_modified_snapshots={},
        removed_snapshots=[],
        requires_backfill=True,
        models_to_backfill={'"test_model"'},
        disabled_restatement_models=set(),
    )

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    client.apply_plan(plan)

    apply_plan_mock.assert_called_once()
    args, data = apply_plan_mock.call_args_list[0]

    assert args[0] == "http://localhost:8080/sqlmesh/api/v1/plans"
    assert data["headers"] == {"Content-Type": "application/json"}
    assert json.loads(data["data"]) == {
        "plan": {
            "start": "2022-01-01",
            "end": "2022-01-01",
            "new_snapshots": [
                {
                    "created_ts": 1665014400000,
                    "ttl": "in 1 week",
                    "fingerprint": snapshot.fingerprint.dict(),
                    "intervals": [],
                    "dev_intervals": [],
                    "dev_table_suffix": "dev",
                    "pending_restatement_intervals": [],
                    "node": {
                        "audits": [],
                        "audit_definitions": {},
                        "clustered_by": [],
                        "cron": "@daily",
                        "dialect": "spark",
                        "pre_statements": ["@DEF(key, " "'value')"],
                        "kind": {
                            "name": "INCREMENTAL_BY_TIME_RANGE",
                            "time_column": {"column": "`ds`"},
                            "forward_only": False,
                            "on_destructive_change": "ERROR",
                            "partition_by_time_column": True,
                            "disable_restatement": False,
                            "dialect": "spark",
                        },
                        "mapping_schema": {},
                        "name": "test_model",
                        "partitioned_by": ["`a`"],
                        "query": "SELECT a, ds FROM tbl",
                        "references": [],
                        "project": "",
                        "python_env": {},
                        "storage_format": "parquet",
                        "jinja_macros": {
                            "create_builtins_module": "sqlmesh.utils.jinja",
                            "global_objs": {},
                            "packages": {},
                            "root_macros": {},
                            "top_level_packages": [],
                        },
                        "source_type": "sql",
                        "tags": [],
                        "grains": [],
                        "allow_partials": False,
                        "signals": [],
                        "enabled": True,
                        "extract_dependencies_from_query": True,
                    },
                    "name": '"test_model"',
                    "parents": [],
                    "previous_versions": [],
                    "updated_ts": 1665014400000,
                    "version": snapshot.version,
                    "dev_version": snapshot.dev_version,
                    "change_category": snapshot.change_category,
                    "migrated": False,
                    "unrestorable": False,
                }
            ],
            "environment": {
                "name": "test_env",
                "snapshots": [
                    {
                        "fingerprint": snapshot.fingerprint.dict(),
                        "name": '"test_model"',
                        "node_type": NodeType.MODEL,
                        "previous_versions": [],
                        "version": snapshot.version,
                        "dev_version": snapshot.dev_version,
                        "physical_schema": "sqlmesh__default",
                        "change_category": snapshot.change_category,
                        "parents": [],
                        "kind_name": "INCREMENTAL_BY_TIME_RANGE",
                        "dev_table_suffix": "dev",
                    }
                ],
                "start_at": "2022-01-01",
                "end_at": "2022-01-01",
                "plan_id": "test_plan_id",
                "previous_plan_id": "previous_plan_id",
                "promoted_snapshot_ids": [
                    {
                        "name": '"test_model"',
                        "identifier": snapshot.identifier,
                    }
                ],
                "suffix_target": "schema",
                "normalize_name": True,
                "requirements": {},
            },
            "no_gaps": False,
            "skip_backfill": False,
            "empty_backfill": False,
            "is_dev": False,
            "forward_only": False,
            "allow_destructive_models": [],
            "models_to_backfill": ['"test_model"'],
            "end_bounded": False,
            "ensure_finalized_snapshots": False,
            "directly_modified_snapshots": [
                {"identifier": snapshot.identifier, "name": '"test_model"'}
            ],
            "indirectly_modified_snapshots": {},
            "removed_snapshots": [],
            "restatements": {
                '"test_model"': [to_timestamp("2024-01-01"), to_timestamp("2024-01-02")]
            },
            "requires_backfill": True,
            "disabled_restatement_models": [],
        },
        "notification_targets": [],
        "backfill_concurrent_tasks": 1,
        "ddl_concurrent_tasks": 1,
        "users": [],
    }

    common.PlanApplicationRequest.parse_raw(data["data"])


def test_get_snapshots(mocker: MockerFixture, snapshot: Snapshot):
    snapshots = common.SnapshotsResponse(snapshots=[snapshot])

    get_snapshots_response_mock = mocker.Mock()
    get_snapshots_response_mock.status_code = 200
    get_snapshots_response_mock.json.return_value = snapshots.dict()
    get_snapshots_mock = mocker.patch("requests.Session.post")
    get_snapshots_mock.return_value = get_snapshots_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_snapshots([snapshot.snapshot_id])

    assert result == [snapshot]

    args, data = get_snapshots_mock.call_args_list[0]
    assert args[0] == "http://localhost:8080/sqlmesh/api/v1/snapshots/search"
    assert data["headers"] == {"Content-Type": "application/json"}
    assert json.loads(data["data"]) == {
        "snapshot_ids": [snapshot.snapshot_id.dict()],
        "check_existence": False,
    }


def test_snapshots_exist(mocker: MockerFixture, snapshot: Snapshot):
    snapshot_ids = common.SnapshotIdsResponse(snapshot_ids=[snapshot.snapshot_id])

    snapshots_exist_response_mock = mocker.Mock()
    snapshots_exist_response_mock.status_code = 200
    snapshots_exist_response_mock.json.return_value = snapshot_ids.dict()
    snapshots_exist_mock = mocker.patch("requests.Session.post")
    snapshots_exist_mock.return_value = snapshots_exist_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.snapshots_exist([snapshot.snapshot_id])

    assert result == {snapshot.snapshot_id}

    args, data = snapshots_exist_mock.call_args_list[0]
    assert args[0] == "http://localhost:8080/sqlmesh/api/v1/snapshots/search"
    assert data["headers"] == {"Content-Type": "application/json"}
    assert json.loads(data["data"]) == {
        "snapshot_ids": [snapshot.snapshot_id.dict()],
        "check_existence": True,
    }


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
        suffix_target=EnvironmentSuffixTarget.TABLE,
    )

    get_environment_response_mock = mocker.Mock()
    get_environment_response_mock.status_code = 200
    get_environment_response_mock.json.return_value = environment.dict()
    get_environment_mock = mocker.patch("requests.Session.get")
    get_environment_mock.return_value = get_environment_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.get_environment("dev")

    assert result is not None
    assert result.dict() == environment.dict()

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

    assert len(result) == 1
    assert result[0].dict() == environment.dict()

    get_environments_mock.assert_called_once_with(
        "http://localhost:8080/sqlmesh/api/v1/environments"
    )


@pytest.mark.parametrize("ensure_finalized_snapshots", [True, False])
def test_max_interval_end_per_model(
    mocker: MockerFixture, snapshot: Snapshot, ensure_finalized_snapshots: bool
):
    response = common.IntervalEndResponse(
        environment="test_environment",
        interval_end_per_model={"model_name": to_timestamp("2023-01-01")},
    )

    max_interval_end_response_mock = mocker.Mock()
    max_interval_end_response_mock.status_code = 200
    max_interval_end_response_mock.json.return_value = response.dict()
    max_interval_end_mock = mocker.patch("requests.Session.post")
    max_interval_end_mock.return_value = max_interval_end_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.max_interval_end_per_model(
        "test_environment", {"a.b.c"}, ensure_finalized_snapshots
    )

    assert result == response.interval_end_per_model

    args, data = max_interval_end_mock.call_args_list[0]
    assert (
        args[0]
        == "http://localhost:8080/sqlmesh/api/v1/environments/test_environment/max_interval_end_per_model"
    )
    assert data["headers"] == {"Content-Type": "application/json"}
    assert json.loads(data["data"]) == {
        "models": ["a.b.c"],
        "ensure_finalized_snapshots": ensure_finalized_snapshots,
    }


def test_max_interval_end_per_model_no_models(mocker: MockerFixture, snapshot: Snapshot):
    response = common.IntervalEndResponse(
        environment="test_environment",
        interval_end_per_model={"model_name": to_timestamp("2023-01-01")},
    )

    max_interval_end_response_mock = mocker.Mock()
    max_interval_end_response_mock.status_code = 200
    max_interval_end_response_mock.json.return_value = response.dict()
    max_interval_end_mock = mocker.patch("requests.Session.post")
    max_interval_end_mock.return_value = max_interval_end_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    result = client.max_interval_end_per_model("test_environment", None, False)

    assert result == response.interval_end_per_model

    args, data = max_interval_end_mock.call_args_list[0]
    assert (
        args[0]
        == "http://localhost:8080/sqlmesh/api/v1/environments/test_environment/max_interval_end_per_model"
    )
    assert data["headers"] == {"Content-Type": "application/json"}
    assert json.loads(data["data"]) == {
        "ensure_finalized_snapshots": False,
    }


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


def test_get_variable(mocker: MockerFixture):
    get_variable_response_mock = mocker.Mock()
    get_variable_response_mock.status_code = 200
    get_variable_response_mock.json.return_value = {"value": "test_value", "key": "test_key"}
    get_variable_mock = mocker.patch("requests.Session.get")
    get_variable_mock.return_value = get_variable_response_mock

    client = AirflowClient(airflow_url=common.AIRFLOW_LOCAL_URL, session=requests.Session())
    assert client.get_variable("test_key") == "test_value"

    get_variable_mock.assert_called_once_with("http://localhost:8080/api/v1/variables/test_key")


def test_url_no_trailing_slash(mocker: MockerFixture, snapshot: Snapshot):
    get_variable_response_mock = mocker.Mock()
    get_variable_response_mock.status_code = 200
    get_variable_response_mock.json.return_value = {"value": "test_value", "key": "test_key"}
    get_variable_mock = mocker.patch("requests.Session.get")
    get_variable_mock.return_value = get_variable_response_mock

    client = AirflowClient(airflow_url="http://localhost:8080/prefix", session=requests.Session())
    client.get_variable("test_key")

    get_variable_mock.assert_called_once_with(
        "http://localhost:8080/prefix/api/v1/variables/test_key"
    )
