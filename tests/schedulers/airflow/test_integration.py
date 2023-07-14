import typing as t
from datetime import timedelta

import pytest
from sqlglot import parse_one
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core import constants as c
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import IncrementalByTimeRangeKind, Model, SqlModel
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils import random_id
from sqlmesh.utils.date import yesterday
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
    snapshot = make_snapshot(_create_model(model_name))
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    environment_name = _random_environment_name()
    environment = _create_environment(snapshot, name=environment_name)
    environment.start_at = yesterday() - timedelta(days=1)
    environment.end_at = None

    assert airflow_client.get_environment(environment_name) is None

    _apply_plan_and_block(airflow_client, [snapshot], environment, is_dev=False)

    assert airflow_client.get_environment(environment_name).snapshots == [  # type: ignore
        snapshot.table_info
    ]

    # Verify that the incremental DAG for the Snapshot has been created.
    dag = _get_snapshot_dag(airflow_client, model_name, snapshot.version)
    assert dag["is_active"]

    # Make sure that the same Snapshot can't be added again.
    with pytest.raises(SQLMeshError, match=r"Snapshots.*already exist.*"):
        airflow_client.apply_plan([snapshot], environment, random_name())

    # Verify full environment demotion.
    environment.snapshots = []
    environment.previous_plan_id = environment.plan_id
    environment.plan_id = "new_plan_id"
    _apply_plan_and_block(airflow_client, [], environment)
    assert not airflow_client.get_environment(environment_name).snapshots  # type: ignore


def _apply_plan_and_block(
    airflow_client: AirflowClient,
    new_snapshots: t.List[Snapshot],
    environment: Environment,
    is_dev: t.Optional[bool] = None,
) -> None:
    if is_dev is None:
        is_dev = environment.name != c.PROD

    plan_request_id = random_id()
    airflow_client.apply_plan(new_snapshots, environment, plan_request_id, is_dev=is_dev)

    plan_application_dag_id = common.plan_application_dag_id(environment.name, plan_request_id)
    plan_application_dag_run_id = airflow_client.wait_for_first_dag_run(
        plan_application_dag_id, DAG_CREATION_WAIT_INTERVAL, DAG_CREATION_RETRY_ATTEMPTS
    )
    assert airflow_client.wait_for_dag_run_completion(
        plan_application_dag_id, plan_application_dag_run_id, DAG_RUN_POLL_INTERVAL
    )


@retry(wait=wait_fixed(3), stop=stop_after_attempt(5), reraise=True)
def _get_snapshot_dag(
    airflow_client: AirflowClient, model_name: str, version: str
) -> t.Dict[str, t.Any]:
    return airflow_client.get_snapshot_dag(model_name, version)


def _create_model(name: str) -> Model:
    return SqlModel(
        name=name,
        kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=30),
        description="Dummy table",
        owner="jen",
        cron="@daily",
        start="2020-01-01",
        partitioned_by=["ds"],
        query=parse_one("SELECT '2022-01-01'::TEXT AS ds, 1::INT AS one"),
    )


def _create_environment(snapshot: Snapshot, name: t.Optional[str] = None) -> Environment:
    return Environment(
        name=name or _random_environment_name(),
        snapshots=[snapshot.table_info],
        start_at="2022-01-01",
        end_at="2022-01-01",
        plan_id="test_plan_id",
        previous_plan_id=None,
    )


def _random_environment_name() -> str:
    return f"test_environment_{random_id()[-8:]}"
