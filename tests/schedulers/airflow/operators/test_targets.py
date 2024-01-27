import typing as t
from unittest.mock import call

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, Seed, SeedKind, SeedModel, SqlModel
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    SnapshotChangeCategory,
    SnapshotTableCleanupTask,
)
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.utils.date import to_datetime

pytestmark = pytest.mark.airflow


@pytest.fixture
def model() -> Model:
    return SqlModel(
        name="test_model",
        query=parse_one("SELECT a, ds FROM tbl"),
    )


def test_evaluation_target_execute(mocker: MockerFixture, make_snapshot: t.Callable, model: Model):
    interval_ds = to_datetime("2022-01-01")
    logical_ds = to_datetime("2022-01-02")

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_start = interval_ds
    dag_run_mock.data_interval_end = interval_ds
    dag_run_mock.logical_date = logical_ds

    context = Context(dag_run=dag_run_mock)  # type: ignore

    evaluator_evaluate_mock = mocker.patch(
        "sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.evaluate"
    )
    evaluator_evaluate_mock.return_value = None

    add_interval_mock = mocker.patch("sqlmesh.core.state_sync.cache.CachingStateSync.add_interval")

    variable_get_mock = mocker.patch("sqlmesh.schedulers.airflow.operators.targets.Variable.get")

    variable_get_mock.return_value = "default_catalog"

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    parent_snapshots = {snapshot.name: snapshot}

    deployability_index = DeployabilityIndex.all_deployable()

    target = targets.SnapshotEvaluationTarget(
        snapshot=snapshot,
        parent_snapshots=parent_snapshots,
        deployability_index=deployability_index,
    )
    target.execute(context, lambda: mocker.Mock(), "spark")

    add_interval_mock.assert_called_once_with(snapshot, interval_ds, interval_ds, is_dev=False)

    evaluator_evaluate_mock.assert_called_once_with(
        snapshot,
        start=interval_ds,
        end=interval_ds,
        execution_time=logical_ds,
        snapshots=parent_snapshots,
        deployability_index=deployability_index,
    )


def test_evaluation_target_execute_seed_model(mocker: MockerFixture, make_snapshot: t.Callable):
    interval_ds = to_datetime("2022-01-01")
    logical_ds = to_datetime("2022-01-02")

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_start = interval_ds
    dag_run_mock.data_interval_end = interval_ds
    dag_run_mock.logical_date = logical_ds

    variable_get_mock = mocker.patch("sqlmesh.schedulers.airflow.operators.targets.Variable.get")

    variable_get_mock.return_value = "default_catalog"

    context = Context(dag_run=dag_run_mock)  # type: ignore

    snapshot = make_snapshot(
        SeedModel(
            name="a",
            kind=SeedKind(path="./path/to/seed"),
            seed=Seed(content="content"),
            column_hashes={"col": "hash1"},
            depends_on=set(),
        ).to_dehydrated()
    )
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator_evaluate_mock = mocker.patch(
        "sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.evaluate"
    )
    evaluator_evaluate_mock.return_value = None

    add_interval_mock = mocker.patch("sqlmesh.core.state_sync.cache.CachingStateSync.add_interval")

    get_snapshots_mock = mocker.patch(
        "sqlmesh.core.state_sync.cache.CachingStateSync.get_snapshots"
    )
    get_snapshots_mock.return_value = {snapshot.snapshot_id: snapshot}

    deployability_index = DeployabilityIndex.all_deployable()

    target = targets.SnapshotEvaluationTarget(
        snapshot=snapshot, parent_snapshots={}, deployability_index=deployability_index
    )
    target.execute(context, lambda: mocker.Mock(), "spark")

    add_interval_mock.assert_called_once_with(snapshot, interval_ds, interval_ds, is_dev=False)

    get_snapshots_mock.assert_called_once_with([snapshot], hydrate_seeds=True)

    evaluator_evaluate_mock.assert_called_once_with(
        snapshot,
        start=interval_ds,
        end=interval_ds,
        execution_time=logical_ds,
        snapshots={snapshot.name: snapshot},
        deployability_index=deployability_index,
    )


def test_cleanup_target_execute(mocker: MockerFixture, make_snapshot: t.Callable, model: Model):
    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    environment = Environment(
        name="test_env", snapshots=[snapshot.table_info], start_at="", plan_id="test_plan_id"
    )

    cleanup_task = SnapshotTableCleanupTask(snapshot=snapshot.table_info, dev_table_only=False)

    command = commands.CleanupCommandPayload(
        environments=[environment],
        tasks=[cleanup_task],
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = command.json()

    variable_get_mock = mocker.patch("sqlmesh.schedulers.airflow.operators.targets.Variable.get")

    variable_get_mock.return_value = "default_catalog"

    context = Context(ti=task_instance_mock)  # type: ignore

    evaluator_cleanup_mock = mocker.patch(
        "sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.cleanup"
    )

    delete_xcom_mock = mocker.patch("sqlmesh.schedulers.airflow.operators.targets._delete_xcom")

    target = targets.SnapshotCleanupTarget()

    evaluator_adapter_mock = mocker.MagicMock()
    target.execute(context, lambda: evaluator_adapter_mock, "spark")

    evaluator_adapter_mock.cursor().execute.assert_has_calls(
        [call("DROP SCHEMA IF EXISTS `default__test_env` CASCADE")]
    )
    evaluator_cleanup_mock.assert_called_once_with([cleanup_task])

    task_instance_mock.xcom_pull.assert_called_once_with(key="snapshot_cleanup_command")

    delete_xcom_mock.assert_called_once()


def test_cleanup_target_skip_execution(
    mocker: MockerFixture, make_snapshot: t.Callable, model: Model
):
    snapshot = make_snapshot(model)
    snapshot.version = "test_version"

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = commands.CleanupCommandPayload(
        tasks=[], environments=[]
    ).json()

    context = Context(ti=task_instance_mock)  # type: ignore

    evaluator_demote_mock = mocker.patch("sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.demote")
    evaluator_cleanup_mock = mocker.patch(
        "sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.cleanup"
    )

    delete_xcom_mock = mocker.patch("sqlmesh.schedulers.airflow.operators.targets._delete_xcom")

    target = targets.SnapshotCleanupTarget()
    with pytest.raises(AirflowSkipException):
        target.execute(context, lambda: mocker.Mock(), "spark")

    evaluator_demote_mock.assert_not_called()
    evaluator_cleanup_mock.assert_not_called()
    delete_xcom_mock.assert_called_once()
