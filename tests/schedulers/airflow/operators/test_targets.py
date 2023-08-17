import typing as t
from unittest.mock import call

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.environment import Environment
from sqlmesh.core.model import Model, Seed, SeedKind, SeedModel, SqlModel
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.utils.date import to_datetime


@pytest.fixture
def model() -> Model:
    return SqlModel(
        name="test_model",
        query=parse_one("SELECT a, ds FROM tbl"),
    )


@pytest.mark.airflow
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

    add_interval_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.add_interval"
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    parent_snapshots = {snapshot.name: snapshot}

    target = targets.SnapshotEvaluationTarget(
        snapshot=snapshot, parent_snapshots=parent_snapshots, is_dev=False
    )
    target.execute(context, lambda: mocker.Mock(), "spark")

    add_interval_mock.assert_called_once_with(snapshot, interval_ds, interval_ds, is_dev=False)

    evaluator_evaluate_mock.assert_called_once_with(
        snapshot,
        interval_ds,
        interval_ds,
        logical_ds,
        snapshots=parent_snapshots,
        is_dev=False,
    )


@pytest.mark.airflow
def test_evaluation_target_execute_seed_model(mocker: MockerFixture, make_snapshot: t.Callable):
    interval_ds = to_datetime("2022-01-01")
    logical_ds = to_datetime("2022-01-02")

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_start = interval_ds
    dag_run_mock.data_interval_end = interval_ds
    dag_run_mock.logical_date = logical_ds

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

    add_interval_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.add_interval"
    )

    get_snapshots_mock = mocker.patch(
        "sqlmesh.core.state_sync.engine_adapter.EngineAdapterStateSync.get_snapshots"
    )
    get_snapshots_mock.return_value = {snapshot.snapshot_id: snapshot}

    target = targets.SnapshotEvaluationTarget(snapshot=snapshot, parent_snapshots={}, is_dev=False)
    target.execute(context, lambda: mocker.Mock(), "spark")

    add_interval_mock.assert_called_once_with(snapshot, interval_ds, interval_ds, is_dev=False)

    get_snapshots_mock.assert_called_once_with([snapshot], hydrate_seeds=True)

    evaluator_evaluate_mock.assert_called_once_with(
        snapshot,
        interval_ds,
        interval_ds,
        logical_ds,
        snapshots={snapshot.name: snapshot},
        is_dev=False,
    )


@pytest.mark.airflow
def test_cleanup_target_execute(mocker: MockerFixture, make_snapshot: t.Callable, model: Model):
    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    environment = Environment(
        name="test_env", snapshots=[snapshot.table_info], start_at="", plan_id="test_plan_id"
    )

    command = commands.CleanupCommandPayload(
        environments=[environment], snapshots=[snapshot.table_info]
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = command.json()

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
    evaluator_cleanup_mock.assert_called_once_with([snapshot.table_info])

    task_instance_mock.xcom_pull.assert_called_once_with(key="snapshot_cleanup_command")

    delete_xcom_mock.assert_called_once()


@pytest.mark.airflow
def test_cleanup_target_skip_execution(
    mocker: MockerFixture, make_snapshot: t.Callable, model: Model
):
    snapshot = make_snapshot(model)
    snapshot.version = "test_version"

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = commands.CleanupCommandPayload(
        snapshots=[], environments=[]
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
