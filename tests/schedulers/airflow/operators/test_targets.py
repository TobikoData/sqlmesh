import json
import typing as t

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.model import Model
from sqlmesh.schedulers.airflow.operators import targets
from sqlmesh.utils.date import to_datetime


@pytest.fixture
def model() -> Model:
    return Model(
        name="test_model",
        query=parse_one("SELECT a, ds FROM tbl"),
    )


@pytest.mark.airflow
def test_evaluation_target_execute(
    mocker: MockerFixture, make_snapshot: t.Callable, model: Model
):
    interval_ds = to_datetime("2022-01-01")
    logical_ds = to_datetime("2022-01-02")

    dag_run_mock = mocker.Mock()
    dag_run_mock.data_interval_start = interval_ds
    dag_run_mock.data_interval_end = interval_ds
    dag_run_mock.logical_date = logical_ds

    context = Context(dag_run=dag_run_mock)  # type: ignore

    evaluator_evaluate_mock = mocker.patch(
        "sqlmesh.core.snapshot_evaluator.SnapshotEvaluator.evaluate"
    )

    add_interval_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.state_sync.xcom.XComStateSync.add_interval"
    )

    snapshot = make_snapshot(model)
    table_mapping = {"tbl": "another_tbl"}

    target = targets.SnapshotEvaluationTarget(
        snapshot=snapshot, table_mapping=table_mapping
    )
    target.execute(context, lambda: mocker.Mock(), "spark")

    evaluator_evaluate_mock.assert_called_once_with(
        snapshot, interval_ds, interval_ds, logical_ds, mapping=table_mapping
    )

    add_interval_mock.assert_called_once_with(
        snapshot.snapshot_id, interval_ds, interval_ds
    )


@pytest.mark.airflow
def test_table_cleanup_target_execute(
    mocker: MockerFixture, make_snapshot: t.Callable, model: Model
):
    snapshot = make_snapshot(model)
    snapshot.version = "test_version"

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = json.dumps([snapshot.table_info.dict()])

    context = Context(ti=task_instance_mock)  # type: ignore

    evaluator_cleanup_mock = mocker.patch(
        "sqlmesh.core.snapshot_evaluator.SnapshotEvaluator.cleanup"
    )

    delete_xcom_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.operators.targets._delete_xcom"
    )

    target = targets.SnapshotTableCleanupTarget()

    target.execute(context, lambda: mocker.Mock(), "spark")

    evaluator_cleanup_mock.assert_called_once_with([snapshot.table_info])

    task_instance_mock.xcom_pull.assert_called_once_with(
        key="snapshot_table_cleanup_task"
    )

    delete_xcom_mock.assert_called_once()


@pytest.mark.airflow
def test_table_cleanup_target_skip_execution(
    mocker: MockerFixture, make_snapshot: t.Callable, model: Model
):
    snapshot = make_snapshot(model)
    snapshot.version = "test_version"

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = "[]"

    context = Context(ti=task_instance_mock)  # type: ignore

    evaluator_cleanup_mock = mocker.patch(
        "sqlmesh.core.snapshot_evaluator.SnapshotEvaluator.cleanup"
    )

    delete_xcom_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.operators.targets._delete_xcom"
    )

    target = targets.SnapshotTableCleanupTarget()
    with pytest.raises(AirflowSkipException):
        target.execute(context, lambda: mocker.Mock(), "spark")

    evaluator_cleanup_mock.assert_not_called()
    delete_xcom_mock.assert_called_once()
