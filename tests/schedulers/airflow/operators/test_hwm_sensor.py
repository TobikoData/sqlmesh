from datetime import datetime, timezone

import pytest
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow.operators.hwm_sensor import HighWaterMarkSensor


@pytest.mark.airflow
def test_no_current_hwm(mocker: MockerFixture):
    task = HighWaterMarkSensor(
        target_dag_id="test_dag_id",
        target_cron="@daily",
        this_cron="@daily",
        task_id="test_hwm_task",
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = None

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = datetime(2022, 1, 1)

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock)  # type: ignore
    assert not task.poke(context)


@pytest.mark.airflow
def test_current_hwm_below_target(mocker: MockerFixture):
    task = HighWaterMarkSensor(
        target_dag_id="test_dag_id",
        target_cron="@daily",
        this_cron="@daily",
        task_id="test_hwm_task",
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = datetime(
        2022, 1, 1, tzinfo=timezone.utc
    ).timestamp()

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = datetime(2022, 1, 2)

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock)  # type: ignore

    assert not task.poke(context)


@pytest.mark.airflow
def test_current_hwm_above_target(mocker: MockerFixture):
    task = HighWaterMarkSensor(
        target_dag_id="test_dag_id",
        target_cron="@daily",
        this_cron="@daily",
        task_id="test_hwm_task",
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = datetime(
        2022, 1, 2, tzinfo=timezone.utc
    ).timestamp()

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = datetime(2022, 1, 1)

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock)  # type: ignore

    assert task.poke(context)
