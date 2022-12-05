from datetime import datetime, timezone
from unittest import mock

import pytest
from airflow.utils.context import Context
from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.operators.hwm_signal import HighWaterMarkSignalOperator


@pytest.mark.airflow
def test_new_hwm_is_set_from_context(mocker: MockerFixture):
    target_hwm = datetime(2022, 1, 1)
    task = HighWaterMarkSignalOperator(task_id="test_hwm_task")

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = None

    dag_mock = mocker.Mock()
    dag_mock.dag_id = "test_dag_id"

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = target_hwm

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock, dag=dag_mock)  # type: ignore

    task.execute(context)

    task_instance_mock.xcom_pull.assert_called_once_with(
        key=common.HWM_UTC_XCOM_KEY, session=mock.ANY
    )

    task_instance_mock.xcom_push.assert_called_once_with(
        key=common.HWM_UTC_XCOM_KEY,
        value=target_hwm.replace(tzinfo=timezone.utc).timestamp(),
        session=mock.ANY,
    )


@pytest.mark.airflow
def test_new_hwm_is_set_explicitly(mocker: MockerFixture):
    target_hwm = datetime(2022, 1, 1)
    task = HighWaterMarkSignalOperator(
        task_id="test_hwm_task", target_high_water_mark=target_hwm
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = None

    dag_mock = mocker.Mock()
    dag_mock.dag_id = "test_dag_id"

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = datetime(2022, 1, 2)

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock, dag=dag_mock)  # type: ignore

    task.execute(context)

    task_instance_mock.xcom_pull.assert_called_once_with(
        key=common.HWM_UTC_XCOM_KEY, session=mock.ANY
    )

    task_instance_mock.xcom_push.assert_called_once_with(
        key=common.HWM_UTC_XCOM_KEY,
        value=target_hwm.replace(tzinfo=timezone.utc).timestamp(),
        session=mock.ANY,
    )


@pytest.mark.airflow
def test_new_hwm_is_behind(mocker: MockerFixture):
    target_hwm = datetime(2022, 1, 1)
    expected_hwm = datetime(2022, 1, 2)
    task = HighWaterMarkSignalOperator(
        task_id="test_hwm_task", target_high_water_mark=target_hwm
    )

    task_instance_mock = mocker.Mock()
    task_instance_mock.xcom_pull.return_value = expected_hwm.replace(
        tzinfo=timezone.utc
    ).timestamp()

    dag_mock = mocker.Mock()
    dag_mock.dag_id = "test_dag_id"

    dag_run_mock = mocker.Mock()
    dag_run_mock.execution_date = datetime(2022, 1, 1)

    context = Context(task_instance=task_instance_mock, dag_run=dag_run_mock, dag=dag_mock)  # type: ignore

    task.execute(context)

    task_instance_mock.xcom_pull.assert_called_once_with(
        key=common.HWM_UTC_XCOM_KEY, session=mock.ANY
    )

    assert not task_instance_mock.xcom_push.called
