import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.core.context import Context
from sqlmesh.core.plan import Plan
from sqlmesh.core.plan_evaluator import AirflowPlanEvaluator
from sqlmesh.schedulers.airflow import common as airflow_common
from sqlmesh.utils.errors import SQLMeshError


@pytest.fixture
def sushi_plan(sushi_context: Context, mocker: MockerFixture) -> Plan:
    mock_prompt = mocker.Mock()
    mock_prompt.ask.return_value = "2022-01-01"
    mocker.patch("sqlmesh.core.console.Prompt", mock_prompt)

    return Plan(
        sushi_context._context_diff("dev"),
        dag=sushi_context.dag,
        state_reader=sushi_context.state_reader,
    )


def test_airflow_evaluator(sushi_plan: Plan, mocker: MockerFixture):
    airflow_client_mock = mocker.Mock()
    airflow_client_mock.apply_plan.return_value = "test_plan_receiver_dag_run_id"
    airflow_client_mock.wait_for_dag_run_completion.return_value = True
    airflow_client_mock.wait_for_first_dag_run.return_value = (
        "test_plan_application_dag_run_id"
    )

    evaluator = AirflowPlanEvaluator(airflow_client_mock)
    evaluator.evaluate(sushi_plan)

    airflow_client_mock.apply_plan.assert_called_once_with(
        sushi_plan.new_snapshots,
        sushi_plan.environment,
        mocker.ANY,
        no_gaps=False,
        notification_targets=[],
        restatements=set(),
    )

    assert airflow_client_mock.wait_for_dag_run_completion.call_count == 2

    airflow_client_mock.wait_for_first_dag_run.assert_called_once()


def test_airflow_evaluator_plan_receiver_dag_fails(
    sushi_plan: Plan, mocker: MockerFixture
):
    def wait_for_dag_run_completion(
        dag_id: str, dag_run_id: str, poll_interval_secs: int
    ) -> bool:
        return dag_id != airflow_common.PLAN_RECEIVER_DAG_ID

    airflow_client_mock = mocker.Mock()
    airflow_client_mock.apply_plan.return_value = "test_plan_receiver_dag_run_id"
    airflow_client_mock.wait_for_dag_run_completion.side_effect = (
        wait_for_dag_run_completion
    )

    evaluator = AirflowPlanEvaluator(airflow_client_mock)

    with pytest.raises(SQLMeshError):
        evaluator.evaluate(sushi_plan)

    airflow_client_mock.apply_plan.assert_called_once()
    airflow_client_mock.wait_for_dag_run_completion.assert_called_once()
    airflow_client_mock.wait_for_first_dag_run.assert_not_called()


def test_airflow_evaluator_plan_application_dag_fails(
    sushi_plan: Plan, mocker: MockerFixture
):
    def wait_for_dag_run_completion(
        dag_id: str, dag_run_id: str, poll_interval_secs: int
    ) -> bool:
        return dag_id == airflow_common.PLAN_RECEIVER_DAG_ID

    airflow_client_mock = mocker.Mock()
    airflow_client_mock.apply_plan.return_value = "test_plan_receiver_dag_run_id"
    airflow_client_mock.wait_for_dag_run_completion.side_effect = (
        wait_for_dag_run_completion
    )
    airflow_client_mock.wait_for_first_dag_run.return_value = (
        "test_plan_application_dag_run_id"
    )

    evaluator = AirflowPlanEvaluator(airflow_client_mock)

    with pytest.raises(SQLMeshError):
        evaluator.evaluate(sushi_plan)

    airflow_client_mock.apply_plan.assert_called_once()
    assert airflow_client_mock.wait_for_dag_run_completion.call_count == 2
    airflow_client_mock.wait_for_first_dag_run.assert_called_once()
