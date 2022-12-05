import pytest
from pytest_mock.plugin import MockerFixture
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.context import Context
from sqlmesh.schedulers.airflow.client import AirflowClient


@pytest.fixture(autouse=True)
def wait_for_airflow(airflow_client: AirflowClient):
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(30), reraise=True)
    def get_receiver_dag() -> None:
        airflow_client.get_plan_receiver_dag()

    get_receiver_dag()


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_sushi(mocker: MockerFixture, is_docker: bool):
    mocker.patch("sqlmesh.core.console.TerminalConsole._prompt_backfill")

    airflow_config = "airflow_config_docker" if is_docker else "airflow_config"
    context = Context(path="./example", config=airflow_config)
    plan = context.plan(start="2022-01-01", end="2022-01-01", skip_tests=True)
    context.apply(plan)
