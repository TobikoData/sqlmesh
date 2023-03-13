import pytest
from pytest_mock.plugin import MockerFixture
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.context import Context
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.date import yesterday_ds


@pytest.fixture(autouse=True)
def wait_for_airflow(airflow_client: AirflowClient):
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(15), reraise=True)
    def get_receiver_dag() -> None:
        airflow_client.get_janitor_dag()

    get_receiver_dag()


@pytest.mark.integration
@pytest.mark.airflow_integration
def test_sushi(mocker: MockerFixture, is_docker: bool):
    start = yesterday_ds()

    airflow_config = "airflow_config_docker" if is_docker else "airflow_config"
    context = Context(path="./examples/sushi", config=airflow_config)

    context.plan(
        environment="test_dev",
        start=start,
        skip_tests=True,
        no_prompts=True,
        auto_apply=True,
    )

    # Ensure that the plan has been applied successfully.
    no_change_plan = context.plan(
        environment="test_dev_two",
        start=start,
        skip_tests=True,
        no_prompts=True,
    )
    assert not no_change_plan.requires_backfill
