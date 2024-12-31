from datetime import timedelta

import pytest
from pytest_mock.plugin import MockerFixture
from tenacity import retry, stop_after_attempt, wait_fixed

from sqlmesh.core.context import Context
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.date import now, to_date
from tests.conftest import SushiDataValidator

pytestmark = [
    pytest.mark.airflow,
    pytest.mark.docker,
]


@pytest.fixture(autouse=True)
def wait_for_airflow(airflow_client: AirflowClient):
    @retry(wait=wait_fixed(2), stop=stop_after_attempt(15), reraise=True)
    def get_receiver_dag() -> None:
        airflow_client.get_janitor_dag()

    get_receiver_dag()


def test_sushi(mocker: MockerFixture, is_docker: bool):
    end = now()
    start = to_date(end - timedelta(days=7))
    yesterday = to_date(end - timedelta(days=1))

    airflow_config = "airflow_config_docker" if is_docker else "airflow_config"
    context = Context(paths="./examples/sushi", config=airflow_config)
    assert context.default_catalog == "spark_catalog"
    for fqn in context.models:
        assert fqn.startswith('"spark_catalog"."')
    data_validator = SushiDataValidator.from_context(context)

    context.plan(
        environment="test_dev",
        start=start,
        end=end,
        skip_tests=True,
        no_prompts=True,
        auto_apply=True,
    )

    data_validator.validate(
        "sushi.customer_revenue_lifetime", start, yesterday, env_name="test_dev"
    )

    # Ensure that the plan has been applied successfully.
    no_change_plan = context.plan_builder(
        environment="test_dev_two",
        start=start,
        end=end,
        skip_tests=True,
    ).build()
    assert not no_change_plan.requires_backfill
