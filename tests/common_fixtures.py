import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow.client import AirflowClient


@pytest.fixture(scope="function")
def mock_airflow_client(mocker: MockerFixture) -> AirflowClient:
    return AirflowClient(airflow_url="", session=mocker.Mock())
