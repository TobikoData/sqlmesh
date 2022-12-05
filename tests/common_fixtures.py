import pickle

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.file_cache import FileCache


@pytest.fixture(scope="function", autouse=True)
def mock_file_cache(mocker: MockerFixture) -> None:
    mocker.patch("sqlmesh.utils.file_cache.FileCache._load")
    mocker.patch(
        "sqlmesh.utils.file_cache.open", mocker.mock_open(read_data=pickle.dumps({}))
    )
    return mocker.create_autospec(FileCache)


@pytest.fixture(scope="function")
def mock_airflow_client(mocker: MockerFixture) -> AirflowClient:
    return AirflowClient(airflow_url="", session=mocker.Mock())
