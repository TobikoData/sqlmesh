import base64
import json

import pytest
from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient

pytestmark = pytest.mark.airflow


def test_get_first_dag_run_id(mocker: MockerFixture):
    list_runs_response_mock = mocker.Mock()
    list_runs_response_mock.json.return_value = {
        "stdout": _encode_output(json.dumps([{"run_id": "test_run_id", "state": "success"}])),
        "stderr": "",
    }
    list_runs_response_mock.status_code = 200
    list_runs_mock = mocker.patch("requests.Session.post")
    list_runs_mock.return_value = list_runs_response_mock

    url_and_auth_token_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.mwaa_client.url_and_auth_token_for_environment"
    )
    url_and_auth_token_mock.return_value = ("https://test_airflow_host", "test_token")

    client = MWAAClient("test_environment")

    assert client.get_first_dag_run_id("test_dag_id") == "test_run_id"

    list_runs_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="dags list-runs -o json -d test_dag_id",
    )
    url_and_auth_token_mock.assert_called_once_with("test_environment")


def test_get_dag_run_state(mocker: MockerFixture):
    list_runs_response_mock = mocker.Mock()
    list_runs_response_mock.json.return_value = {
        "stdout": _encode_output(
            json.dumps(
                [
                    {"run_id": "test_run_id_a", "state": "success"},
                    {"run_id": "test_run_id_b", "state": "failed"},
                ]
            )
        ),
        "stderr": "",
    }
    list_runs_response_mock.status_code = 200
    list_runs_mock = mocker.patch("requests.Session.post")
    list_runs_mock.return_value = list_runs_response_mock

    url_and_auth_token_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.mwaa_client.url_and_auth_token_for_environment"
    )
    url_and_auth_token_mock.return_value = ("https://test_airflow_host", "test_token")

    client = MWAAClient("test_environment")

    assert client.get_dag_run_state("test_dag_id", "test_run_id_b") == "failed"

    list_runs_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="dags list-runs -o json -d test_dag_id",
    )
    url_and_auth_token_mock.assert_called_once_with("test_environment")


def test_get_variable(mocker: MockerFixture):
    get_variable_response_mock = mocker.Mock()
    get_variable_response_mock.json.return_value = {
        "stdout": _encode_output("test_value"),
        "stderr": "",
    }
    get_variable_response_mock.status_code = 200
    get_variable_mock = mocker.patch("requests.Session.post")
    get_variable_mock.return_value = get_variable_response_mock

    url_and_auth_token_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.mwaa_client.url_and_auth_token_for_environment"
    )
    url_and_auth_token_mock.return_value = ("https://test_airflow_host", "test_token")

    client = MWAAClient("test_environment")

    assert client.get_variable("test_key") == "test_value"

    get_variable_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="variables get test_key",
    )
    url_and_auth_token_mock.assert_called_once_with("test_environment")


def test_get_variable_not_found(mocker: MockerFixture):
    get_variable_response_mock = mocker.Mock()
    get_variable_response_mock.json.return_value = {
        "stdout": "",
        "stderr": _encode_output("Variable test_key does not exist"),
    }
    get_variable_response_mock.status_code = 200
    get_variable_mock = mocker.patch("requests.Session.post")
    get_variable_mock.return_value = get_variable_response_mock

    url_and_auth_token_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.mwaa_client.url_and_auth_token_for_environment"
    )
    url_and_auth_token_mock.return_value = ("https://test_airflow_host", "test_token")

    client = MWAAClient("test_environment")

    assert client.get_variable("test_key") is None

    get_variable_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="variables get test_key",
    )


def test_token_refresh(mocker: MockerFixture):
    list_runs_response_mock = mocker.Mock()
    list_runs_response_mock.json.return_value = {
        "stdout": _encode_output(json.dumps([{"run_id": "test_run_id", "state": "success"}])),
        "stderr": "",
    }
    list_runs_response_mock.status_code = 200
    list_runs_mock = mocker.patch("requests.Session.post")
    list_runs_mock.return_value = list_runs_response_mock

    url_and_auth_token_mock = mocker.patch(
        "sqlmesh.schedulers.airflow.mwaa_client.url_and_auth_token_for_environment"
    )
    url_and_auth_token_mock.return_value = ("https://test_airflow_host", "test_token")

    now_mock = mocker.patch("sqlmesh.schedulers.airflow.mwaa_client.now_timestamp")
    now_mock.return_value = 0

    client = MWAAClient("test_environment")
    client.get_first_dag_run_id("test_dag_id")

    now_mock.return_value = 15000  # 15 seconds later
    client.get_first_dag_run_id("test_dag_id")

    now_mock.return_value = 31000  # 31 seconds later
    client.get_first_dag_run_id("test_dag_id")

    now_mock.return_value = 45000  # 45 seconds later
    client.get_first_dag_run_id("test_dag_id")

    now_mock.return_value = 63000  # 63 seconds later
    client.get_first_dag_run_id("test_dag_id")

    assert url_and_auth_token_mock.call_count == 3


def _encode_output(out: str) -> str:
    return base64.b64encode(out.encode("utf-8")).decode("utf-8")
