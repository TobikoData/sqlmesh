import base64
import json

from pytest_mock.plugin import MockerFixture

from sqlmesh.schedulers.airflow.mwaa_client import MWAAClient


def test_get_first_dag_run_id(mocker: MockerFixture):
    list_runs_response_mock = mocker.Mock()
    list_runs_response_mock.json.return_value = {
        "stdout": _encode_output(json.dumps([{"run_id": "test_run_id", "state": "success"}])),
        "stderr": "",
    }
    list_runs_response_mock.status_code = 200
    list_runs_mock = mocker.patch("requests.Session.post")
    list_runs_mock.return_value = list_runs_response_mock

    client = MWAAClient("https://test_airflow_host", "test_token")

    assert client.get_first_dag_run_id("test_dag_id") == "test_run_id"

    list_runs_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="dags list-runs -o json -d test_dag_id",
    )


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

    client = MWAAClient("https://test_airflow_host", "test_token")

    assert client.get_dag_run_state("test_dag_id", "test_run_id_b") == "failed"

    list_runs_mock.assert_called_once_with(
        "https://test_airflow_host/aws_mwaa/cli",
        data="dags list-runs -o json -d test_dag_id",
    )


def _encode_output(out: str) -> str:
    return base64.b64encode(out.encode("utf-8")).decode("utf-8")
