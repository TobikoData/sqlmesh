from __future__ import annotations

import base64
import json
import typing as t
from urllib.parse import urljoin

from requests import Session

from sqlmesh.core.console import Console
from sqlmesh.schedulers.airflow.client import BaseAirflowClient, raise_for_status
from sqlmesh.utils.errors import NotFoundError


class MWAAClient(BaseAirflowClient):
    def __init__(self, airflow_url: str, auth_token: str, console: t.Optional[Console] = None):
        super().__init__(airflow_url, console)

        self._session = Session()
        self._session.headers.update(
            {"Authorization": f"Bearer {auth_token}", "Content-Type": "text/plain"}
        )

    def get_first_dag_run_id(self, dag_id: str) -> t.Optional[str]:
        dag_runs = self._list_dag_runs(dag_id)
        if dag_runs:
            return dag_runs[-1]["run_id"]
        return None

    def get_dag_run_state(self, dag_id: str, dag_run_id: str) -> str:
        dag_runs = self._list_dag_runs(dag_id) or []
        for dag_run in dag_runs:
            if dag_run["run_id"] == dag_run_id:
                return dag_run["state"].lower()
        raise NotFoundError(f"DAG run '{dag_run_id}' was not found for DAG '{dag_id}'")

    def _list_dag_runs(self, dag_id: str) -> t.Optional[t.List[t.Dict[str, t.Any]]]:
        stdout, stderr = self._post(f"dags list-runs -o json -d {dag_id}")
        if stdout:
            return json.loads(stdout)
        return None

    def _post(self, data: str) -> t.Tuple[str, str]:
        response = self._session.post(urljoin(self._airflow_url, "aws_mwaa/cli"), data=data)
        raise_for_status(response)
        response_body = response.json()

        cli_stdout = base64.b64decode(response_body["stdout"]).decode("utf8").strip()
        cli_stderr = base64.b64decode(response_body["stderr"]).decode("utf8").strip()
        return cli_stdout, cli_stderr


def url_and_auth_token_for_environment(environment_name: str) -> t.Tuple[str, str]:
    import boto3

    client = boto3.client("mwaa")
    cli_token = client.create_cli_token(Name=environment_name)

    url = f"https://{cli_token['WebServerHostname']}/"
    auth_token = cli_token["CliToken"]
    return url, auth_token
