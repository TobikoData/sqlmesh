import json
import time
import typing as t
import uuid
from datetime import datetime
from urllib.parse import urlencode, urljoin

import requests
from requests.exceptions import HTTPError
from requests.models import Response

from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError

DAG_RUN_PATH_TEMPLATE = "api/v1/dags/{}/dagRuns"
VARIABLES_PATH: str = "api/v1/variables"


SQLMESH_BASE_PATH = "sqlmesh/api/v1"
PLANS_PATH = f"{SQLMESH_BASE_PATH}/plans"


class AirflowClient:
    def __init__(
        self,
        session: requests.Session,
        airflow_url: str,
        console: t.Optional[Console] = None,
    ):
        self._session = session
        self._airflow_url = airflow_url
        self._console = console

    def apply_plan(
        self,
        new_snapshots: t.Iterable[Snapshot],
        environment: Environment,
        request_id: str,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        restatements: t.Optional[t.Iterable[str]] = None,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        backfill_concurrent_tasks: int = 1,
        ddl_concurrent_tasks: int = 1,
        timestamp: t.Optional[datetime] = None,
        users: t.Optional[t.List[User]] = None,
        is_dev: bool = False,
    ) -> None:
        request = common.PlanApplicationRequest(
            new_snapshots=list(new_snapshots),
            environment=environment,
            no_gaps=no_gaps,
            skip_backfill=skip_backfill,
            request_id=request_id,
            restatements=set(restatements or []),
            notification_targets=notification_targets or [],
            backfill_concurrent_tasks=backfill_concurrent_tasks,
            ddl_concurrent_tasks=ddl_concurrent_tasks,
            users=users or [],
            is_dev=is_dev,
        )

        response = self._session.post(
            urljoin(self._airflow_url, PLANS_PATH),
            data=request.json(),
        )
        self._raise_for_status(response)

    def get_snapshot_ids(self) -> t.List[SnapshotId]:
        response = self._get(f"{VARIABLES_PATH}?limit=10000000")
        result = []
        for entry in response["variables"]:
            key = entry["key"]
            if key.startswith(common.SNAPSHOT_PAYLOAD_KEY_PREFIX):
                name_identifier = key.replace(
                    f"{common.SNAPSHOT_PAYLOAD_KEY_PREFIX}__", ""
                )
                sep_index = name_identifier.rindex("__")
                name = name_identifier[:sep_index]
                identifier = name_identifier[sep_index + 2 :]
                result.append(SnapshotId(name=name, identifier=identifier))
        return result

    def get_snapshot_identifiers_for_version(
        self, name: str, version: t.Optional[str] = None
    ) -> t.List[str]:
        if not version:
            raise SQLMeshError("Version cannot be empty")
        key = common.snapshot_version_key(name, version)
        try:
            entry = self._get(f"{VARIABLES_PATH}/{key}")
            return json.loads(entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return []

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        key = common.environment_key(environment)
        try:
            entry = self._get(f"{VARIABLES_PATH}/{key}")
            return Environment.parse_raw(entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return None

    def get_snapshot(self, name: str, identifier: str) -> t.Optional[Snapshot]:
        key = common.snapshot_key_from_name_identifier(name, identifier)
        try:
            entry = self._get(f"{VARIABLES_PATH}/{key}")
            return Snapshot.parse_raw(entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return None

    def get_dag_run_state(self, dag_id: str, dag_run_id: str) -> str:
        url = f"{DAG_RUN_PATH_TEMPLATE.format(dag_id)}/{dag_run_id}"
        return self._get(url)["state"].lower()

    def get_janitor_dag(self) -> t.Dict[str, t.Any]:
        return self._get_dag(common.JANITOR_DAG_ID)

    def get_snapshot_dag(self, name: str, version: str) -> t.Dict[str, t.Any]:
        return self._get_dag(common.dag_id_for_name_version(name, version))

    def get_all_dags(self) -> t.Dict[str, t.Any]:
        return self._get("api/v1/dags")

    def wait_for_dag_run_completion(
        self, dag_id: str, dag_run_id: str, poll_interval_secs: int
    ) -> bool:
        loading_id = self._console_loading_start()

        while True:
            state = self.get_dag_run_state(dag_id, dag_run_id)
            if state in ("failed", "success"):
                if self._console and loading_id:
                    self._console.loading_stop(loading_id)
                return state == "success"

            time.sleep(poll_interval_secs)

    def wait_for_first_dag_run(
        self, dag_id: str, poll_interval_secs: int, max_retries: int
    ) -> str:
        loading_id = self._console_loading_start()

        attempt_num = 1

        try:
            while True:
                try:
                    first_dag_run_id = self._get_first_dag_run_id(dag_id)
                    if first_dag_run_id is None:
                        raise SQLMeshError(f"Missing a DAG Run for DAG '{dag_id}'")
                    return first_dag_run_id
                except HTTPError as e:
                    if not _is_not_found(e) or attempt_num > max_retries:
                        raise
                except SQLMeshError:
                    if attempt_num > max_retries:
                        raise

                attempt_num += 1
                time.sleep(poll_interval_secs)
        finally:
            if self._console and loading_id:
                self._console.loading_stop(loading_id)

    def print_tracking_url(self, dag_id: str, dag_run_id: str, op_name: str) -> None:
        if not self._console:
            return

        tracking_url = self.dag_run_tracking_url(dag_id, dag_run_id)
        # TODO: Figure out generalized solution for links
        self._console.log_status_update(
            f"Track [green]{op_name}[/green] progress using [link={tracking_url}]link[/link]"
        )

    def dag_run_tracking_url(self, dag_id: str, dag_run_id: str) -> str:
        url_params = urlencode(
            dict(
                dag_id=dag_id,
                run_id=dag_run_id,
            )
        )
        return urljoin(self._airflow_url, f"dagrun_details?{url_params}")

    def close(self) -> None:
        self._session.close()

    def _get_first_dag_run_id(self, dag_id: str) -> t.Optional[str]:
        dag_runs_response = self._get(f"{DAG_RUN_PATH_TEMPLATE.format(dag_id)}?limit=1")
        dag_runs = dag_runs_response["dag_runs"]
        if not dag_runs:
            return None
        return dag_runs[0]["dag_run_id"]

    def _get_dag(self, dag_id: str) -> t.Dict[str, t.Any]:
        return self._get(f"api/v1/dags/{dag_id}")

    def _get(self, path: str) -> t.Dict[str, t.Any]:
        response = self._session.get(urljoin(self._airflow_url, path))
        response.raise_for_status()
        return response.json()

    def _console_loading_start(self) -> t.Optional[uuid.UUID]:
        if self._console:
            return self._console.loading_start()
        return None

    def _raise_for_status(self, response: Response) -> None:
        if 400 <= response.status_code < 500:
            raise SQLMeshError(response.json()["message"])
        elif 500 <= response.status_code < 600:
            raise SQLMeshError(f"Unexpected server error ({response.status_code}).")


def _logical_date(timestamp: t.Optional[datetime]) -> str:
    if not timestamp:
        timestamp = now()
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _is_not_found(error: HTTPError) -> bool:
    return error.response.status_code == 404
