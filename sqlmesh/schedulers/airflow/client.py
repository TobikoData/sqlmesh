import json
import time
import typing as t
import uuid
from datetime import datetime
from urllib.parse import urlencode, urljoin

import requests
from requests.exceptions import HTTPError

from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.date import now
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

DAG_RUN_PATH_TEMPLATE = "api/v1/dags/{}/dagRuns"
PLAN_RECEIVER_DAG_RUN_PATH = DAG_RUN_PATH_TEMPLATE.format(common.PLAN_RECEIVER_DAG_ID)
STATE_XCOM_PATH: str = f"{DAG_RUN_PATH_TEMPLATE.format(common.SQLMESH_XCOM_DAG_ID)}/{common.INIT_RUN_ID}/taskInstances/{common.SQLMESH_XCOM_TASK_ID}/xcomEntries"


DagConf = common.PlanReceiverDagConf


class DagRunRequest(PydanticModel):
    dag_run_id: str
    logical_date: str
    conf: DagConf


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
    ) -> str:
        is_first_run = self._get_first_dag_run_id(common.PLAN_RECEIVER_DAG_ID) is None
        return self._trigger_dag_run(
            PLAN_RECEIVER_DAG_RUN_PATH,
            common.PlanReceiverDagConf(
                new_snapshots=list(new_snapshots),
                environment=environment,
                no_gaps=no_gaps,
                skip_backfill=skip_backfill,
                request_id=request_id,
                restatements=set(restatements or []),
                notification_targets=notification_targets or [],
                backfill_concurrent_tasks=backfill_concurrent_tasks,
                ddl_concurrent_tasks=ddl_concurrent_tasks,
            ),
            dag_run_id=common.INIT_RUN_ID if is_first_run else None,
            timestamp=timestamp,
        )

    def get_snapshot_ids(self) -> t.List[SnapshotId]:
        xcom_entries_response = self._get(f"{STATE_XCOM_PATH}?limit=10000000")
        result = []
        for xcom_entry in xcom_entries_response["xcom_entries"]:
            xcom_key = xcom_entry["key"]
            if xcom_key.startswith(common.SNAPSHOT_PAYLOAD_KEY_PREFIX):
                name_fingerprint = xcom_key.replace(
                    f"{common.SNAPSHOT_PAYLOAD_KEY_PREFIX}__", ""
                )
                sep_index = name_fingerprint.rindex("__")
                name = name_fingerprint[:sep_index]
                fingerprint = name_fingerprint[sep_index + 2 :]
                result.append(SnapshotId(name=name, fingerprint=fingerprint))
        return result

    def get_snapshot_fingerprints_for_version(
        self, name: str, version: t.Optional[str] = None
    ) -> t.List[str]:
        if not version:
            raise SQLMeshError("Version cannot be empty")
        xcom_key = common.snapshot_version_xcom_key(name, version)
        try:
            xcom_entry = self._get(f"{STATE_XCOM_PATH}/{xcom_key}")
            return json.loads(xcom_entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return []

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        xcom_key = common.environment_xcom_key(environment)
        try:
            xcom_entry = self._get(f"{STATE_XCOM_PATH}/{xcom_key}")
            return Environment.parse_raw(xcom_entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return None

    def get_environments(self) -> t.List[Environment]:
        raise NotImplementedError

    def get_snapshot(self, name: str, fingerprint: str) -> t.Optional[Snapshot]:
        xcom_key = common.snapshot_xcom_key_from_name_fingerprint(name, fingerprint)
        try:
            xcom_entry = self._get(f"{STATE_XCOM_PATH}/{xcom_key}")
            return Snapshot.parse_raw(xcom_entry["value"])
        except HTTPError as e:
            if not _is_not_found(e):
                raise
            return None

    def get_dag_run_state(self, dag_id: str, dag_run_id: str) -> str:
        url = f"{DAG_RUN_PATH_TEMPLATE.format(dag_id)}/{dag_run_id}"
        return self._get(url)["state"].lower()

    def get_plan_receiver_dag(self) -> t.Dict[str, t.Any]:
        return self._get_dag(common.PLAN_RECEIVER_DAG_ID)

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

    def _trigger_dag_run(
        self,
        dag_run_path: str,
        dag_conf: DagConf,
        dag_run_id: t.Optional[str] = None,
        timestamp: t.Optional[datetime] = None,
    ) -> str:
        logical_date = _logical_date(timestamp)
        request = DagRunRequest(
            dag_run_id=dag_run_id or logical_date,
            logical_date=logical_date,
            conf=dag_conf,
        )

        response = self._session.post(
            urljoin(self._airflow_url, dag_run_path),
            data=request.json(),
        )
        response.raise_for_status()

        return response.json()["dag_run_id"]

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


def _logical_date(timestamp: t.Optional[datetime]) -> str:
    if not timestamp:
        timestamp = now()
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _is_not_found(error: HTTPError) -> bool:
    return error.response.status_code == 404
