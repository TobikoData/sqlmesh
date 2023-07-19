import json
import time
import typing as t
import uuid
from urllib.parse import urlencode, urljoin

import requests
from requests.models import Response

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.core.state_sync import Versions
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common
from sqlmesh.utils.errors import (
    ApiClientError,
    ApiServerError,
    NotFoundError,
    SQLMeshError,
)
from sqlmesh.utils.pydantic import PydanticModel

DAG_RUN_PATH_TEMPLATE = "api/v1/dags/{}/dagRuns"


PLANS_PATH = f"{common.SQLMESH_API_BASE_PATH}/plans"
ENVIRONMENTS_PATH = f"{common.SQLMESH_API_BASE_PATH}/environments"
SNAPSHOTS_PATH = f"{common.SQLMESH_API_BASE_PATH}/snapshots"
SEEDS_PATH = f"{common.SQLMESH_API_BASE_PATH}/seeds"
INTERVALS_PATH = f"{common.SQLMESH_API_BASE_PATH}/intervals"
MODELS_PATH = f"{common.SQLMESH_API_BASE_PATH}/models"
VERSIONS_PATH = f"{common.SQLMESH_API_BASE_PATH}/versions"


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
        users: t.Optional[t.List[User]] = None,
        is_dev: bool = False,
        forward_only: bool = False,
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
            forward_only=forward_only,
        )

        response = self._session.post(
            urljoin(self._airflow_url, PLANS_PATH),
            data=request.json(),
        )
        self._raise_for_status(response)

    def get_snapshots(
        self, snapshot_ids: t.Optional[t.List[SnapshotId]], hydrate_seeds: bool = False
    ) -> t.List[Snapshot]:
        params: t.Dict[str, str] = {}
        if snapshot_ids is not None:
            params["ids"] = _list_to_json(snapshot_ids)

        flags = ["hydrate_seeds"] if hydrate_seeds else []

        return common.SnapshotsResponse.parse_obj(
            self._get(SNAPSHOTS_PATH, *flags, **params)
        ).snapshots

    def snapshots_exist(self, snapshot_ids: t.List[SnapshotId]) -> t.Set[SnapshotId]:
        return set(
            common.SnapshotIdsResponse.parse_obj(
                self._get(SNAPSHOTS_PATH, "check_existence", ids=_list_to_json(snapshot_ids))
            ).snapshot_ids
        )

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        flags = ["exclude_external"] if exclude_external else []
        return set(
            common.ExistingModelsResponse.parse_obj(
                self._get(MODELS_PATH, *flags, names=",".join(names))
            ).names
        )

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        try:
            response = self._get(f"{ENVIRONMENTS_PATH}/{environment}")
            return Environment.parse_obj(response)
        except NotFoundError:
            return None

    def get_environments(self) -> t.List[Environment]:
        response = self._get(ENVIRONMENTS_PATH)
        return common.EnvironmentsResponse.parse_obj(response).environments

    def invalidate_environment(self, environment: str) -> None:
        response = self._session.delete(
            urljoin(self._airflow_url, f"{ENVIRONMENTS_PATH}/{environment}")
        )
        self._raise_for_status(response)

    def get_versions(self) -> Versions:
        return Versions.parse_obj(self._get(VERSIONS_PATH))

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

    def wait_for_first_dag_run(self, dag_id: str, poll_interval_secs: int, max_retries: int) -> str:
        loading_id = self._console_loading_start()

        attempt_num = 1

        try:
            while True:
                try:
                    first_dag_run_id = self._get_first_dag_run_id(dag_id)
                    if first_dag_run_id is None:
                        raise SQLMeshError(f"Missing a DAG Run for DAG '{dag_id}'")
                    return first_dag_run_id
                except ApiServerError:
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
        dag_runs_response = self._get(f"{DAG_RUN_PATH_TEMPLATE.format(dag_id)}", limit="1")
        dag_runs = dag_runs_response["dag_runs"]
        if not dag_runs:
            return None
        return dag_runs[0]["dag_run_id"]

    def _get_dag(self, dag_id: str) -> t.Dict[str, t.Any]:
        return self._get(f"api/v1/dags/{dag_id}")

    def _get(self, path: str, *flags: str, **params: str) -> t.Dict[str, t.Any]:
        all_params = [*flags, *([urlencode(params)] if params else [])]
        query_string = "&".join(all_params)
        if query_string:
            path = f"{path}?{query_string}"
        response = self._session.get(urljoin(self._airflow_url, path))
        self._raise_for_status(response)
        return response.json()

    def _console_loading_start(self) -> t.Optional[uuid.UUID]:
        if self._console:
            return self._console.loading_start()
        return None

    def _raise_for_status(self, response: Response) -> None:
        if response.status_code == 404:
            raise NotFoundError(response.text)
        elif 400 <= response.status_code < 500:
            raise ApiClientError(response.text)
        elif 500 <= response.status_code < 600:
            raise ApiServerError(response.text)


T = t.TypeVar("T", bound=PydanticModel)


def _list_to_json(models: t.List[T]) -> str:
    return json.dumps([m.dict() for m in models], separators=(",", ":"))
