import abc
import time
import typing as t
import uuid
from urllib.parse import urlencode, urljoin

import requests

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.snapshot import Snapshot, SnapshotId
from sqlmesh.core.state_sync import Versions
from sqlmesh.core.user import User
from sqlmesh.schedulers.airflow import common, NO_DEFAULT_CATALOG
from sqlmesh.utils.errors import (
    ApiServerError,
    NotFoundError,
    SQLMeshError,
    raise_for_status,
)

DAG_RUN_PATH_TEMPLATE = "api/v1/dags/{}/dagRuns"


PLANS_PATH = f"{common.SQLMESH_API_BASE_PATH}/plans"
ENVIRONMENTS_PATH = f"{common.SQLMESH_API_BASE_PATH}/environments"
SNAPSHOTS_PATH = f"{common.SQLMESH_API_BASE_PATH}/snapshots/search"
SEEDS_PATH = f"{common.SQLMESH_API_BASE_PATH}/seeds"
INTERVALS_PATH = f"{common.SQLMESH_API_BASE_PATH}/intervals"
MODELS_PATH = f"{common.SQLMESH_API_BASE_PATH}/models"
VERSIONS_PATH = f"{common.SQLMESH_API_BASE_PATH}/versions"


class BaseAirflowClient(abc.ABC):
    def __init__(self, airflow_url: str, console: t.Optional[Console]):
        self._airflow_url = airflow_url
        if not self._airflow_url.endswith("/"):
            self._airflow_url += "/"
        self._console = console

    @property
    def default_catalog(self) -> t.Optional[str]:
        default_catalog = self.get_variable(common.DEFAULT_CATALOG_VARIABLE_NAME)
        if not default_catalog:
            raise SQLMeshError(
                "Must define `default_catalog` when creating `SQLMeshAirflow` object. See docs for more info: https://sqlmesh.readthedocs.io/en/stable/integrations/airflow/#airflow-cluster-configuration"
            )
        if default_catalog == NO_DEFAULT_CATALOG:
            return None
        return default_catalog

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

    def wait_for_dag_run_completion(
        self, dag_id: str, dag_run_id: str, poll_interval_secs: int
    ) -> bool:
        """Blocks until the given DAG Run completes.

        Args:
            dag_id: The DAG ID.
            dag_run_id: The DAG Run ID.
            poll_interval_secs: The number of seconds to wait between polling for the DAG Run state.

        Returns:
            True if the DAG Run completed successfully, False otherwise.
        """
        loading_id = self._console_loading_start()

        while True:
            state = self.get_dag_run_state(dag_id, dag_run_id)
            if state in ("failed", "success"):
                if self._console and loading_id:
                    self._console.loading_stop(loading_id)
                return state == "success"

            time.sleep(poll_interval_secs)

    def wait_for_first_dag_run(self, dag_id: str, poll_interval_secs: int, max_retries: int) -> str:
        """Blocks until the first DAG Run for the given DAG ID is created.

        Args:
            dag_id: The DAG ID.
            poll_interval_secs: The number of seconds to wait between polling for the DAG Run.
            max_retries: The maximum number of retries.

        Returns:
            The ID of the first DAG Run for the given DAG ID.
        """

        loading_id = self._console_loading_start()

        attempt_num = 1

        try:
            while True:
                try:
                    first_dag_run_id = self.get_first_dag_run_id(dag_id)
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

    @abc.abstractmethod
    def get_first_dag_run_id(self, dag_id: str) -> t.Optional[str]:
        """Returns the ID of the first DAG Run for the given DAG ID, or None if no DAG Runs exist.

        Args:
            dag_id: The DAG ID.

        Returns:
            The ID of the first DAG Run for the given DAG ID, or None if no DAG Runs exist.
        """

    @abc.abstractmethod
    def get_dag_run_state(self, dag_id: str, dag_run_id: str) -> str:
        """Returns the state of the given DAG Run.

        Args:
            dag_id: The DAG ID.
            dag_run_id: The DAG Run ID.

        Returns:
            The state of the given DAG Run.
        """

    @abc.abstractmethod
    def get_variable(self, key: str) -> t.Optional[str]:
        """Returns the value of an Airflow variable with the given key.

        Args:
            key: The variable key.

        Returns:
            The variable value or None if no variable with the given key exists.
        """

    def _console_loading_start(self) -> t.Optional[uuid.UUID]:
        if self._console:
            return self._console.loading_start()
        return None


class AirflowClient(BaseAirflowClient):
    def __init__(
        self,
        session: requests.Session,
        airflow_url: str,
        console: t.Optional[Console] = None,
    ):
        super().__init__(airflow_url, console)
        self._session = session

    def apply_plan(
        self,
        plan: EvaluatablePlan,
        notification_targets: t.Optional[t.List[NotificationTarget]] = None,
        backfill_concurrent_tasks: int = 1,
        ddl_concurrent_tasks: int = 1,
        users: t.Optional[t.List[User]] = None,
    ) -> None:
        request = common.PlanApplicationRequest(
            plan=plan,
            notification_targets=notification_targets or [],
            backfill_concurrent_tasks=backfill_concurrent_tasks,
            ddl_concurrent_tasks=ddl_concurrent_tasks,
            users=users or [],
        )
        self._post(PLANS_PATH, request.json())

    def get_snapshots(self, snapshot_ids: t.Optional[t.List[SnapshotId]]) -> t.List[Snapshot]:
        snapshots_request = common.SnapshotsRequest(snapshot_ids=snapshot_ids)
        response = self._post(SNAPSHOTS_PATH, snapshots_request.json())
        return common.SnapshotsResponse.parse_obj(response).snapshots

    def snapshots_exist(self, snapshot_ids: t.List[SnapshotId]) -> t.Set[SnapshotId]:
        snapshots_request = common.SnapshotsRequest(snapshot_ids=snapshot_ids, check_existence=True)
        response = self._post(SNAPSHOTS_PATH, snapshots_request.json())
        return set(common.SnapshotIdsResponse.parse_obj(response).snapshot_ids)

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

    def max_interval_end_per_model(
        self,
        environment: str,
        models: t.Optional[t.Collection[str]],
        ensure_finalized_snapshots: bool,
    ) -> t.Dict[str, int]:
        max_interval_end_per_model_request = common.MaxIntervalEndPerModelRequest(
            models=models, ensure_finalized_snapshots=ensure_finalized_snapshots
        )
        response = self._post(
            f"{ENVIRONMENTS_PATH}/{environment}/max_interval_end_per_model",
            max_interval_end_per_model_request.json(),
        )
        return common.IntervalEndResponse.parse_obj(response).interval_end_per_model

    def invalidate_environment(self, environment: str) -> None:
        response = self._session.delete(
            urljoin(self._airflow_url, f"{ENVIRONMENTS_PATH}/{environment}")
        )
        raise_for_status(response)

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

    def get_first_dag_run_id(self, dag_id: str) -> t.Optional[str]:
        dag_runs_response = self._get(f"{DAG_RUN_PATH_TEMPLATE.format(dag_id)}", limit="1")
        dag_runs = dag_runs_response["dag_runs"]
        if not dag_runs:
            return None
        return dag_runs[0]["dag_run_id"]

    def get_variable(self, key: str) -> t.Optional[str]:
        try:
            variables_response = self._get(f"api/v1/variables/{key}")
            return variables_response["value"]
        except NotFoundError:
            return None

    def close(self) -> None:
        self._session.close()

    def _get_dag(self, dag_id: str) -> t.Dict[str, t.Any]:
        return self._get(f"api/v1/dags/{dag_id}")

    def _get(self, path: str, *flags: str, **params: str) -> t.Dict[str, t.Any]:
        response = self._session.get(self._url(path, *flags, **params))
        raise_for_status(response)
        return response.json()

    def _post(self, path: str, data: str, *flags: str, **params: str) -> t.Dict[str, t.Any]:
        response = self._session.post(
            self._url(path, *flags, **params),
            data=data,
            headers={"Content-Type": "application/json"},
        )
        raise_for_status(response)
        return response.json()

    def _url(self, path: str, *flags: str, **params: str) -> str:
        all_params = [*flags, *([urlencode(params)] if params else [])]
        query_string = "&".join(all_params)
        if query_string:
            path = f"{path}?{query_string}"
        return urljoin(self._airflow_url, path)
