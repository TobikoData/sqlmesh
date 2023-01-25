from __future__ import annotations

import logging
import typing as t
from concurrent.futures import ThreadPoolExecutor

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotIdLike, SnapshotInfoLike
from sqlmesh.core.state_sync import StateReader
from sqlmesh.schedulers.airflow.client import AirflowClient
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.file_cache import FileCache

logger = logging.getLogger(__name__)


class HttpStateReader(StateReader):
    """Reads state of models and snapshot through the Airflow REST API.

    Args:
        airflow_url: URL pointing to the airflow rest api.
        username: Username for Airflow.
        password: Password for Airflow.
        max_concurrent_requests: Max number of http requests to make concurrently.
        blocking_updates: Indicates whether calls that cause state updates should be blocking.
        dag_run_poll_interval_secs: Determines how frequently the state of a DAG run should be checked.
            Used to block on calls that update the state.
        console: Used to print out tracking URLs.
    """

    def __init__(
        self,
        table_info_cache: FileCache,
        client: AirflowClient,
        max_concurrent_requests: int = 2,
        blocking_updates: bool = True,
        dag_run_poll_interval_secs: int = 2,
        console: t.Optional[Console] = None,
    ):
        self.table_info_cache = table_info_cache
        self._client = client
        self.max_concurrent_requests = max_concurrent_requests
        self.blocking_updates = blocking_updates
        self.dag_run_poll_interval_secs = dag_run_poll_interval_secs
        self.console = console

    def get_environment(self, environment: str) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The environment

        Returns:
            The environment object.
        """
        env = self._client.get_environment(environment)
        if env:
            self._update_cache(env.snapshots)
        return env

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        raise NotImplementedError(
            "Fetching all environments is not supported by the Airflow HTTP State Sync"
        )

    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Gets multiple snapshots from the rest api.

        Because of the limitations of the Airflow API, this method is inherently inefficient.
        It's impossible to bulkfetch the snapshots and thus every snapshot needs to make an individual
        call to the rest api. Multiple threads can be used, but it could possibly have detrimental effects
        on the production server.
        """
        snapshot_ids = snapshot_ids or self._client.get_snapshot_ids()

        with ThreadPoolExecutor(
            max_workers=self.max_concurrent_requests,
            thread_name_prefix="airflow_get_snapshots",
        ) as executor:
            snapshots = executor.map(
                lambda snapshot_id: self._client.get_snapshot(
                    snapshot_id.name,
                    snapshot_id.identifier,
                ),
                snapshot_ids,
            )

        snapshots_dict = {
            snapshot.snapshot_id: snapshot for snapshot in snapshots if snapshot
        }
        self._update_cache(snapshots_dict.values())
        return snapshots_dict

    def snapshots_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Set[SnapshotId]:
        """Checks if multiple snapshots exist in the state sync.

        Args:
            snapshot_ids: Iterable of snapshot ids to bulk check.

        Returns:
            A set of existing snapshot IDs.
        """
        target_ids = {
            SnapshotId(name=s.name, identifier=s.identifier) for s in snapshot_ids
        }
        return target_ids.intersection(set(self._client.get_snapshot_ids()))

    def get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotInfoLike]
    ) -> t.List[Snapshot]:
        if not snapshots:
            return []

        target_snapshot_ids = set()
        for s in snapshots:
            for i in self._client.get_snapshot_identifiers_for_version(
                s.name, version=s.version
            ):
                target_snapshot_ids.add(SnapshotId(name=s.name, identifier=i))
        return list(self.get_snapshots(target_snapshot_ids).values())

    def get_snapshots_by_models(self, *names: str) -> t.List[Snapshot]:
        """
        Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """
        raise NotImplementedError(
            "Getting snapshots by model names is not supported by the Airflow HTTP State Sync"
        )

    def _wait_for_dag_run_completion(
        self, dag_id: str, dag_run_id: str, op_name: str
    ) -> None:
        succeeded = self._client.wait_for_dag_run_completion(
            dag_id, dag_run_id, self.dag_run_poll_interval_secs
        )
        if not succeeded:
            raise SQLMeshError(
                f"{op_name.capitalize()} failed. Check details at {self._client.dag_run_tracking_url(dag_id, dag_run_id)}"
            )

    def _update_cache(self, snapshots: t.Iterable[SnapshotInfoLike]) -> None:
        with self.table_info_cache:
            self.table_info_cache.update(
                {snapshot.snapshot_id: snapshot.table_info for snapshot in snapshots}
            )
