from __future__ import annotations

import logging
import typing as t

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotNameVersion,
    SnapshotNameVersionLike,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.schedulers.airflow.client import AirflowClient

logger = logging.getLogger(__name__)


class HttpStateReader(StateReader):
    """Reads state of models and snapshot through the Airflow REST API.

    Args:
        airflow_url: URL pointing to the airflow rest api.
        username: Username for Airflow.
        password: Password for Airflow.
        blocking_updates: Indicates whether calls that cause state updates should be blocking.
        dag_run_poll_interval_secs: Determines how frequently the state of a DAG run should be checked.
            Used to block on calls that update the state.
        console: Used to print out tracking URLs.
    """

    def __init__(
        self,
        client: AirflowClient,
        blocking_updates: bool = True,
        dag_run_poll_interval_secs: int = 2,
        console: t.Optional[Console] = None,
    ):
        self._client = client
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
        return self._client.get_environment(environment)

    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """
        return self._client.get_environments()

    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Gets multiple snapshots from the rest api.

        Because of the limitations of the Airflow API, this method is inherently inefficient.
        It's impossible to bulkfetch the snapshots and thus every snapshot needs to make an individual
        call to the rest api. Multiple threads can be used, but it could possibly have detrimental effects
        on the production server.
        """
        snapshots = self._client.get_snapshots(
            [s.snapshot_id for s in snapshot_ids] if snapshot_ids is not None else None
        )
        return {snapshot.snapshot_id: snapshot for snapshot in snapshots}

    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        """Checks if multiple snapshots exist in the state sync.

        Args:
            snapshot_ids: Iterable of snapshot ids to bulk check.

        Returns:
            A set of existing snapshot IDs.
        """
        if not snapshot_ids:
            return set()
        return self._client.snapshots_exist([s.snapshot_id for s in snapshot_ids])

    def get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotNameVersionLike]
    ) -> t.List[Snapshot]:
        if not snapshots:
            return []
        return self._client.get_snapshots_with_same_version(
            [SnapshotNameVersion(name=s.name, version=s.version) for s in snapshots]
        )

    def get_snapshots_by_models(self, *names: str) -> t.List[Snapshot]:
        """
        Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """
        raise NotImplementedError(
            "Getting snapshots by model names is not supported by the Airflow HTTP State Sync"
        )
