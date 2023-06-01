from __future__ import annotations

import logging
import typing as t

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotIntervals,
    SnapshotNameVersion,
    SnapshotNameVersionLike,
)
from sqlmesh.core.state_sync import StateReader, Versions
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
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]], hydrate_seeds: bool = False
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Gets multiple snapshots from the rest api.

        Because of the limitations of the Airflow API, this method is inherently inefficient.
        It's impossible to bulkfetch the snapshots and thus every snapshot needs to make an individual
        call to the rest api. Multiple threads can be used, but it could possibly have detrimental effects
        on the production server.
        """
        snapshots = self._client.get_snapshots(
            [s.snapshot_id for s in snapshot_ids] if snapshot_ids is not None else None,
            hydrate_seeds=hydrate_seeds,
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

    def models_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        """Returns the model names that exist in the state sync.

        Args:
            names: Iterable of model names to check.
            exclude_external: Whether to exclude external models from the output.

        Returns:
            A set of all the existing model names.
        """
        return self._client.models_exist(names, exclude_external=exclude_external)

    def get_snapshot_intervals(
        self, snapshots: t.Optional[t.Iterable[SnapshotNameVersionLike]]
    ) -> t.List[SnapshotIntervals]:
        """Fetch intervals for given snapshots as well as for snapshots that share a version with the given ones.

        Args:
            snapshots: Target snapshot IDs. If not specified all intervals will be fetched.
            current_only: Whether to only fetch intervals for snapshots provided as input as opposed
                to fetching intervals for all snapshots that share the same version as the input ones.

        Returns:
            The list of snapshot intervals, one per unique version.
        """
        if not snapshots:
            return []
        return self._client.get_snapshot_intervals(
            [SnapshotNameVersion(name=s.name, version=s.version) for s in snapshots]
        )

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        """Queries the store to get the migration.

        Args:
            lock_for_update: Whether or not the usage of this method plans to update the row.

        Returns:
            The versions object.
        """
        return self._client.get_versions()
