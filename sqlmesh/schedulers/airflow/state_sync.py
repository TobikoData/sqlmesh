from __future__ import annotations

import logging
import typing as t

from sqlmesh.core.console import Console
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotTableCleanupTask,
)
from sqlmesh.core.snapshot.definition import Interval, SnapshotIntervals
from sqlmesh.core.state_sync import StateSync, Versions
from sqlmesh.core.state_sync.base import PromotionResult
from sqlmesh.schedulers.airflow.client import AirflowClient

if t.TYPE_CHECKING:
    from sqlmesh.utils.date import TimeLike

logger = logging.getLogger(__name__)


class HttpStateSync(StateSync):
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

    def max_interval_end_for_environment(
        self, environment: str, ensure_finalized_snapshots: bool = False
    ) -> t.Optional[int]:
        """Returns the max interval end for the given environment.

        Args:
            environment: The environment.
            ensure_finalized_snapshots: Whether to use snapshots from the latest finalized environment state,
                or to use whatever snapshots are in the current environment state even if the environment is not finalized.

        Returns:
            A timestamp or None if no interval or environment exists.
        """
        return self._client.max_interval_end_for_environment(
            environment, ensure_finalized_snapshots
        )

    def greatest_common_interval_end(
        self, environment: str, models: t.Set[str], ensure_finalized_snapshots: bool = False
    ) -> t.Optional[int]:
        """Returns the greatest common interval end for given models in the target environment.

        Args:
            environment: The environment.
            models: The model FQNs to select intervals from.
            ensure_finalized_snapshots: Whether to use snapshots from the latest finalized environment state,
                or to use whatever snapshots are in the current environment state even if the environment is not finalized.

        Returns:
            A timestamp or None if no interval or environment exists.
        """
        return self._client.greatest_common_interval_end(
            environment, models, ensure_finalized_snapshots
        )

    def get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]],
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

    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        """Returns the node names that exist in the state sync.

        Args:
            names: Iterable of node names to check.
            exclude_external: Whether to exclude external models from the output.

        Returns:
            A set of all the existing node names.
        """
        return self._client.nodes_exist(names, exclude_external=exclude_external)

    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        """Queries the store to get the migration.

        Args:
            lock_for_update: Whether or not the usage of this method plans to update the row.

        Returns:
            The versions object.
        """
        return self._client.get_versions()

    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Push snapshots into the state sync.

        This method only allows for pushing new snapshots. If existing snapshots are found,
        this method should raise an error.

        Raises:
            SQLMeshError when existing snapshots are pushed.

        Args:
            snapshots: A list of snapshots to save in the state sync.
        """
        raise NotImplementedError("Pushing snapshots is not supported by the Airflow state sync.")

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        """Delete snapshots from the state sync.

        Args:
            snapshot_ids: A list of snapshot like objects to delete.
        """
        raise NotImplementedError("Deleting snapshots is not supported by the Airflow state sync.")

    def delete_expired_snapshots(
        self, ignore_ttl: bool = False
    ) -> t.List[SnapshotTableCleanupTask]:
        """Removes expired snapshots.

        Expired snapshots are snapshots that have exceeded their time-to-live
        and are no longer in use within an environment.

        Returns:
            The list of table cleanup tasks.
        """
        raise NotImplementedError(
            "Deleting expired snapshots is not supported by the Airflow state sync."
        )

    def invalidate_environment(self, name: str) -> None:
        """Invalidates the target environment by setting its expiration timestamp to now.

        Args:
            name: The name of the environment to invalidate.
        """
        self._client.invalidate_environment(name)

    def add_interval(
        self,
        snapshot: Snapshot,
        start: TimeLike,
        end: TimeLike,
        is_dev: bool = False,
    ) -> None:
        """Add an interval to a snapshot and sync it to the store.

        Snapshots must be pushed before adding intervals to them.

        Args:
            snapshot: The snapshot like object to add an interval to.
            start: The start of the interval to add.
            end: The end of the interval to add.
            is_dev: Indicates whether the given interval is being added while in
                development mode.
        """
        raise NotImplementedError("Adding intervals is not supported by the Airflow state sync.")

    def _add_snapshot_intervals(self, snapshot_intervals: SnapshotIntervals) -> None:
        raise NotImplementedError("Adding intervals is not supported by the Airflow state sync.")

    def remove_interval(
        self,
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        remove_shared_versions: bool = False,
    ) -> None:
        """Remove an interval from a list of snapshots and sync it to the store.

        Because multiple snapshots can be pointing to the same version or physical table, this method
        can also grab all snapshots tied to the passed in version.

        Args:
            snapshots: The snapshot info like object to remove intervals from.
            start: The start of the interval to add.
            end: The end of the interval to add.
            all_snapshots: All snapshots can be passed in to skip fetching matching snapshot versions.
        """
        raise NotImplementedError("Removing intervals is not supported by the Airflow state sync.")

    def refresh_snapshot_intervals(self, snapshots: t.Collection[Snapshot]) -> t.List[Snapshot]:
        """Updates given snapshots with latest intervals from the state.

        Args:
            snapshots: The snapshots to refresh.

        Returns:
            The updated snapshots.
        """
        raise NotImplementedError(
            "Refreshing snapshot intervals is not supported by the Airflow state sync."
        )

    def promote(
        self,
        environment: Environment,
        no_gaps_snapshot_names: t.Optional[t.Set[str]] = None,
    ) -> PromotionResult:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            no_gaps_snapshot_names: A set of snapshot names to check for data gaps. If None,
                all snapshots will be checked. The data gap check ensures that models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
           A promotion result object containing added, removed, and removed environment naming info
        """
        raise NotImplementedError(
            "Promoting environments is not supported by the Airflow state sync."
        )

    def finalize(self, environment: Environment) -> None:
        """Finalize the target environment, indicating that this environment has been
        fully promoted and is ready for use.

        Args:
            environment: The target environment to finalize.
        """
        raise NotImplementedError(
            "Finalizing environments is not supported by the Airflow state sync."
        )

    def delete_expired_environments(self) -> t.List[Environment]:
        """Removes expired environments.

        Expired environments are environments that have exceeded their time-to-live value.

        Returns:
            The list of removed environments.
        """
        raise NotImplementedError(
            "Deleting expired environments is not supported by the Airflow state sync."
        )

    def unpause_snapshots(
        self, snapshots: t.Collection[SnapshotInfoLike], unpaused_dt: TimeLike
    ) -> None:
        """Unpauses target snapshots.

        Unpaused snapshots are scheduled for evaluation on a recurring basis.
        Once unpaused a snapshot can't be paused again.

        Args:
            snapshots: Target snapshots.
            unpaused_dt: The datetime object which indicates when target snapshots
                were unpaused.
        """
        raise NotImplementedError("Unpausing snapshots is not supported by the Airflow state sync.")

    def compact_intervals(self) -> None:
        """Compacts intervals for all snapshots.

        Compaction process involves merging of existing interval records into new records and
        then deleting the old ones.
        """
        raise NotImplementedError(
            "Compacting intervals is not supported by the Airflow state sync."
        )

    def migrate(
        self,
        default_catalog: t.Optional[str],
        skip_backup: bool = False,
        promoted_snapshots_only: bool = True,
    ) -> None:
        """Migrate the state sync to the latest SQLMesh / SQLGlot version."""
        raise NotImplementedError("Migration is not supported by the Airflow state sync.")

    def rollback(self) -> None:
        """Rollback to previous backed up state."""
        raise NotImplementedError("Rollback is not supported by the Airflow state sync.")

    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""

    def close(self) -> None:
        """Closes all open connections and releases all allocated resources."""

    def state_type(self) -> str:
        """Returns the type of state sync."""
        return "airflow_http"
