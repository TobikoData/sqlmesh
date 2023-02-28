from __future__ import annotations

import abc
import typing as t

from sqlmesh.core import scheduler
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotNameVersionLike,
    SnapshotTableInfo,
)
from sqlmesh.utils.date import TimeLike, now, to_datetime
from sqlmesh.utils.errors import SQLMeshError


class StateReader(abc.ABC):
    """Abstract base class for read-only operations on snapshot and environment state."""

    @abc.abstractmethod
    def get_snapshots(
        self, snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]]
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Bulk fetch snapshots given the corresponding snapshot ids.

        Args:
            snapshot_ids: Iterable of snapshot ids to get. If not provided all
                available snapshots will be returned.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
        """

    @abc.abstractmethod
    def get_snapshots_with_same_version(
        self, snapshots: t.Iterable[SnapshotNameVersionLike]
    ) -> t.List[Snapshot]:
        """Fetches all snapshots that share the same version as the snapshots.

        The output includes the snapshots with the specified version.

        Args:
            snapshots: The collection of target name / version pairs.

        Returns:
            The list of Snapshot objects.
        """

    @abc.abstractmethod
    def snapshots_exist(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> t.Set[SnapshotId]:
        """Checks if multiple snapshots exist in the state sync.

        Args:
            snapshot_ids: Iterable of snapshot ids to bulk check.

        Returns:
            A set of all the existing snapshot ids.
        """

    @abc.abstractmethod
    def get_environment(self, environment: str) -> t.Optional[Environment]:
        """Fetches the environment if it exists.

        Args:
            environment: The environment

        Returns:
            The environment object.
        """

    @abc.abstractmethod
    def get_environments(self) -> t.List[Environment]:
        """Fetches all environments.

        Returns:
            A list of all environments.
        """

    @abc.abstractmethod
    def get_snapshots_by_models(self, *names: str) -> t.List[Snapshot]:
        """Get all snapshots by model name.

        Returns:
            The list of snapshots.
        """

    def missing_intervals(
        self,
        env_or_snapshots: str | Environment | t.Iterable[Snapshot],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        latest: t.Optional[TimeLike] = None,
        restatements: t.Optional[t.Iterable[str]] = None,
    ) -> t.Dict[Snapshot, Intervals]:
        """Find missing intervals for an environment or a list of snapshots.

        Args:
            env_or_snapshots: The environment or snapshots to find missing intervals for.
            start: The start of the time range to look for.
            end: The end of the time range to look for.
            latest: The latest datetime to use for non-incremental queries.

        Returns:
            A dictionary of SnapshotId to Intervals.
        """

        if isinstance(env_or_snapshots, str):
            env = self.get_environment(env_or_snapshots)
        elif isinstance(env_or_snapshots, Environment):
            env = env_or_snapshots
        else:
            env = None

        if env:
            snapshots_by_id = self.get_snapshots(env.snapshots)
            start = start or env.start_at
            end = end or env.end_at
        elif isinstance(env_or_snapshots, str):
            snapshots_by_id = {}
        elif not isinstance(env_or_snapshots, Environment):
            snapshots_by_id = {snapshot.snapshot_id: snapshot for snapshot in env_or_snapshots}
        else:
            raise SQLMeshError("This shouldn't be possible.")

        if not snapshots_by_id:
            return {}

        unversioned = [snapshot for snapshot in snapshots_by_id.values() if not snapshot.version]

        snapshots_by_id = {
            **snapshots_by_id,
            **(self.get_snapshots(unversioned) if unversioned else {}),
        }

        all_snapshots = {
            **snapshots_by_id,
            **{
                snapshot.snapshot_id: snapshot
                for snapshot in self.get_snapshots_with_same_version(
                    snapshot for snapshot in snapshots_by_id.values() if snapshot.version
                )
            },
        }

        missing = {}
        start_date = to_datetime(start or scheduler.earliest_start_date(snapshots_by_id.values()))
        end_date = end or now()
        restatements = set(restatements or [])

        for snapshot in Snapshot.merge_snapshots(snapshots_by_id, all_snapshots):
            if snapshot.name in restatements:
                snapshot.remove_interval(start_date, end_date)
            intervals = snapshot.missing_intervals(
                max(
                    start_date,
                    to_datetime(
                        scheduler.start_date(snapshot, snapshots_by_id.values()) or start_date
                    ),
                ),
                end_date,
                latest=latest,
            )
            if intervals:
                missing[snapshot] = intervals
        return missing


class StateSync(StateReader, abc.ABC):
    """Abstract base class for snapshot and environment state management."""

    @abc.abstractmethod
    def init_schema(self) -> None:
        """Optional initialization of the sync."""

    @abc.abstractmethod
    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        """Push snapshots into the state sync.

        This method only allows for pushing new snapshots. If existing snapshots are found,
        this method should raise an error.

        Raises:
            SQLMeshError when existing snapshots are pushed.

        Args:
            snapshots: A list of snapshots to save in the state sync.
        """

    @abc.abstractmethod
    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        """Delete snapshots from the state sync.

        Args:
            snapshot_ids: A list of snapshot like objects to delete.
        """

    @abc.abstractmethod
    def delete_expired_snapshots(self) -> t.List[Snapshot]:
        """Removes expired snapshots.

        Expired snapshots are snapshots that have exceeded their time-to-live
        and are no longer in use within an environment.

        Returns:
            The list of removed snapshots.
        """

    @abc.abstractmethod
    def add_interval(
        self,
        snapshot_id: SnapshotIdLike,
        start: TimeLike,
        end: TimeLike,
        is_dev: bool = False,
    ) -> None:
        """Add an interval to a snapshot and sync it to the store.

        Snapshots must be pushed before adding intervals to them.

        Args:
            snapshot_id: The snapshot like object to add an interval to.
            start: The start of the interval to add.
            end: The end of the interval to add.
            is_dev: Indicates whether the given interval is being added while in
                development mode.
        """

    @abc.abstractmethod
    def remove_interval(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        start: TimeLike,
        end: TimeLike,
        all_snapshots: t.Optional[t.Iterable[Snapshot]] = None,
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

    @abc.abstractmethod
    def promote(
        self, environment: Environment, no_gaps: bool = False
    ) -> t.Tuple[t.List[SnapshotTableInfo], t.List[SnapshotTableInfo]]:
        """Update the environment to reflect the current state.

        This method verifies that snapshots have been pushed.

        Args:
            environment: The environment to promote.
            no_gaps:  Whether to ensure that new snapshots for models that are already a
                part of the target environment have no data gaps when compared against previous
                snapshots for same models.

        Returns:
           A tuple of (added snapshot table infos, removed snapshot table infos)
        """

    @abc.abstractmethod
    def delete_expired_environments(self) -> t.List[Environment]:
        """Removes expired environments.

        Expired environments are environments that have exceeded their time-to-live value.

        Returns:
            The list of removed environments.
        """

    @abc.abstractmethod
    def unpause_snapshots(
        self, snapshots: t.Iterable[SnapshotInfoLike], unpaused_dt: TimeLike
    ) -> None:
        """Unpauses target snapshots.

        Unpaused snapshots are scheduled for evaluation on a recurring basis.
        Once unpaused a snapshot can't be paused again.

        Args:
            snapshots: Target snapshots.
            unpaused_dt: The datetime object which indicates when target snapshots
                were unpaused.
        """
