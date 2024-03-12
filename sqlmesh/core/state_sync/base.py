from __future__ import annotations

import abc
import importlib
import logging
import pkgutil
import typing as t

from sqlglot import __version__ as SQLGLOT_VERSION

from sqlmesh import migrations
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
)
from sqlmesh.core.snapshot.definition import Interval
from sqlmesh.utils import major_minor
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import (
    PydanticModel,
    field_validator,
    field_validator_v1_args,
)

logger = logging.getLogger(__name__)


class Versions(PydanticModel):
    """Represents the various versions of dependencies in the state sync."""

    schema_version: int = 0
    sqlglot_version: str = "0.0.0"
    sqlmesh_version: str = "0.0.0"

    @property
    def minor_sqlglot_version(self) -> t.Tuple[int, int]:
        return major_minor(self.sqlglot_version)

    @property
    def minor_sqlmesh_version(self) -> t.Tuple[int, int]:
        return major_minor(self.sqlmesh_version)

    @field_validator("sqlglot_version", "sqlmesh_version", mode="before")
    @classmethod
    def _package_version_validator(cls, v: t.Any) -> str:
        return "0.0.0" if v is None else str(v)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _schema_version_validator(cls, v: t.Any) -> int:
        return 0 if v is None else int(v)


MIGRATIONS = [
    importlib.import_module(f"sqlmesh.migrations.{migration}")
    for migration in sorted(info.name for info in pkgutil.iter_modules(migrations.__path__))
]
SCHEMA_VERSION: int = len(MIGRATIONS)


class PromotionResult(PydanticModel):
    added: t.List[SnapshotTableInfo]
    removed: t.List[SnapshotTableInfo]
    removed_environment_naming_info: t.Optional[EnvironmentNamingInfo]

    @field_validator("removed_environment_naming_info")
    @field_validator_v1_args
    def _validate_removed_environment_naming_info(
        cls, v: t.Optional[EnvironmentNamingInfo], values: t.Any
    ) -> t.Optional[EnvironmentNamingInfo]:
        if v and not values["removed"]:
            raise ValueError("removed_environment_naming_info must be None if removed is empty")
        return v


class StateReader(abc.ABC):
    """Abstract base class for read-only operations on snapshot and environment state."""

    @abc.abstractmethod
    def get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]],
        hydrate_seeds: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Bulk fetch snapshots given the corresponding snapshot ids.

        Args:
            snapshot_ids: Iterable of snapshot ids to get. If not provided all
                available snapshots will be returned.
            hydrate_seeds: Whether to hydrate seed snapshots with the content.

        Returns:
            A dictionary of snapshot ids to snapshots for ones that could be found.
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
    def nodes_exist(self, names: t.Iterable[str], exclude_external: bool = False) -> t.Set[str]:
        """Returns the node names that exist in the state sync.

        Args:
            names: Iterable of node names to check.
            exclude_external: Whether to exclude external models from the output.

        Returns:
            A set of all the existing node names.
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

    @abc.abstractmethod
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

    @abc.abstractmethod
    def recycle(self) -> None:
        """Closes all open connections and releases all allocated resources associated with any thread
        except the calling one."""

    @abc.abstractmethod
    def close(self) -> None:
        """Closes all open connections and releases all allocated resources."""

    def get_versions(self, validate: bool = True) -> Versions:
        """Get the current versions of the SQLMesh schema and libraries.

        Args:
            validate: Whether or not to raise error if the running version is different from what's in state.

        Returns:
            The versions object.
        """
        from sqlmesh._version import __version__ as SQLMESH_VERSION

        versions = self._get_versions()

        if validate:

            def raise_error(
                lib: str,
                local: str | int,
                remote: str | int,
                remote_package_version: t.Optional[str] = None,
                ahead: bool = False,
            ) -> None:
                if ahead:
                    raise SQLMeshError(
                        f"{lib} (local) is using version '{local}' which is ahead of '{remote}' (remote). "
                        "Please run a migration ('sqlmesh migrate' command)."
                    )

                if remote_package_version:
                    upgrade_suggestion = f" Please upgrade {lib} ('pip install --upgrade \"{lib.lower()}=={remote_package_version}\"' command)."
                else:
                    upgrade_suggestion = ""

                raise SQLMeshError(
                    f"{lib} (local) is using version '{local}' which is behind '{remote}' (remote).{upgrade_suggestion}"
                )

            if SCHEMA_VERSION != versions.schema_version:
                raise_error(
                    "SQLMesh",
                    SCHEMA_VERSION,
                    versions.schema_version,
                    remote_package_version=versions.sqlmesh_version,
                    ahead=SCHEMA_VERSION > versions.schema_version,
                )

            if major_minor(SQLGLOT_VERSION) != major_minor(versions.sqlglot_version):
                raise_error(
                    "SQLGlot",
                    SQLGLOT_VERSION,
                    versions.sqlglot_version,
                    remote_package_version=versions.sqlglot_version,
                    ahead=major_minor(SQLGLOT_VERSION) > major_minor(versions.sqlglot_version),
                )

            if major_minor(SQLMESH_VERSION) != major_minor(versions.sqlmesh_version):
                raise_error(
                    "SQLMesh",
                    SQLMESH_VERSION,
                    versions.sqlmesh_version,
                    remote_package_version=versions.sqlmesh_version,
                    ahead=major_minor(SQLMESH_VERSION) > major_minor(versions.sqlmesh_version),
                )

        return versions

    @abc.abstractmethod
    def _get_versions(self, lock_for_update: bool = False) -> Versions:
        """Queries the store to get the current versions of SQLMesh and deps.

        Args:
            lock_for_update: Whether or not the usage of this method plans to update the row.

        Returns:
            The versions object.
        """


class StateSync(StateReader, abc.ABC):
    """Abstract base class for snapshot and environment state management."""

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
    def delete_expired_snapshots(self) -> t.List[SnapshotTableCleanupTask]:
        """Removes expired snapshots.

        Expired snapshots are snapshots that have exceeded their time-to-live
        and are no longer in use within an environment.

        Returns:
            The list of table cleanup tasks.
        """

    @abc.abstractmethod
    def invalidate_environment(self, name: str) -> None:
        """Invalidates the target environment by setting its expiration timestamp to now.

        Args:
            name: The name of the environment to invalidate.
        """

    @abc.abstractmethod
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
                development mode.is_dev.
        """

    @abc.abstractmethod
    def remove_interval(
        self,
        snapshot_intervals: t.Sequence[t.Tuple[SnapshotInfoLike, Interval]],
        execution_time: t.Optional[TimeLike] = None,
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

    @abc.abstractmethod
    def refresh_snapshot_intervals(self, snapshots: t.Collection[Snapshot]) -> t.List[Snapshot]:
        """Updates given snapshots with latest intervals from the state.

        Args:
            snapshots: The snapshots to refresh.

        Returns:
            The updated snapshots.
        """

    @abc.abstractmethod
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
           A tuple of (added snapshot table infos, removed snapshot table infos)
        """

    @abc.abstractmethod
    def finalize(self, environment: Environment) -> None:
        """Finalize the target environment, indicating that this environment has been
        fully promoted and is ready for use.

        Args:
            environment: The target environment to finalize.
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

    @abc.abstractmethod
    def compact_intervals(self) -> None:
        """Compacts intervals for all snapshots.

        Compaction process involves merging of existing interval records into new records and
        then deleting the old ones.
        """

    @abc.abstractmethod
    def migrate(self, default_catalog: t.Optional[str], skip_backup: bool = False) -> None:
        """Migrate the state sync to the latest SQLMesh / SQLGlot version."""

    @abc.abstractmethod
    def rollback(self) -> None:
        """Rollback to previous backed up state."""


class DelegatingStateSync(StateSync):
    def __init__(self, state_sync: StateSync) -> None:
        self.state_sync = state_sync


def _create_delegate_method(name: str) -> t.Callable:
    def delegate(self: t.Any, *args: t.Any, **kwargs: t.Any) -> t.Any:
        return getattr(self.state_sync, name)(*args, **kwargs)

    return delegate


DelegatingStateSync.__abstractmethods__ = frozenset()
for name in StateSync.__abstractmethods__:
    setattr(DelegatingStateSync, name, _create_delegate_method(name))
