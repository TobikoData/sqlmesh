"""
# ContextDiff

ContextDiff encapsulates the differences between two environments. The two environments can be the local
environment and a remote environment, or two remote environments. ContextDiff is an important part of
SQLMesh. SQLMesh plans use ContextDiff to determine what nodes were changed between two environments.
The SQLMesh CLI diff command uses ContextDiff to determine what to visualize.

When creating a ContextDiff object, SQLMesh will compare the snapshots from one environment with those of
another remote environment and determine if nodes have been added, removed, or modified.
"""

from __future__ import annotations

import typing as t
from functools import cached_property

from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotIndirectVersion,
    SnapshotTableInfo,
)
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

if t.TYPE_CHECKING:
    from sqlmesh.core.state_sync import StateReader


class ContextDiff(PydanticModel):
    """ContextDiff is an object representing the difference between two environments.

    The two environments can be the local environment and a remote environment, or two remote
    environments.
    """

    environment: str
    """The environment to diff."""
    is_new_environment: bool
    """Whether the target environment is new."""
    is_unfinalized_environment: bool
    """Whether the currently stored environment record is in unfinalized state."""
    create_from: str
    """The name of the environment the target environment will be created from if new."""
    added: t.Set[SnapshotId]
    """New nodes."""
    removed_snapshots: t.Dict[SnapshotId, SnapshotTableInfo]
    """Deleted nodes."""
    modified_snapshots: t.Dict[str, t.Tuple[Snapshot, Snapshot]]
    """Modified snapshots."""
    snapshots: t.Dict[SnapshotId, Snapshot]
    """Merged snapshots."""
    new_snapshots: t.Dict[SnapshotId, Snapshot]
    """New snapshots."""
    previous_plan_id: t.Optional[str]
    """Previous plan id."""
    previously_promoted_snapshot_ids: t.Set[SnapshotId]
    """Snapshot IDs that were promoted by the previous plan."""
    previous_finalized_snapshots: t.Optional[t.List[SnapshotTableInfo]]
    """Snapshots from the previous finalized state."""

    @classmethod
    def create(
        cls,
        environment: str,
        snapshots: t.Dict[str, Snapshot],
        create_from: str,
        state_reader: StateReader,
        ensure_finalized_snapshots: bool = False,
    ) -> ContextDiff:
        """Create a ContextDiff object.

        Args:
            environment: The remote environment to diff.
            snapshots: The snapshots of the current environment.
            create_from: The environment to create the target environment from if it
                doesn't exist.
            state_reader: StateReader to access the remote environment to diff.
            ensure_finalized_snapshots: Whether to compare against snapshots from the latest finalized
                environment state, or to use whatever snapshots are in the current environment state even if
                the environment is not finalized.

        Returns:
            The ContextDiff object.
        """
        environment = environment.lower()
        env = state_reader.get_environment(environment)

        if env is None:
            env = state_reader.get_environment(create_from.lower())
            is_new_environment = True
            previously_promoted_snapshot_ids = set()
        else:
            is_new_environment = False
            previously_promoted_snapshot_ids = {s.snapshot_id for s in env.promoted_snapshots}

        environment_snapshot_infos = []
        if env:
            environment_snapshot_infos = (
                env.snapshots
                if not ensure_finalized_snapshots
                else env.finalized_or_current_snapshots
            )
        remote_snapshot_name_to_info = {
            snapshot_info.name: snapshot_info for snapshot_info in environment_snapshot_infos
        }
        removed = {
            snapshot_table_info.snapshot_id: snapshot_table_info
            for snapshot_table_info in environment_snapshot_infos
            if snapshot_table_info.name not in snapshots
        }
        added = {
            snapshot.snapshot_id
            for snapshot in snapshots.values()
            if snapshot.name not in remote_snapshot_name_to_info
        }
        modified_snapshot_name_to_snapshot_info = {
            snapshot.name: remote_snapshot_name_to_info[snapshot.name]
            for snapshot in snapshots.values()
            if snapshot.snapshot_id not in added
            and snapshot.fingerprint != remote_snapshot_name_to_info[snapshot.name].fingerprint
        }
        modified_local_seed_snapshot_ids = {
            s.snapshot_id
            for s in snapshots.values()
            if s.is_seed and s.name in modified_snapshot_name_to_snapshot_info
        }
        modified_remote_snapshot_ids = {
            s.snapshot_id for s in modified_snapshot_name_to_snapshot_info.values()
        }

        stored = {
            **state_reader.get_snapshots(
                [
                    snapshot.snapshot_id
                    for snapshot in snapshots.values()
                    if snapshot.snapshot_id not in modified_local_seed_snapshot_ids
                ]
            ),
            **state_reader.get_snapshots(
                modified_remote_snapshot_ids | modified_local_seed_snapshot_ids,
                hydrate_seeds=True,
            ),
        }

        merged_snapshots = {}
        modified_snapshots = {}
        new_snapshots = {}
        snapshot_remote_versions: t.Dict[
            str, t.Tuple[t.Tuple[SnapshotIndirectVersion, ...], int]
        ] = {}

        for snapshot in snapshots.values():
            s_id = snapshot.snapshot_id
            modified_snapshot_info = modified_snapshot_name_to_snapshot_info.get(snapshot.name)
            existing_snapshot = stored.get(s_id)

            if modified_snapshot_info and snapshot.node_type != modified_snapshot_info.node_type:
                added.add(snapshot.snapshot_id)
                removed[modified_snapshot_info.snapshot_id] = modified_snapshot_info
                modified_snapshot_name_to_snapshot_info.pop(snapshot.name)
            elif existing_snapshot:
                # Keep the original node instance to preserve the query cache.
                existing_snapshot.node = snapshot.node

                merged_snapshots[s_id] = existing_snapshot.copy()
                if modified_snapshot_info:
                    modified_snapshots[s_id.name] = (
                        existing_snapshot,
                        stored[modified_snapshot_info.snapshot_id],
                    )
                    for child_name, versions in existing_snapshot.indirect_versions.items():
                        existing_versions = snapshot_remote_versions.get(child_name)
                        if (
                            not existing_versions
                            or existing_versions[1] < existing_snapshot.created_ts
                        ):
                            snapshot_remote_versions[child_name] = (
                                versions,
                                existing_snapshot.created_ts,
                            )
            else:
                snapshot = snapshot.copy()
                merged_snapshots[s_id] = snapshot
                new_snapshots[snapshot.snapshot_id] = snapshot
                if modified_snapshot_info:
                    snapshot.previous_versions = modified_snapshot_info.all_versions
                    modified_snapshots[s_id.name] = (
                        snapshot,
                        stored[modified_snapshot_info.snapshot_id],
                    )

        for snapshot in new_snapshots.values():
            if (
                snapshot.name in snapshot_remote_versions
                and snapshot.previous_version
                and snapshot.data_hash_matches(snapshot.previous_version)
            ):
                remote_versions = snapshot_remote_versions[snapshot.name][0]
                remote_head = remote_versions[-1]
                local_head = snapshot.previous_version

                if remote_head.version in (local.version for local in snapshot.previous_versions):
                    snapshot.version = local_head.version
                    snapshot.change_category = local_head.change_category
                elif local_head.version in (remote.version for remote in remote_versions):
                    snapshot.version = remote_head.version
                    snapshot.change_category = remote_head.change_category
                else:
                    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

        return ContextDiff(
            environment=environment,
            is_new_environment=is_new_environment,
            is_unfinalized_environment=bool(env and not env.finalized_ts),
            create_from=create_from,
            added=added,
            removed_snapshots=removed,
            modified_snapshots=modified_snapshots,
            snapshots=merged_snapshots,
            new_snapshots=new_snapshots,
            previous_plan_id=env.plan_id if env and not is_new_environment else None,
            previously_promoted_snapshot_ids=previously_promoted_snapshot_ids,
            previous_finalized_snapshots=env.previous_finalized_snapshots if env else None,
        )

    @classmethod
    def create_no_diff(cls, environment: str) -> ContextDiff:
        """Create a no-op ContextDiff object.

        Args:
            environment: The environment to diff.

        Returns:
            The ContextDiff object.
        """
        return ContextDiff(
            environment=environment,
            is_new_environment=False,
            is_unfinalized_environment=False,
            create_from="",
            added=set(),
            removed_snapshots={},
            modified_snapshots={},
            snapshots={},
            new_snapshots={},
            previous_plan_id=None,
            previously_promoted_snapshot_ids=set(),
            previous_finalized_snapshots=None,
        )

    @property
    def has_changes(self) -> bool:
        return (
            self.has_snapshot_changes or self.is_new_environment or self.is_unfinalized_environment
        )

    @property
    def has_snapshot_changes(self) -> bool:
        return bool(self.added or self.removed_snapshots or self.modified_snapshots)

    @property
    def added_materialized_snapshot_ids(self) -> t.Set[SnapshotId]:
        """Returns the set of added internal snapshot ids."""
        return {
            s_id
            for s_id in self.added
            if self.snapshots[s_id].model_kind_name
            and self.snapshots[s_id].model_kind_name.is_materialized  # type: ignore
        }

    @property
    def promotable_snapshot_ids(self) -> t.Set[SnapshotId]:
        """The set of snapshot ids that have to be promoted in the target environment."""
        return {
            *self.previously_promoted_snapshot_ids,
            *self.added,
            *self.current_modified_snapshot_ids,
        } - set(self.removed_snapshots)

    @property
    def unpromoted_models(self) -> t.Set[SnapshotId]:
        """The set of snapshot IDs that have not yet been promoted in the target environment."""
        return set(self.snapshots) - self.previously_promoted_snapshot_ids

    @property
    def current_modified_snapshot_ids(self) -> t.Set[SnapshotId]:
        return {current.snapshot_id for current, _ in self.modified_snapshots.values()}

    @cached_property
    def snapshots_by_name(self) -> t.Dict[str, Snapshot]:
        return {x.name: x for x in self.snapshots.values()}

    @property
    def environment_snapshots(self) -> t.List[SnapshotTableInfo]:
        """Returns current snapshots in the environment."""
        return [
            *self.removed_snapshots.values(),
            *(old.table_info for _, old in self.modified_snapshots.values()),
            *[
                s.table_info
                for s_id, s in self.snapshots.items()
                if s_id not in self.added and s.name not in self.modified_snapshots
            ],
        ]

    def directly_modified(self, name: str) -> bool:
        """Returns whether or not a node was directly modified in this context.

        Args:
            name: The snapshot name to check.

        Returns:
            Whether or not the node was directly modified.
        """

        if name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[name]
        return current.fingerprint.data_hash != previous.fingerprint.data_hash

    def indirectly_modified(self, name: str) -> bool:
        """Returns whether or not a node was indirectly modified in this context.

        Args:
            name: The snapshot name to check.

        Returns:
            Whether or not the node was indirectly modified.
        """

        if name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[name]
        return (
            current.fingerprint.data_hash == previous.fingerprint.data_hash
            and current.fingerprint.parent_data_hash != previous.fingerprint.parent_data_hash
        )

    def metadata_updated(self, name: str) -> bool:
        """Returns whether or not the given node's metadata has been updated.

        Args:
            name: The node to check.

        Returns:
            Whether or not the node's metadata has been updated.
        """

        if name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[name]
        return current.fingerprint.metadata_hash != previous.fingerprint.metadata_hash

    def text_diff(self, name: str) -> str:
        """Finds the difference of a node between the current and remote environment.

        Args:
            name: The Snapshot name.

        Returns:
            A unified text diff of the node.
        """
        if name not in self.snapshots_by_name:
            raise SQLMeshError(f"`{name}` does not exist.")
        if name not in self.modified_snapshots:
            return ""

        new, old = self.modified_snapshots[name]
        return old.node.text_diff(new.node)
