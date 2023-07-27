"""
# ContextDiff

ContextDiff encapsulates the differences between two environments. The two environments can be the local
environment and a remote environment, or two remote environments. ContextDiff is an important part of
SQLMesh. SQLMesh plans use ContextDiff to determine what models were changed between two environments.
The SQLMesh CLI diff command uses ContextDiff to determine what to visualize.

When creating a ContextDiff object, SQLMesh will compare the snapshots from one environment with those of
another remote environment and determine if models have been added, removed, or modified.
"""
from __future__ import annotations

import typing as t

from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotDataVersion,
    SnapshotId,
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
    added: t.Set[str]
    """New models."""
    removed: t.Set[str]
    """Deleted models."""
    modified_snapshots: t.Dict[str, t.Tuple[Snapshot, Snapshot]]
    """Modified snapshots."""
    snapshots: t.Dict[str, Snapshot]
    """Merged snapshots."""
    new_snapshots: t.Dict[SnapshotId, Snapshot]
    """New snapshots."""
    previous_plan_id: t.Optional[str]
    """Previous plan id."""
    previously_promoted_model_names: t.Set[str]
    """Models that were promoted by the previous plan."""

    @classmethod
    def create(
        cls,
        environment: str,
        snapshots: t.Dict[str, Snapshot],
        create_from: str,
        state_reader: StateReader,
    ) -> ContextDiff:
        """Create a ContextDiff object.

        Args:
            environment: The remote environment to diff.
            snapshots: The snapshots of the current environment.
            create_from: The environment to create the target environment from if it
                doesn't exist.
            state_reader: StateReader to access the remote environment to diff.

        Returns:
            The ContextDiff object.
        """
        environment = environment.lower()
        env = state_reader.get_environment(environment)

        if env is None:
            env = state_reader.get_environment(create_from.lower())
            is_new_environment = True
            previously_promoted_model_names = set()
        else:
            is_new_environment = False
            previously_promoted_model_names = {s.name for s in env.promoted_snapshots}

        existing_info = {info.name: info for info in (env.snapshots if env else [])}
        existing_models = set(existing_info)
        current_models = set(snapshots)
        removed = existing_models - current_models
        added = current_models - existing_models
        modified_info = {
            name: existing_info[name]
            for name, snapshot in snapshots.items()
            if name not in added and snapshot.fingerprint != existing_info[name].fingerprint
        }

        modified_local_seed_snapshot_ids = {
            s.snapshot_id for s in snapshots.values() if s.is_seed and s.name in modified_info
        }
        modified_remote_snapshot_ids = {s.snapshot_id for s in modified_info.values()}

        stored = {
            **state_reader.get_snapshots(
                [
                    snapshot.snapshot_id
                    for snapshot in snapshots.values()
                    if snapshot.snapshot_id not in modified_local_seed_snapshot_ids
                ]
            ),
            **state_reader.get_snapshots(
                modified_remote_snapshot_ids | modified_local_seed_snapshot_ids, hydrate_seeds=True
            ),
        }

        merged_snapshots = {}
        modified_snapshots = {}
        new_snapshots = {}
        snapshot_remote_versions: t.Dict[str, t.Tuple[t.Tuple[SnapshotDataVersion, ...], int]] = {}

        for name, snapshot in snapshots.items():
            modified = modified_info.get(name)
            existing = stored.get(snapshot.snapshot_id)

            if existing:
                # Keep the original model instance to preserve the query cache.
                existing.node = snapshot.node

                merged_snapshots[name] = existing.copy()
                if modified:
                    modified_snapshots[name] = (existing, stored[modified.snapshot_id])
                    for child, versions in existing.indirect_versions.items():
                        existing_versions = snapshot_remote_versions.get(child)
                        if not existing_versions or existing_versions[1] < existing.created_ts:
                            snapshot_remote_versions[child] = (
                                versions,
                                existing.created_ts,
                            )
            else:
                snapshot = snapshot.copy()
                merged_snapshots[name] = snapshot
                new_snapshots[snapshot.snapshot_id] = snapshot
                if modified:
                    snapshot.previous_versions = modified.all_versions
                    modified_snapshots[name] = (snapshot, stored[modified.snapshot_id])

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
            removed=removed,
            modified_snapshots=modified_snapshots,
            snapshots=merged_snapshots,
            new_snapshots=new_snapshots,
            previous_plan_id=env.plan_id if env and not is_new_environment else None,
            previously_promoted_model_names=previously_promoted_model_names,
        )

    @property
    def has_changes(self) -> bool:
        return (
            self.has_snapshot_changes or self.is_new_environment or self.is_unfinalized_environment
        )

    @property
    def has_snapshot_changes(self) -> bool:
        return bool(self.added or self.removed or self.modified_snapshots)

    @property
    def added_materialized_models(self) -> t.Set[str]:
        """Returns the set of added internal models."""
        return {name for name in self.added if self.snapshots[name].model_kind_name.is_materialized}

    @property
    def promotable_models(self) -> t.Set[str]:
        """The set of model names that have to be promoted in the target environment."""
        return {
            *self.previously_promoted_model_names,
            *self.added,
            *self.modified_snapshots,
        } - self.removed

    def directly_modified(self, model_name: str) -> bool:
        """Returns whether or not a model was directly modified in this context.

        Args:
            model_name: The model name to check.

        Returns:
            Whether or not the model was directly modified.
        """

        if model_name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[model_name]
        return current.fingerprint.data_hash != previous.fingerprint.data_hash

    def indirectly_modified(self, model_name: str) -> bool:
        """Returns whether or not a model was indirectly modified in this context.

        Args:
            model_name: The model name to check.

        Returns:
            Whether or not the model was indirectly modified.
        """

        if model_name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[model_name]
        return (
            current.fingerprint.data_hash == previous.fingerprint.data_hash
            and current.fingerprint.parent_data_hash != previous.fingerprint.parent_data_hash
        )

    def metadata_updated(self, model_name: str) -> bool:
        """Returns whether or not the given model's metadata has been updated.

        Args:
            model_name: The model name to check.

        Returns:
            Whether or not the model's metadata has been updated.
        """

        if model_name not in self.modified_snapshots:
            return False

        current, previous = self.modified_snapshots[model_name]
        return current.fingerprint.metadata_hash != previous.fingerprint.metadata_hash

    def text_diff(self, model: str) -> str:
        """Finds the difference of a model between the current and remote environment.

        Args:
            model: The model name.

        Returns:
            A unified text diff of the model.
        """
        if model not in self.snapshots:
            raise SQLMeshError(f"`{model}` does not exist.")
        if model not in self.modified_snapshots:
            return ""

        new, old = self.modified_snapshots[model]
        return old.model.text_diff(new.model)
