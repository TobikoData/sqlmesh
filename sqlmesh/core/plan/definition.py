from __future__ import annotations

import logging
import re
import sys
import typing as t
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from sqlmesh.core.config import (
    AutoCategorizationMode,
    CategorizerConfig,
    EnvironmentSuffixTarget,
)
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Intervals,
    Snapshot,
    SnapshotChangeCategory,
    categorize_change,
    earliest_start_date,
    merge_intervals,
    missing_intervals,
)
from sqlmesh.core.snapshot.definition import (
    Interval,
    SnapshotId,
    format_intervals,
    start_date,
)
from sqlmesh.utils import random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    now,
    to_datetime,
    to_timestamp,
    validate_date_range,
    yesterday_ds,
)
from sqlmesh.utils.errors import NoChangesPlanError, PlanError, SQLMeshError

logger = logging.getLogger(__name__)


SnapshotMapping = t.Dict[SnapshotId, t.Set[SnapshotId]]


class Plan:
    """Plan is the main class to represent user choices on how they want to backfill and version their nodes.

    Args:
        context_diff: The context diff that the plan is based on.
        start: The start time to backfill data.
        end: The end time to backfill data.
        execution_time: The date/time time reference to use for execution time. Defaults to now.
        apply: The callback to apply the plan.
        restate_models: A list of models for which the data should be restated for the time range
            specified in this plan. Note: models defined outside SQLMesh (external) won't be a part
            of the restatement.
        backfill_models: A list of fully qualified model names for which the data should be backfilled as part of this plan.
        no_gaps:  Whether to ensure that new snapshots for nodes that are already a
            part of the target environment have no data gaps when compared against previous
            snapshots for same nodes.
        skip_backfill: Whether to skip the backfill step.
        is_dev: Whether this plan is for development purposes.
        forward_only: Whether the purpose of the plan is to make forward only changes.
        environment_ttl: The period of time that a development environment should exist before being deleted.
        categorizer_config: Auto categorization settings.
        auto_categorization_enabled: Whether to apply auto categorization.
        effective_from: The effective date from which to apply forward-only changes on production.
        include_unmodified: Indicates whether to include unmodified nodes in the target development environment.
        environment_suffix_target: Indicates whether to append the environment name to the schema or table name.
        default_start: The default plan start to use if not specified.
        default_end: The default plan end to use if not specified.
    """

    def __init__(
        self,
        context_diff: ContextDiff,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        apply: t.Optional[t.Callable[[Plan], None]] = None,
        restate_models: t.Optional[t.Iterable[str]] = None,
        backfill_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        is_dev: bool = False,
        forward_only: bool = False,
        environment_ttl: t.Optional[str] = None,
        environment_suffix_target: EnvironmentSuffixTarget = EnvironmentSuffixTarget.default,
        environment_catalog_mapping: t.Optional[t.Dict[re.Pattern, str]] = None,
        categorizer_config: t.Optional[CategorizerConfig] = None,
        auto_categorization_enabled: bool = True,
        effective_from: t.Optional[TimeLike] = None,
        include_unmodified: bool = False,
        default_start: t.Optional[TimeLike] = None,
        default_end: t.Optional[TimeLike] = None,
    ):
        self.context_diff = context_diff
        self.override_start = start is not None
        self.override_end = end is not None
        self.plan_id: str = random_id()
        self.no_gaps = no_gaps
        self.skip_backfill = skip_backfill
        self.is_dev = is_dev
        self.forward_only = forward_only
        self.environment_ttl = environment_ttl
        self.environment_suffix_target = environment_suffix_target
        self.environment_catalog_mapping = environment_catalog_mapping or {}
        self.categorizer_config = categorizer_config or CategorizerConfig()
        self.auto_categorization_enabled = auto_categorization_enabled
        self.include_unmodified = include_unmodified

        self.__snapshot_mapping: t.Optional[t.Dict[SnapshotId, Snapshot]] = None
        self.__model_fqn_to_snapshot: t.Optional[t.Dict[str, Snapshot]] = None
        self.__dag: t.Optional[DAG[SnapshotId]] = None

        self._start = start
        if not self._start and is_dev and forward_only:
            self._start = default_start or yesterday_ds()

        self._end = end if end or not is_dev else (default_end or now())
        self._restate_models = set(restate_models or [])
        self._effective_from: t.Optional[TimeLike] = None
        self._execution_time = execution_time or now()
        self._apply = apply
        self.__missing_intervals: t.Optional[t.Dict[t.Tuple[str, str], Intervals]] = None
        self._categorized: t.Optional[t.List[Snapshot]] = None
        self._uncategorized: t.Optional[t.List[Snapshot]] = None
        self._modified_snapshots: t.Optional[t.Dict[SnapshotId, Snapshot]] = None

        self._input_backfill_models = backfill_models
        self._models_to_backfill: t.Optional[t.Set[str]] = None

        self._refresh_dag_and_ignored_snapshots()

        if effective_from:
            self._set_effective_from(effective_from)

    @property
    def categorized(self) -> t.List[Snapshot]:
        """Returns the already categorized snapshots."""
        if self._categorized is None:
            self._categorized = [s for s in self.directly_modified if s.version]
        return self._categorized

    @property
    def uncategorized(self) -> t.List[Snapshot]:
        """Returns the uncategorized snapshots."""
        if self._uncategorized is None:
            self._uncategorized = [s for s in self.directly_modified if not s.version]
        return self._uncategorized

    @property
    def modified_snapshots(self) -> t.Dict[SnapshotId, Snapshot]:
        """Returns the modified (either directly or indirectly) snapshots."""
        if self.__snapshot_mapping is None:
            raise SQLMeshError("Internal Error: Tried to access modified snapshots before init")
        if self._modified_snapshots is None:
            self._modified_snapshots = {
                **{s.snapshot_id: s for s in self.directly_modified},
                **{
                    s_id: self.__snapshot_mapping[s_id]
                    for downstream_s_ids in self.indirectly_modified.values()
                    for s_id in downstream_s_ids
                },
            }
        return self._modified_snapshots

    @property
    def start(self) -> TimeLike:
        """Returns the start of the plan or the earliest date of all snapshots."""
        if not self.override_start and not self._missing_intervals:
            earliest_start = earliest_start_date(self.snapshots)
            earliest_interval_starts = [s.intervals[0][0] for s in self.snapshots if s.intervals]
            return (
                min(earliest_start, to_datetime(min(earliest_interval_starts)))
                if earliest_interval_starts
                else earliest_start
            )
        return self._start or (
            min(
                start
                for intervals_per_node in self._missing_intervals.values()
                for start, _ in intervals_per_node
            )
            if self._missing_intervals
            else yesterday_ds()
        )

    @start.setter
    def start(self, new_start: TimeLike) -> None:
        self._ensure_valid_date_range(new_start, self._end)
        self._start = new_start
        self.override_start = True
        self.__missing_intervals = None
        self._refresh_dag_and_ignored_snapshots()

    @property
    def end(self) -> TimeLike:
        """Returns the end of the plan or now."""
        return self._end or now()

    @end.setter
    def end(self, new_end: TimeLike) -> None:
        self._end = new_end
        self.override_end = True
        self._refresh_dag_and_ignored_snapshots()

    @property
    def is_start_and_end_allowed(self) -> bool:
        """Indicates whether this plan allows to set the start and end dates."""
        return self.is_dev or bool(self.restatements)

    @property
    def requires_backfill(self) -> bool:
        return not self.skip_backfill and (bool(self.restatements) or bool(self.missing_intervals))

    @property
    def missing_intervals(self) -> t.List[SnapshotIntervals]:
        """Returns a list of missing intervals."""
        missing_intervals = (
            SnapshotIntervals(
                snapshot_id=snapshot.snapshot_id,
                intervals=self._missing_intervals.get(
                    (snapshot.name, snapshot.version_get_or_generate())
                )
                or [],
            )
            for snapshot in self.snapshots
            if snapshot.is_model
        )

        return [interval for interval in missing_intervals if interval.intervals]

    @property
    def snapshots(self) -> t.List[Snapshot]:
        """Gets all the snapshots in the plan/environment."""
        return self._snapshots

    @property
    def snapshot_mapping(self) -> t.Dict[SnapshotId, Snapshot]:
        """Gets a mapping of snapshot ID to snapshot."""
        return self.__snapshot_mapping or self.context_diff.snapshots

    @property
    def _model_fqn_to_snapshot(self) -> t.Dict[str, Snapshot]:
        if self.__model_fqn_to_snapshot is None:
            self.__model_fqn_to_snapshot = {s.name: s for s in self._snapshots}
        return self.__model_fqn_to_snapshot

    @property
    def _dag(self) -> DAG[SnapshotId]:
        if self.__dag is None:
            self.__dag = DAG()
            for snapshot in self.snapshot_mapping.values():
                self.__dag.add(snapshot.snapshot_id, snapshot.parents)
        return self.__dag

    @property
    def new_snapshots(self) -> t.List[Snapshot]:
        """Gets only new snapshots in the plan/environment."""
        return self._new_snapshots

    @property
    def environment(self) -> Environment:
        """The environment of the plan."""
        expiration_ts = (
            to_timestamp(self.environment_ttl, relative_base=now())
            if self.is_dev and self.environment_ttl is not None
            else None
        )

        snapshots = [s.table_info for s in self.snapshots]
        promoted_snapshot_ids = None
        if self.is_dev and not self.include_unmodified:
            promoted_snapshot_ids = [
                s.snapshot_id
                for s in snapshots
                if s.snapshot_id in self.context_diff.promotable_snapshot_ids
            ]

        return Environment.from_environment_catalog_mapping(
            self.environment_catalog_mapping,
            name=self.context_diff.environment,
            snapshots=snapshots,
            start_at=self.start,
            end_at=self._end,
            plan_id=self.plan_id,
            previous_plan_id=self.context_diff.previous_plan_id,
            expiration_ts=expiration_ts,
            promoted_snapshot_ids=promoted_snapshot_ids,
            suffix_target=self.environment_suffix_target,
        )

    @property
    def environment_naming_info(self) -> EnvironmentNamingInfo:
        return EnvironmentNamingInfo.from_environment_catalog_mapping(
            self.environment_catalog_mapping,
            name=self.context_diff.environment,
            suffix_target=self.environment_suffix_target,
        )

    @property
    def restatements(self) -> t.Dict[SnapshotId, Interval]:
        return self._restatements

    @property
    def models_to_backfill(self) -> t.Optional[t.Set[str]]:
        """Names of the models that should be backfilled as part of this plan. None means all models."""
        return self._models_to_backfill

    @property
    def has_changes(self) -> bool:
        modified_snapshot_ids = {
            *self.context_diff.added,
            *self.context_diff.removed_snapshots,
            *self.context_diff.current_modified_snapshot_ids,
        } - self.ignored_snapshot_ids
        return (
            self.context_diff.is_new_environment
            or self.context_diff.is_unfinalized_environment
            or bool(modified_snapshot_ids)
        )

    def is_new_snapshot(self, snapshot: Snapshot) -> bool:
        """Returns True if the given snapshot is a new snapshot in this plan."""
        return snapshot.snapshot_id in {s.snapshot_id for s in self.new_snapshots}

    def is_selected_for_backfill(self, model_fqn: str) -> bool:
        """Returns True if a model with the given FQN should be backfilled as part of this plan."""
        return self._models_to_backfill is None or model_fqn in self._models_to_backfill

    def apply(self) -> None:
        """Runs apply if an apply function was passed in."""
        if not self._apply:
            raise SQLMeshError(f"Plan was not initialized with an applier.")
        validate_date_range(self.start, self.end)
        self._apply(self)

    def set_choice(self, snapshot: Snapshot, choice: SnapshotChangeCategory) -> None:
        """Sets a snapshot version based on the user choice.

        Args:
            snapshot: The target snapshot.
            choice: The user decision on how to version the target snapshot and its children.
        """
        if self.forward_only:
            raise PlanError("Choice setting is not supported by a forward-only plan.")
        if not self.is_new_snapshot(snapshot):
            raise SQLMeshError(
                f"A choice can't be changed for the existing version of '{snapshot.name}'."
            )

        snapshot.categorize_as(choice)

        is_breaking_choice = choice in (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        )

        for child_s_id in self.indirectly_modified[snapshot.snapshot_id]:
            child_snapshot = self.snapshot_mapping[child_s_id]
            # If the snapshot isn't new then we are reverting to a previously existing snapshot
            # and therefore we don't want to recategorize it.
            if not self.is_new_snapshot(child_snapshot):
                continue

            is_forward_only_child = self._is_forward_only_model(child_s_id)

            if is_breaking_choice:
                child_snapshot.categorize_as(
                    SnapshotChangeCategory.FORWARD_ONLY
                    if is_forward_only_child
                    else SnapshotChangeCategory.INDIRECT_BREAKING
                )
            elif choice == SnapshotChangeCategory.FORWARD_ONLY:
                child_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
            else:
                child_snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)

            snapshot.indirect_versions[child_s_id.name] = child_snapshot.all_versions

            for upstream in self.directly_modified:
                if child_s_id.name in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child_s_id.name][-1]
                    if data_version.is_new_version:
                        # If any other snapshot specified breaking this child, then that child
                        # needs to be backfilled as a part of the plan.
                        child_snapshot.categorize_as(
                            SnapshotChangeCategory.FORWARD_ONLY
                            if is_forward_only_child
                            else SnapshotChangeCategory.INDIRECT_BREAKING
                        )
                        break
                    elif (
                        data_version.change_category == SnapshotChangeCategory.FORWARD_ONLY
                        and child_snapshot.is_indirect_non_breaking
                    ):
                        # FORWARD_ONLY takes precedence over INDIRECT_NON_BREAKING.
                        child_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

        # Invalidate caches.
        self._categorized = None
        self._uncategorized = None
        self.__missing_intervals = None

    @property
    def effective_from(self) -> t.Optional[TimeLike]:
        """The effective date for all new snapshots in the plan.

        Note: this is only applicable for forward-only plans.

        Returns:
            The effective date.
        """
        return self._effective_from

    @effective_from.setter
    def effective_from(self, effective_from: t.Optional[TimeLike]) -> None:
        """Sets the effective date for all new snapshots in the plan.

        Note: this is only applicable for forward-only plans.

        Args:
            effective_from: The effective date to set.
        """
        self._set_effective_from(effective_from)
        if effective_from and self.is_dev and not self.override_start:
            self._start = effective_from
            self._refresh_dag_and_ignored_snapshots()

    def _set_effective_from(self, effective_from: t.Optional[TimeLike]) -> None:
        if not self.forward_only:
            raise PlanError("Effective date can only be set for a forward-only plan.")
        if effective_from and to_datetime(effective_from) > now():
            raise PlanError("Effective date cannot be in the future.")

        self.__missing_intervals = None
        self._effective_from = effective_from

        for snapshot in self.new_snapshots:
            if not snapshot.disable_restatement:
                snapshot.effective_from = effective_from

    @property
    def _missing_intervals(self) -> t.Dict[t.Tuple[str, str], Intervals]:
        if self.__missing_intervals is None:
            old_snapshots = {
                (old.name, old.version_get_or_generate()): old
                for _, old in self.context_diff.modified_snapshots.values()
            }

            for new in self.context_diff.new_snapshots.values():
                new.intervals = []
                new.dev_intervals = []
                old = old_snapshots.get((new.name, new.version_get_or_generate()))
                if not old:
                    continue
                new.merge_intervals(old)
                if new.is_forward_only:
                    new.dev_intervals = new.intervals.copy()

            deployability_index = (
                DeployabilityIndex.create(self.snapshots)
                if self.is_dev
                else DeployabilityIndex.all_deployable()
            )

            self.__missing_intervals = {
                (snapshot.name, snapshot.version_get_or_generate()): missing
                for snapshot, missing in missing_intervals(
                    [s for s in self.snapshots if self.is_selected_for_backfill(s.name)],
                    start=self._start,
                    end=self._end,
                    execution_time=self._execution_time,
                    restatements=self.restatements,
                    deployability_index=deployability_index,
                    ignore_cron=True,
                ).items()
            }

        return self.__missing_intervals

    @property
    def has_unmodified_unpromoted(self) -> bool:
        """Is the plan for an existing dev environment, has the include unmodified flag, and contains unmodified nodes that have not been promoted."""
        return (
            self.is_dev
            and not self.context_diff.is_new_environment
            and self.include_unmodified
            and bool(self.context_diff.unpromoted_models)
        )

    def _add_restatements(self) -> None:
        def is_restateable_snapshot(snapshot: Snapshot) -> bool:
            if not self.is_dev and snapshot.disable_restatement:
                return False
            return not snapshot.is_symbolic and not snapshot.is_seed

        restatements: t.Dict[SnapshotId, Interval] = {}
        dummy_interval = (sys.maxsize, -sys.maxsize)
        restate_models = self._restate_models
        if not restate_models and self.is_dev and self.forward_only:
            # Add model names for new forward-only snapshots to the restatement list
            # in order to compute previews.
            restate_models = {
                s.name for s in self.context_diff.new_snapshots.values() if s.is_materialized
            }
        if not restate_models:
            self._restatements = restatements
            return

        # Add restate snapshots and their downstream snapshots
        for model_fqn in restate_models:
            snapshot = self._model_fqn_to_snapshot.get(model_fqn)
            if not snapshot or not is_restateable_snapshot(snapshot):
                raise PlanError(
                    f"Cannot restate from '{model_fqn}'. Either such model doesn't exist, no other materialized model references it, or restatement was disabled for this model."
                )
            restatements[snapshot.snapshot_id] = dummy_interval
            for downstream_s_id in self._dag.downstream(snapshot.snapshot_id):
                if is_restateable_snapshot(self.snapshot_mapping[downstream_s_id]):
                    restatements[downstream_s_id] = dummy_interval
        # Get restatement intervals for all restated snapshots and make sure that if a snapshot expands it's
        # restatement range that it's downstream dependencies all expand their restatement ranges as well.
        for s_id in self._dag:
            if s_id not in restatements:
                continue
            snapshot = self.snapshot_mapping[s_id]
            interval = snapshot.get_removal_interval(
                self.start, self.end, self._execution_time, strict=False
            )
            # Since we are traversing the graph in topological order and the largest interval range is pushed down
            # the graph we just have to check our immediate parents in the graph and not the whole upstream graph.
            snapshot_dependencies = snapshot.parents
            possible_intervals = [
                restatements.get(s, dummy_interval) for s in snapshot_dependencies
            ] + [interval]
            snapshot_start = min(i[0] for i in possible_intervals)
            snapshot_end = max(i[1] for i in possible_intervals)
            restatements[s_id] = (snapshot_start, snapshot_end)
        self._restatements = restatements

    def _build_directly_and_indirectly_modified(self) -> t.Tuple[t.List[Snapshot], SnapshotMapping]:
        """Builds collections of directly and indirectly modified snapshots.

        Returns:
            The tuple in which the first element contains a list of added and directly modified
            snapshots while the second element contains a mapping of indirectly modified snapshots.
        """
        directly_modified = []
        all_indirectly_modified = set()

        for s_id, snapshot in self.snapshot_mapping.items():
            if s_id.name in self.context_diff.modified_snapshots:
                if self.context_diff.directly_modified(s_id.name):
                    directly_modified.append(snapshot)
                else:
                    all_indirectly_modified.add(s_id)
            elif s_id in self.context_diff.added:
                directly_modified.append(snapshot)

        indirectly_modified: SnapshotMapping = defaultdict(set)
        for snapshot in directly_modified:
            for downstream_s_id in self._dag.downstream(snapshot.snapshot_id):
                if downstream_s_id in all_indirectly_modified:
                    indirectly_modified[snapshot.snapshot_id].add(downstream_s_id)

        return (
            directly_modified,
            indirectly_modified,
        )

    def _categorize_snapshots(self) -> None:
        """Automatically categorizes snapshots that can be automatically categorized and
        returns a list of added and directly modified snapshots as well as the mapping of
        indirectly modified snapshots.
        """

        # Iterating in DAG order since a category for a snapshot may depend on the categories
        # assigned to its upstream dependencies.
        for s_id in self._dag:
            snapshot = self.snapshot_mapping.get(s_id)
            if not snapshot or snapshot.change_category:
                continue

            if s_id.name in self.context_diff.modified_snapshots:
                is_directly_modified = self.context_diff.directly_modified(s_id.name)
                if self.is_new_snapshot(snapshot):
                    if self.forward_only:
                        # In case of the forward only plan any modifications result in reuse of the
                        # previous version for non-seed models.
                        # New snapshots of seed models are considered non-breaking ones.
                        if not snapshot.is_seed:
                            snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
                        else:
                            snapshot.categorize_as(SnapshotChangeCategory.NON_BREAKING)
                    elif self._is_forward_only_model(s_id) and is_directly_modified:
                        self.set_choice(snapshot, SnapshotChangeCategory.FORWARD_ONLY)
                    elif self.auto_categorization_enabled and is_directly_modified:
                        s_id_with_missing_columns: t.Optional[SnapshotId] = None
                        this_sid_with_downstream = self.indirectly_modified.get(s_id, set()) | {
                            s_id
                        }
                        for downstream_s_id in this_sid_with_downstream:
                            downstream_snapshot = self.snapshot_mapping[downstream_s_id]
                            if (
                                downstream_snapshot.is_model
                                and downstream_snapshot.model.columns_to_types is None
                            ):
                                s_id_with_missing_columns = downstream_s_id
                                break

                        new, old = self.context_diff.modified_snapshots[s_id.name]
                        if s_id_with_missing_columns is None:
                            change_category = categorize_change(
                                new, old, config=self.categorizer_config
                            )
                            if change_category is not None:
                                self.set_choice(new, change_category)
                        else:
                            mode = self.categorizer_config.dict().get(
                                new.model.source_type, AutoCategorizationMode.OFF
                            )
                            if mode == AutoCategorizationMode.FULL:
                                self.set_choice(new, SnapshotChangeCategory.BREAKING)

                # set to breaking if an indirect child has no directly modified parents
                # that need a decision. this can happen when a revert to a parent causes
                # an indirectly modified snapshot to be created because of a new parent
                if (
                    not is_directly_modified
                    and not snapshot.version
                    and not any(
                        self.context_diff.directly_modified(upstream.name)
                        and not self.snapshot_mapping[upstream].version
                        for upstream in self._dag.upstream(s_id)
                    )
                ):
                    snapshot.categorize_as(
                        SnapshotChangeCategory.FORWARD_ONLY
                        if self._is_forward_only_model(s_id)
                        else SnapshotChangeCategory.INDIRECT_BREAKING
                    )

            elif s_id in self.context_diff.added and self.is_new_snapshot(snapshot):
                snapshot.categorize_as(
                    SnapshotChangeCategory.FORWARD_ONLY
                    if self._is_forward_only_model(s_id)
                    else SnapshotChangeCategory.BREAKING
                )

    def _ensure_valid_date_range(
        self, start: t.Optional[TimeLike], end: t.Optional[TimeLike]
    ) -> None:
        if (start or end) and not self.is_start_and_end_allowed:
            raise PlanError(
                "The start and end dates can't be set for a production plan without restatements."
            )

    def _ensure_no_forward_only_revert(self) -> None:
        """Ensures that a previously superseded breaking / non-breaking snapshot is not being
        used again to replace an existing forward-only snapshot with the same version.

        In other words there is no going back to the original non-forward-only snapshot with
        the same version once a forward-only change for that version has been introduced.
        """
        for name, (candidate, promoted) in self.context_diff.modified_snapshots.items():
            if (
                candidate.snapshot_id not in self.context_diff.new_snapshots
                and promoted.is_forward_only
                and not promoted.is_paused
                and not candidate.is_forward_only
                and not candidate.is_indirect_non_breaking
                and (
                    promoted.version == candidate.version
                    or candidate.data_version in promoted.previous_versions
                )
            ):
                raise PlanError(
                    f"Attempted to revert to an unrevertable version of model '{name}'. Run `sqlmesh plan` again to mitigate the issue."
                )

    def _ensure_no_broken_references(self) -> None:
        for snapshot in self.context_diff.snapshots.values():
            broken_references = {x.name for x in self.context_diff.removed_snapshots} & {
                x.name for x in snapshot.parents
            }
            if broken_references:
                broken_references_msg = ", ".join(f"'{x}'" for x in broken_references)
                raise PlanError(
                    f"""Removed {broken_references_msg} are referenced in '{snapshot.name}'. Please remove broken references before proceeding."""
                )

    def _ensure_new_env_with_changes(self) -> None:
        if (
            self.is_dev
            and not self.include_unmodified
            and self.context_diff.is_new_environment
            and not self.context_diff.has_snapshot_changes
        ):
            raise NoChangesPlanError(
                "No changes were detected. Make a change or run with --include-unmodified to create a new environment without changes."
            )

    def _refresh_dag_and_ignored_snapshots(self) -> None:
        self._restatements = {}
        (
            self.__dag,
            self._snapshots,
            self._new_snapshots,
            self.__snapshot_mapping,
            self.ignored_snapshot_ids,
        ) = self._build_snapshots_and_dag()

        if self._restate_models and self.new_snapshots:
            raise PlanError(
                "Model changes and restatements can't be a part of the same plan. "
                "Revert or apply changes before proceeding with restatements."
            )

        if self._input_backfill_models is not None:
            if not self.is_dev:
                raise PlanError(
                    "Selecting models to backfill is only supported for development environments."
                )
            self._models_to_backfill = {
                self.__snapshot_mapping[s_id].name
                for s_id in self.__dag.subdag(
                    *[
                        self._model_fqn_to_snapshot[m].snapshot_id
                        for m in self._input_backfill_models
                    ]
                ).sorted
            }

        self._add_restatements()
        self.__missing_intervals = None

        self._ensure_new_env_with_changes()
        self._ensure_valid_date_range(self._start, self._end)
        self._ensure_no_forward_only_revert()
        self._ensure_no_broken_references()

        (
            self.directly_modified,
            self.indirectly_modified,
        ) = self._build_directly_and_indirectly_modified()

        self._categorize_snapshots()

        self._categorized = None
        self._uncategorized = None
        self._modified_snapshots = None

    def _build_snapshots_and_dag(
        self,
    ) -> t.Tuple[
        DAG[SnapshotId],
        t.List[Snapshot],
        t.List[Snapshot],
        t.Dict[SnapshotId, Snapshot],
        t.Set[SnapshotId],
    ]:
        ignored_snapshot_ids: t.Set[SnapshotId] = set()
        full_dag: DAG[SnapshotId] = DAG()
        filtered_dag: DAG[SnapshotId] = DAG()
        filtered_snapshots = []
        filtered_new_snapshots = []
        filtered_snapshot_mapping = {}
        cache: t.Optional[t.Dict[str, datetime]] = {}
        for s_id, context_snapshot in self.context_diff.snapshots.items():
            full_dag.add(s_id, context_snapshot.parents)
        for s_id in full_dag:
            snapshot = self.context_diff.snapshots.get(s_id)
            # If the snapshot doesn't exist then it must be an external model
            if not snapshot:
                continue
            if snapshot.is_valid_start(
                self._start, start_date(snapshot, self.context_diff.snapshots.values(), cache)
            ) and set(snapshot.parents).isdisjoint(ignored_snapshot_ids):
                filtered_dag.add(snapshot.snapshot_id, snapshot.parents)
                filtered_snapshots.append(snapshot)
                filtered_snapshot_mapping[snapshot.snapshot_id] = snapshot
                if snapshot.snapshot_id in self.context_diff.new_snapshots:
                    filtered_new_snapshots.append(snapshot)
            else:
                ignored_snapshot_ids.add(snapshot.snapshot_id)
        return (
            filtered_dag,
            filtered_snapshots,
            filtered_new_snapshots,
            filtered_snapshot_mapping,
            ignored_snapshot_ids,
        )

    def _is_forward_only_model(self, s_id: SnapshotId) -> bool:
        snapshot = self.snapshot_mapping[s_id]
        return (
            snapshot.is_model
            and snapshot.model.forward_only
            and not snapshot.change_category
            and bool(snapshot.previous_versions)
        )


class PlanStatus(str, Enum):
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"

    @property
    def is_started(self) -> bool:
        return self == PlanStatus.STARTED

    @property
    def is_failed(self) -> bool:
        return self == PlanStatus.FAILED

    @property
    def is_finished(self) -> bool:
        return self == PlanStatus.FINISHED


# millions of these can be created, pydantic has significant overhead
@dataclass
class SnapshotIntervals:
    snapshot_id: SnapshotId
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_intervals(self, unit: t.Optional[IntervalUnit] = None) -> str:
        return format_intervals(self.merged_intervals, unit)
