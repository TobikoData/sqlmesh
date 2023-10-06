from __future__ import annotations

import logging
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
from sqlmesh.core.console import SNAPSHOT_CHANGE_CATEGORY_STR
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotChangeCategory,
    categorize_change,
    earliest_start_date,
    merge_intervals,
    missing_intervals,
)
from sqlmesh.core.snapshot.definition import Interval, format_intervals, start_date
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


SnapshotMapping = t.Dict[str, t.Set[str]]


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
        no_gaps: bool = False,
        skip_backfill: bool = False,
        is_dev: bool = False,
        forward_only: bool = False,
        environment_ttl: t.Optional[str] = None,
        environment_suffix_target: EnvironmentSuffixTarget = EnvironmentSuffixTarget.default,
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
        self.categorizer_config = categorizer_config or CategorizerConfig()
        self.auto_categorization_enabled = auto_categorization_enabled
        self.include_unmodified = include_unmodified
        self._restate_models = set(restate_models or [])
        self._effective_from: t.Optional[TimeLike] = None
        self._start = (
            start if start or not (is_dev and forward_only) else (default_start or yesterday_ds())
        )
        self._end = end if end or not is_dev else (default_end or now())
        self._execution_time = execution_time or now()
        self._apply = apply
        self.__missing_intervals: t.Optional[t.Dict[t.Tuple[str, str], Intervals]] = None
        self._categorized: t.Optional[t.List[Snapshot]] = None
        self._uncategorized: t.Optional[t.List[Snapshot]] = None

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
        self.set_start(new_start)
        self.override_start = True

    def set_start(self, new_start: TimeLike) -> None:
        self._start = new_start
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
                snapshot_name=snapshot.name,
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
    def _snapshot_mapping(self) -> t.Dict[str, Snapshot]:
        """Gets a mapping of snapshot name to snapshot."""
        return self.__snapshot_mapping

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
                s.snapshot_id for s in snapshots if s.name in self.context_diff.promotable_models
            ]

        return Environment(
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
        return EnvironmentNamingInfo(
            name=self.context_diff.environment, suffix_target=self.environment_suffix_target
        )

    @property
    def restatements(self) -> t.Dict[str, Interval]:
        return self._restatements

    @property
    def loaded_snapshot_intervals(self) -> t.List[LoadedSnapshotIntervals]:
        loaded_snapshots = []
        for snapshot in self.directly_modified:
            if not snapshot.change_category:
                continue
            loaded_snapshots.append(LoadedSnapshotIntervals.from_snapshot(snapshot))
            for downstream_indirect in self.indirectly_modified.get(snapshot.name, set()):
                downstream_snapshot = self._snapshot_mapping[downstream_indirect]
                # We don't want to display indirect non-breaking since to users these are effectively no-op changes
                if downstream_snapshot.is_indirect_non_breaking:
                    continue
                loaded_snapshots.append(LoadedSnapshotIntervals.from_snapshot(downstream_snapshot))
        return loaded_snapshots

    @property
    def has_changes(self) -> bool:
        modified_names = {
            *self.context_diff.added,
            *self.context_diff.removed_snapshots,
            *self.context_diff.modified_snapshots,
        } - self.ignored_snapshot_names
        return (
            self.context_diff.is_new_environment
            or self.context_diff.is_unfinalized_environment
            or bool(modified_names)
        )

    def is_new_snapshot(self, snapshot: Snapshot) -> bool:
        """Returns True if the given snapshot is a new snapshot in this plan."""
        return snapshot.snapshot_id in {s.snapshot_id for s in self.new_snapshots}

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

        for child in self.indirectly_modified[snapshot.name]:
            child_snapshot = self._snapshot_mapping[child]
            # If the snapshot isn't new then we are reverting to a previously existing snapshot
            # and therefore we don't want to recategorize it.
            if not self.is_new_snapshot(child_snapshot):
                continue

            is_forward_only_child = self._is_forward_only_model(child)

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

            snapshot.indirect_versions[child] = child_snapshot.all_versions

            # If any other snapshot specified breaking this child, then that child
            # needs to be backfilled as a part of the plan.
            for upstream in self.directly_modified:
                if child in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child][-1]
                    if data_version.is_new_version:
                        child_snapshot.categorize_as(
                            SnapshotChangeCategory.FORWARD_ONLY
                            if is_forward_only_child
                            else SnapshotChangeCategory.INDIRECT_BREAKING
                        )
                        break

        # Invalidate caches.
        self._categorized = None
        self._uncategorized = None

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
            # we need previous snapshots because this method is cached and users have the option
            # to choose non-breaking / forward only. this will change the version of the snapshot on the fly
            # thus changing the missing intervals. additionally we replace any snapshots with the old copies
            # because they have intervals and the ephemeral ones don't
            snapshots = {
                (snapshot.name, snapshot.version_get_or_generate()): snapshot
                for snapshot in self.snapshots
            }

            for new, old in self.context_diff.modified_snapshots.values():
                # Never override forward-only snapshots to preserve the effect
                # of the effective_from setting. Instead re-merge the intervals.
                if not new.is_forward_only:
                    snapshots[(old.name, old.version_get_or_generate())] = old
                else:
                    new.intervals = []
                    new.merge_intervals(old)

            self.__missing_intervals = {
                (snapshot.name, snapshot.version_get_or_generate()): missing
                for snapshot, missing in missing_intervals(
                    snapshots.values(),
                    start=self._start,
                    end=self._end,
                    execution_time=self._execution_time,
                    restatements=self.restatements,
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
                logger.debug("Restatement is disabled for model '%s'.", snapshot.name)
                return False
            return not snapshot.is_symbolic and not snapshot.is_seed

        restatements: t.Dict[str, Interval] = {}
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
        for snapshot_name in restate_models:
            if snapshot_name not in self._snapshot_mapping or not is_restateable_snapshot(
                self._snapshot_mapping[snapshot_name]
            ):
                raise PlanError(
                    f"Cannot restate from '{snapshot_name}'. Either such model doesn't exist, no other materialized model references it, or restatement was disabled for this model."
                )
            restatements[snapshot_name] = dummy_interval
            for downstream_snapshot_name in self._dag.downstream(snapshot_name):
                if is_restateable_snapshot(self._snapshot_mapping[downstream_snapshot_name]):
                    restatements[downstream_snapshot_name] = dummy_interval
        # Get restatement intervals for all restated snapshots and make sure that if a snapshot expands it's
        # restatement range that it's downstream dependencies all expand their restatement ranges as well.
        for snapshot_name in self._dag:
            if snapshot_name not in restatements:
                continue
            snapshot = self._snapshot_mapping[snapshot_name]
            interval = snapshot.get_removal_interval(
                self.start, self.end, self._execution_time, strict=False
            )
            # Since we are traversing the graph in topological order and the largest interval range is pushed down
            # the graph we just have to check our immediate parents in the graph and not the whole upstream graph.
            snapshot_dependencies = snapshot.node.depends_on
            possible_intervals = [
                restatements.get(s, dummy_interval) for s in snapshot_dependencies
            ] + [interval]
            snapshot_start = min(i[0] for i in possible_intervals)
            snapshot_end = max(i[1] for i in possible_intervals)
            restatements[snapshot_name] = (snapshot_start, snapshot_end)
        self._restatements = restatements

    def _build_directly_and_indirectly_modified(self) -> t.Tuple[t.List[Snapshot], SnapshotMapping]:
        """Builds collections of directly and indirectly modified snapshots.

        Returns:
            The tuple in which the first element contains a list of added and directly modified
            snapshots while the second element contains a mapping of indirectly modified snapshots.
        """
        directly_modified = []
        all_indirectly_modified = set()

        for name, snapshot in self._snapshot_mapping.items():
            if name in self.context_diff.modified_snapshots:
                if self.context_diff.directly_modified(name):
                    directly_modified.append(snapshot)
                else:
                    all_indirectly_modified.add(name)
            elif name in self.context_diff.added:
                directly_modified.append(snapshot)

        indirectly_modified: SnapshotMapping = defaultdict(set)
        for snapshot in directly_modified:
            for downstream in self._dag.downstream(snapshot.name):
                if downstream in all_indirectly_modified:
                    indirectly_modified[snapshot.name].add(downstream)

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
        for name in self._dag:
            snapshot = self._snapshot_mapping.get(name)
            if not snapshot or snapshot.change_category:
                continue

            if name in self.context_diff.modified_snapshots:
                is_directly_modified = self.context_diff.directly_modified(name)
                if self.is_new_snapshot(snapshot):
                    if self.forward_only:
                        # In case of the forward only plan any modifications result in reuse of the
                        # previous version for non-seed models.
                        # New snapshots of seed models are considered non-breaking ones.
                        if not snapshot.is_seed:
                            snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
                        else:
                            snapshot.categorize_as(SnapshotChangeCategory.NON_BREAKING)
                    elif self._is_forward_only_model(name) and is_directly_modified:
                        self.set_choice(snapshot, SnapshotChangeCategory.FORWARD_ONLY)
                    elif self.auto_categorization_enabled and is_directly_modified:
                        model_with_missing_columns: t.Optional[str] = None
                        this_node_with_downstream = self.indirectly_modified.get(name, set()) | {
                            name
                        }
                        for downstream in this_node_with_downstream:
                            downstream_snapshot = self._snapshot_mapping[downstream]
                            if (
                                downstream_snapshot.is_model
                                and downstream_snapshot.model.columns_to_types is None
                            ):
                                model_with_missing_columns = downstream
                                break

                        new, old = self.context_diff.modified_snapshots[name]
                        if model_with_missing_columns is None:
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
                        self.context_diff.directly_modified(upstream)
                        and not self._snapshot_mapping[upstream].version
                        for upstream in self._dag.upstream(name)
                    )
                ):
                    snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)

            elif name in self.context_diff.added and self.is_new_snapshot(snapshot):
                snapshot.categorize_as(
                    SnapshotChangeCategory.FORWARD_ONLY
                    if self._is_forward_only_model(name)
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
                and not candidate.is_forward_only
                and not candidate.is_indirect_non_breaking
                and (
                    promoted.version == candidate.version
                    or candidate.data_version in promoted.previous_versions
                )
            ):
                raise PlanError(
                    f"Detected an existing version of model '{name}' that has been previously superseded by a forward-only change. "
                    "To proceed with the change, restamp this model's definition to produce a new version."
                )

    def _ensure_no_broken_references(self) -> None:
        for snapshot in self.context_diff.snapshots.values():
            broken_references = set(self.context_diff.removed_snapshots) & snapshot.node.depends_on
            if broken_references:
                raise PlanError(
                    f"Removed {broken_references} are referenced in '{snapshot.name}'. Please remove broken references before proceeding."
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
            self._dag,
            self._snapshots,
            self._new_snapshots,
            self.__snapshot_mapping,
            self.ignored_snapshot_names,
        ) = self._build_snapshots_and_dag()

        if self._restate_models and self.new_snapshots:
            raise PlanError(
                "Model changes and restatements can't be a part of the same plan. "
                "Revert or apply changes before proceeding with restatements."
            )

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

    def _build_snapshots_and_dag(
        self,
    ) -> t.Tuple[DAG[str], t.List[Snapshot], t.List[Snapshot], t.Dict[str, Snapshot], t.Set[str]]:
        ignored_snapshot_names: t.Set[str] = set()
        full_dag: DAG[str] = DAG()
        filtered_dag: DAG[str] = DAG()
        filtered_snapshots = []
        filtered_new_snapshots = []
        filtered_snapshot_mapping = {}
        cache: t.Optional[t.Dict[str, datetime]] = {}
        for name, context_snapshot in self.context_diff.snapshots.items():
            full_dag.add(name, context_snapshot.node.depends_on)
        for snapshot_name in full_dag:
            snapshot = self.context_diff.snapshots.get(snapshot_name)
            # If the snapshot doesn't exist then it must be an external model
            if not snapshot:
                continue
            if snapshot.is_valid_start(
                self._start, start_date(snapshot, self.context_diff.snapshots.values(), cache)
            ) and snapshot.node.depends_on.isdisjoint(ignored_snapshot_names):
                filtered_dag.add(snapshot_name, snapshot.node.depends_on)
                filtered_snapshots.append(snapshot)
                filtered_snapshot_mapping[snapshot_name] = snapshot
                if snapshot.snapshot_id in self.context_diff.new_snapshots:
                    filtered_new_snapshots.append(snapshot)
            else:
                ignored_snapshot_names.add(snapshot_name)
        return (
            filtered_dag,
            filtered_snapshots,
            filtered_new_snapshots,
            filtered_snapshot_mapping,
            ignored_snapshot_names,
        )

    def _is_forward_only_model(self, model_name: str) -> bool:
        def _is_forward_only_expected(snapshot: Snapshot) -> bool:
            # Returns True if the snapshot is not categorized yet but is expected
            # to be categorized as forward-only. Checking the previous versions to make
            # sure that the snapshot doesn't represent a newly added model.
            return (
                snapshot.node.is_model
                and snapshot.model.forward_only
                and not snapshot.change_category
                and bool(snapshot.previous_versions)
            )

        snapshot = self._snapshot_mapping[model_name]
        if _is_forward_only_expected(snapshot):
            return True

        for upstream in self._dag.upstream(model_name):
            upstream_snapshot = self._snapshot_mapping.get(upstream)
            if (
                upstream_snapshot
                and upstream_snapshot.is_paused
                and (
                    upstream_snapshot.is_forward_only
                    or _is_forward_only_expected(upstream_snapshot)
                )
            ):
                return True
        return False


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
    snapshot_name: str
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_intervals(self, unit: t.Optional[IntervalUnit] = None) -> str:
        return format_intervals(self.merged_intervals, unit)


@dataclass
class LoadedSnapshotIntervals(SnapshotIntervals):
    change_category: SnapshotChangeCategory
    interval_unit: t.Optional[IntervalUnit]
    node_name: str
    view_name: t.Optional[str] = None

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot) -> LoadedSnapshotIntervals:
        assert snapshot.change_category
        return cls(
            snapshot_name=snapshot.name,
            intervals=snapshot.dev_intervals
            if snapshot.change_category.is_forward_only
            else snapshot.intervals,
            interval_unit=snapshot.node.interval_unit,
            node_name=snapshot.node.name,
            view_name=snapshot.model.view_name if snapshot.is_model else None,
            change_category=snapshot.change_category,
        )

    @property
    def change_category_str(self) -> str:
        return SNAPSHOT_CHANGE_CATEGORY_STR[self.change_category]
