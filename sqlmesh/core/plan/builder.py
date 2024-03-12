from __future__ import annotations

import re
import sys
import typing as t
from collections import defaultdict
from datetime import datetime
from functools import cached_property

from sqlmesh.core.config import (
    AutoCategorizationMode,
    CategorizerConfig,
    EnvironmentSuffixTarget,
)
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import Plan, SnapshotMapping, earliest_interval_start
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
    categorize_change,
)
from sqlmesh.core.snapshot.definition import Interval, SnapshotId, start_date
from sqlmesh.utils import random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now, to_datetime, yesterday_ds
from sqlmesh.utils.errors import NoChangesPlanError, PlanError, SQLMeshError


class PlanBuilder:
    """Plan Builder constructs a Plan based on user choices for how they want to backfill, preview, etc. their changes.

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
        enable_preview: Whether to enable preview for forward-only models in development environments.
        end_bounded: If set to true, the missing intervals will be bounded by the target end date, disregarding lookback,
            allow_partials, and other attributes that could cause the intervals to exceed the target end date.
        ensure_finalized_snapshots: Whether to compare against snapshots from the latest finalized
            environment state, or to use whatever snapshots are in the current environment state even if
            the environment is not finalized.
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
        enable_preview: bool = False,
        end_bounded: bool = False,
        ensure_finalized_snapshots: bool = False,
    ):
        self._context_diff = context_diff
        self._no_gaps = no_gaps
        self._skip_backfill = skip_backfill
        self._is_dev = is_dev
        self._forward_only = forward_only
        self._enable_preview = enable_preview
        self._end_bounded = end_bounded
        self._ensure_finalized_snapshots = ensure_finalized_snapshots
        self._environment_ttl = environment_ttl
        self._categorizer_config = categorizer_config or CategorizerConfig()
        self._auto_categorization_enabled = auto_categorization_enabled
        self._include_unmodified = include_unmodified
        self._restate_models = set(restate_models or [])
        self._effective_from = effective_from
        self._execution_time = execution_time
        self._backfill_models = backfill_models
        self._end = end or default_end
        self._apply = apply

        self._start = start
        if not self._start and self._forward_only_preview_needed:
            self._start = default_start or yesterday_ds()

        self._plan_id: str = random_id()
        self._model_fqn_to_snapshot = {s.name: s for s in self._context_diff.snapshots.values()}

        self.override_start = start is not None
        self.override_end = end is not None
        self.environment_naming_info = EnvironmentNamingInfo.from_environment_catalog_mapping(
            environment_catalog_mapping or {},
            name=self._context_diff.environment,
            suffix_target=environment_suffix_target,
        )

        self._latest_plan: t.Optional[Plan] = None

    @property
    def is_start_and_end_allowed(self) -> bool:
        """Indicates whether this plan allows to set the start and end dates."""
        return self._is_dev or bool(self._restate_models)

    def set_start(self, new_start: TimeLike) -> PlanBuilder:
        self._start = new_start
        self.override_start = True
        self._latest_plan = None
        return self

    def set_end(self, new_end: TimeLike) -> PlanBuilder:
        self._end = new_end
        self.override_end = True
        self._latest_plan = None
        return self

    def set_effective_from(self, effective_from: t.Optional[TimeLike]) -> PlanBuilder:
        """Sets the effective date for all new snapshots in the plan.

        Note: this is only applicable for forward-only plans.

        Args:
            effective_from: The effective date to set.
        """
        self._effective_from = effective_from
        if effective_from and self._is_dev and not self.override_start:
            self._start = effective_from
        self._latest_plan = None
        return self

    def set_choice(self, snapshot: Snapshot, choice: SnapshotChangeCategory) -> PlanBuilder:
        """Sets a snapshot version based on the user choice.

        Args:
            snapshot: The target snapshot.
            choice: The user decision on how to version the target snapshot and its children.
        """
        plan = self.build()
        self._set_choice(snapshot, choice, plan.directly_modified, plan.indirectly_modified)
        self._adjust_new_snapshot_intervals()
        return self

    def apply(self) -> None:
        """Builds and applies the plan."""
        if not self._apply:
            raise SQLMeshError(f"Plan was not initialized with an applier.")
        self._apply(self.build())

    def build(self) -> Plan:
        """Builds the plan."""
        if self._latest_plan:
            return self._latest_plan

        self._ensure_no_new_snapshots_with_restatements()
        self._ensure_new_env_with_changes()
        self._ensure_valid_date_range()
        self._ensure_no_forward_only_revert()
        self._ensure_no_broken_references()

        self._apply_effective_from()

        dag = self._build_dag()
        directly_modified, indirectly_modified = self._build_directly_and_indirectly_modified(dag)

        self._categorize_snapshots(dag, directly_modified, indirectly_modified)
        self._adjust_new_snapshot_intervals()

        deployability_index = (
            DeployabilityIndex.create(self._context_diff.snapshots.values())
            if self._is_dev
            else DeployabilityIndex.all_deployable()
        )

        filtered_dag, ignored = self._build_filtered_dag(dag, deployability_index)

        # Exclude ignored snapshots from the modified sets.
        directly_modified = {s_id for s_id in directly_modified if s_id not in ignored}
        for s_id in list(indirectly_modified):
            if s_id in ignored:
                indirectly_modified.pop(s_id, None)
            else:
                indirectly_modified[s_id] = {
                    s_id for s_id in indirectly_modified[s_id] if s_id not in ignored
                }

        filtered_snapshots = {
            s.snapshot_id: s
            for s in self._context_diff.snapshots.values()
            if s.snapshot_id not in ignored
        }

        models_to_backfill = self._build_models_to_backfill(filtered_dag)
        restatements = self._build_restatements(
            dag, earliest_interval_start(filtered_snapshots.values())
        )

        plan = Plan(
            context_diff=self._context_diff,
            plan_id=self._plan_id,
            provided_start=self._start,
            provided_end=self._end,
            is_dev=self._is_dev,
            skip_backfill=self._skip_backfill,
            no_gaps=self._no_gaps,
            forward_only=self._forward_only,
            include_unmodified=self._include_unmodified,
            environment_ttl=self._environment_ttl,
            environment_naming_info=self.environment_naming_info,
            directly_modified=directly_modified,
            indirectly_modified=indirectly_modified,
            ignored=ignored,
            deployability_index=deployability_index,
            restatements=restatements,
            models_to_backfill=models_to_backfill,
            effective_from=self._effective_from,
            execution_time=self._execution_time,
            end_bounded=self._end_bounded,
            ensure_finalized_snapshots=self._ensure_finalized_snapshots,
        )
        self._latest_plan = plan
        return plan

    def _build_dag(self) -> DAG[SnapshotId]:
        dag: DAG[SnapshotId] = DAG()
        for s_id, context_snapshot in self._context_diff.snapshots.items():
            dag.add(s_id, context_snapshot.parents)
        return dag

    def _build_filtered_dag(
        self, full_dag: DAG[SnapshotId], deployability_index: DeployabilityIndex
    ) -> t.Tuple[DAG[SnapshotId], t.Set[SnapshotId]]:
        ignored_snapshot_ids: t.Set[SnapshotId] = set()
        filtered_dag: DAG[SnapshotId] = DAG()
        cache: t.Optional[t.Dict[str, datetime]] = {}
        for s_id in full_dag:
            snapshot = self._context_diff.snapshots.get(s_id)
            # If the snapshot doesn't exist then it must be an external model
            if not snapshot:
                continue

            is_deployable = deployability_index.is_deployable(s_id)
            is_valid_start = snapshot.is_valid_start(
                self._start, start_date(snapshot, self._context_diff.snapshots.values(), cache)
            )
            if not is_deployable or (
                is_valid_start and set(snapshot.parents).isdisjoint(ignored_snapshot_ids)
            ):
                filtered_dag.add(s_id, snapshot.parents)
            else:
                ignored_snapshot_ids.add(s_id)
        return filtered_dag, ignored_snapshot_ids

    def _build_restatements(
        self, dag: DAG[SnapshotId], earliest_interval_start: TimeLike
    ) -> t.Dict[SnapshotId, Interval]:
        def is_restateable_snapshot(snapshot: Snapshot) -> bool:
            if not self._is_dev and snapshot.disable_restatement:
                return False
            return not snapshot.is_symbolic and not snapshot.is_seed

        restatements: t.Dict[SnapshotId, Interval] = {}
        dummy_interval = (sys.maxsize, -sys.maxsize)
        restate_models = self._restate_models
        forward_only_preview_needed = self._forward_only_preview_needed
        if not restate_models and forward_only_preview_needed:
            # Add model names for new forward-only snapshots to the restatement list
            # in order to compute previews.
            restate_models = {
                s.name
                for s in self._context_diff.new_snapshots.values()
                if s.is_materialized and (self._forward_only or s.model.forward_only)
            }
        if not restate_models:
            return restatements

        # Add restate snapshots and their downstream snapshots
        for model_fqn in restate_models:
            snapshot = self._model_fqn_to_snapshot.get(model_fqn)
            if not snapshot or (
                not forward_only_preview_needed and not is_restateable_snapshot(snapshot)
            ):
                raise PlanError(
                    f"Cannot restate from '{model_fqn}'. Either such model doesn't exist, no other materialized model references it, or restatement was disabled for this model."
                )
            restatements[snapshot.snapshot_id] = dummy_interval
            for downstream_s_id in dag.downstream(snapshot.snapshot_id):
                if is_restateable_snapshot(self._context_diff.snapshots[downstream_s_id]):
                    restatements[downstream_s_id] = dummy_interval
        # Get restatement intervals for all restated snapshots and make sure that if a snapshot expands it's
        # restatement range that it's downstream dependencies all expand their restatement ranges as well.
        for s_id in dag:
            if s_id not in restatements:
                continue
            snapshot = self._context_diff.snapshots[s_id]
            interval = snapshot.get_removal_interval(
                self._start or earliest_interval_start,
                self._end or now(),
                self._execution_time,
                strict=False,
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
        return restatements

    def _build_directly_and_indirectly_modified(
        self, dag: DAG[SnapshotId]
    ) -> t.Tuple[t.Set[SnapshotId], SnapshotMapping]:
        """Builds collections of directly and indirectly modified snapshots.

        Returns:
            The tuple in which the first element contains a list of added and directly modified
            snapshots while the second element contains a mapping of indirectly modified snapshots.
        """
        directly_modified = set()
        all_indirectly_modified = set()

        for s_id in dag:
            if s_id.name in self._context_diff.modified_snapshots:
                if self._context_diff.directly_modified(s_id.name):
                    directly_modified.add(s_id)
                else:
                    all_indirectly_modified.add(s_id)
            elif s_id in self._context_diff.added:
                directly_modified.add(s_id)

        indirectly_modified: SnapshotMapping = defaultdict(set)
        for snapshot in directly_modified:
            for downstream_s_id in dag.downstream(snapshot.snapshot_id):
                if downstream_s_id in all_indirectly_modified:
                    indirectly_modified[snapshot.snapshot_id].add(downstream_s_id)

        return (
            directly_modified,
            indirectly_modified,
        )

    def _build_models_to_backfill(self, dag: DAG[SnapshotId]) -> t.Optional[t.Set[str]]:
        if self._backfill_models is None:
            return None
        if not self._is_dev:
            raise PlanError(
                "Selecting models to backfill is only supported for development environments."
            )
        return {
            self._context_diff.snapshots[s_id].name
            for s_id in dag.subdag(
                *[
                    self._model_fqn_to_snapshot[m].snapshot_id
                    for m in self._backfill_models
                    if m in self._model_fqn_to_snapshot
                ]
            ).sorted
        }

    def _adjust_new_snapshot_intervals(self) -> None:
        old_snapshots = {
            (old.name, old.version_get_or_generate()): old
            for _, old in self._context_diff.modified_snapshots.values()
        }

        for new in self._context_diff.new_snapshots.values():
            new.intervals = []
            new.dev_intervals = []
            old = old_snapshots.get((new.name, new.version_get_or_generate()))
            if not old:
                continue
            new.merge_intervals(old)
            if new.is_forward_only:
                new.dev_intervals = new.intervals.copy()

    def _categorize_snapshots(
        self,
        dag: DAG[SnapshotId],
        directly_modified: t.Set[SnapshotId],
        indirectly_modified: SnapshotMapping,
    ) -> None:
        """Automatically categorizes snapshots that can be automatically categorized and
        returns a list of added and directly modified snapshots as well as the mapping of
        indirectly modified snapshots.
        """

        # Iterating in DAG order since a category for a snapshot may depend on the categories
        # assigned to its upstream dependencies.
        for s_id in dag:
            snapshot = self._context_diff.snapshots.get(s_id)
            if not snapshot or snapshot.change_category:
                continue

            if s_id.name in self._context_diff.modified_snapshots:
                is_directly_modified = self._context_diff.directly_modified(s_id.name)
                if self._is_new_snapshot(snapshot):
                    if self._forward_only:
                        # In case of the forward only plan any modifications result in reuse of the
                        # previous version for non-seed models.
                        # New snapshots of seed models are considered non-breaking ones.
                        if not snapshot.is_seed:
                            snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
                        else:
                            snapshot.categorize_as(SnapshotChangeCategory.NON_BREAKING)
                    elif self._is_forward_only_model(s_id) and is_directly_modified:
                        self._set_choice(
                            snapshot,
                            SnapshotChangeCategory.FORWARD_ONLY,
                            directly_modified,
                            indirectly_modified,
                        )
                    elif self._auto_categorization_enabled and is_directly_modified:
                        s_id_with_missing_columns: t.Optional[SnapshotId] = None
                        this_sid_with_downstream = indirectly_modified.get(s_id, set()) | {s_id}
                        for downstream_s_id in this_sid_with_downstream:
                            downstream_snapshot = self._context_diff.snapshots[downstream_s_id]
                            if (
                                downstream_snapshot.is_model
                                and downstream_snapshot.model.columns_to_types is None
                            ):
                                s_id_with_missing_columns = downstream_s_id
                                break

                        new, old = self._context_diff.modified_snapshots[s_id.name]
                        if s_id_with_missing_columns is None:
                            change_category = categorize_change(
                                new, old, config=self._categorizer_config
                            )
                            if change_category is not None:
                                self._set_choice(
                                    new, change_category, directly_modified, indirectly_modified
                                )
                        else:
                            mode = self._categorizer_config.dict().get(
                                new.model.source_type, AutoCategorizationMode.OFF
                            )
                            if mode == AutoCategorizationMode.FULL:
                                self._set_choice(
                                    new,
                                    SnapshotChangeCategory.BREAKING,
                                    directly_modified,
                                    indirectly_modified,
                                )

                # set to breaking if an indirect child has no directly modified parents
                # that need a decision. this can happen when a revert to a parent causes
                # an indirectly modified snapshot to be created because of a new parent
                if (
                    not is_directly_modified
                    and not snapshot.version
                    and not any(
                        self._context_diff.directly_modified(upstream.name)
                        and not self._context_diff.snapshots[upstream].version
                        for upstream in dag.upstream(s_id)
                    )
                ):
                    snapshot.categorize_as(
                        SnapshotChangeCategory.FORWARD_ONLY
                        if self._is_forward_only_model(s_id)
                        else SnapshotChangeCategory.INDIRECT_BREAKING
                    )

            elif s_id in self._context_diff.added and self._is_new_snapshot(snapshot):
                snapshot.categorize_as(
                    SnapshotChangeCategory.FORWARD_ONLY
                    if self._is_forward_only_model(s_id)
                    else SnapshotChangeCategory.BREAKING
                )

    def _set_choice(
        self,
        snapshot: Snapshot,
        choice: SnapshotChangeCategory,
        directly_modified: t.Set[SnapshotId],
        indirectly_modified: SnapshotMapping,
    ) -> None:
        if self._forward_only:
            raise PlanError("Choice setting is not supported by a forward-only plan.")
        if not self._is_new_snapshot(snapshot):
            raise SQLMeshError(
                f"A choice can't be changed for the existing version of '{snapshot.name}'."
            )

        snapshot.categorize_as(choice)

        is_breaking_choice = choice in (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        )

        for child_s_id in indirectly_modified.get(snapshot.snapshot_id, set()):
            child_snapshot = self._context_diff.snapshots[child_s_id]
            # If the snapshot isn't new then we are reverting to a previously existing snapshot
            # and therefore we don't want to recategorize it.
            if not self._is_new_snapshot(child_snapshot):
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

            snapshot.indirect_versions[child_s_id.name] = tuple(
                v.indirect_version for v in child_snapshot.all_versions
            )

            for upstream_id in directly_modified:
                upstream = self._context_diff.snapshots[upstream_id]
                if child_s_id.name in upstream.indirect_versions:
                    indirect_version = upstream.indirect_versions[child_s_id.name][-1]
                    if indirect_version.change_category == SnapshotChangeCategory.INDIRECT_BREAKING:
                        # If any other snapshot specified breaking this child, then that child
                        # needs to be backfilled as a part of the plan.
                        child_snapshot.categorize_as(
                            SnapshotChangeCategory.FORWARD_ONLY
                            if is_forward_only_child
                            else SnapshotChangeCategory.INDIRECT_BREAKING
                        )
                        break
                    elif (
                        indirect_version.change_category == SnapshotChangeCategory.FORWARD_ONLY
                        and child_snapshot.is_indirect_non_breaking
                    ):
                        # FORWARD_ONLY takes precedence over INDIRECT_NON_BREAKING.
                        child_snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)

    def _apply_effective_from(self) -> None:
        if self._effective_from:
            if not self._forward_only:
                raise PlanError("Effective date can only be set for a forward-only plan.")
            if to_datetime(self._effective_from) > now():
                raise PlanError("Effective date cannot be in the future.")

        for snapshot in self._context_diff.new_snapshots.values():
            if not snapshot.disable_restatement:
                snapshot.effective_from = self._effective_from

    def _is_forward_only_model(self, s_id: SnapshotId) -> bool:
        snapshot = self._context_diff.snapshots[s_id]
        return (
            snapshot.is_model
            and snapshot.model.forward_only
            and not snapshot.change_category
            and bool(snapshot.previous_versions)
        )

    def _is_new_snapshot(self, snapshot: Snapshot) -> bool:
        """Returns True if the given snapshot is a new snapshot in this plan."""
        return snapshot.snapshot_id in self._context_diff.new_snapshots

    def _ensure_valid_date_range(self) -> None:
        if (self.override_start or self.override_end) and not self.is_start_and_end_allowed:
            raise PlanError(
                "The start and end dates can't be set for a production plan without restatements."
            )

    def _ensure_no_forward_only_revert(self) -> None:
        """Ensures that a previously superseded breaking / non-breaking snapshot is not being
        used again to replace an existing forward-only snapshot with the same version.

        In other words there is no going back to the original non-forward-only snapshot with
        the same version once a forward-only change for that version has been introduced.
        """
        for name, (candidate, promoted) in self._context_diff.modified_snapshots.items():
            if (
                candidate.snapshot_id not in self._context_diff.new_snapshots
                and promoted.is_forward_only
                and not promoted.is_paused
                and not candidate.is_forward_only
                and not candidate.is_indirect_non_breaking
                and promoted.version == candidate.version
            ):
                raise PlanError(
                    f"Attempted to revert to an unrevertable version of model '{name}'. Run `sqlmesh plan` again to mitigate the issue."
                )

    def _ensure_no_broken_references(self) -> None:
        for snapshot in self._context_diff.snapshots.values():
            broken_references = {x.name for x in self._context_diff.removed_snapshots} & {
                x.name for x in snapshot.parents
            }
            if broken_references:
                broken_references_msg = ", ".join(f"'{x}'" for x in broken_references)
                raise PlanError(
                    f"""Removed {broken_references_msg} are referenced in '{snapshot.name}'. Please remove broken references before proceeding."""
                )

    def _ensure_no_new_snapshots_with_restatements(self) -> None:
        if self._restate_models and self._context_diff.new_snapshots:
            raise PlanError(
                "Model changes and restatements can't be a part of the same plan. "
                "Revert or apply changes before proceeding with restatements."
            )

    def _ensure_new_env_with_changes(self) -> None:
        if (
            self._is_dev
            and not self._include_unmodified
            and self._context_diff.is_new_environment
            and not self._context_diff.has_snapshot_changes
        ):
            raise NoChangesPlanError(
                "No changes were detected. Make a change or run with --include-unmodified to create a new environment without changes."
            )

    @cached_property
    def _forward_only_preview_needed(self) -> bool:
        """Determines whether the plan should compute previews for forward-only changes (if there are any)."""
        return self._is_dev and (
            self._forward_only
            or (
                self._enable_preview
                and any(
                    snapshot.model.forward_only
                    for snapshot in self._context_diff.new_snapshots.values()
                    if snapshot.is_model
                )
            )
        )
