from __future__ import annotations

import logging
import re
import sys
import typing as t
from collections import defaultdict
from functools import cached_property


from sqlmesh.core.console import Console, get_console
from sqlmesh.core.config import (
    AutoCategorizationMode,
    CategorizerConfig,
    EnvironmentSuffixTarget,
)
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.plan.definition import Plan, SnapshotMapping, earliest_interval_start
from sqlmesh.core.schema_diff import SchemaDiffer, has_drop_alteration
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
)
from sqlmesh.core.snapshot.categorizer import categorize_change
from sqlmesh.core.snapshot.definition import Interval, SnapshotId
from sqlmesh.utils import columns_to_types_all_known, random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import TimeLike, now, to_datetime, yesterday_ds
from sqlmesh.utils.errors import NoChangesPlanError, PlanError, SQLMeshError

logger = logging.getLogger(__name__)


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
        empty_backfill: Like skip_backfill, but also records processed intervals.
        is_dev: Whether this plan is for development purposes.
        forward_only: Whether the purpose of the plan is to make forward only changes.
        allow_destructive_models: A list of fully qualified model names whose forward-only changes are allowed to be destructive.
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
        engine_schema_differ: Schema differ from the context engine adapter.
        interval_end_per_model: The mapping from model FQNs to target end dates.
    """

    def __init__(
        self,
        context_diff: ContextDiff,
        engine_schema_differ: SchemaDiffer,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
        apply: t.Optional[t.Callable[[Plan], None]] = None,
        restate_models: t.Optional[t.Iterable[str]] = None,
        backfill_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        empty_backfill: bool = False,
        is_dev: bool = False,
        forward_only: bool = False,
        allow_destructive_models: t.Optional[t.Iterable[str]] = None,
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
        interval_end_per_model: t.Optional[t.Dict[str, int]] = None,
        console: t.Optional[Console] = None,
    ):
        self._context_diff = context_diff
        self._no_gaps = no_gaps
        self._skip_backfill = skip_backfill
        self._empty_backfill = empty_backfill
        self._is_dev = is_dev
        self._forward_only = forward_only
        self._allow_destructive_models = set(
            allow_destructive_models if allow_destructive_models is not None else []
        )
        self._enable_preview = enable_preview
        self._end_bounded = end_bounded
        self._ensure_finalized_snapshots = ensure_finalized_snapshots
        self._interval_end_per_model = interval_end_per_model
        self._environment_ttl = environment_ttl
        self._categorizer_config = categorizer_config or CategorizerConfig()
        self._auto_categorization_enabled = auto_categorization_enabled
        self._include_unmodified = include_unmodified
        self._restate_models = set(restate_models) if restate_models is not None else None
        self._effective_from = effective_from
        self._execution_time = execution_time
        self._backfill_models = backfill_models
        self._end = end or default_end
        self._apply = apply
        self._engine_schema_differ = engine_schema_differ
        self._console = console or get_console()
        self._choices: t.Dict[SnapshotId, SnapshotChangeCategory] = {}

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
            normalize_name=self._context_diff.normalize_environment_name,
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
        if self._forward_only:
            raise PlanError("Choice setting is not supported by a forward-only plan.")
        if not self._is_new_snapshot(snapshot):
            raise SQLMeshError(
                f"A choice can't be changed for the existing version of '{snapshot.name}'."
            )
        if (
            not self._context_diff.directly_modified(snapshot.name)
            and snapshot.snapshot_id not in self._context_diff.added
        ):
            raise SQLMeshError(
                f"Only directly modified models can be categorized ({snapshot.name})."
            )

        self._choices[snapshot.snapshot_id] = choice
        self._latest_plan = None
        return self

    def apply(self) -> None:
        """Builds and applies the plan."""
        if not self._apply:
            raise SQLMeshError("Plan was not initialized with an applier.")
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

        self._check_destructive_changes(directly_modified)
        self._categorize_snapshots(dag, indirectly_modified)
        self._adjust_new_snapshot_intervals()

        deployability_index = (
            DeployabilityIndex.create(self._context_diff.snapshots.values(), start=self._start)
            if self._is_dev
            else DeployabilityIndex.all_deployable()
        )

        restatements = self._build_restatements(
            dag, earliest_interval_start(self._context_diff.snapshots.values())
        )
        models_to_backfill = self._build_models_to_backfill(dag, restatements)

        interval_end_per_model = self._interval_end_per_model
        if interval_end_per_model and self.override_end:
            # If the end date was provided explicitly by a user, then interval end for each individual
            # model should be ignored.
            interval_end_per_model = None

        plan = Plan(
            context_diff=self._context_diff,
            plan_id=self._plan_id,
            provided_start=self._start,
            provided_end=self._end,
            is_dev=self._is_dev,
            skip_backfill=self._skip_backfill,
            empty_backfill=self._empty_backfill,
            no_gaps=self._no_gaps,
            forward_only=self._forward_only,
            allow_destructive_models=t.cast(t.Set, self._allow_destructive_models),
            include_unmodified=self._include_unmodified,
            environment_ttl=self._environment_ttl,
            environment_naming_info=self.environment_naming_info,
            directly_modified=directly_modified,
            indirectly_modified=indirectly_modified,
            deployability_index=deployability_index,
            restatements=restatements,
            interval_end_per_model=interval_end_per_model,
            selected_models_to_backfill=self._backfill_models,
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

    def _build_restatements(
        self, dag: DAG[SnapshotId], earliest_interval_start: TimeLike
    ) -> t.Dict[SnapshotId, Interval]:
        def is_restateable_snapshot(snapshot: Snapshot) -> bool:
            if not self._is_dev and snapshot.disable_restatement:
                return False
            return not snapshot.is_symbolic and not snapshot.is_seed

        restate_models = self._restate_models
        if restate_models == set():
            # This is a warning but we print this as error since the Console is lacking API for warnings.
            self._console.log_error(
                "Provided restated models do not match any models. No models will be included in plan."
            )
            return {}

        restatements: t.Dict[SnapshotId, Interval] = {}
        forward_only_preview_needed = self._forward_only_preview_needed
        is_preview = False
        if not restate_models and forward_only_preview_needed:
            # Add model names for new forward-only snapshots to the restatement list
            # in order to compute previews.
            restate_models = {
                s.name
                for s in self._context_diff.new_snapshots.values()
                if s.is_materialized
                and (self._forward_only or s.model.forward_only)
                and (
                    # Metadata changes should not be previewed.
                    self._context_diff.directly_modified(s.name)
                    or self._context_diff.indirectly_modified(s.name)
                )
            }
            is_preview = True

        if not restate_models:
            return {}

        # Add restate snapshots and their downstream snapshots
        dummy_interval = (sys.maxsize, -sys.maxsize)
        for model_fqn in restate_models:
            snapshot = self._model_fqn_to_snapshot.get(model_fqn)
            if not snapshot:
                raise PlanError(f"Cannot restate model '{model_fqn}'. Model does not exist.")
            if not forward_only_preview_needed:
                if not self._is_dev and snapshot.disable_restatement:
                    # This is a warning but we print this as error since the Console is lacking API for warnings.
                    self._console.log_error(
                        f"Cannot restate model '{model_fqn}'. Restatement is disabled for this model."
                    )
                    continue
                elif snapshot.is_symbolic or snapshot.is_seed:
                    logger.info("Skipping restatement for model '%s'", model_fqn)
                    continue

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
                is_preview=is_preview,
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

    def _build_models_to_backfill(
        self, dag: DAG[SnapshotId], restatements: t.Collection[SnapshotId]
    ) -> t.Optional[t.Set[str]]:
        backfill_models = (
            self._backfill_models
            if self._backfill_models is not None
            else [r.name for r in restatements]
            # Only backfill models explicitly marked for restatement.
            if self._restate_models
            else None
        )
        if backfill_models is None:
            return None
        return {
            self._context_diff.snapshots[s_id].name
            for s_id in dag.subdag(
                *[
                    self._model_fqn_to_snapshot[m].snapshot_id
                    for m in backfill_models
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

    def _check_destructive_changes(self, directly_modified: t.Set[SnapshotId]) -> None:
        for s_id in sorted(directly_modified):
            snapshot = self._context_diff.snapshots[s_id]
            # should we raise/warn if this snapshot has/inherits a destructive change?
            should_raise_or_warn = (
                self._is_forward_only_change(s_id) or self._forward_only
            ) and snapshot.needs_destructive_check(self._allow_destructive_models)

            if not should_raise_or_warn or not snapshot.is_model:
                continue

            new, old = self._context_diff.modified_snapshots[snapshot.name]

            # we must know all columns_to_types to determine whether a change is destructive
            old_columns_to_types = old.model.columns_to_types or {}
            new_columns_to_types = new.model.columns_to_types or {}

            if columns_to_types_all_known(old_columns_to_types) and columns_to_types_all_known(
                new_columns_to_types
            ):
                schema_diff = self._engine_schema_differ.compare_columns(
                    new.name,
                    old_columns_to_types,
                    new_columns_to_types,
                )

                if has_drop_alteration(schema_diff):
                    warning_msg = f"Plan results in a destructive change to forward-only model '{snapshot.name}'s schema"
                    if snapshot.model.on_destructive_change.is_warn:
                        logger.warning(warning_msg)
                    else:
                        raise PlanError(
                            f"{warning_msg}. To allow this, change the model's `on_destructive_change` setting to `warn` or `allow` or include it in the plan's `--allow-destructive-model` option."
                        )

    def _categorize_snapshots(
        self, dag: DAG[SnapshotId], indirectly_modified: SnapshotMapping
    ) -> None:
        """Automatically categorizes snapshots that can be automatically categorized and
        returns a list of added and directly modified snapshots as well as the mapping of
        indirectly modified snapshots.
        """

        # Iterating in DAG order since a category for a snapshot may depend on the categories
        # assigned to its upstream dependencies.
        for s_id in dag:
            snapshot = self._context_diff.snapshots.get(s_id)

            if not snapshot or not self._is_new_snapshot(snapshot):
                continue

            if s_id in self._choices:
                snapshot.categorize_as(self._choices[s_id])
                continue

            if s_id in self._context_diff.added:
                snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
            elif self._is_forward_only_change(s_id) or self._forward_only:
                # In case of the forward only plan any modifications result in reuse of the
                # previous version for non-seed models.
                # New snapshots of seed models are considered non-breaking ones.
                category = (
                    SnapshotChangeCategory.NON_BREAKING
                    if snapshot.is_seed
                    else SnapshotChangeCategory.FORWARD_ONLY
                )
                # If the model kind changes mark as breaking
                if snapshot.is_model and snapshot.name in self._context_diff.modified_snapshots:
                    _, old = self._context_diff.modified_snapshots[snapshot.name]
                    if old.model.kind.name != snapshot.model.kind.name:
                        category = SnapshotChangeCategory.BREAKING

                snapshot.categorize_as(category)
            elif s_id.name in self._context_diff.modified_snapshots:
                self._categorize_snapshot(snapshot, dag, indirectly_modified)

    def _categorize_snapshot(
        self, snapshot: Snapshot, dag: DAG[SnapshotId], indirectly_modified: SnapshotMapping
    ) -> None:
        s_id = snapshot.snapshot_id

        if self._context_diff.directly_modified(s_id.name):
            if self._auto_categorization_enabled:
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
                    change_category = categorize_change(new, old, config=self._categorizer_config)
                    if change_category is not None:
                        snapshot.categorize_as(change_category)
                else:
                    mode = self._categorizer_config.dict().get(
                        new.model.source_type, AutoCategorizationMode.OFF
                    )
                    if mode == AutoCategorizationMode.FULL:
                        snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
        elif self._context_diff.indirectly_modified(snapshot.name):
            categories = []

            for p_id in dag.upstream(s_id):
                parent = self._context_diff.snapshots.get(p_id)

                if parent and self._is_new_snapshot(parent):
                    categories.append(parent.change_category)

            if not categories or any(
                category.is_breaking or category.is_indirect_breaking
                for category in categories
                if category
            ):
                snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
            elif any(category.is_forward_only for category in categories if category):
                snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
            else:
                snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
        else:
            # Metadata updated.
            snapshot.categorize_as(SnapshotChangeCategory.METADATA)

    def _apply_effective_from(self) -> None:
        if self._effective_from:
            if not self._forward_only:
                raise PlanError("Effective date can only be set for a forward-only plan.")
            if to_datetime(self._effective_from) > now():
                raise PlanError("Effective date cannot be in the future.")

        for snapshot in self._context_diff.new_snapshots.values():
            if (
                snapshot.evaluatable
                and not snapshot.disable_restatement
                and not snapshot.full_history_restatement_only
            ):
                snapshot.effective_from = self._effective_from

    def _is_forward_only_change(self, s_id: SnapshotId) -> bool:
        snapshot = self._context_diff.snapshots[s_id]
        if snapshot.name in self._context_diff.modified_snapshots:
            _, old = self._context_diff.modified_snapshots[snapshot.name]
            # If the model kind has changed, then we should not consider this to be a forward-only change.
            if snapshot.is_model and old.model.kind.name != snapshot.model.kind.name:
                return False
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
                and not candidate.reuses_previous_version
                and promoted.version == candidate.version
            ):
                raise PlanError(
                    f"Attempted to revert to an unrevertable version of model '{name}'. Run `sqlmesh plan` again to mitigate the issue."
                )

    def _ensure_no_broken_references(self) -> None:
        for snapshot in self._context_diff.snapshots.values():
            broken_references = {
                x.name for x in self._context_diff.removed_snapshots.values() if not x.is_external
            } & {x for x in snapshot.node.depends_on}
            if broken_references:
                broken_references_msg = ", ".join(f"'{x}'" for x in broken_references)
                raise PlanError(
                    f"""Removed {broken_references_msg} are referenced in '{snapshot.name}'. Please remove broken references before proceeding."""
                )

    def _ensure_no_new_snapshots_with_restatements(self) -> None:
        if self._restate_models is not None and (
            self._context_diff.new_snapshots or self._context_diff.modified_snapshots
        ):
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
            and not self._backfill_models
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
                    for snapshot, _ in self._context_diff.modified_snapshots.values()
                    if snapshot.is_model
                )
            )
        )
