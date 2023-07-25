from __future__ import annotations

import logging
import typing as t
from collections import defaultdict
from enum import Enum

from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.console import SNAPSHOT_CHANGE_CATEGORY_STR
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment
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
from sqlmesh.core.snapshot.definition import format_intervals
from sqlmesh.utils import random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    make_inclusive_end,
    now,
    to_date,
    to_datetime,
    to_timestamp,
    validate_date_range,
    yesterday_ds,
)
from sqlmesh.utils.errors import NoChangesPlanError, PlanError, SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

logger = logging.getLogger(__name__)


SnapshotMapping = t.Dict[str, t.Set[str]]


class Plan:
    """Plan is the main class to represent user choices on how they want to backfill and version their models.

    Args:
        context_diff: The context diff that the plan is based on.
        start: The start time to backfill data.
        end: The end time to backfill data.
        execution_time: The date/time time reference to use for execution time. Defaults to now.
        apply: The callback to apply the plan.
        restate_models: A list of models for which the data should be restated for the time range
            specified in this plan. Note: models defined outside SQLMesh (external) won't be a part
            of the restatement.
        no_gaps:  Whether to ensure that new snapshots for models that are already a
            part of the target environment have no data gaps when compared against previous
            snapshots for same models.
        skip_backfill: Whether to skip the backfill step.
        is_dev: Whether this plan is for development purposes.
        forward_only: Whether the purpose of the plan is to make forward only changes.
        environment_ttl: The period of time that a development environment should exist before being deleted.
        categorizer_config: Auto categorization settings.
        auto_categorization_enabled: Whether to apply auto categorization.
        effective_from: The effective date from which to apply forward-only changes on production.
        include_unmodified: Indicates whether to include unmodified models in the target development environment.
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
        categorizer_config: t.Optional[CategorizerConfig] = None,
        auto_categorization_enabled: bool = True,
        effective_from: t.Optional[TimeLike] = None,
        include_unmodified: bool = False,
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
        self.categorizer_config = categorizer_config or CategorizerConfig()
        self.auto_categorization_enabled = auto_categorization_enabled
        self.include_unmodified = include_unmodified
        self._effective_from: t.Optional[TimeLike] = None
        self._start = start if start or not (is_dev and forward_only) else yesterday_ds()
        self._end = end if end or not is_dev else now()
        self._execution_time = execution_time or now()
        self._apply = apply
        self._dag: DAG[str] = DAG()

        for name, snapshot in self.context_diff.snapshots.items():
            self._dag.add(name, snapshot.model.depends_on)

        self.__missing_intervals: t.Optional[t.Dict[t.Tuple[str, str], Intervals]] = None
        self._restatements: t.Set[str] = set()

        if restate_models and context_diff.new_snapshots:
            raise PlanError(
                "Model changes and restatements can't be a part of the same plan. "
                "Revert or apply changes before proceeding with restatements."
            )

        if not restate_models and is_dev and forward_only:
            # Add model names for new forward-only snapshots to the restatement list
            # in order to compute previews.
            restate_models = [
                s.name for s in context_diff.new_snapshots.values() if s.is_materialized
            ]

        self._add_restatements(restate_models or [])

        self._ensure_new_env_with_changes()
        self._ensure_valid_date_range(self._start, self._end)
        self._ensure_no_forward_only_revert()
        self._ensure_forward_only_models_compatibility()
        self._ensure_no_forward_only_new_models()
        self._ensure_no_broken_references()

        directly_indirectly_modified = self._build_directly_and_indirectly_modified()
        self.directly_modified = directly_indirectly_modified[0]
        self.indirectly_modified = directly_indirectly_modified[1]

        self._categorize_snapshots()

        self._categorized: t.Optional[t.List[Snapshot]] = None
        self._uncategorized: t.Optional[t.List[Snapshot]] = None

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
            return earliest_start_date(self.snapshots)
        return self._start or (
            min(
                start
                for intervals_per_model in self._missing_intervals.values()
                for start, _ in intervals_per_model
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

    def _get_end_date(self, end_and_units: t.List[t.Tuple[int, IntervalUnit]]) -> TimeLike:
        if end_and_units:
            end, unit = max(end_and_units)

            if unit.is_date_granularity:
                return to_date(make_inclusive_end(end))
            return end
        return now()

    @property
    def end(self) -> TimeLike:
        """Returns the end of the plan or now."""
        if not self._end or not self.override_end:
            if self._missing_intervals:
                return self._get_end_date(
                    [
                        (end, snapshot.model.interval_unit)
                        for snapshot in self.snapshots
                        if (snapshot.name, snapshot.version_get_or_generate())
                        in self._missing_intervals
                        for _, end in self._missing_intervals[
                            (snapshot.name, snapshot.version_get_or_generate())
                        ]
                    ]
                )
            return self._get_end_date(
                [
                    (snapshot.intervals[-1][1], snapshot.model.interval_unit)
                    for snapshot in self.snapshots
                    if snapshot.intervals
                ]
            )
        return self._end

    @end.setter
    def end(self, new_end: TimeLike) -> None:
        self._ensure_valid_date_range(self._start, new_end)
        self._end = new_end
        self.override_end = True
        self.__missing_intervals = None

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
        )

        return [interval for interval in missing_intervals if interval.intervals]

    @property
    def snapshots(self) -> t.List[Snapshot]:
        """Gets all the snapshots in the plan/environment."""
        return list(self.context_diff.snapshots.values())

    @property
    def new_snapshots(self) -> t.List[Snapshot]:
        """Gets only new snapshots in the plan/environment."""
        return list(self.context_diff.new_snapshots.values())

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
        )

    @property
    def environment_name(self) -> str:
        return self.context_diff.environment

    @property
    def restatements(self) -> t.Set[str]:
        return self._restatements

    @property
    def loaded_snapshot_intervals(self) -> t.List[LoadedSnapshotIntervals]:
        loaded_snapshots = []
        for snapshot in self.directly_modified:
            if not snapshot.change_category:
                continue
            loaded_snapshots.append(LoadedSnapshotIntervals.from_snapshot(snapshot))
            for downstream_indirect in self.indirectly_modified.get(snapshot.name, set()):
                downstream_snapshot = self.context_diff.snapshots[downstream_indirect]
                # We don't want to display indirect non-breaking since to users these are effectively no-op changes
                if downstream_snapshot.is_indirect_non_breaking:
                    continue
                loaded_snapshots.append(LoadedSnapshotIntervals.from_snapshot(downstream_snapshot))
        return loaded_snapshots

    def is_new_snapshot(self, snapshot: Snapshot) -> bool:
        """Returns True if the given snapshot is a new snapshot in this plan."""
        return snapshot.snapshot_id in self.context_diff.new_snapshots

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
                f"A choice can't be changed for the existing version of model '{snapshot.name}'."
            )

        snapshot.categorize_as(choice)

        for child in self.indirectly_modified[snapshot.name]:
            child_snapshot = self.context_diff.snapshots[child]
            # If the snapshot isn't new then we are reverting to a previously existing snapshot
            # and therefore we don't want to recategorize it.
            if not self.is_new_snapshot(child_snapshot):
                continue

            if choice in (
                SnapshotChangeCategory.BREAKING,
                SnapshotChangeCategory.INDIRECT_BREAKING,
            ):
                child_snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
            else:
                child_snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_NON_BREAKING)
            snapshot.indirect_versions[child] = child_snapshot.all_versions

            # If any other snapshot specified breaking this child, then that child
            # needs to be backfilled as a part of the plan.
            for upstream in self.directly_modified:
                if child in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child][-1]
                    if data_version.is_new_version:
                        child_snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)
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
            if not snapshot.model.disable_restatement:
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

    def _add_restatements(self, restate_models: t.Iterable[str]) -> None:
        for table in restate_models:
            downstream = self._dag.downstream(table)
            if table in self.context_diff.snapshots:
                downstream.append(table)

            snapshots = self.context_diff.snapshots
            downstream = [
                d for d in downstream if snapshots[d].is_materialized and not snapshots[d].is_seed
            ]

            if not self.is_dev:
                models_with_disabled_restatement = [
                    f"'{d}'" for d in downstream if snapshots[d].model.disable_restatement
                ]
                if models_with_disabled_restatement:
                    raise PlanError(
                        f"Restatement is disabled for models: {', '.join(models_with_disabled_restatement)}."
                    )

            if not downstream:
                raise PlanError(
                    f"Cannot restate from '{table}'. Either such model doesn't exist, no other materialized model references it, or restatement was disabled fror this model."
                )
            self._restatements.update(downstream)

    def _build_directly_and_indirectly_modified(self) -> t.Tuple[t.List[Snapshot], SnapshotMapping]:
        """Builds collections of directly and indirectly modified snapshots.

        Returns:
            The tuple in which the first element contains a list of added and directly modified
            snapshots while the second element contains a mapping of indirectly modified snapshots.
        """
        directly_modified = []
        all_indirectly_modified = set()

        for model_name, snapshot in self.context_diff.snapshots.items():
            if model_name in self.context_diff.modified_snapshots:
                if self.context_diff.directly_modified(model_name):
                    directly_modified.append(snapshot)
                else:
                    all_indirectly_modified.add(model_name)
            elif model_name in self.context_diff.added:
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
        for model_name, snapshot in self.context_diff.snapshots.items():
            upstream_model_names = self._dag.upstream(model_name)

            if model_name in self.context_diff.modified_snapshots:
                is_directly_modified = self.context_diff.directly_modified(model_name)

                if self.is_new_snapshot(snapshot):
                    if not self.forward_only:
                        self._ensure_no_paused_forward_only_upstream(
                            model_name, upstream_model_names
                        )

                    if self.forward_only:
                        # In case of the forward only plan any modifications result in reuse of the
                        # previous version for non-seed models.
                        # New snapshots of seed models are considered non-breaking ones.
                        if not snapshot.is_seed:
                            snapshot.categorize_as(SnapshotChangeCategory.FORWARD_ONLY)
                        else:
                            snapshot.categorize_as(SnapshotChangeCategory.NON_BREAKING)
                    elif self.auto_categorization_enabled and is_directly_modified:
                        model_with_missing_columns: t.Optional[str] = None
                        this_model_with_downstream = self.indirectly_modified.get(
                            model_name, set()
                        ) | {model_name}
                        for downstream in this_model_with_downstream:
                            if (
                                self.context_diff.snapshots[downstream].model.columns_to_types
                                is None
                            ):
                                model_with_missing_columns = downstream
                                break

                        if model_with_missing_columns is None:
                            new, old = self.context_diff.modified_snapshots[model_name]
                            change_category = categorize_change(
                                new, old, config=self.categorizer_config
                            )
                            if change_category is not None:
                                self.set_choice(new, change_category)
                        else:
                            logger.warning(
                                "Changes to model '%s' cannot be automatically categorized due to missing schema for model '%s'",
                                model_name,
                                model_with_missing_columns,
                            )

                # set to breaking if an indirect child has no directly modified parents
                # that need a decision. this can happen when a revert to a parent causes
                # an indirectly modified snapshot to be created because of a new parent
                if (
                    not is_directly_modified
                    and not snapshot.version
                    and not any(
                        self.context_diff.directly_modified(upstream)
                        and not self.context_diff.snapshots[upstream].version
                        for upstream in upstream_model_names
                    )
                ):
                    snapshot.categorize_as(SnapshotChangeCategory.INDIRECT_BREAKING)

            elif model_name in self.context_diff.added and self.is_new_snapshot(snapshot):
                snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    def _ensure_no_paused_forward_only_upstream(
        self, model_name: str, upstream_model_names: t.Iterable[str]
    ) -> None:
        for upstream in upstream_model_names:
            upstream_snapshot = self.context_diff.snapshots.get(upstream)
            if (
                upstream_snapshot
                and upstream_snapshot.version
                and upstream_snapshot.is_forward_only
                and upstream_snapshot.is_paused
            ):
                raise PlanError(
                    f"Model '{model_name}' depends on a paused version of model '{upstream}'. "
                    "Possible remedies: "
                    "1) make sure your codebase is up-to-date; "
                    f"2) promote the current version of model '{upstream}' in the production environment; "
                    "3) recreate this plan in a forward-only mode."
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

    def _ensure_no_forward_only_new_models(self) -> None:
        if self.forward_only and self.context_diff.added_materialized_models:
            raise PlanError(
                "New models that require materialization can't be added as part of the forward-only plan."
            )

    def _ensure_no_broken_references(self) -> None:
        for snapshot in self.context_diff.snapshots.values():
            broken_references = self.context_diff.removed & snapshot.model.depends_on
            if broken_references:
                raise PlanError(
                    f"Removed models {broken_references} are referenced in model '{snapshot.name}'. Please remove broken references before proceeding."
                )

    def _ensure_forward_only_models_compatibility(self) -> None:
        if not self.forward_only:
            for new in self.context_diff.new_snapshots.values():
                if new.model.forward_only and new.name in self.context_diff.modified_snapshots:
                    raise PlanError(
                        f"Model '{new.name}' can only be changed as part of a forward-only plan. Please run this plan with --forward-only flag."
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


class SnapshotIntervals(PydanticModel, frozen=True):
    snapshot_name: str
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_intervals(self, unit: t.Optional[IntervalUnit] = None) -> str:
        return format_intervals(self.merged_intervals, unit)


class LoadedSnapshotIntervals(SnapshotIntervals):
    interval_unit: t.Optional[IntervalUnit]
    model_name: str
    view_name: str
    change_category: SnapshotChangeCategory

    @classmethod
    def from_snapshot(cls, snapshot: Snapshot) -> LoadedSnapshotIntervals:
        assert snapshot.change_category
        return cls(
            snapshot_name=snapshot.name,
            intervals=snapshot.dev_intervals
            if snapshot.change_category.is_forward_only
            else snapshot.intervals,
            interval_unit=snapshot.model.interval_unit,
            model_name=snapshot.model.name,
            view_name=snapshot.model.view_name,
            change_category=snapshot.change_category,
        )

    @property
    def change_category_str(self) -> str:
        return SNAPSHOT_CHANGE_CATEGORY_STR[self.change_category]
