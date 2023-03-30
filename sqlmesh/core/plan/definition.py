from __future__ import annotations

import typing as t
from collections import defaultdict
from enum import Enum

from sqlmesh.core import scheduler
from sqlmesh.core.config import CategorizerConfig
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment
from sqlmesh.core.model.meta import IntervalUnit
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    categorize_change,
    merge_intervals,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    make_inclusive,
    make_inclusive_end,
    now,
    to_date,
    to_ds,
    to_timestamp,
    validate_date_range,
    yesterday_ds,
)
from sqlmesh.utils.errors import PlanError, SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

SnapshotMapping = t.Dict[str, t.Set[str]]


class Plan:
    """Plan is the main class to represent user choices on how they want to backfill and version their models.

    Args:
        context_diff: The context diff that the plan is based on.
        dag: The dag object to determine relationships.
        state_reader: The state_reader to get metadata with.
        start: The start time to backfill data.
        end: The end time to backfill data.
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
    """

    def __init__(
        self,
        context_diff: ContextDiff,
        dag: DAG,
        state_reader: StateReader,
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        apply: t.Optional[t.Callable[[Plan], None]] = None,
        restate_models: t.Optional[t.Iterable[str]] = None,
        no_gaps: bool = False,
        skip_backfill: bool = False,
        is_dev: bool = False,
        forward_only: bool = False,
        environment_ttl: t.Optional[str] = None,
        categorizer_config: t.Optional[CategorizerConfig] = None,
        auto_categorization_enabled: bool = True,
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
        self._start = start if start or not (is_dev and forward_only) else yesterday_ds()
        self._end = end if end or not is_dev else now()
        self._apply = apply
        self._dag = dag
        self._state_reader = state_reader
        self.__missing_intervals: t.Optional[t.Dict[str, Intervals]] = None
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

        self._ensure_valid_date_range(self._start, self._end)
        self._ensure_no_forward_only_revert()
        self._ensure_no_forward_only_new_models()

        directly_indirectly_modified = self._build_directly_and_indirectly_modified()
        self.directly_modified = directly_indirectly_modified[0]
        self.indirectly_modified = directly_indirectly_modified[1]

        self._categorize_snapshots()

        self._categorized: t.Optional[t.List[Snapshot]] = None
        self._uncategorized: t.Optional[t.List[Snapshot]] = None

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
            return scheduler.earliest_start_date(self.snapshots)
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

            if unit == IntervalUnit.DAY:
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
                        (end, snapshot.model.interval_unit())
                        for snapshot in self.snapshots
                        if snapshot.version_get_or_generate() in self._missing_intervals
                        for _, end in self._missing_intervals[snapshot.version_get_or_generate()]
                    ]
                )
            return self._get_end_date(
                [
                    (snapshot.intervals[-1][1], snapshot.model.interval_unit())
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
    def missing_intervals(self) -> t.List[MissingIntervals]:
        """Returns a list of missing intervals."""
        return [
            MissingIntervals(
                snapshot_name=snapshot.name,
                intervals=self._missing_intervals[snapshot.version_get_or_generate()],
            )
            for snapshot in self.snapshots
            if snapshot.version_get_or_generate() in self._missing_intervals
        ]

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
        return Environment(
            name=self.context_diff.environment,
            snapshots=[snapshot.table_info for snapshot in self.snapshots],
            start_at=self.start,
            end_at=self._end,
            plan_id=self.plan_id,
            previous_plan_id=self.context_diff.previous_plan_id,
            expiration_ts=expiration_ts,
        )

    @property
    def restatements(self) -> t.Set[str]:
        return self._restatements

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

        snapshot.change_category = choice
        if choice in (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.NON_BREAKING,
        ):
            snapshot.set_version()
        else:
            snapshot.set_version(snapshot.previous_version)

        for child in self.indirectly_modified[snapshot.name]:
            child_snapshot = self.context_diff.snapshots[child]

            if choice == SnapshotChangeCategory.BREAKING:
                child_snapshot.set_version()
            else:
                child_snapshot.set_version(child_snapshot.previous_version)
            snapshot.indirect_versions[child] = child_snapshot.all_versions

            # If any other snapshot specified breaking this child, then that child
            # needs to be backfilled as a part of the plan.
            for upstream in self.directly_modified:
                if child in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child][-1]
                    if data_version.is_new_version:
                        child_snapshot.set_version()
                        break

        # Invalidate caches.
        self._categorized = None
        self._uncategorized = None

    def snapshot_change_category(self, snapshot: Snapshot) -> SnapshotChangeCategory:
        """
        Determines the SnapshotChangeCategory for a modified snapshot using its available history.

        A snapshot may be modified (directly or indirectly) multiple times. Each time
        it is directly changed, the categorization is stored in its history. Look
        through the snapshot's history to find where it deviated from the previous
        snapshot and then find the most conservative categorization recorded.

        Args:
            snapshot: The snapshot within this plan
        """
        if snapshot not in self.snapshots:
            raise SQLMeshError(f"Snapshot {snapshot.snapshot_id} does not exist in this plan.")

        if not snapshot.version:
            raise SQLMeshError(f"Snapshot {snapshot.snapshot_id} has not be categorized yet.")

        if snapshot.name not in self.context_diff.modified_snapshots:
            raise SQLMeshError(f"Snapshot {snapshot.snapshot_id} has not been modified.")

        current, previous = self.context_diff.modified_snapshots[snapshot.name]
        if current.version == previous.version:
            # Versions match, so no further history to check
            return SnapshotChangeCategory.FORWARD_ONLY
        elif previous.data_version in current.all_versions:
            # Previous snapshot in the current snapshot's history. Get all versions
            # since the two matched.
            index = current.all_versions.index(previous.data_version)
            versions = current.all_versions[index + 1 :]
        elif current.data_version in previous.all_versions:
            # Snapshot is a revert. Look through the previous snapshot's history
            # and get all versions since it matched the current snapshot.
            index = previous.all_versions.index(current.data_version)
            versions = previous.all_versions[index:]
        else:
            # Insufficient history, so err on the side of safety
            return SnapshotChangeCategory.BREAKING

        change_categories = [
            version.change_category for version in versions if version.change_category
        ]
        # Return the most conservative categorization found in the snapshot's history
        return min(change_categories, key=lambda x: x.value)

    @property
    def _missing_intervals(self) -> t.Dict[str, Intervals]:
        if self.__missing_intervals is None:
            previous_ids = [
                SnapshotId(
                    name=snapshot.name,
                    identifier=snapshot.previous_version.fingerprint.to_identifier(),
                )
                for snapshot in self.snapshots
                if snapshot.previous_version
            ]

            previous_snapshots = (
                list(self._state_reader.get_snapshots(previous_ids).values())
                if previous_ids
                else []
            )

            end = self._end or now()
            self.__missing_intervals = {
                snapshot.version_get_or_generate(): missing
                for snapshot, missing in self._state_reader.missing_intervals(
                    previous_snapshots + list(self.snapshots),
                    start=self._start or scheduler.earliest_start_date(self.snapshots),
                    end=end,
                    latest=end,
                    restatements=self.restatements,
                ).items()
            }

        return self.__missing_intervals

    def _add_restatements(self, restate_models: t.Iterable[str]) -> None:
        for table in restate_models:
            downstream = self._dag.downstream(table)
            if table in self.context_diff.snapshots:
                downstream.append(table)

            snapshots = self.context_diff.snapshots
            downstream = [d for d in downstream if snapshots[d].is_materialized]

            if not downstream:
                raise PlanError(
                    f"Cannot restate from '{table}'. Either such model doesn't exist or no other model references it."
                )
            self._restatements.update(downstream)

    def _build_directly_and_indirectly_modified(self) -> t.Tuple[t.List[Snapshot], SnapshotMapping]:
        """Builds collections of directly and inderectly modified snapshots.

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

            if not self.forward_only:
                self._ensure_no_paused_forward_only_upstream(model_name, upstream_model_names)

            if model_name in self.context_diff.modified_snapshots:
                is_directly_modified = self.context_diff.directly_modified(model_name)

                if self.is_new_snapshot(snapshot):
                    if self.forward_only:
                        # In case of the forward only plan any modifications result in reuse of the
                        # previous version for non-seed models.
                        # New snapshots of seed models are considered non-breaking ones.
                        if not snapshot.is_seed_kind:
                            snapshot.set_version(snapshot.previous_version)
                            snapshot.change_category = SnapshotChangeCategory.FORWARD_ONLY
                        else:
                            snapshot.set_version()
                            snapshot.change_category = SnapshotChangeCategory.NON_BREAKING
                    elif self.auto_categorization_enabled and is_directly_modified:
                        new, old = self.context_diff.modified_snapshots[model_name]
                        change_category = categorize_change(
                            new, old, config=self.categorizer_config
                        )
                        if change_category is not None:
                            self.set_choice(new, change_category)

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
                    snapshot.set_version()

            elif model_name in self.context_diff.added and self.is_new_snapshot(snapshot):
                snapshot.set_version()

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
        if self.forward_only and self.context_diff.added:
            raise PlanError("New models can't be added as part of the forward-only plan.")


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


class MissingIntervals(PydanticModel, frozen=True):
    snapshot_name: str
    intervals: Intervals

    @property
    def merged_intervals(self) -> Intervals:
        return merge_intervals(self.intervals)

    def format_missing_range(self) -> str:
        intervals = [make_inclusive(start, end) for start, end in self.merged_intervals]
        return ", ".join(f"({to_ds(start)}, {to_ds(end)})" for start, end in intervals)
