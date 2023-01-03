from __future__ import annotations

import typing as t
from collections import defaultdict, deque
from enum import Enum

from sqlmesh.core import scheduler
from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Intervals,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    merge_intervals,
)
from sqlmesh.core.state_sync import StateReader
from sqlmesh.utils import random_id
from sqlmesh.utils.dag import DAG
from sqlmesh.utils.date import (
    TimeLike,
    make_inclusive,
    now,
    to_ds,
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
    ):
        self.context_diff = context_diff
        self.override_start = start is not None
        self.override_end = end is not None
        self.plan_id: str = random_id()
        self.restatements = set()
        self.no_gaps = no_gaps
        self.skip_backfill = skip_backfill
        self.is_dev = is_dev
        self.forward_only = forward_only
        self._start = (
            start if start or not (is_dev and forward_only) else yesterday_ds()
        )
        self._end = end if end or not is_dev else now()
        self._apply = apply
        self._dag = dag
        self._state_reader = state_reader
        self._missing_intervals: t.Optional[t.Dict[str, Intervals]] = None

        if not restate_models and is_dev and forward_only:
            # Add model names for new forward-only snapshots to the restatement list
            # in order to compute previews.
            restate_models = [s.name for s in context_diff.new_snapshots]

        for table in restate_models or []:
            downstream = self._dag.downstream(table)
            if table in self.context_diff.snapshots:
                downstream.append(table)

            if not downstream:
                raise PlanError(
                    f"Cannot restate from '{table}'. Either such model doesn't exist or no other model references it."
                )
            self.restatements.update(downstream)

        categorized_snapshots = self._categorize_snapshots()
        self.added_and_directly_modified = categorized_snapshots[0]
        self.indirectly_modified = categorized_snapshots[1]

        self._categorized: t.Optional[t.List[Snapshot]] = None
        self._uncategorized: t.Optional[t.List[Snapshot]] = None

    @property
    def categorized(self) -> t.List[Snapshot]:
        """Returns the already categorized snapshots."""
        if self._categorized is None:
            self._categorized = [
                s for s in self.added_and_directly_modified if s.version
            ]
        return self._categorized

    @property
    def uncategorized(self) -> t.List[Snapshot]:
        """Returns the uncategorized snapshots."""
        if self._uncategorized is None:
            self._uncategorized = [
                s for s in self.added_and_directly_modified if not s.version
            ]
        return self._uncategorized

    @property
    def start(self) -> TimeLike:
        """Returns the start of the plan or the earliest date of all snapshots."""
        return self._start or scheduler.earliest_start_date(self.snapshots)

    @start.setter
    def start(self, new_start) -> None:
        self._start = new_start
        self._missing_intervals = None

    @property
    def end(self) -> TimeLike:
        """Returns the end of the plan or now."""
        return self._end or now()

    @end.setter
    def end(self, new_end: TimeLike) -> None:
        self._end = new_end
        self._missing_intervals = None

    @property
    def is_unbounded_end(self) -> bool:
        """Indicates whether this plan has an unbounded end."""
        return not self._end

    @property
    def requires_backfill(self) -> bool:
        return not self.skip_backfill and (
            bool(self.restatements) or bool(self.missing_intervals)
        )

    @property
    def missing_intervals(self) -> t.List[MissingIntervals]:
        """Returns a list of missing intervals."""
        if self._missing_intervals is None:
            previous_ids = [
                SnapshotId(
                    name=snapshot.name,
                    fingerprint=snapshot.previous_version.fingerprint,
                )
                for snapshot in self.snapshots
                if snapshot.previous_version
            ]

            previous_snapshots = (
                list(self._state_reader.get_snapshots(previous_ids).values())
                if previous_ids
                else []
            )

            end = self.end
            self._missing_intervals = {
                snapshot.version_or_fingerprint: missing
                for snapshot, missing in self._state_reader.missing_intervals(
                    previous_snapshots + list(self.snapshots),
                    start=self.start,
                    end=end,
                    latest=end,
                    restatements=self.restatements,
                ).items()
            }
        return [
            MissingIntervals(
                snapshot_name=snapshot.name,
                intervals=self._missing_intervals[snapshot.version_or_fingerprint],
            )
            for snapshot in self.snapshots
            if snapshot.version_or_fingerprint in self._missing_intervals
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
        return Environment(
            name=self.context_diff.environment,
            snapshots=[snapshot.table_info for snapshot in self.snapshots],
            start=self.start,
            end=self._end,
            plan_id=self.plan_id,
            previous_plan_id=self.context_diff.previous_plan_id,
        )

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
            for upstream in self.added_and_directly_modified:
                if child in upstream.indirect_versions:
                    data_version = upstream.indirect_versions[child][-1]
                    if data_version.is_new_version:
                        child_snapshot.set_version()
                        break

        # Invalidate caches.
        self._categorized = None
        self._uncategorized = None

    def snapshot_change_category(self, snapshot: Snapshot) -> SnapshotChangeCategory:
        """Returns the SnapshotChangeCategory for the specified snapshot within this plan.

        Args:
            snapshot: The snapshot within this plan
        """
        if snapshot not in self.snapshots:
            raise SQLMeshError(
                f"Snapshot {snapshot.snapshot_id} does not exist in this plan."
            )

        if not snapshot.version:
            raise SQLMeshError(
                f"Snapshot {snapshot.snapshot_id} has not be categorized yet."
            )

        if snapshot.name not in self.context_diff.modified_snapshots:
            raise SQLMeshError(
                f"Snapshot {snapshot.snapshot_id} has not been modified."
            )

        current, previous = self.context_diff.modified_snapshots[snapshot.name]
        if current.version == previous.version:
            return SnapshotChangeCategory.FORWARD_ONLY

        if current.data_hash_matches(previous):
            return SnapshotChangeCategory.BREAKING

        if previous.data_version in current.all_versions:
            index = current.all_versions.index(previous.data_version)
            versions = current.all_versions[index + 1 :]
        elif current.data_version in previous.all_versions:
            # Snapshot is a revert to a previous snapshot
            index = previous.all_versions.index(current.data_version)
            versions = previous.all_versions[index:]
        else:
            # Insufficient history, so err on the side of safety
            return SnapshotChangeCategory.BREAKING

        change_categories = [
            version.change_category for version in versions if version.change_category
        ]
        return min(change_categories, key=lambda x: x.value)

    def _categorize_snapshots(self) -> t.Tuple[t.List[Snapshot], SnapshotMapping]:
        """Automatically categorizes snapshots that can be automatically categorized and
        returns a list of added and directly modified snapshots as well as the mapping of
        indirectly modified snapshots.

        Returns:
            The tuple in which the first element contains a list of added and directly modified
            snapshots while the second element contains a mapping of indirectly modified snapshots.
        """
        queue = deque(self._dag.sorted())
        added_and_directly_modified = []
        all_indirectly_modified = set()

        while queue:
            model_name = queue.popleft()

            if model_name not in self.context_diff.snapshots:
                continue

            snapshot = self.context_diff.snapshots[model_name]

            if model_name in self.context_diff.modified_snapshots:
                if self.forward_only and self.is_new_snapshot(snapshot):
                    # In case of the forward only plan any modifications result in reuse of the
                    # previous version.
                    snapshot.set_version(snapshot.previous_version)

                upstream_model_names = self._dag.upstream(model_name)

                if self.context_diff.directly_modified(model_name):
                    added_and_directly_modified.append(snapshot)
                    if not self.forward_only:
                        self._ensure_no_paused_forward_only_upstream(
                            model_name, upstream_model_names
                        )
                else:
                    all_indirectly_modified.add(model_name)

                    # set to breaking if an indirect child has no directly modified parents
                    # that need a decision. this can happen when a revert to a parent causes
                    # an indirectly modified snapshot to be created because of a new parent
                    if not snapshot.version and not any(
                        self.context_diff.directly_modified(upstream)
                        and not self.context_diff.snapshots[upstream].version
                        for upstream in upstream_model_names
                    ):
                        snapshot.set_version()

            elif model_name in self.context_diff.added:
                if self.forward_only:
                    raise PlanError(
                        "New models can't be added as part of the forward-only plan."
                    )

                if self.is_new_snapshot(snapshot):
                    snapshot.set_version()
                added_and_directly_modified.append(snapshot)

        indirectly_modified: SnapshotMapping = defaultdict(set)

        for snapshot in added_and_directly_modified:
            for downstream in self._dag.downstream(snapshot.name):
                if downstream in all_indirectly_modified:
                    indirectly_modified[snapshot.name].add(downstream)

        return (
            added_and_directly_modified,
            indirectly_modified,
        )

    def _ensure_no_paused_forward_only_upstream(
        self, model_name: str, upstream_model_names: t.Iterable[str]
    ) -> None:
        for upstream in upstream_model_names:
            upstream_snapshot = self.context_diff.snapshots[upstream]
            if (
                upstream_snapshot.version
                and upstream_snapshot.is_forward_only
                and upstream_snapshot.is_paused
            ):
                raise PlanError(
                    f"Modified model '{model_name}' depends on a paused version of model '{upstream}'. "
                    "Possible remedies: "
                    "1) make sure your codebase is up-to-date; "
                    f"2) promote the current version of model '{upstream}' in the production environment; "
                    "3) recreate this plan in a forward-only mode."
                )


class PlanStatus(str, Enum):
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"

    @property
    def is_started(self):
        return self == PlanStatus.STARTED

    @property
    def is_failed(self):
        return self == PlanStatus.FAILED

    @property
    def is_finished(self):
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
