from __future__ import annotations

import sys
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from enum import IntEnum

from pydantic import Field
from sqlglot import exp
from sqlglot.helper import seq_get

from sqlmesh.core import constants as c
from sqlmesh.core.audit import BUILT_IN_AUDITS, Audit, ModelAudit, StandaloneAudit
from sqlmesh.core.model import Model, ModelKindMixin, ModelKindName, ViewKind
from sqlmesh.core.model.definition import _Model
from sqlmesh.core.node import IntervalUnit, NodeType
from sqlmesh.utils import sanitize_name
from sqlmesh.utils.date import (
    TimeLike,
    is_date,
    make_inclusive,
    make_inclusive_end,
    now,
    now_timestamp,
    to_date,
    to_datetime,
    to_ds,
    to_timestamp,
    yesterday,
)
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.hashing import hash_data
from sqlmesh.utils.pydantic import PydanticModel, field_validator

if sys.version_info >= (3, 9):
    from typing import Annotated
else:
    from typing_extensions import Annotated

if t.TYPE_CHECKING:
    from sqlmesh.core.environment import EnvironmentNamingInfo

Interval = t.Tuple[int, int]
Intervals = t.List[Interval]

Node = Annotated[t.Union[Model, StandaloneAudit], Field(descriminator="source_type")]


class SnapshotChangeCategory(IntEnum):
    """
    Values are ordered by decreasing severity and that ordering is required.

    BREAKING: The change requires that snapshot modified and downstream dependencies be rebuilt
    NON_BREAKING: The change requires that only the snapshot modified be rebuilt
    FORWARD_ONLY: The change requires no rebuilding
    INDIRECT_BREAKING: The change was caused indirectly and is breaking.
    INDIRECT_NON_BREAKING: The change was caused indirectly by a non-breaking change.
    """

    BREAKING = 1
    NON_BREAKING = 2
    FORWARD_ONLY = 3
    INDIRECT_BREAKING = 4
    INDIRECT_NON_BREAKING = 5

    @property
    def is_breaking(self) -> bool:
        return self == self.BREAKING

    @property
    def is_non_breaking(self) -> bool:
        return self == self.NON_BREAKING

    @property
    def is_forward_only(self) -> bool:
        return self == self.FORWARD_ONLY

    @property
    def is_indirect_breaking(self) -> bool:
        return self == self.INDIRECT_BREAKING

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self == self.INDIRECT_NON_BREAKING


class SnapshotFingerprint(PydanticModel, frozen=True):
    data_hash: str
    metadata_hash: str
    parent_data_hash: str = "0"
    parent_metadata_hash: str = "0"

    def to_version(self) -> str:
        return hash_data([self.data_hash, self.parent_data_hash])

    def to_identifier(self) -> str:
        return hash_data(
            [
                self.data_hash,
                self.metadata_hash,
                self.parent_data_hash,
                self.parent_metadata_hash,
            ]
        )


class SnapshotId(PydanticModel, frozen=True):
    name: str
    identifier: str

    @property
    def snapshot_id(self) -> SnapshotId:
        """Helper method to return self."""
        return self

    def __lt__(self, other: SnapshotId) -> bool:
        return self.name < other.name


class SnapshotNameVersion(PydanticModel, frozen=True):
    name: str
    version: str


class SnapshotIntervals(PydanticModel, frozen=True):
    name: str
    identifier: str
    version: str
    intervals: Intervals
    dev_intervals: Intervals

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)


class SnapshotDataVersion(PydanticModel, frozen=True):
    fingerprint: SnapshotFingerprint
    version: str
    temp_version: t.Optional[str] = None
    change_category: t.Optional[SnapshotChangeCategory] = None
    physical_schema_: t.Optional[str] = Field(default=None, alias="physical_schema")

    def snapshot_id(self, name: str) -> SnapshotId:
        return SnapshotId(name=name, identifier=self.fingerprint.to_identifier())

    @property
    def physical_schema(self) -> str:
        # The physical schema here is optional to maintain backwards compatibility with
        # records stored by previous versions of SQLMesh.
        return self.physical_schema_ or c.SQLMESH

    @property
    def data_version(self) -> SnapshotDataVersion:
        return self

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint.to_version() == self.version


class QualifiedViewName(PydanticModel, frozen=True):
    catalog: t.Optional[str] = None
    schema_name: t.Optional[str] = None
    table: str

    def for_environment(self, environment_naming_info: EnvironmentNamingInfo) -> str:
        return exp.table_name(self.table_for_environment(environment_naming_info))

    def table_for_environment(self, environment_naming_info: EnvironmentNamingInfo) -> exp.Table:
        return exp.table_(
            self.table_name_for_environment(environment_naming_info),
            db=self.schema_for_environment(environment_naming_info),
            catalog=self.catalog,
        )

    def schema_for_environment(self, environment_naming_info: EnvironmentNamingInfo) -> str:
        schema = self.schema_name or c.DEFAULT_SCHEMA
        if (
            environment_naming_info.name.lower() != c.PROD
            and environment_naming_info.suffix_target.is_schema
        ):
            schema = f"{schema}__{environment_naming_info.name}"
        return schema

    def table_name_for_environment(self, environment_naming_info: EnvironmentNamingInfo) -> str:
        table = self.table
        if (
            environment_naming_info.name.lower() != c.PROD
            and environment_naming_info.suffix_target.is_table
        ):
            table = f"{table}__{environment_naming_info.name}"
        return table


class SnapshotInfoMixin(ModelKindMixin):
    name: str
    temp_version: t.Optional[str]
    change_category: t.Optional[SnapshotChangeCategory]
    fingerprint: SnapshotFingerprint
    previous_versions: t.Tuple[SnapshotDataVersion, ...]

    @property
    def identifier(self) -> str:
        return self.fingerprint.to_identifier()

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

    @property
    def qualified_view_name(self) -> QualifiedViewName:
        view_name = exp.to_table(self.name)
        return QualifiedViewName(
            catalog=view_name.catalog or None,
            schema_name=view_name.db or None,
            table=view_name.name,
        )

    @property
    def previous_version(self) -> t.Optional[SnapshotDataVersion]:
        """Helper method to get the previous data version."""
        if self.previous_versions:
            return self.previous_versions[-1]
        return None

    @property
    def physical_schema(self) -> str:
        raise NotImplementedError

    @property
    def data_version(self) -> SnapshotDataVersion:
        raise NotImplementedError

    @property
    def is_new_version(self) -> bool:
        raise NotImplementedError

    @property
    def is_forward_only(self) -> bool:
        return self.change_category == SnapshotChangeCategory.FORWARD_ONLY

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self.change_category == SnapshotChangeCategory.INDIRECT_NON_BREAKING

    @property
    def all_versions(self) -> t.Tuple[SnapshotDataVersion, ...]:
        """Returns previous versions with the current version trimmed to DATA_VERSION_LIMIT."""
        return (*self.previous_versions, self.data_version)[-c.DATA_VERSION_LIMIT :]

    def data_hash_matches(self, other: t.Optional[SnapshotInfoMixin | SnapshotDataVersion]) -> bool:
        return other is not None and self.fingerprint.data_hash == other.fingerprint.data_hash

    def _table_name(self, version: str, is_dev: bool, for_read: bool) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            version: The snapshot version.
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        if is_dev and for_read:
            # If this snapshot is used for **reading**, return a temporary table
            # only if this snapshot captures a direct forward-only change applied to its model.
            is_temp = self.is_forward_only
        elif is_dev:
            # Use a temporary table in the dev environment when **writing** a forward-only snapshot
            # which was modified either directly or indirectly.
            is_temp = self.is_forward_only or self.is_indirect_non_breaking
        else:
            is_temp = False

        if is_temp:
            version = self.temp_version or self.fingerprint.to_version()

        return table_name(
            self.physical_schema,
            self.name,
            version,
            is_temp=is_temp,
        )

    @property
    def node_type(self) -> NodeType:
        raise NotImplementedError

    @property
    def is_model(self) -> bool:
        return self.node_type == NodeType.MODEL

    @property
    def is_audit(self) -> bool:
        return self.node_type == NodeType.AUDIT


class SnapshotTableInfo(PydanticModel, SnapshotInfoMixin, frozen=True):
    name: str
    fingerprint: SnapshotFingerprint
    version: str
    temp_version: t.Optional[str] = None
    physical_schema_: str = Field(alias="physical_schema")
    parents: t.Tuple[SnapshotId, ...]
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    change_category: t.Optional[SnapshotChangeCategory] = None
    kind_name: t.Optional[ModelKindName] = None
    node_type_: NodeType = Field(default=NodeType.MODEL, alias="node_type")

    def __lt__(self, other: SnapshotTableInfo) -> bool:
        return self.name < other.name

    def table_name(self, is_dev: bool = False, for_read: bool = False) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        return self._table_name(self.version, is_dev, for_read)

    @property
    def physical_schema(self) -> str:
        return self.physical_schema_

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to return self."""
        return self

    @property
    def data_version(self) -> SnapshotDataVersion:
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            change_category=self.change_category,
            physical_schema=self.physical_schema,
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        return self.fingerprint.to_version() == self.version

    @property
    def model_kind_name(self) -> t.Optional[ModelKindName]:
        return self.kind_name

    @property
    def node_type(self) -> NodeType:
        return self.node_type_

    @property
    def name_version(self) -> SnapshotNameVersion:
        """Returns the name and version of the snapshot."""
        return SnapshotNameVersion(name=self.name, version=self.version)


class Snapshot(PydanticModel, SnapshotInfoMixin):
    """A snapshot represents a node at a certain point in time.

    Snapshots are used to encapsulate everything needed to evaluate a node.
    They are standalone objects that hold all state and dynamic content necessary
    to render a node's query including things like macros. Snapshots also store intervals
    (timestamp ranges for what data we've processed).

    Nodes can be dynamically rendered due to macros. Rendering a node to its full extent
    requires storing variables and macro definitions. We store all of the macro definitions and
    global variable references in `python_env` in raw text to avoid pickling. The helper methods
    to achieve this are defined in utils.metaprogramming.

    Args:
        name: The snapshot name which is the same as the node name and should be unique per node.
        fingerprint: A unique hash of the node definition so that nodes can be reused across environments.
        node: Node object that the snapshot encapsulates.
        parents: The list of parent snapshots (upstream dependencies).
        audits: The list of generic audits used by the node.
        intervals: List of [start, end) intervals showing which time ranges a snapshot has data for.
        dev_intervals: List of [start, end) intervals showing development intervals (forward-only).
        created_ts: Epoch millis timestamp when a snapshot was first created.
        updated_ts: Epoch millis timestamp when a snapshot was last updated.
        ttl: The time-to-live of a snapshot determines when it should be deleted after it's no longer referenced
            in any environment.
        previous: The snapshot data version that this snapshot was based on. If this snapshot is new, then previous will be None.
        version: User specified version for a snapshot that is used for physical storage.
            By default, the version is the fingerprint, but not all changes to nodes require a backfill.
            If a user passes a previous version, that will be used instead and no backfill will be required.
        change_category: User specified change category indicating which nodes require backfill from node changes made in this snapshot.
        unpaused_ts: The timestamp which indicates when this snapshot was unpaused. Unpaused means that
            this snapshot is evaluated on a recurring basis. None indicates that this snapshot is paused.
        effective_from: The timestamp which indicates when this snapshot should be considered effective.
            Applicable for forward-only snapshots only.
        migrated: Whether or not this snapshot has been created as a result of migration.
        unrestorable: Whether or not this snapshot can be used to revert its model to a previous version.
    """

    name: str
    fingerprint: SnapshotFingerprint
    physical_schema_: t.Optional[str] = Field(default=None, alias="physical_schema")
    node: Node
    parents: t.Tuple[SnapshotId, ...]
    audits: t.Tuple[ModelAudit, ...] = tuple()
    intervals: Intervals = []
    dev_intervals: Intervals = []
    created_ts: int
    updated_ts: int
    ttl: str
    previous_versions: t.Tuple[SnapshotDataVersion, ...] = ()
    indirect_versions: t.Dict[str, t.Tuple[SnapshotDataVersion, ...]] = {}
    version: t.Optional[str] = None
    temp_version: t.Optional[str] = None
    change_category: t.Optional[SnapshotChangeCategory] = None
    unpaused_ts: t.Optional[int] = None
    effective_from: t.Optional[TimeLike] = None
    migrated: bool = False
    unrestorable: bool = False

    @field_validator("ttl")
    @classmethod
    def _time_delta_must_be_positive(cls, v: str) -> str:
        current_time = now()
        if to_datetime(v, current_time) < current_time:
            raise ValueError(
                "Must be positive. Use the 'in' keyword to denote a positive time interval. For example, 'in 7 days'."
            )
        return v

    @staticmethod
    def hydrate_with_intervals_by_version(
        snapshots: t.Iterable[Snapshot],
        intervals: t.Iterable[SnapshotIntervals],
        is_dev: bool = False,
    ) -> t.List[Snapshot]:
        """Hydrates target snapshots with given intervals.

        This will match snapshots with intervals by name and version rather than identifiers.

        Args:
            snapshots: Target snapshots.
            intervals: Target snapshot intervals.
            is_dev: If in development mode ignores same version intervals for paused forward-only snapshots.

        Returns:
            List of target snapshots with hydrated intervals.
        """
        intervals_by_name_version = defaultdict(list)
        for interval in intervals:
            intervals_by_name_version[(interval.name, interval.version)].append(interval)

        result = []
        for snapshot in snapshots:
            snapshot_intervals = intervals_by_name_version.get(
                (snapshot.name, snapshot.version_get_or_generate()), []
            )
            for interval in snapshot_intervals:
                snapshot.merge_intervals(interval)
            result.append(snapshot)

        return result

    @staticmethod
    def hydrate_with_intervals_by_identifier(
        snapshots: t.Iterable[Snapshot],
        intervals: t.Iterable[SnapshotIntervals],
    ) -> t.List[Snapshot]:
        """Hydrates target snapshots with given intervals.

        This will match snapshots with intervals by name and identifier rather than versions.

        Args:
            snapshots: Target snapshots.
            intervals: Target snapshot intervals.

        Returns:
            List of target snapshots with hydrated intervals.
        """
        intervals_by_snapshot_id = {i.snapshot_id: i for i in intervals}

        result = []
        for snapshot in snapshots:
            if snapshot.snapshot_id in intervals_by_snapshot_id:
                snapshot.merge_intervals(intervals_by_snapshot_id[snapshot.snapshot_id])
            result.append(snapshot)

        return result

    @classmethod
    def from_node(
        cls,
        node: Node,
        *,
        nodes: t.Dict[str, Node],
        ttl: str = c.DEFAULT_SNAPSHOT_TTL,
        version: t.Optional[str] = None,
        audits: t.Optional[t.Dict[str, ModelAudit]] = None,
        cache: t.Optional[t.Dict[str, SnapshotFingerprint]] = None,
    ) -> Snapshot:
        """Creates a new snapshot for a node.

        Args:
            Node: Node to snapshot.
            nodes: Dictionary of all nodes in the graph to make the fingerprint dependent on parent changes.
                If no dictionary is passed in the fingerprint will not be dependent on a node's parents.
            ttl: A TTL to determine how long orphaned (snapshots that are not promoted anywhere) should live.
            version: The version that a snapshot is associated with. Usually set during the planning phase.
            audits: Available audits by name.
            cache: Cache of node name to fingerprints.

        Returns:
            The newly created snapshot.
        """
        created_ts = now_timestamp()
        kwargs = {}
        if node.is_model:
            kwargs["audits"] = tuple(t.cast(_Model, node).referenced_audits(audits or {}))

        return cls(
            name=node.name,
            fingerprint=fingerprint_from_node(
                node,
                nodes=nodes,
                audits=audits,
                cache=cache,
            ),
            node=node,
            parents=tuple(
                SnapshotId(
                    name=name,
                    identifier=fingerprint_from_node(
                        nodes[name],
                        nodes=nodes,
                        audits=audits,
                        cache=cache,
                    ).to_identifier(),
                )
                for name in _parents_from_node(node, nodes)
            ),
            intervals=[],
            dev_intervals=[],
            created_ts=created_ts,
            updated_ts=created_ts,
            ttl=ttl,
            version=version,
            **kwargs,
        )

    def __eq__(self, other: t.Any) -> bool:
        return isinstance(other, Snapshot) and self.fingerprint == other.fingerprint

    def __hash__(self) -> int:
        return hash((self.__class__, self.name, self.fingerprint))

    def __lt__(self, other: Snapshot) -> bool:
        return self.name < other.name

    def add_interval(self, start: TimeLike, end: TimeLike, is_dev: bool = False) -> None:
        """Add a newly processed time interval to the snapshot.

        The actual stored intervals are [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive. This allows merging of ranges to be easier.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval. If end is a date, then it is considered inclusive.
                If it is a datetime object, then it is exclusive.
            is_dev: Indicates whether the given interval is being added while in development mode.
        """
        if to_timestamp(start) > to_timestamp(end):
            raise ValueError(
                f"Attempted to add an Invalid interval ({start}, {end}) to snapshot {self.snapshot_id}"
            )

        start_ts, end_ts = self.inclusive_exclusive(start, end, strict=False)
        if start_ts == end_ts:
            # Skipping partial interval.
            return

        intervals = self.dev_intervals if is_dev else self.intervals
        intervals.append((start_ts, end_ts))

        if len(intervals) < 2:
            return

        merged_intervals = merge_intervals(intervals)
        if is_dev:
            self.dev_intervals = merged_intervals
        else:
            self.intervals = merged_intervals

    def remove_interval(
        self, interval: Interval, execution_time: t.Optional[TimeLike] = None
    ) -> None:
        """Remove an interval from the snapshot.

        Args:
            interval: The interval to remove.
            execution_time: The date/time time reference to use for execution time. Defaults to now.
        """
        self.intervals = remove_interval(self.intervals, *interval)
        self.dev_intervals = remove_interval(self.dev_intervals, *interval)

    def get_removal_interval(
        self,
        start: TimeLike,
        end: TimeLike,
        execution_time: t.Optional[TimeLike] = None,
        *,
        strict: bool = True,
    ) -> Interval:
        end = execution_time or now() if self.depends_on_past else end
        return self.inclusive_exclusive(start, end, strict)

    def inclusive_exclusive(
        self,
        start: TimeLike,
        end: TimeLike,
        strict: bool = True,
        allow_partial: bool = False,
    ) -> Interval:
        """Transform the inclusive start and end into a [start, end) pair.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive)
            strict: Whether to fail when the inclusive start is the same as the exclusive end.
            allow_partial: Whether the interval can be partial or not.

        Returns:
            A [start, end) pair.
        """
        interval_unit = self.node.interval_unit
        start_ts = to_timestamp(interval_unit.cron_floor(start))
        if start_ts < to_timestamp(start):
            start_ts = to_timestamp(interval_unit.cron_next(start_ts))

        if is_date(end):
            end = to_datetime(end) + timedelta(days=1)
        end_ts = to_timestamp(interval_unit.cron_floor(end) if not allow_partial else end)

        if (strict and start_ts >= end_ts) or (start_ts > end_ts):
            raise ValueError(
                f"`end` ({to_datetime(end_ts)}) must be greater than `start` ({to_datetime(start_ts)})"
            )
        return (start_ts, end_ts)

    def merge_intervals(self, other: t.Union[Snapshot, SnapshotIntervals]) -> None:
        """Inherits intervals from the target snapshot.

        Args:
            other: The target snapshot to inherit intervals from.
        """
        effective_from_ts = self.normalized_effective_from_ts or 0
        apply_effective_from = effective_from_ts > 0 and self.identifier != other.identifier

        for start, end in other.intervals:
            # If the effective_from is set, then intervals that come after it must come from
            # the current snapshost.
            if apply_effective_from and start < effective_from_ts:
                end = min(end, effective_from_ts)
            if not apply_effective_from or end <= effective_from_ts:
                self.add_interval(start, end)

        if self.identifier == other.identifier:
            for start, end in other.dev_intervals:
                self.add_interval(start, end, is_dev=True)

    def missing_intervals(
        self,
        start: TimeLike,
        end: TimeLike,
        execution_time: t.Optional[TimeLike] = None,
        is_dev: bool = False,
        ignore_cron: bool = False,
    ) -> Intervals:
        """Find all missing intervals between [start, end].

        Although the inputs are inclusive, the returned stored intervals are
        [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive)
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            restatements: A set of snapshot names being restated
            is_dev: Indicates whether missing intervals are computed for the development environment.
            ignore_cron: Whether to ignore the node's cron schedule.

        Returns:
            A list of all the missing intervals as epoch timestamps.
        """
        intervals = self.dev_intervals if is_dev and self.is_paused_forward_only else self.intervals

        if self.is_symbolic or (self.is_seed and intervals):
            return []

        execution_time = execution_time or now()
        allow_partials = self.is_model and self.model.allow_partials
        start_ts, end_ts = (
            to_timestamp(ts)
            for ts in self.inclusive_exclusive(
                start,
                end,
                strict=False,
                allow_partial=allow_partials,
            )
        )

        interval_unit = self.node.interval_unit

        if allow_partials:
            upper_bound_ts = to_timestamp(execution_time)
            end_ts = min(end_ts, upper_bound_ts)
        else:
            upper_bound_ts = to_timestamp(
                self.node.cron_floor(execution_time) if not ignore_cron else execution_time
            )
            end_ts = min(end_ts, to_timestamp(interval_unit.cron_floor(upper_bound_ts)))

        croniter = interval_unit.croniter(start_ts)
        dates = [start_ts]

        # get all individual dates with the addition of extra lookback dates up to the execution date
        # when a model has lookback, we need to check all the intervals between itself and its lookback exist.
        while True:
            ts = to_timestamp(croniter.get_next(estimate=True))

            if ts < end_ts:
                dates.append(ts)
            else:
                croniter.get_prev()
                break

        lookback = self.model.lookback if self.is_model else 0

        for _ in range(lookback):
            ts = to_timestamp(croniter.get_next(estimate=True))
            if ts < upper_bound_ts:
                dates.append(ts)
            else:
                break

        missing = []
        for i in range(len(dates)):
            if dates[i] >= end_ts:
                break
            current_ts = dates[i]
            next_ts = (
                dates[i + 1]
                if i + 1 < len(dates)
                else min(to_timestamp(interval_unit.cron_next(current_ts)), upper_bound_ts)
            )
            compare_ts = seq_get(dates, i + lookback) or dates[-1]

            for low, high in intervals:
                if compare_ts < low:
                    missing.append((current_ts, next_ts))
                    break
                elif current_ts >= low and compare_ts < high:
                    break
            else:
                missing.append((current_ts, next_ts))

        return missing

    def categorize_as(self, category: SnapshotChangeCategory) -> None:
        """Assigns the given category to this snapshot.

        Args:
            category: The change category to assign to this snapshot.
        """
        is_forward_only = category in (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        )
        if is_forward_only and self.previous_version:
            self.version = self.previous_version.data_version.version
            self.physical_schema_ = self.previous_version.physical_schema
        else:
            self.version = self.fingerprint.to_version()

        self.change_category = category

    def set_unpaused_ts(self, unpaused_dt: t.Optional[TimeLike]) -> None:
        """Sets the timestamp for when this snapshot was unpaused.

        Args:
            unpaused_dt: The datetime object of when this snapshot was unpaused.
        """
        self.unpaused_ts = (
            to_timestamp(self.node.interval_unit.cron_floor(unpaused_dt)) if unpaused_dt else None
        )

    def table_name(self, is_dev: bool = False, for_read: bool = False) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_dev: Whether the table name will be used in development mode.
            for_read: Whether the table name will be used for reading by a different snapshot.
        """
        self._ensure_categorized()
        assert self.version
        return self._table_name(self.version, is_dev, for_read)

    def table_name_for_mapping(self, is_dev: bool = False) -> str:
        """Full table name used by a child snapshot for table mapping during evaluation.

        Args:
            is_dev: Whether the table name will be used in development mode.
        """
        self._ensure_categorized()
        assert self.version

        if is_dev and self.is_forward_only:
            # If this snapshot is unpaused we shouldn't be using a temporary
            # table for mapping purposes.
            is_dev = self.is_paused

        return self._table_name(self.version, is_dev, True)

    def is_temporary_table(self, is_dev: bool) -> bool:
        """Provided whether the snapshot is used in a development mode or not, returns True
        if the snapshot targets a temporary table or a clone and False otherwise.
        """
        return is_dev and (self.is_forward_only or self.is_indirect_non_breaking) and self.is_paused

    def version_get_or_generate(self) -> str:
        """Helper method to get the version or generate it from the fingerprint."""
        return self.version or self.fingerprint.to_version()

    def is_valid_start(
        self,
        start: t.Optional[TimeLike],
        snapshot_start: t.Optional[TimeLike] = None,
        execution_time: t.Optional[TimeLike] = None,
    ) -> bool:
        """Checks if the given start and end are valid for this snapshot.
        Args:
            start: The start date/time of the interval (inclusive)
            snapshot_start: The start date/time of the snapshot (inclusive)
        """
        # The snapshot may not have a start defined. If so we use the provided snapshot start.
        if self.depends_on_past and start:
            if not snapshot_start:
                raise SQLMeshError("Snapshot must have a start defined if it depends on past")

            interval_unit = self.node.interval_unit
            start_ts = to_timestamp(interval_unit.cron_floor(start))

            if not self.intervals:
                # The start date must be aligned by the interval unit.
                snapshot_start_ts = to_timestamp(interval_unit.cron_floor(snapshot_start))
                if snapshot_start_ts < to_timestamp(snapshot_start):
                    snapshot_start_ts = to_timestamp(interval_unit.cron_next(snapshot_start_ts))
                return snapshot_start_ts >= start_ts
            # Make sure that if there are missing intervals for this snapshot that they all occur at or after the
            # provided start_ts. Otherwise we know that we are doing a non-contiguous load and therefore this is not
            # a valid start.
            missing_intervals = self.missing_intervals(
                snapshot_start, now(), execution_time=execution_time
            )
            earliest_interval = missing_intervals[0][0] if missing_intervals else None
            if earliest_interval:
                return earliest_interval >= start_ts
        return True

    def get_latest(self, default: t.Optional[TimeLike] = None) -> t.Optional[TimeLike]:
        """The latest interval loaded for the snapshot. Default is used if intervals are not defined"""

        def to_end_date(end: int, unit: IntervalUnit) -> TimeLike:
            if unit.is_day:
                return to_date(make_inclusive_end(end))
            return end

        return (
            to_end_date(to_timestamp(self.intervals[-1][1]), self.node.interval_unit)
            if self.intervals
            else default
        )

    @property
    def physical_schema(self) -> str:
        if self.physical_schema_ is not None:
            return self.physical_schema_
        return self.model.physical_schema if self.is_model else ""

    @property
    def table_info(self) -> SnapshotTableInfo:
        """Helper method to get the SnapshotTableInfo from the Snapshot."""
        self._ensure_categorized()
        return SnapshotTableInfo(
            physical_schema=self.physical_schema,
            name=self.name,
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            parents=self.parents,
            previous_versions=self.previous_versions,
            change_category=self.change_category,
            kind_name=self.model_kind_name,
            node_type=self.node_type,
        )

    @property
    def data_version(self) -> SnapshotDataVersion:
        self._ensure_categorized()
        return SnapshotDataVersion(
            fingerprint=self.fingerprint,
            version=self.version,
            temp_version=self.temp_version,
            change_category=self.change_category,
            physical_schema=self.physical_schema,
        )

    @property
    def snapshot_intervals(self) -> SnapshotIntervals:
        self._ensure_categorized()
        return SnapshotIntervals(
            name=self.name,
            identifier=self.identifier,
            version=self.version,
            intervals=self.intervals.copy(),
            dev_intervals=self.dev_intervals.copy(),
        )

    @property
    def is_materialized_view(self) -> bool:
        """Returns whether or not this snapshot's model represents a materialized view."""
        return (
            self.is_model and isinstance(self.model.kind, ViewKind) and self.model.kind.materialized
        )

    @property
    def is_new_version(self) -> bool:
        """Returns whether or not this version is new and requires a backfill."""
        self._ensure_categorized()
        return self.fingerprint.to_version() == self.version

    @property
    def is_paused(self) -> bool:
        return self.unpaused_ts is None

    @property
    def is_paused_forward_only(self) -> bool:
        return self.is_paused and self.is_forward_only

    @property
    def normalized_effective_from_ts(self) -> t.Optional[int]:
        return (
            to_timestamp(self.node.interval_unit.cron_floor(self.effective_from))
            if self.effective_from
            else None
        )

    @property
    def model_kind_name(self) -> t.Optional[ModelKindName]:
        return self.model.kind.name if self.is_model else None

    @property
    def node_type(self) -> NodeType:
        if self.node.is_model:
            return NodeType.MODEL
        elif self.node.is_audit:
            return NodeType.AUDIT
        raise SQLMeshError(f"Snapshot {self.snapshot_id} has an unknown node type.")

    @property
    def model(self) -> Model:
        model = self.model_or_none
        if model:
            return model
        raise SQLMeshError(f"Snapshot {self.snapshot_id} is not a model snapshot.")

    @property
    def model_or_none(self) -> t.Optional[Model]:
        if self.is_model:
            return t.cast(Model, self.node)
        return None

    @property
    def audit(self) -> StandaloneAudit:
        if self.is_audit:
            return t.cast(StandaloneAudit, self.node)
        raise SQLMeshError(f"Snapshot {self.snapshot_id} is not an audit snapshot.")

    @property
    def depends_on_past(self) -> bool:
        """Whether or not this models depends on past intervals to be accurate before loading following intervals."""
        return self.is_model and self.model.depends_on_past

    @property
    def audits_with_args(self) -> t.List[t.Tuple[Audit, t.Dict[str, exp.Expression]]]:
        if self.is_model:
            audits_by_name = {**BUILT_IN_AUDITS, **{a.name: a for a in self.audits}}
            return [
                (audits_by_name[audit_name], audit_args)
                for audit_name, audit_args in self.model.audits
            ]
        elif self.is_audit:
            return [(self.audit, {})]

        return []

    @property
    def name_version(self) -> SnapshotNameVersion:
        """Returns the name and version of the snapshot."""
        return SnapshotNameVersion(name=self.name, version=self.version)

    @property
    def disable_restatement(self) -> bool:
        """Is restatement disabled for the node"""
        return self.is_model and self.model.disable_restatement

    def _ensure_categorized(self) -> None:
        if not self.change_category:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been categorized yet.")
        if not self.version:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been versioned yet.")


SnapshotIdLike = t.Union[SnapshotId, SnapshotTableInfo, Snapshot]
SnapshotInfoLike = t.Union[SnapshotTableInfo, Snapshot]
SnapshotNameVersionLike = t.Union[SnapshotNameVersion, SnapshotTableInfo, Snapshot]


def table_name(physical_schema: str, name: str, version: str, is_temp: bool = False) -> str:
    table = exp.to_table(name)

    # bigquery projects usually have "-" in them which is illegal in the table name, so we aggressively prune
    name = "__".join(sanitize_name(part.name) for part in table.parts)
    temp_suffix = "__temp" if is_temp else ""

    table.set("this", exp.to_identifier(f"{name}__{version}{temp_suffix}"))
    table.set("db", exp.to_identifier(physical_schema))
    return exp.table_name(table)


def fingerprint_from_node(
    node: Node,
    *,
    nodes: t.Dict[str, Node],
    audits: t.Optional[t.Dict[str, ModelAudit]] = None,
    cache: t.Optional[t.Dict[str, SnapshotFingerprint]] = None,
) -> SnapshotFingerprint:
    """Helper function to generate a fingerprint based on the data and metadata of the node and its parents.

    This method tries to remove non-meaningful differences to avoid ever-changing fingerprints.
    The fingerprint is made up of two parts split by an underscore -- query_metadata. The query hash is
    determined purely by the rendered query and the metadata by everything else.

    Args:
        node: Node to fingerprint.
        nodes: Dictionary of all nodes in the graph to make the fingerprint dependent on parent changes.
            If no dictionary is passed in the fingerprint will not be dependent on a node's parents.
        audits: Available audits by name.
        cache: Cache of node name to fingerprints.

    Returns:
        The fingerprint.
    """
    cache = {} if cache is None else cache

    if node.name not in cache:
        parents = [
            fingerprint_from_node(
                nodes[table],
                nodes=nodes,
                audits=audits,
                cache=cache,
            )
            for table in node.depends_on
            if table in nodes
        ]

        parent_data_hash = hash_data(sorted(p.to_version() for p in parents))

        parent_metadata_hash = hash_data(
            sorted(h for p in parents for h in (p.metadata_hash, p.parent_metadata_hash))
        )

        cache[node.name] = SnapshotFingerprint(
            data_hash=node.data_hash,
            metadata_hash=node.metadata_hash(audits or {}),
            parent_data_hash=parent_data_hash,
            parent_metadata_hash=parent_metadata_hash,
        )

    return cache[node.name]


def _parents_from_node(
    node: Node,
    nodes: t.Dict[str, Node],
) -> t.Set[str]:
    parent_nodes = set()
    for parent in node.depends_on:
        if parent in nodes:
            parent_nodes.add(parent)
            if nodes[parent].is_model and t.cast(_Model, nodes[parent]).kind.is_embedded:
                parent_nodes.update(_parents_from_node(nodes[parent], nodes))

    return parent_nodes


def merge_intervals(intervals: Intervals) -> Intervals:
    """Merge a list of intervals.

    Args:
        intervals: A list of intervals to merge together.

    Returns:
        A new list of sorted and merged intervals.
    """
    intervals = sorted(intervals)

    merged = [intervals[0]]

    for interval in intervals[1:]:
        current = merged[-1]

        if interval[0] <= current[1]:
            merged[-1] = (current[0], max(current[1], interval[1]))
        else:
            merged.append(interval)

    return merged


def _format_date_time(time_like: TimeLike, unit: t.Optional[IntervalUnit]) -> str:
    if unit is None or unit.is_date_granularity:
        return to_ds(time_like)
    return to_datetime(time_like).isoformat()[:19]


def format_intervals(intervals: Intervals, unit: t.Optional[IntervalUnit]) -> str:
    inclusive_intervals = [make_inclusive(start, end) for start, end in intervals]
    return ", ".join(
        " - ".join([_format_date_time(start, unit), _format_date_time(end, unit)])
        for start, end in inclusive_intervals
    )


def remove_interval(intervals: Intervals, remove_start: int, remove_end: int) -> Intervals:
    """Remove an interval from a list of intervals. Assumes that the correct start and end intervals have been
    passed in. Use `get_remove_interval` method of `Snapshot` to get the correct start/end given the snapshot's
    information.

    Args:
        intervals: A list of exclusive intervals.
        remove_start: The inclusive start to remove.
        remove_end: The exclusive end to remove.

    Returns:
        A new list of intervals.
    """
    modified: Intervals = []

    for start, end in intervals:
        if remove_start > start and remove_end < end:
            modified.extend(
                (
                    (start, remove_start),
                    (remove_end, end),
                )
            )
        elif remove_start > start:
            modified.append((start, min(remove_start, end)))
        elif remove_end < end:
            modified.append((max(remove_end, start), end))

    return modified


def to_table_mapping(snapshots: t.Iterable[Snapshot], is_dev: bool) -> t.Dict[str, str]:
    return {
        snapshot.name: snapshot.table_name_for_mapping(is_dev=is_dev)
        for snapshot in snapshots
        if snapshot.version and not snapshot.is_symbolic
    }


def has_paused_forward_only(
    targets: t.Iterable[SnapshotIdLike],
    snapshots: t.Union[t.List[Snapshot], t.Dict[SnapshotId, Snapshot]],
) -> bool:
    if not isinstance(snapshots, dict):
        snapshots = {s.snapshot_id: s for s in snapshots}
    for target in targets:
        target_snapshot = snapshots[target.snapshot_id]
        if target_snapshot.is_paused_forward_only:
            return True
    return False


def missing_intervals(
    snapshots: t.Collection[Snapshot],
    start: t.Optional[TimeLike] = None,
    end: t.Optional[TimeLike] = None,
    execution_time: t.Optional[TimeLike] = None,
    restatements: t.Optional[t.Dict[str, Interval]] = None,
    is_dev: bool = False,
    ignore_cron: bool = False,
) -> t.Dict[Snapshot, Intervals]:
    """Returns all missing intervals given a collection of snapshots."""
    missing = {}
    cache: t.Dict[str, datetime] = {}
    end_date = end or now()
    start_dt = (
        to_datetime(start)
        if start
        else earliest_start_date(
            snapshots, cache, default_value=to_date(end_date) - timedelta(days=1)
        )
    )
    restatements = restatements or {}

    for snapshot in snapshots:
        interval = restatements.get(snapshot.name)
        snapshot_start_date = start_dt
        snapshot_end_date = end_date
        if interval:
            snapshot_start_date, snapshot_end_date = (to_datetime(i) for i in interval)
            snapshot = snapshot.copy()
            snapshot.intervals = snapshot.intervals.copy()
            snapshot.remove_interval(interval, execution_time)

        intervals = snapshot.missing_intervals(
            max(
                to_datetime(snapshot_start_date),
                to_datetime(start_date(snapshot, snapshots, cache) or snapshot_start_date),
            ),
            snapshot_end_date,
            execution_time=execution_time,
            is_dev=is_dev,
            ignore_cron=ignore_cron,
        )
        if intervals:
            missing[snapshot] = intervals

    return missing


def earliest_start_date(
    snapshots: t.Iterable[Snapshot],
    cache: t.Optional[t.Dict[str, datetime]] = None,
    default_value: t.Optional[TimeLike] = None,
) -> datetime:
    """Get the earliest start date from a collection of snapshots.

    Args:
        snapshots: Snapshots to find earliest start date.
        cache: optional cache to make computing cache date more efficient
    Returns:
        The earliest start date or yesterday if none is found.
    """
    cache = {} if cache is None else cache
    snapshots = list(snapshots)
    earliest = to_datetime(default_value or yesterday().date())
    if snapshots:
        return min(start_date(snapshot, snapshots, cache) or earliest for snapshot in snapshots)
    return earliest


def start_date(
    snapshot: Snapshot,
    snapshots: t.Dict[SnapshotId, Snapshot] | t.Iterable[Snapshot],
    cache: t.Optional[t.Dict[str, datetime]] = None,
) -> t.Optional[datetime]:
    """Get the effective/inferred start date for a snapshot.

    Not all snapshots define a start date. In those cases, the node's start date
    can be inferred from its parent's start date.

    Args:
        snapshot: snapshot to infer start date.
        snapshots: a catalog of available snapshots.
        cache: optional cache to make computing cache date more efficient

    Returns:
        Start datetime object.
    """
    cache = {} if cache is None else cache
    if snapshot.name in cache:
        return cache[snapshot.name]
    if snapshot.node.start:
        start = to_datetime(snapshot.node.start)
        cache[snapshot.name] = start
        return start

    if not isinstance(snapshots, dict):
        snapshots = {snapshot.snapshot_id: snapshot for snapshot in snapshots}

    earliest = None

    for parent in snapshot.parents:
        if parent not in snapshots:
            continue

        start_dt = start_date(snapshots[parent], snapshots, cache)

        if not earliest:
            earliest = start_dt
        elif start_dt:
            earliest = min(earliest, start_dt)

    if earliest:
        cache[snapshot.name] = earliest

    return earliest
