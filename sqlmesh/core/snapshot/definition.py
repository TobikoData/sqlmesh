from __future__ import annotations

import sys
import typing as t
from collections import defaultdict
from datetime import datetime, timedelta
from enum import IntEnum
from functools import cached_property, lru_cache

from pydantic import Field
from sqlglot import exp
from sqlglot.helper import seq_get
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers

from sqlmesh.core import constants as c
from sqlmesh.core.audit import BUILT_IN_AUDITS, Audit, ModelAudit, StandaloneAudit
from sqlmesh.core.model import Model, ModelKindMixin, ModelKindName, ViewKind, CustomKind
from sqlmesh.core.model.definition import _Model
from sqlmesh.core.node import IntervalUnit, NodeType
from sqlmesh.utils import sanitize_name
from sqlmesh.utils.dag import DAG
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
    to_ts,
    validate_date_range,
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
    from sqlglot.dialects.dialect import DialectType
    from sqlmesh.core.environment import EnvironmentNamingInfo
    from sqlmesh.core.config import Config

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
    METADATA: The change was caused by a metadata update.
    """

    BREAKING = 1
    NON_BREAKING = 2
    FORWARD_ONLY = 3
    INDIRECT_BREAKING = 4
    INDIRECT_NON_BREAKING = 5
    METADATA = 6

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
    def is_metadata(self) -> bool:
        return self == self.METADATA

    @property
    def is_indirect_breaking(self) -> bool:
        return self == self.INDIRECT_BREAKING

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self == self.INDIRECT_NON_BREAKING

    def __repr__(self) -> str:
        return self.name


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

    def __str__(self) -> str:
        return f"SnapshotFingerprint<{self.to_identifier()}, data: {self.data_hash}, meta: {self.metadata_hash}, pdata: {self.parent_data_hash}, pmeta: {self.parent_metadata_hash}>"


class SnapshotId(PydanticModel, frozen=True):
    name: str
    identifier: str

    @property
    def snapshot_id(self) -> SnapshotId:
        """Helper method to return self."""
        return self

    def __eq__(self, other: t.Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self.name == other.name
            and self.identifier == other.identifier
        )

    def __hash__(self) -> int:
        return hash((self.__class__, self.name, self.identifier))

    def __lt__(self, other: SnapshotId) -> bool:
        return self.name < other.name

    def __str__(self) -> str:
        return f"SnapshotId<{self.name}: {self.identifier}>"


class SnapshotNameVersion(PydanticModel, frozen=True):
    name: str
    version: str

    @property
    def name_version(self) -> SnapshotNameVersion:
        """Helper method to return self."""
        return self


class SnapshotIntervals(PydanticModel, frozen=True):
    name: str
    identifier: str
    version: str
    intervals: Intervals
    dev_intervals: Intervals

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

    @property
    def name_version(self) -> SnapshotNameVersion:
        return SnapshotNameVersion(name=self.name, version=self.version)


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

    def for_environment(
        self, environment_naming_info: EnvironmentNamingInfo, dialect: DialectType = None
    ) -> str:
        return exp.table_name(self.table_for_environment(environment_naming_info, dialect=dialect))

    def table_for_environment(
        self, environment_naming_info: EnvironmentNamingInfo, dialect: DialectType = None
    ) -> exp.Table:
        return exp.table_(
            self.table_name_for_environment(environment_naming_info, dialect=dialect),
            db=self.schema_for_environment(environment_naming_info, dialect=dialect),
            catalog=self.catalog_for_environment(environment_naming_info),
        )

    def catalog_for_environment(
        self, environment_naming_info: EnvironmentNamingInfo
    ) -> t.Optional[str]:
        return environment_naming_info.catalog_name_override or self.catalog

    def schema_for_environment(
        self, environment_naming_info: EnvironmentNamingInfo, dialect: DialectType = None
    ) -> str:
        normalize = environment_naming_info.normalize_name

        if self.schema_name:
            schema = self.schema_name
        else:
            schema = c.DEFAULT_SCHEMA
            if normalize:
                schema = normalize_identifiers(schema, dialect=dialect).name

        if (
            environment_naming_info.name.lower() != c.PROD
            and environment_naming_info.suffix_target.is_schema
        ):
            env_name = environment_naming_info.name
            if normalize:
                env_name = normalize_identifiers(env_name, dialect=dialect).name

            schema = f"{schema}__{env_name}"

        return schema

    def table_name_for_environment(
        self, environment_naming_info: EnvironmentNamingInfo, dialect: DialectType = None
    ) -> str:
        table = self.table
        if (
            environment_naming_info.name.lower() != c.PROD
            and environment_naming_info.suffix_target.is_table
        ):
            env_name = environment_naming_info.name
            if environment_naming_info.normalize_name:
                env_name = normalize_identifiers(env_name, dialect=dialect).name

            table = f"{table}__{env_name}"

        return table


class SnapshotInfoMixin(ModelKindMixin):
    name: str
    temp_version: t.Optional[str]
    change_category: t.Optional[SnapshotChangeCategory]
    fingerprint: SnapshotFingerprint
    previous_versions: t.Tuple[SnapshotDataVersion, ...]
    # Added to support Migration # 34 (default catalog)
    # This can be removed from this model once Pydantic 1 support is dropped (must remain in `Snapshot` though)
    base_table_name_override: t.Optional[str]

    @property
    def identifier(self) -> str:
        return self.fingerprint.to_identifier()

    @property
    def snapshot_id(self) -> SnapshotId:
        return SnapshotId(name=self.name, identifier=self.identifier)

    @property
    def qualified_view_name(self) -> QualifiedViewName:
        view_name = exp.to_table(self.fully_qualified_table or self.name)
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

    @cached_property
    def fully_qualified_table(self) -> t.Optional[exp.Table]:
        raise NotImplementedError

    @property
    def is_forward_only(self) -> bool:
        return self.change_category == SnapshotChangeCategory.FORWARD_ONLY

    @property
    def is_metadata(self) -> bool:
        return self.change_category == SnapshotChangeCategory.METADATA

    @property
    def is_indirect_non_breaking(self) -> bool:
        return self.change_category == SnapshotChangeCategory.INDIRECT_NON_BREAKING

    @property
    def reuses_previous_version(self) -> bool:
        return self.change_category in (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        )

    @property
    def all_versions(self) -> t.Tuple[SnapshotDataVersion, ...]:
        """Returns previous versions with the current version trimmed to DATA_VERSION_LIMIT."""
        return (*self.previous_versions, self.data_version)[-c.DATA_VERSION_LIMIT :]

    def display_name(
        self,
        environment_naming_info: EnvironmentNamingInfo,
        default_catalog: t.Optional[str],
        dialect: DialectType = None,
    ) -> str:
        """
        Returns the model name as a qualified view name.
        This is just used for presenting information back to the user and `qualified_view_name` should be used
        when wanting a view name in all other cases.
        """
        return display_name(self, environment_naming_info, default_catalog, dialect=dialect)

    def data_hash_matches(self, other: t.Optional[SnapshotInfoMixin | SnapshotDataVersion]) -> bool:
        return other is not None and self.fingerprint.data_hash == other.fingerprint.data_hash

    def temp_version_get_or_generate(self) -> str:
        """Helper method to get the temp version or generate it from the fingerprint."""
        return self.temp_version or self.fingerprint.to_version()

    def _table_name(self, version: str, is_deployable: bool) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            version: The snapshot version.
            is_deployable: Indicates whether to return the table name for deployment to production.
        """
        if self.is_external:
            return self.name

        is_dev_table = not is_deployable
        if is_dev_table:
            version = self.temp_version_get_or_generate()

        if self.fully_qualified_table is None:
            raise SQLMeshError(
                f"Tried to get a table name for a snapshot that does not have a table. {self.name}"
            )
        # We want to exclude the catalog from the name but still include catalog when determining the fqn
        # for the table.
        if self.base_table_name_override:
            base_table_name = self.base_table_name_override
        else:
            fqt = self.fully_qualified_table.copy()
            fqt.set("catalog", None)
            base_table_name = fqt.sql()
        return table_name(
            self.physical_schema,
            base_table_name,
            version,
            is_dev_table=is_dev_table,
            catalog=self.fully_qualified_table.catalog,
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
    # Added to support Migration # 34 (default catalog)
    # This can be removed from this model once Pydantic 1 support is dropped (must remain in `Snapshot` though)
    base_table_name_override: t.Optional[str] = None

    custom_materialization: t.Optional[str] = None

    def __lt__(self, other: SnapshotTableInfo) -> bool:
        return self.name < other.name

    def table_name(self, is_deployable: bool = True) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_deployable: Indicates whether to return the table name for deployment to production.
        """
        return self._table_name(self.version, is_deployable)

    @property
    def physical_schema(self) -> str:
        return self.physical_schema_

    @cached_property
    def fully_qualified_table(self) -> exp.Table:
        return exp.to_table(self.name)

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
    version: t.Optional[str] = None
    temp_version: t.Optional[str] = None
    change_category: t.Optional[SnapshotChangeCategory] = None
    unpaused_ts: t.Optional[int] = None
    effective_from: t.Optional[TimeLike] = None
    migrated: bool = False
    unrestorable: bool = False
    # Added to support Migration # 34 (default catalog)
    base_table_name_override: t.Optional[str] = None

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
        config: t.Optional[Config] = None,
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
        default_audits = (
            config.model_defaults.audits if (config and config.model_defaults.audits) else []
        )
        if node.is_model:
            kwargs["audits"] = tuple(
                t.cast(_Model, node).referenced_audits(audits or {}, default_audits)
            )

        return cls(
            name=node.fqn,
            fingerprint=fingerprint_from_node(
                node,
                nodes=nodes,
                audits=audits,
                cache=cache,
            ),
            node=node,
            parents=tuple(
                SnapshotId(
                    name=parent_node.fqn,
                    identifier=fingerprint_from_node(
                        parent_node,
                        nodes=nodes,
                        audits=audits,
                        cache=cache,
                    ).to_identifier(),
                )
                for parent_node in _parents_from_node(node, nodes).values()
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
        if start_ts >= end_ts:
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

    def remove_interval(self, interval: Interval) -> None:
        """Remove an interval from the snapshot.

        Args:
            interval: The interval to remove.
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
        is_preview: bool = False,
    ) -> Interval:
        """Get the interval that should be removed from the snapshot.

        Args:
            start: The start date/time of the interval to remove.
            end: The end date/time of the interval to removed.
            execution_time: The time the interval is being removed.
            strict: Whether to fail when the inclusive start is the same as the exclusive end.
            is_preview: Whether the interval needs to be removed for a preview of forward-only changes.
                When previewing, we are not actually restating a model, but removing an interval to trigger
                a run.
        """
        end = execution_time or now() if self.depends_on_past else end
        if not is_preview and self.full_history_restatement_only and self.intervals:
            start = self.intervals[0][0]
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
        if start_ts < to_timestamp(start) and not self.model.allow_partials:
            start_ts = to_timestamp(interval_unit.cron_next(start_ts))

        if is_date(end):
            end = to_datetime(end) + timedelta(days=1)
        end_ts = to_timestamp(interval_unit.cron_floor(end) if not allow_partial else end)
        if end_ts < start_ts and to_timestamp(end) > to_timestamp(start) and not strict:
            # This can happen when the interval unit is coarser than the size of the input interval.
            # For example, if the interval unit is monthly, but the input interval is only 1 hour long.
            return (start_ts, end_ts)

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

        previous_ids = {s.snapshot_id(self.name) for s in self.previous_versions}
        if self.identifier == other.identifier or (
            # Indirect Non-Breaking snapshots share the dev table with its previous version.
            # The same applies to migrated snapshots.
            (self.is_indirect_non_breaking or self.is_metadata or self.migrated)
            and other.snapshot_id in previous_ids
        ):
            for start, end in other.dev_intervals:
                self.add_interval(start, end, is_dev=True)

    @property
    def evaluatable(self) -> bool:
        """Whether or not a snapshot should be evaluated and have intervals."""
        return bool(not self.is_symbolic or self.model.audits)

    def missing_intervals(
        self,
        start: TimeLike,
        end: TimeLike,
        execution_time: t.Optional[TimeLike] = None,
        deployability_index: t.Optional[DeployabilityIndex] = None,
        ignore_cron: bool = False,
        end_bounded: bool = False,
    ) -> Intervals:
        """Find all missing intervals between [start, end].

        Although the inputs are inclusive, the returned stored intervals are
        [start_ts, end_ts) or start epoch timestamp inclusive and end epoch
        timestamp exclusive.

        Args:
            start: The start date/time of the interval (inclusive)
            end: The end date/time of the interval (inclusive if the type is date, exclusive otherwise)
            execution_time: The date/time time reference to use for execution time. Defaults to now.
            deployability_index: Determines snapshots that are deployable in the context of this evaluation.
            ignore_cron: Whether to ignore the node's cron schedule.
            end_bounded: If set to true, the returned intervals will be bounded by the target end date, disregarding lookback,
                allow_partials, and other attributes that could cause the intervals to exceed the target end date.

        Returns:
            A list of all the missing intervals as epoch timestamps.
        """
        # If the node says that it has an end, and we are wanting to load past it, then we can return no empty intervals
        # Also if a node's start is after the end of the range we are checking then we can return no empty intervals
        if (self.node.end and to_datetime(start) > to_datetime(self.node.end)) or (
            self.node.start and to_datetime(self.node.start) > to_datetime(end)
        ):
            return []
        if self.node.start and to_datetime(start) < to_datetime(self.node.start):
            start = self.node.start
        if self.node.end and make_inclusive_end(end) > make_inclusive_end(self.node.end):
            end = self.node.end
        # If the amount of time being checked is less than the size of a single interval then we
        # know that there can't being missing intervals within that range and return
        validate_date_range(start, end)
        if (
            not is_date(end)
            and not (self.is_model and self.model.allow_partials)
            and to_timestamp(end) - to_timestamp(start) < self.node.interval_unit.milliseconds
        ):
            return []

        deployability_index = deployability_index or DeployabilityIndex.all_deployable()
        intervals = (
            self.intervals if deployability_index.is_representative(self) else self.dev_intervals
        )

        if not self.evaluatable or (self.is_seed and intervals):
            return []

        allow_partials = not end_bounded and self.is_model and self.model.allow_partials
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

        execution_time = execution_time or now()
        if end_bounded:
            execution_time = min(to_timestamp(execution_time), end_ts)

        if not allow_partials:
            upper_bound_ts = to_timestamp(
                self.node.cron_floor(execution_time) if not ignore_cron else execution_time
            )
            end_ts = min(end_ts, to_timestamp(interval_unit.cron_floor(upper_bound_ts)))
        else:
            upper_bound_ts = to_timestamp(execution_time)
            end_ts = min(end_ts, upper_bound_ts)

        lookback = self.model.lookback if self.is_model else 0

        return compute_missing_intervals(
            interval_unit, tuple(intervals), start_ts, end_ts, upper_bound_ts, lookback
        )

    def categorize_as(self, category: SnapshotChangeCategory) -> None:
        """Assigns the given category to this snapshot.

        Args:
            category: The change category to assign to this snapshot.
        """
        self.temp_version = None
        reuse_previous_version = category in (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
            SnapshotChangeCategory.METADATA,
        )
        if reuse_previous_version and self.previous_version:
            previous_version = self.previous_version
            self.version = previous_version.data_version.version
            self.physical_schema_ = previous_version.physical_schema
            if category.is_indirect_non_breaking or category.is_metadata:
                # Reuse the dev table for indirect non-breaking changes.
                self.temp_version = (
                    previous_version.data_version.temp_version
                    or previous_version.fingerprint.to_version()
                )
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

    def table_name(self, is_deployable: bool = True) -> str:
        """Full table name pointing to the materialized location of the snapshot.

        Args:
            is_deployable: Indicates whether to return the table name for deployment to production.
        """
        self._ensure_categorized()
        assert self.version
        return self._table_name(self.version, is_deployable)

    def version_get_or_generate(self) -> str:
        """Helper method to get the version or generate it from the fingerprint."""
        return self.version or self.fingerprint.to_version()

    def is_valid_start(
        self,
        start: t.Optional[TimeLike],
        snapshot_start: TimeLike,
        execution_time: t.Optional[TimeLike] = None,
    ) -> bool:
        """Checks if the given start and end are valid for this snapshot.
        Args:
            start: The start date/time of the interval (inclusive)
            snapshot_start: The start date/time of the snapshot (inclusive)
        """
        # The snapshot may not have a start defined. If so we use the provided snapshot start.
        if self.depends_on_past and start:
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

    def needs_destructive_check(
        self,
        allow_destructive_snapshots: t.Set[str],
    ) -> bool:
        return (
            self.is_model
            and not self.model.on_destructive_change.is_allow
            and self.name not in allow_destructive_snapshots
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

        custom_materialization = (
            self.model.kind.materialization
            if self.is_model and isinstance(self.model.kind, CustomKind)
            else None
        )

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
            custom_materialization=custom_materialization,
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
        """Whether or not this models depends on past intervals to be populated before loading following intervals.

        This represents a superset of the following types of models:
        1. Models that depend on themselves but can be restated from an arbitrary point in time (any start date) as long as interval batches are processed sequentially.
           An example of this can be an INCREMENTAL_BY_TIME_RANGE model that references previous records from itself.
        2. Models that can only be restated from the beginning of history *and* their interval batches must be processed sequentially.
        """
        return self.depends_on_self or self.full_history_restatement_only

    @property
    def depends_on_self(self) -> bool:
        """Whether or not this models depends on self."""
        return self.is_model and self.model.depends_on_self

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

    @cached_property
    def fully_qualified_table(self) -> t.Optional[exp.Table]:
        if not self.is_model:
            return None
        return t.cast(Model, self.node).fully_qualified_table

    @property
    def expiration_ts(self) -> int:
        return to_timestamp(self.ttl, relative_base=to_datetime(self.updated_ts))

    @property
    def custom_materialization(self) -> t.Optional[str]:
        if self.is_custom:
            return t.cast(CustomKind, self.model.kind).materialization
        return None

    def _ensure_categorized(self) -> None:
        if not self.change_category:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been categorized yet.")
        if not self.version:
            raise SQLMeshError(f"Snapshot {self.snapshot_id} has not been versioned yet.")


class SnapshotTableCleanupTask(PydanticModel):
    snapshot: SnapshotTableInfo
    dev_table_only: bool


SnapshotIdLike = t.Union[SnapshotId, SnapshotTableInfo, Snapshot]
SnapshotInfoLike = t.Union[SnapshotTableInfo, Snapshot]
SnapshotNameVersionLike = t.Union[SnapshotNameVersion, SnapshotTableInfo, Snapshot]


class DeployabilityIndex(PydanticModel, frozen=True):
    """Contains information about deployability of every snapshot.

    Deployability is defined as whether or not the output that a snapshot produces during the
    current evaluation can be reused in (deployed to) the production environment.
    """

    indexed_ids: t.FrozenSet[str]
    is_opposite_index: bool = False
    representative_shared_version_ids: t.FrozenSet[str] = frozenset()

    @field_validator("indexed_ids", "representative_shared_version_ids", mode="before")
    @classmethod
    def _snapshot_ids_set_validator(cls, v: t.Any) -> t.Optional[t.FrozenSet[t.Tuple[str, str]]]:
        if v is None:
            return v
        # Transforming into strings because the serialization of sets of objects / lists is broken in Pydantic.
        return frozenset(
            {
                (
                    cls._snapshot_id_key(snapshot_id)
                    if isinstance(snapshot_id, SnapshotId)
                    else snapshot_id
                )
                for snapshot_id in v
            }
        )

    def is_deployable(self, snapshot: SnapshotIdLike) -> bool:
        """Returns true if the output produced by the given snapshot in a development environment can be reused
        in (deployed to) production

        Args:
            snapshot: The snapshot to check.

        Returns:
            True if the snapshot is deployable, False otherwise.
        """
        snapshot_id = self._snapshot_id_key(snapshot.snapshot_id)
        return (self.is_opposite_index and snapshot_id not in self.indexed_ids) or (
            not self.is_opposite_index and snapshot_id in self.indexed_ids
        )

    def is_representative(self, snapshot: SnapshotIdLike) -> bool:
        """Returns true if the output produced by the given snapshot in a development environment can be reused
        in (deployed to) production, or if this snapshot already represents what is currently in production.

        Unlike `is_deployable`, this variant also captures FORWARD_ONLY and INDIRECT_NON_BREAKING snapshots that
        are not deployable by their nature but are currently promoted in production. Therefore, it's safe to consider
        them as such when constructing a plan, building a physical table mapping or computing missing intervals.

        Args:
            snapshot: The snapshot to check.

        Returns:
            True if the snapshot is representative, False otherwise.
        """
        snapshot_id = self._snapshot_id_key(snapshot.snapshot_id)
        representative = snapshot_id in self.representative_shared_version_ids
        return representative or self.is_deployable(snapshot)

    def with_non_deployable(self, snapshot: SnapshotIdLike) -> DeployabilityIndex:
        """Creates a new index with the given snapshot marked as non-deployable."""
        return self._add_snapshot(snapshot, False)

    def with_deployable(self, snapshot: SnapshotIdLike) -> DeployabilityIndex:
        """Creates a new index with the given snapshot marked as deployable."""
        return self._add_snapshot(snapshot, True)

    def _add_snapshot(self, snapshot: SnapshotIdLike, deployable: bool) -> DeployabilityIndex:
        snapshot_id = {self._snapshot_id_key(snapshot.snapshot_id)}
        indexed_ids = self.indexed_ids
        if self.is_opposite_index:
            indexed_ids = indexed_ids - snapshot_id if deployable else indexed_ids | snapshot_id
        else:
            indexed_ids = indexed_ids | snapshot_id if deployable else indexed_ids - snapshot_id

        return DeployabilityIndex(
            indexed_ids=indexed_ids,
            is_opposite_index=self.is_opposite_index,
            representative_shared_version_ids=self.representative_shared_version_ids,
        )

    @classmethod
    def all_deployable(cls) -> DeployabilityIndex:
        return cls(indexed_ids=frozenset(), is_opposite_index=True)

    @classmethod
    def none_deployable(cls) -> DeployabilityIndex:
        return cls(indexed_ids=frozenset())

    @classmethod
    def create(
        cls, snapshots: t.Dict[SnapshotId, Snapshot] | t.Collection[Snapshot]
    ) -> DeployabilityIndex:
        if not isinstance(snapshots, dict):
            snapshots = {s.snapshot_id: s for s in snapshots}
        dag = snapshots_to_dag(snapshots.values())
        reversed_dag = dag.reversed.graph

        deployability_mapping: t.Dict[SnapshotId, bool] = {}
        representative_shared_version_ids: t.Set[SnapshotId] = set()

        def _visit(node: SnapshotId, deployable: bool = True) -> None:
            if deployability_mapping.get(node) in (False, deployable) and (
                deployable or node not in representative_shared_version_ids
            ):
                return

            if deployable and node in snapshots:
                snapshot = snapshots[node]
                # Capture uncategorized snapshot which represents a forward-only model.
                is_uncategorized_forward_only_model = (
                    snapshot.change_category is None
                    and snapshot.previous_versions
                    and snapshot.is_model
                    and snapshot.model.forward_only
                )
                if (
                    snapshot.is_forward_only
                    or snapshot.is_indirect_non_breaking
                    or is_uncategorized_forward_only_model
                ):
                    # FORWARD_ONLY and INDIRECT_NON_BREAKING snapshots are not deployable by nature.
                    this_deployable = False
                    if not snapshot.is_paused or snapshot.is_indirect_non_breaking:
                        # This snapshot represents what's currently deployed in prod.
                        representative_shared_version_ids.add(node)
                else:
                    this_deployable = True
                children_deployable = not (
                    snapshot.is_paused
                    and (snapshot.is_forward_only or is_uncategorized_forward_only_model)
                )
            else:
                this_deployable, children_deployable = False, False
                representative_shared_version_ids.discard(node)

            deployability_mapping[node] = deployability_mapping.get(node, True) and this_deployable
            for child in reversed_dag[node]:
                _visit(child, children_deployable)

        for node in dag.roots:
            _visit(node)

        deployable_ids = {
            snapshot_id for snapshot_id, deployable in deployability_mapping.items() if deployable
        }
        non_deployable_ids = set(snapshots) - deployable_ids

        # Pick the smaller set to reduce the size of the serialized object.
        if len(deployable_ids) <= len(non_deployable_ids):
            return cls(
                indexed_ids=deployable_ids,
                representative_shared_version_ids=representative_shared_version_ids,
            )
        return cls(
            indexed_ids=non_deployable_ids,
            is_opposite_index=True,
            representative_shared_version_ids=representative_shared_version_ids,
        )

    @staticmethod
    def _snapshot_id_key(snapshot_id: SnapshotId) -> str:
        return f"{snapshot_id.name}__{snapshot_id.identifier}"


def table_name(
    physical_schema: str,
    name: str,
    version: str,
    is_dev_table: bool = False,
    catalog: t.Optional[str] = None,
) -> str:
    table = exp.to_table(name)

    # bigquery projects usually have "-" in them which is illegal in the table name, so we aggressively prune
    name = "__".join(sanitize_name(part.name) for part in table.parts)
    temp_suffix = "__temp" if is_dev_table else ""

    table.set("this", exp.to_identifier(f"{name}__{version}{temp_suffix}"))
    table.set("db", exp.to_identifier(physical_schema))
    if not table.catalog and catalog:
        table.set("catalog", exp.parse_identifier(catalog))
    return exp.table_name(table)


def display_name(
    snapshot_info_like: t.Union[SnapshotInfoLike, SnapshotInfoMixin],
    environment_naming_info: EnvironmentNamingInfo,
    default_catalog: t.Optional[str],
    dialect: DialectType = None,
) -> str:
    """
    Returns the model name as a qualified view name.
    This is just used for presenting information back to the user and `qualified_view_name` should be used
    when wanting a view name in all other cases.
    """
    if snapshot_info_like.is_audit:
        return snapshot_info_like.name
    view_name = exp.to_table(snapshot_info_like.name)
    qvn = QualifiedViewName(
        catalog=(
            view_name.catalog
            if view_name.catalog and view_name.catalog != default_catalog
            else None
        ),
        schema_name=view_name.db or None,
        table=view_name.name,
    )
    return qvn.for_environment(environment_naming_info, dialect=dialect)


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

    if node.fqn not in cache:
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

        cache[node.fqn] = SnapshotFingerprint(
            data_hash=node.data_hash,
            metadata_hash=node.metadata_hash(audits or {}),
            parent_data_hash=parent_data_hash,
            parent_metadata_hash=parent_metadata_hash,
        )

    return cache[node.fqn]


def _parents_from_node(
    node: Node,
    nodes: t.Dict[str, Node],
) -> t.Dict[str, Node]:
    parent_nodes = {}
    for parent_fqn in node.depends_on:
        parent = nodes.get(parent_fqn)
        if parent:
            parent_nodes[parent.fqn] = parent
            if parent.is_model and t.cast(_Model, parent).kind.is_embedded:
                parent_nodes.update(_parents_from_node(parent, nodes))

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
    # TODO: Remove `[0:19]` once `to_ts` always returns a timestamp without timezone
    return to_ts(time_like)[0:19]


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


def to_table_mapping(
    snapshots: t.Iterable[Snapshot], deployability_index: t.Optional[DeployabilityIndex]
) -> t.Dict[str, str]:
    deployability_index = deployability_index or DeployabilityIndex.all_deployable()
    return {
        snapshot.name: snapshot.table_name(deployability_index.is_representative(snapshot))
        for snapshot in snapshots
        if snapshot.version and not snapshot.is_embedded and snapshot.is_model
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
    restatements: t.Optional[t.Dict[SnapshotId, Interval]] = None,
    deployability_index: t.Optional[DeployabilityIndex] = None,
    ignore_cron: bool = False,
    end_bounded: bool = False,
) -> t.Dict[Snapshot, Intervals]:
    """Returns all missing intervals given a collection of snapshots."""
    missing = {}
    cache: t.Dict[str, datetime] = {}
    end_date = end or now()
    start_dt = (
        to_datetime(start)
        if start
        else earliest_start_date(snapshots, cache=cache, relative_to=end_date)
    )
    restatements = restatements or {}

    deployability_index = deployability_index or DeployabilityIndex.all_deployable()

    for snapshot in snapshots:
        if not snapshot.evaluatable:
            continue
        interval = restatements.get(snapshot.snapshot_id)
        snapshot_start_date = start_dt
        snapshot_end_date = end_date
        if interval:
            snapshot_start_date, snapshot_end_date = (to_datetime(i) for i in interval)
            snapshot = snapshot.copy()
            snapshot.intervals = snapshot.intervals.copy()
            snapshot.remove_interval(interval)

        missing_interval_end_date = snapshot_end_date
        node_end_date = snapshot.node.end
        if node_end_date and (to_datetime(node_end_date) < to_datetime(snapshot_end_date)):
            missing_interval_end_date = node_end_date

        intervals = snapshot.missing_intervals(
            max(
                to_datetime(snapshot_start_date),
                to_datetime(start_date(snapshot, snapshots, cache, relative_to=snapshot_end_date)),
            ),
            missing_interval_end_date,
            execution_time=execution_time,
            deployability_index=deployability_index,
            ignore_cron=ignore_cron,
            end_bounded=end_bounded,
        )
        if intervals:
            missing[snapshot] = intervals

    return missing


@lru_cache(maxsize=None)
def compute_missing_intervals(
    interval_unit: IntervalUnit,
    intervals: t.Tuple[Interval, ...],
    start_ts: int,
    end_ts: int,
    upper_bound_ts: int,
    lookback: int,
) -> Intervals:
    """Computes all missing intervals between start and end given intervals.

    Args:
        interval_unit: The interval unit.
        intervals: The intervals to check what's missing.
        start_ts: Inclusive timestamp start.
        end_ts: Exclusive timestamp end.
        upper_bound_ts: The exclusive upper bound timestamp for lookback.
        lookback: A lookback window.

    Returns:
        A list of all timestamps in this range.
    """
    croniter = interval_unit.croniter(start_ts)
    timestamps = [start_ts]

    # get all individual timestamps with the addition of extra lookback timestamps up to the execution date
    # when a model has lookback, we need to check all the intervals between itself and its lookback exist.
    while True:
        ts = to_timestamp(croniter.get_next(estimate=True))

        if ts < end_ts:
            timestamps.append(ts)
        else:
            croniter.get_prev(estimate=True)
            break

    for _ in range(lookback):
        ts = to_timestamp(croniter.get_next(estimate=True))
        if ts < upper_bound_ts:
            timestamps.append(ts)
        else:
            break

    missing = []
    for i in range(len(timestamps)):
        if timestamps[i] >= end_ts:
            break
        current_ts = timestamps[i]
        next_ts = (
            timestamps[i + 1]
            if i + 1 < len(timestamps)
            else min(
                to_timestamp(interval_unit.cron_next(current_ts, estimate=True)), upper_bound_ts
            )
        )
        compare_ts = seq_get(timestamps, i + lookback) or timestamps[-1]

        for low, high in intervals:
            if compare_ts < low:
                missing.append((current_ts, next_ts))
                break
            elif current_ts >= low and compare_ts < high:
                break
        else:
            missing.append((current_ts, next_ts))

    return missing


def earliest_start_date(
    snapshots: t.Collection[Snapshot],
    cache: t.Optional[t.Dict[str, datetime]] = None,
    relative_to: t.Optional[TimeLike] = None,
) -> datetime:
    """Get the earliest start date from a collection of snapshots.

    Args:
        snapshots: Snapshots to find earliest start date.
        cache: optional cache to make computing cache date more efficient
        relative_to: the base date to compute start from if inferred from cron
    Returns:
        The earliest start date or yesterday if none is found.
    """
    cache = {} if cache is None else cache
    if snapshots:
        return min(
            start_date(snapshot, snapshots, cache=cache, relative_to=relative_to)
            for snapshot in snapshots
        )
    return yesterday()


def start_date(
    snapshot: Snapshot,
    snapshots: t.Dict[SnapshotId, Snapshot] | t.Iterable[Snapshot],
    cache: t.Optional[t.Dict[str, datetime]] = None,
    relative_to: t.Optional[TimeLike] = None,
) -> datetime:
    """Get the effective/inferred start date for a snapshot.

    Not all snapshots define a start date. In those cases, the node's start date
    can be inferred from its parent's start date or from its cron.

    Args:
        snapshot: snapshot to infer start date.
        snapshots: a catalog of available snapshots.
        cache: optional cache to make computing cache date more efficient
        relative_to: the base date to compute start from if inferred from cron

    Returns:
        Start datetime object.
    """
    cache = {} if cache is None else cache
    key = f"{snapshot.name}_{to_timestamp(relative_to)}" if relative_to else snapshot.name
    if key in cache:
        return cache[key]
    if snapshot.node.start:
        start = to_datetime(snapshot.node.start)
        cache[key] = start
        return start

    if not isinstance(snapshots, dict):
        snapshots = {snapshot.snapshot_id: snapshot for snapshot in snapshots}

    earliest = snapshot.node.cron_prev(snapshot.node.cron_floor(relative_to or now()))

    for parent in snapshot.parents:
        if parent in snapshots:
            earliest = min(
                earliest,
                start_date(snapshots[parent], snapshots, cache=cache, relative_to=relative_to),
            )

    cache[key] = earliest
    return earliest


def snapshots_to_dag(snapshots: t.Collection[Snapshot]) -> DAG[SnapshotId]:
    dag: DAG[SnapshotId] = DAG()
    for snapshot in snapshots:
        dag.add(snapshot.snapshot_id, snapshot.parents)
    return dag
