from __future__ import annotations

import json
import typing as t

from pydantic import validator

from sqlmesh.core.snapshot import SnapshotId, SnapshotTableInfo
from sqlmesh.utils import word_characters_only
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel


class Environment(PydanticModel):
    """Represents an isolated environment.

    Environments are isolated workspaces that hold pointers to physical tables.

    Args:
        name: The name of the environment.
        snapshots: The snapshots that are part of this environment.
        start_at: The start time of the environment.
        end_at: The end time of the environment.
        plan_id: The ID of the plan that last updated this environment.
        previous_plan_id: The ID of the previous plan that updated this enviornment.
        expiration_ts: The timestamp when this environment will expire.
        finalized_ts: The timestamp when this environment was finalized.
        promotion_snapshot_ids: The IDs of the snapshots that are promoted in this environment
            (i.e. for which the views are created). If not specified, all snapshots are promoted.
    """

    name: str
    snapshots: t.List[SnapshotTableInfo]
    start_at: TimeLike
    end_at: t.Optional[TimeLike]
    plan_id: str
    previous_plan_id: t.Optional[str]
    expiration_ts: t.Optional[int]
    finalized_ts: t.Optional[int]
    promoted_snapshot_ids: t.Optional[t.List[SnapshotId]] = None

    @validator("snapshots", pre=True)
    @classmethod
    def _convert_snapshots(cls, v: str | t.List[SnapshotTableInfo]) -> t.List[SnapshotTableInfo]:
        if isinstance(v, str):
            return [SnapshotTableInfo.parse_obj(obj) for obj in json.loads(v)]
        return v

    @validator("promoted_snapshot_ids", pre=True)
    @classmethod
    def _convert_snapshot_ids(cls, v: str | t.List[SnapshotId]) -> t.List[SnapshotId]:
        if isinstance(v, str):
            return [SnapshotId.parse_obj(obj) for obj in json.loads(v)]
        return v

    @validator("name", pre=True)
    @classmethod
    def _normalize_name(cls, v: str) -> str:
        return word_characters_only(v).lower()

    @t.overload
    @classmethod
    def normalize_name(cls, v: str) -> str:
        ...

    @t.overload
    @classmethod
    def normalize_name(cls, v: Environment) -> Environment:
        ...

    @classmethod
    def normalize_name(cls, v: str | Environment) -> str | Environment:
        """
        Normalizes the environment name so we create names that are valid names for database objects.
        This means alphanumeric and underscores only. Invalid characters are replaced with underscores.
        """
        if isinstance(v, Environment):
            return v
        if not isinstance(v, str):
            raise TypeError(f"Expected str or Environment, got {type(v).__name__}")
        return cls._normalize_name(v)

    @classmethod
    def normalize_names(cls, values: t.Iterable[str]) -> t.Set[str]:
        return {cls.normalize_name(value) for value in values}

    @property
    def promoted_snapshots(self) -> t.List[SnapshotTableInfo]:
        if self.promoted_snapshot_ids is None:
            return self.snapshots

        promoted_snapshot_ids = set(self.promoted_snapshot_ids)
        return [s for s in self.snapshots if s.snapshot_id in promoted_snapshot_ids]
