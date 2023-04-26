from __future__ import annotations

import json
import typing as t

from pydantic import validator

from sqlmesh.core.snapshot import SnapshotTableInfo
from sqlmesh.utils import word_characters_only
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel


class Environment(PydanticModel):
    """Represents an isolated environment.

    Environments are isolated workspaces that hold pointers to physical tables.
    """

    name: str
    snapshots: t.List[SnapshotTableInfo]
    start_at: TimeLike
    end_at: t.Optional[TimeLike]
    plan_id: str
    previous_plan_id: t.Optional[str]
    expiration_ts: t.Optional[int]
    finalized_ts: t.Optional[int]

    @validator("snapshots", pre=True)
    @classmethod
    def _convert_snapshots(cls, v: str | t.List[SnapshotTableInfo]) -> t.List[SnapshotTableInfo]:
        if isinstance(v, str):
            return [SnapshotTableInfo.parse_obj(obj) for obj in json.loads(v)]
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
