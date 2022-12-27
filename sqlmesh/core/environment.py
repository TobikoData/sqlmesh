"""
# Environment
"""
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
    start: TimeLike
    end: t.Optional[TimeLike]
    plan_id: str
    previous_plan_id: t.Optional[str]

    @validator("snapshots", pre=True)
    @classmethod
    def _convert_snapshots(cls, v: t.Any):
        if isinstance(v, str):
            return [SnapshotTableInfo.parse_obj(obj) for obj in json.loads(v)]
        return v

    @validator("name", pre=True)
    @classmethod
    def _normalize_name(cls, v: t.Any):
        if isinstance(v, str):
            return word_characters_only(v).lower()
        return v

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
        if isinstance(v, Environment):
            return v
        if not isinstance(v, str):
            raise TypeError(f"Expected str or Environment, got {type(v).__name__}")
        return cls._normalize_name(v)
