"""
# Environment
"""
from __future__ import annotations

import json
import typing as t

from pydantic import validator

from sqlmesh.core.snapshot import SnapshotTableInfo
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
