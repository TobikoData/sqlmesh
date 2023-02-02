from __future__ import annotations

import pathlib
import typing as t

from pydantic import validator

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.utils.pydantic import PydanticModel

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml"}


class File(PydanticModel):
    name: str
    path: str
    extension: str = ""
    is_supported: bool = False
    content: t.Optional[str]

    @validator("extension", always=True)
    def default_extension(cls, v: str, values: t.Dict[str, t.Any]) -> str:
        if "name" in values:
            return pathlib.Path(values["name"]).suffix
        return v

    @validator("is_supported", always=True)
    def default_is_supported(cls, v: bool, values: t.Dict[str, t.Any]) -> bool:
        if "extension" in values:
            return values["extension"] in SUPPORTED_EXTENSIONS
        return v


class Directory(PydanticModel):
    name: str
    path: str
    directories: t.List[Directory] = []
    files: t.List[File] = []


class Context(PydanticModel):
    concurrent_tasks: int
    engine_adapter: str
    time_column_format: str
    scheduler: str
    models: t.List[str] = []
    config: str


class ModelsDiff(PydanticModel):
    direct: t.List[t.Dict[str, str]]
    indirect: t.Set[str]
    metadata: t.Set[str]

    @classmethod
    def get_modified_snapshots(
        cls,
        context_diff: ContextDiff,
    ) -> ModelsDiff:
        """Get the modified snapshots for a environment."""

        direct = []
        indirect = set()
        metadata = set()

        for snapshot_name in context_diff.snapshots.keys():
            if context_diff.directly_modified(snapshot_name):
                direct.append(
                    {
                        "model_name": snapshot_name,
                        "diff": context_diff.text_diff(snapshot_name),
                    }
                )
            elif context_diff.indirectly_modified(snapshot_name):
                indirect.add(snapshot_name)
            elif context_diff.metadata_updated(snapshot_name):
                metadata.add(snapshot_name)

        return ModelsDiff(
            direct=direct,
            indirect=indirect,
            metadata=metadata,
        )


class ContextEnvironmentChanges(PydanticModel):
    added: t.Set[str]
    removed: t.Set[str]
    modified: ModelsDiff


class ContextEnvironmentBackfill(PydanticModel):
    model_name: str
    interval: t.Tuple[str, str]
    batches: int


class ContextEnvironment(PydanticModel):
    environment: str
    changes: t.Optional[ContextEnvironmentChanges]
    backfills: t.List[ContextEnvironmentBackfill] = []
