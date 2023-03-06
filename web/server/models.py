from __future__ import annotations

import enum
import pathlib
import typing as t

from pydantic import BaseModel, validator

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.utils.date import TimeLike

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".csv"}


class FileType(str, enum.Enum):
    """An enumeration of possible file types."""

    audit = "audit"
    hooks = "hooks"
    macros = "macros"
    model = "model"
    tests = "tests"


class File(BaseModel):
    name: str
    path: str
    extension: str = ""
    is_supported: bool = False
    content: t.Optional[str]
    type: t.Optional[FileType]

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


class Directory(BaseModel):
    name: str
    path: str
    directories: t.List[Directory] = []
    files: t.List[File] = []


class Context(BaseModel):
    concurrent_tasks: int
    engine_adapter: str
    time_column_format: str
    scheduler: str
    models: t.List[str] = []
    config: str


class ModelsDiff(BaseModel):
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

        for snapshot_name in context_diff.modified_snapshots:
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


class ContextEnvironmentChanges(BaseModel):
    added: t.Set[str]
    removed: t.Set[str]
    modified: ModelsDiff


class ContextEnvironmentBackfill(BaseModel):
    model_name: str
    interval: t.Tuple[str, str]
    batches: int


class ContextEnvironment(BaseModel):
    environment: str
    start: TimeLike
    end: TimeLike
    changes: t.Optional[ContextEnvironmentChanges]
    backfills: t.List[ContextEnvironmentBackfill] = []


class EvaluateInput(BaseModel):
    model: str
    start: TimeLike
    end: TimeLike
    latest: TimeLike
    limit: int = 1000


class Model(BaseModel):
    name: str
    path: str
    description: t.Optional[str]
    owner: t.Optional[str]


class Models(BaseModel):
    models: t.Dict[str, Model]


class RenderInput(BaseModel):
    model: str
    start: t.Optional[TimeLike] = None
    end: t.Optional[TimeLike] = None
    latest: t.Optional[TimeLike] = None
    expand: t.Union[bool, t.Iterable[str]] = False
    pretty: bool = True
    dialect: t.Optional[str] = None


class Query(BaseModel):
    sql: str
