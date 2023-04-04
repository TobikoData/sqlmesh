from __future__ import annotations

import enum
import pathlib
import typing as t

from pydantic import BaseModel, validator

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.utils.date import TimeLike

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".csv"}


class FileType(str, enum.Enum):
    """An enumeration of possible file types."""

    audit = "audit"
    hooks = "hooks"
    macros = "macros"
    model = "model"
    tests = "tests"


class ApplyType(str, enum.Enum):
    """An enumeration of possible apply types."""

    virtual = "virtual"
    backfill = "backfill"


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


class ChangeDirect(BaseModel):
    model_name: str
    diff: str
    indirect: t.List[str] = []
    change_category: t.Optional[SnapshotChangeCategory] = None


class ChangeIndirect(BaseModel):
    model_name: str
    direct: t.List[str] = []


class ModelsDiff(BaseModel):
    direct: t.List[ChangeDirect] = []
    indirect: t.List[ChangeIndirect] = []
    metadata: t.Set[str] = set()

    @classmethod
    def get_modified_snapshots(
        cls,
        context_diff: ContextDiff,
    ) -> ModelsDiff:
        """Get the modified snapshots for a environment."""

        indirect = [
            ChangeIndirect(
                model_name=current.name, direct=[parent.name for parent in current.parents]
            )
            for current, _ in context_diff.modified_snapshots.values()
            if context_diff.indirectly_modified(current.name)
        ]
        direct: t.List[ChangeDirect] = []
        metadata = set()

        for snapshot_name in context_diff.modified_snapshots:
            current, _ = context_diff.modified_snapshots[snapshot_name]
            if context_diff.directly_modified(snapshot_name):
                direct.append(
                    ChangeDirect(
                        model_name=snapshot_name,
                        diff=context_diff.text_diff(snapshot_name),
                        indirect=[
                            change.model_name
                            for change in indirect
                            if snapshot_name in change.direct
                        ],
                        change_category=current.change_category,
                    )
                )
            elif context_diff.metadata_updated(snapshot_name):
                metadata.add(snapshot_name)

        direct_change_model_names = [change.model_name for change in direct]

        return ModelsDiff(
            direct=direct,
            indirect=[
                ChangeIndirect(
                    model_name=change.model_name,
                    direct=[
                        model_name
                        for model_name in change.direct
                        if model_name in direct_change_model_names
                    ],
                )
                for change in indirect
            ],
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


class Column(BaseModel):
    name: str
    type: str
    description: t.Optional[str]


class Model(BaseModel):
    name: str
    path: str
    dialect: str
    description: t.Optional[str]
    owner: t.Optional[str]
    columns: t.List[Column]


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


class PlanDates(BaseModel):
    start: t.Optional[TimeLike] = None
    end: t.Optional[TimeLike] = None


class PlanOptions(BaseModel):
    skip_tests: bool = False
    skip_backfill: bool = False
    no_gaps: bool = False
    forward_only: bool = False
    no_auto_categorization: bool = False
    create_from: t.Optional[str] = None
    restate_models: t.Optional[str] = None

    @validator("restate_models")
    def validate_restate_models(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return v.split(",")
        return v


class Query(BaseModel):
    sql: str


class ApplyResponse(BaseModel):
    type: ApplyType
