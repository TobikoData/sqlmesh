from __future__ import annotations

import enum
import pathlib
import typing as t

import pydantic
from pydantic import BaseModel
from sqlglot import exp
from watchfiles import Change

from sqlmesh.core.context_diff import ContextDiff
from sqlmesh.core.environment import Environment
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.snapshot.definition import SnapshotChangeCategory
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import (
    PYDANTIC_MAJOR_VERSION,
    field_validator,
    field_validator_v1_args,
)

SUPPORTED_EXTENSIONS = {".py", ".sql", ".yaml", ".yml", ".csv"}


class FileType(str, enum.Enum):
    """An enumeration of possible file types."""

    audit = "audit"
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
    content: t.Optional[str] = None
    type: t.Optional[FileType] = None

    if PYDANTIC_MAJOR_VERSION >= 2:
        model_config = pydantic.ConfigDict(validate_default=True)  # type: ignore

    @field_validator("extension", always=True, mode="before")
    @field_validator_v1_args
    def default_extension(cls, v: str, values: t.Dict[str, t.Any]) -> str:
        if "name" in values:
            return pathlib.Path(values["name"]).suffix
        return v

    @field_validator("is_supported", always=True, mode="before")
    @field_validator_v1_args
    def default_is_supported(cls, v: bool, values: t.Dict[str, t.Any]) -> bool:
        if "extension" in values:
            return values["extension"] in SUPPORTED_EXTENSIONS
        return v


class Directory(BaseModel):
    name: str
    path: str
    directories: t.List[Directory] = []
    files: t.List[File] = []


class Meta(BaseModel):
    version: str


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
            elif context_diff.indirectly_modified(snapshot_name):
                continue
            elif context_diff.metadata_updated(snapshot_name):
                metadata.add(snapshot_name)

        direct_change_model_names = [change.model_name for change in direct]
        indirect_change_model_names = [change.model_name for change in indirect]

        for change in indirect:
            change.direct = [
                model_name
                for model_name in change.direct
                if model_name in direct_change_model_names
                or model_name in indirect_change_model_names
            ]
            change.direct.reverse()

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
    view_name: str
    interval: t.Tuple[str, str]
    batches: int


class ContextEnvironment(BaseModel):
    environment: str
    start: TimeLike
    end: TimeLike
    changes: t.Optional[ContextEnvironmentChanges] = None
    backfills: t.List[ContextEnvironmentBackfill] = []


class Environments(BaseModel):
    environments: t.Dict[str, Environment] = {}
    pinned_environments: t.Set[str] = set()
    default_target_environment: str = ""


class EvaluateInput(BaseModel):
    model: str
    start: TimeLike
    end: TimeLike
    execution_time: TimeLike
    limit: int = 1000


class FetchdfInput(BaseModel):
    sql: str
    limit: int = 1000


class Column(BaseModel):
    name: str
    type: str
    description: t.Optional[str]


class Reference(BaseModel):
    name: str
    expression: str
    unique: bool


class ModelDetails(BaseModel):
    owner: t.Optional[str] = None
    kind: t.Optional[str] = None
    batch_size: t.Optional[int] = None
    cron: t.Optional[str] = None
    stamp: t.Optional[TimeLike] = None
    start: t.Optional[TimeLike] = None
    retention: t.Optional[int] = None
    storage_format: t.Optional[str] = None
    time_column: t.Optional[str] = None
    tags: t.Optional[str] = None
    references: t.List[Reference] = []
    partitioned_by: t.Optional[str] = None
    clustered_by: t.Optional[str] = None
    lookback: t.Optional[int] = None
    cron_prev: t.Optional[TimeLike] = None
    cron_next: t.Optional[TimeLike] = None
    interval_unit: t.Optional[IntervalUnit] = None
    annotated: t.Optional[bool] = None


class Model(BaseModel):
    name: str
    path: str
    dialect: str
    type: str
    columns: t.List[Column]
    description: t.Optional[str] = None
    details: t.Optional[ModelDetails] = None
    sql: t.Optional[str] = None


class RenderInput(BaseModel):
    model: str
    start: t.Optional[TimeLike] = None
    end: t.Optional[TimeLike] = None
    execution_time: t.Optional[TimeLike] = None
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
    include_unmodified: bool = False
    create_from: t.Optional[str] = None
    restate_models: t.Optional[str] = None

    @field_validator("restate_models")
    @classmethod
    def validate_restate_models(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return v.split(",")
        return v


class LineageColumn(BaseModel):
    source: t.Optional[str] = None
    expression: t.Optional[str] = None
    models: t.Optional[t.Dict[str, t.List[str]]]


class Query(BaseModel):
    sql: str


class ApplyResponse(BaseModel):
    type: ApplyType


class ApiExceptionPayload(BaseModel):
    timestamp: int
    message: str
    origin: str
    status: t.Optional[int] = None
    trigger: t.Optional[str] = None
    type: t.Optional[str] = None
    description: t.Optional[str] = None
    traceback: t.Optional[str] = None
    stack: t.Optional[t.List[str]] = None


class SchemaDiff(BaseModel):
    source: str
    target: str
    source_schema: t.Dict[str, str]
    target_schema: t.Dict[str, str]
    added: t.Dict[str, str]
    removed: t.Dict[str, str]
    modified: t.Dict[str, str]

    @field_validator(
        "source_schema", "target_schema", "added", "removed", "modified", mode="before"
    )
    @classmethod
    def validate_schema(
        cls,
        v: t.Union[t.Dict[str, exp.DataType], t.List[t.Tuple[str, exp.DataType]], t.Dict[str, str]],
    ) -> t.Dict[str, str]:
        if isinstance(v, dict):
            return {k: str(v) for k, v in v.items()}
        if isinstance(v, list):
            return {k: str(v) for k, v in v}
        return v


class RowDiff(BaseModel):
    source: str
    target: str
    stats: t.Dict[str, float]
    sample: t.Dict[str, t.Any]
    source_count: int
    target_count: int
    count_pct_change: float


class TableDiff(BaseModel):
    schema_diff: SchemaDiff
    row_diff: RowDiff
    on: t.List[t.Tuple[str, str]]


class TestCase(BaseModel):
    name: str
    path: pathlib.Path


class TestErrorOrFailure(TestCase):
    tb: str


class TestSkipped(TestCase):
    reason: str


class TestResult(BaseModel):
    tests_run: int
    failures: t.List[TestErrorOrFailure]
    errors: t.List[TestErrorOrFailure]
    skipped: t.List[TestSkipped]


class ArtifactType(str, enum.Enum):
    file = "file"
    directory = "directory"


class ArtifactChange(BaseModel):
    change: Change
    path: str
    type: t.Optional[ArtifactType] = None
    file: t.Optional[File] = None
