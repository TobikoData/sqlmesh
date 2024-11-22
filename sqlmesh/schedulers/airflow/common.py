from __future__ import annotations

import typing as t

from sqlmesh.core import constants as c
from sqlmesh.core.environment import Environment
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.plan.definition import EvaluatablePlan
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotTableInfo,
)
from sqlmesh.core.user import User
from sqlmesh.utils import sanitize_name
from sqlmesh.utils.date import TimeLike, DatetimeRanges
from sqlmesh.utils.pydantic import PydanticModel

JANITOR_DAG_ID = "sqlmesh_janitor_dag"
JANITOR_TASK_ID = "janitor_task"

SQLMESH_AIRFLOW_TAG = "sqlmesh"
SNAPSHOT_AIRFLOW_TAG = "sqlmesh_snapshot"
PLAN_AIRFLOW_TAG = "sqlmesh_plan"

SNAPSHOT_CLEANUP_COMMAND_XCOM_KEY = "snapshot_cleanup_command"

DEFAULT_CATALOG_VARIABLE_NAME = "sqlmesh_default_catalog"

AIRFLOW_LOCAL_URL = "http://localhost:8080/"

SQLMESH_API_BASE_PATH: str = f"{c.SQLMESH}/api/v1"

SnapshotToDatetimeRanges = t.Dict[Snapshot, DatetimeRanges]


class PlanApplicationRequest(PydanticModel):
    plan: EvaluatablePlan
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]


class BackfillIntervalsPerSnapshot(PydanticModel):
    snapshot_id: SnapshotId
    intervals: DatetimeRanges
    before_promote: bool = True


class PlanDagSpec(PydanticModel):
    request_id: str
    environment: Environment
    new_snapshots: t.List[Snapshot]
    backfill_intervals_per_snapshot: t.List[BackfillIntervalsPerSnapshot]
    demoted_snapshots: t.List[SnapshotTableInfo]
    unpaused_dt: t.Optional[TimeLike] = None
    no_gaps: bool
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]
    is_dev: bool
    allow_destructive_snapshots: t.Set[str]
    forward_only: t.Optional[bool] = None
    dag_start_ts: t.Optional[int] = None
    deployability_index: DeployabilityIndex = DeployabilityIndex.all_deployable()
    deployability_index_for_creation: DeployabilityIndex = DeployabilityIndex.all_deployable()
    no_gaps_snapshot_names: t.Optional[t.Set[str]] = None
    models_to_backfill: t.Optional[t.Set[str]] = None
    ensure_finalized_snapshots: bool = False
    directly_modified_snapshots: t.Optional[t.List[SnapshotId]] = None
    indirectly_modified_snapshots: t.Optional[t.Dict[str, t.List[SnapshotId]]] = None
    removed_snapshots: t.Optional[t.List[SnapshotId]] = None
    execution_time: t.Optional[TimeLike] = None


class EnvironmentsResponse(PydanticModel):
    environments: t.List[Environment]


class SnapshotsRequest(PydanticModel):
    snapshot_ids: t.Optional[t.List[SnapshotId]] = None
    check_existence: bool = False


class SnapshotsResponse(PydanticModel):
    snapshots: t.List[Snapshot]


class SnapshotIntervalsResponse(PydanticModel):
    snapshot_intervals: t.List[SnapshotIntervals]


class SnapshotIdsResponse(PydanticModel):
    snapshot_ids: t.List[SnapshotId]


class ExistingModelsResponse(PydanticModel):
    names: t.List[str]


class InvalidateEnvironmentResponse(PydanticModel):
    name: str


class MaxIntervalEndPerModelRequest(PydanticModel):
    models: t.Optional[t.List[str]] = None
    ensure_finalized_snapshots: bool = False


class IntervalEndResponse(PydanticModel):
    environment: str
    interval_end_per_model: t.Dict[str, int]


def dag_id_for_snapshot_info(info: SnapshotInfoLike) -> str:
    assert info.version
    return dag_id_for_name_version(info.name, info.version)


def dag_id_for_name_version(name: str, version: str) -> str:
    return f"sqlmesh_snapshot_{sanitize_name(name)}_{version}_dag"


def plan_application_dag_id(environment: str, request_id: str) -> str:
    return f"sqlmesh_plan_application__{environment}__{request_id}"
