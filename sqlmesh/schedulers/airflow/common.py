from __future__ import annotations

import typing as t

from sqlmesh.core import constants as c
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.notification_target import NotificationTarget
from sqlmesh.core.scheduler import Interval
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotInfoLike,
    SnapshotIntervals,
    SnapshotTableInfo,
)
from sqlmesh.core.snapshot.definition import Interval as SnapshotInterval
from sqlmesh.core.user import User
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

JANITOR_DAG_ID = "sqlmesh_janitor_dag"
JANITOR_TASK_ID = "janitor_task"

SQLMESH_AIRFLOW_TAG = "sqlmesh"
SNAPSHOT_AIRFLOW_TAG = "sqlmesh_snapshot"
PLAN_AIRFLOW_TAG = "sqlmesh_plan"

SNAPSHOT_CLEANUP_COMMAND_XCOM_KEY = "snapshot_cleanup_command"

PLAN_DAG_SPEC_KEY_PREFIX = "sqlmesh__plan_dag_spec"
SNAPSHOT_PAYLOAD_KEY_PREFIX = "sqlmesh__snapshot_payload"
SNAPSHOT_VERSION_KEY_PREFIX = "sqlmesh__snapshot_version_index"
ENV_KEY_PREFIX = "sqlmesh__environment"

AIRFLOW_LOCAL_URL = "http://localhost:8080/"

SQLMESH_API_BASE_PATH: str = f"{c.SQLMESH}/api/v1"


class PlanApplicationRequest(PydanticModel):
    request_id: str
    new_snapshots: t.List[Snapshot]
    environment: Environment
    no_gaps: bool
    skip_backfill: bool
    restatements: t.Dict[str, SnapshotInterval]
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]
    is_dev: bool
    forward_only: bool


class BackfillIntervalsPerSnapshot(PydanticModel):
    snapshot_id: SnapshotId
    intervals: t.List[Interval]
    before_promote: bool = True


class PlanDagSpec(PydanticModel):
    request_id: str
    environment_naming_info: EnvironmentNamingInfo
    new_snapshots: t.List[Snapshot]
    backfill_intervals_per_snapshot: t.List[BackfillIntervalsPerSnapshot]
    promoted_snapshots: t.List[SnapshotTableInfo]
    demoted_snapshots: t.List[SnapshotTableInfo]
    start: TimeLike
    end: t.Optional[TimeLike] = None
    unpaused_dt: t.Optional[TimeLike] = None
    no_gaps: bool
    plan_id: str
    previous_plan_id: t.Optional[str] = None
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]
    is_dev: bool
    forward_only: t.Optional[bool] = None
    environment_expiration_ts: t.Optional[int] = None
    dag_start_ts: t.Optional[int] = None
    deployability_index: DeployabilityIndex = DeployabilityIndex.all_deployable()


class EnvironmentsResponse(PydanticModel):
    environments: t.List[Environment]


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


class MaxIntervalEndResponse(PydanticModel):
    environment: str
    max_interval_end: t.Optional[int]


def snapshot_key(snapshot: SnapshotIdLike) -> str:
    return snapshot_key_from_name_identifier(snapshot.name, snapshot.identifier)


def snapshot_key_from_name_identifier(name: str, identifier: str) -> str:
    return f"{SNAPSHOT_PAYLOAD_KEY_PREFIX}__{name}__{identifier}"


def snapshot_version_key(name: str, version: t.Optional[str] = None) -> str:
    if not version:
        raise SQLMeshError("Version cannot be empty")
    return f"{SNAPSHOT_VERSION_KEY_PREFIX}__{name}__{version}"


def name_from_snapshot_version_key(key: str) -> str:
    return key[len(f"{SNAPSHOT_VERSION_KEY_PREFIX}__") : key.rindex("__")]


def dag_id_for_snapshot_info(info: SnapshotInfoLike) -> str:
    assert info.version
    return dag_id_for_name_version(info.name, info.version)


def dag_id_for_name_version(name: str, version: str) -> str:
    return f"sqlmesh_snapshot_{name}_{version}_dag"


def plan_application_dag_id(environment: str, request_id: str) -> str:
    return f"sqlmesh_plan_application__{environment}__{request_id}"


def plan_dag_spec_key(request_id: str) -> str:
    return f"{PLAN_DAG_SPEC_KEY_PREFIX}__{request_id}"


def plan_dag_spec_key_from_dag_id(dag_id: str) -> str:
    request_id = dag_id[dag_id.rindex("__") + 2 :]
    return plan_dag_spec_key(request_id)
