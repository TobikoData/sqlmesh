import typing as t
from datetime import datetime

from sqlmesh.core._typing import NotificationTarget
from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotTableInfo,
)
from sqlmesh.core.user import User
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.errors import SQLMeshError
from sqlmesh.utils.pydantic import PydanticModel

PLAN_RECEIVER_DAG_ID = "sqlmesh_plan_receiver_dag"
PLAN_RECEIVER_TASK_ID = "plan_receiver_task"

SQLMESH_XCOM_DAG_ID: str = PLAN_RECEIVER_DAG_ID
SQLMESH_XCOM_TASK_ID: str = PLAN_RECEIVER_TASK_ID

JANITOR_DAG_ID = "sqlmesh_janitor_dag"
JANITOR_TASK_ID = "janitor_task"

INIT_RUN_ID = "init_run"

SQLMESH_AIRFLOW_TAG = "sqlmesh"
SNAPSHOT_AIRFLOW_TAG = "sqlmesh_snapshot"
PLAN_AIRFLOW_TAG = "sqlmesh_plan"

SNAPSHOT_TABLE_CLEANUP_XCOM_KEY = "snapshot_table_cleanup_task"
HWM_UTC_XCOM_KEY = "high_water_mark_utc"

PLAN_APPLICATION_REQUEST_KEY_PREFIX = "plan_application_request"
SNAPSHOT_PAYLOAD_KEY_PREFIX = "snapshot_payload"
SNAPSHOT_VERSION_KEY_PREFIX = "snapshot_version_index"
ENV_KEY_PREFIX = "environment"

AIRFLOW_LOCAL_URL = "http://localhost:8080/"


class PlanReceiverDagConf(PydanticModel):
    request_id: str
    new_snapshots: t.List[Snapshot]
    environment: Environment
    no_gaps: bool
    skip_backfill: bool
    restatements: t.Set[str]
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]
    is_dev: bool


class BackfillIntervalsPerSnapshot(PydanticModel):
    snapshot_id: SnapshotId
    intervals: t.List[t.Tuple[datetime, datetime]]


class PlanApplicationRequest(PydanticModel):
    request_id: str
    environment_name: str
    new_snapshots: t.List[Snapshot]
    backfill_intervals_per_snapshot: t.List[BackfillIntervalsPerSnapshot]
    promoted_snapshots: t.List[SnapshotTableInfo]
    demoted_snapshots: t.List[SnapshotTableInfo]
    start: TimeLike
    end: t.Optional[TimeLike]
    unpaused_dt: t.Optional[TimeLike]
    no_gaps: bool
    plan_id: str
    previous_plan_id: t.Optional[str]
    notification_targets: t.List[NotificationTarget]
    backfill_concurrent_tasks: int
    ddl_concurrent_tasks: int
    users: t.List[User]
    is_dev: bool


def snapshot_xcom_key(snapshot: SnapshotIdLike) -> str:
    return snapshot_xcom_key_from_name_fingerprint(snapshot.name, snapshot.fingerprint)


def snapshot_xcom_key_from_name_fingerprint(name: str, fingerprint: str) -> str:
    return f"{SNAPSHOT_PAYLOAD_KEY_PREFIX}__{name}__{fingerprint}"


def snapshot_version_xcom_key(name: str, version: t.Optional[str] = None) -> str:
    if not version:
        raise SQLMeshError("Version cannot be empty")
    return f"{SNAPSHOT_VERSION_KEY_PREFIX}__{name}__{version}"


def name_from_snapshot_version_xcom_key(key: str) -> str:
    return key[len(f"{SNAPSHOT_VERSION_KEY_PREFIX}__") : key.rindex("__")]


def dag_id_for_snapshot_info(info: SnapshotTableInfo) -> str:
    return dag_id_for_name_version(info.name, info.version)


def dag_id_for_name_version(name: str, version: str) -> str:
    return f"sqlmesh_snapshot_{name}_{version}_dag"


def plan_application_dag_id(environment: str, request_id: str) -> str:
    return f"sqlmesh_plan_application__{environment}__{request_id}"


def environment_xcom_key(env: str) -> str:
    return f"{ENV_KEY_PREFIX}__{env}"


def plan_application_request_xcom_key(request_id: str) -> str:
    return f"{PLAN_APPLICATION_REQUEST_KEY_PREFIX}__{request_id}"


def plan_application_request_xcom_key_from_dag_id(dag_id: str) -> str:
    request_id = dag_id[dag_id.rindex("__") + 2 :]
    return plan_application_request_xcom_key(request_id)
