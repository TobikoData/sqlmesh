import typing as t
from enum import Enum

from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotEvaluator,
    SnapshotId,
    SnapshotTableCleanupTask,
    SnapshotTableInfo,
)
from sqlmesh.core.state_sync import cleanup_expired_views
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel

COMMAND_PAYLOAD_FILE_NAME = "payload.json"


class CommandType(str, Enum):
    EVALUATE = "evaluate"
    PROMOTE = "promote"
    DEMOTE = "demote"
    CLEANUP = "cleanup"
    CREATE_TABLES = "create_tables"
    MIGRATE_TABLES = "migrate_tables"

    # This makes it easy to integrate with argparse
    def __str__(self) -> str:
        return self.value


class EvaluateCommandPayload(PydanticModel):
    snapshot: Snapshot
    parent_snapshots: t.Dict[str, Snapshot]
    start: TimeLike
    end: TimeLike
    execution_time: TimeLike
    deployability_index: DeployabilityIndex


class PromoteCommandPayload(PydanticModel):
    snapshots: t.List[Snapshot]
    environment_naming_info: EnvironmentNamingInfo
    deployability_index: DeployabilityIndex


class DemoteCommandPayload(PydanticModel):
    snapshots: t.List[SnapshotTableInfo]
    environment_naming_info: EnvironmentNamingInfo


class CleanupCommandPayload(PydanticModel):
    environments: t.List[Environment]
    tasks: t.List[SnapshotTableCleanupTask]


class CreateTablesCommandPayload(PydanticModel):
    target_snapshot_ids: t.List[SnapshotId]
    snapshots: t.List[Snapshot]
    deployability_index: DeployabilityIndex


class MigrateTablesCommandPayload(PydanticModel):
    target_snapshot_ids: t.List[SnapshotId]
    snapshots: t.List[Snapshot]


def evaluate(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, EvaluateCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = EvaluateCommandPayload.parse_raw(command_payload)

    parent_snapshots = command_payload.parent_snapshots
    parent_snapshots[command_payload.snapshot.name] = command_payload.snapshot

    wap_id = evaluator.evaluate(
        command_payload.snapshot,
        start=command_payload.start,
        end=command_payload.end,
        execution_time=command_payload.execution_time,
        snapshots=parent_snapshots,
        deployability_index=command_payload.deployability_index,
    )
    evaluator.audit(
        snapshot=command_payload.snapshot,
        start=command_payload.start,
        end=command_payload.end,
        execution_time=command_payload.execution_time,
        snapshots=parent_snapshots,
        deployability_index=command_payload.deployability_index,
        wap_id=wap_id,
    )


def promote(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, PromoteCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = PromoteCommandPayload.parse_raw(command_payload)
    evaluator.promote(
        command_payload.snapshots,
        command_payload.environment_naming_info,
        deployability_index=command_payload.deployability_index,
    )


def demote(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, DemoteCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = DemoteCommandPayload.parse_raw(command_payload)
    evaluator.demote(
        command_payload.snapshots,
        command_payload.environment_naming_info,
    )


def cleanup(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, CleanupCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = CleanupCommandPayload.parse_raw(command_payload)

    cleanup_expired_views(evaluator.adapter, command_payload.environments)
    evaluator.cleanup(command_payload.tasks)


def create_tables(
    evaluator: SnapshotEvaluator,
    command_payload: t.Union[str, CreateTablesCommandPayload],
) -> None:
    if isinstance(command_payload, str):
        command_payload = CreateTablesCommandPayload.parse_raw(command_payload)

    snapshots_by_id = {s.snapshot_id: s for s in command_payload.snapshots}
    target_snapshots = [snapshots_by_id[sid] for sid in command_payload.target_snapshot_ids]
    evaluator.create(
        target_snapshots,
        snapshots_by_id,
        deployability_index=command_payload.deployability_index,
    )


def migrate_tables(
    evaluator: SnapshotEvaluator,
    command_payload: t.Union[str, MigrateTablesCommandPayload],
) -> None:
    if isinstance(command_payload, str):
        command_payload = MigrateTablesCommandPayload.parse_raw(command_payload)
    snapshots_by_id = {s.snapshot_id: s for s in command_payload.snapshots}
    target_snapshots = [snapshots_by_id[sid] for sid in command_payload.target_snapshot_ids]
    evaluator.migrate(target_snapshots, snapshots_by_id)


COMMAND_HANDLERS: t.Dict[CommandType, t.Callable[[SnapshotEvaluator, str], None]] = {
    CommandType.EVALUATE: evaluate,
    CommandType.PROMOTE: promote,
    CommandType.DEMOTE: demote,
    CommandType.CLEANUP: cleanup,
    CommandType.CREATE_TABLES: create_tables,
    CommandType.MIGRATE_TABLES: migrate_tables,
}
