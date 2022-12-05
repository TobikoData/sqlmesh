import typing as t
from enum import Enum

from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotTableInfo
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel

COMMAND_PAYLOAD_FILE_NAME = "payload.json"


class CommandType(Enum):
    EVALUATE = "evaluate"
    PROMOTE = "promote"
    DEMOTE = "demote"
    CLEANUP = "cleanup"
    CREATE_TABLES = "create_tables"


class EvaluateCommandPayload(PydanticModel):
    snapshot: Snapshot
    table_mapping: t.Dict[str, str]
    start: TimeLike
    end: TimeLike
    latest: TimeLike


class PromoteCommandPayload(PydanticModel):
    snapshots: t.List[SnapshotTableInfo]
    environment: str


class DemoteCommandPayload(PydanticModel):
    snapshots: t.List[SnapshotTableInfo]
    environment: str


class CleanupCommandPayload(PydanticModel):
    snapshots: t.List[SnapshotTableInfo]


class CreateTablesCommandPayload(PydanticModel):
    target_snapshot_ids: t.List[SnapshotId]
    snapshots: t.List[Snapshot]


def evaluate(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, EvaluateCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = EvaluateCommandPayload.parse_raw(command_payload)
    evaluator.evaluate(
        command_payload.snapshot,
        command_payload.start,
        command_payload.end,
        command_payload.latest,
        {},
        mapping=command_payload.table_mapping,
    )


def promote(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, PromoteCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = PromoteCommandPayload.parse_raw(command_payload)
    for s in command_payload.snapshots:
        evaluator.promote(s, command_payload.environment)


def demote(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, DemoteCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = DemoteCommandPayload.parse_raw(command_payload)
    for s in command_payload.snapshots:
        evaluator.demote(s, command_payload.environment)


def cleanup(
    evaluator: SnapshotEvaluator, command_payload: t.Union[str, CleanupCommandPayload]
) -> None:
    if isinstance(command_payload, str):
        command_payload = CleanupCommandPayload.parse_raw(command_payload)
    evaluator.cleanup(command_payload.snapshots)


def create_tables(
    evaluator: SnapshotEvaluator,
    command_payload: t.Union[str, CreateTablesCommandPayload],
) -> None:
    if isinstance(command_payload, str):
        command_payload = CreateTablesCommandPayload.parse_raw(command_payload)

    snapshots = {s.snapshot_id: s for s in command_payload.snapshots}
    for target_sid in command_payload.target_snapshot_ids:
        target_snapshot = snapshots[target_sid]
        parent_snapshots = {
            p.name: p
            for p in [
                snapshots[sid] for sid in target_snapshot.parents if sid in snapshots
            ]
        }
        evaluator.create(target_snapshot, parent_snapshots)


COMMAND_HANDLERS: t.Dict[CommandType, t.Callable[[SnapshotEvaluator, str], None]] = {
    CommandType.EVALUATE: evaluate,
    CommandType.PROMOTE: promote,
    CommandType.DEMOTE: demote,
    CommandType.CLEANUP: cleanup,
    CommandType.CREATE_TABLES: create_tables,
}
