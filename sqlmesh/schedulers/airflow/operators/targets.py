import abc
import json
import typing as t

from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotTableInfo
from sqlmesh.core.snapshot_evaluator import SnapshotEvaluator
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.schedulers.airflow.state_sync.xcom import XComStateSync
from sqlmesh.utils.date import TimeLike
from sqlmesh.utils.pydantic import PydanticModel

CP = t.TypeVar("CP", bound=PydanticModel)


class BaseTarget(abc.ABC, t.Generic[CP]):
    command_type: commands.CommandType
    command_handler: t.Callable[[SnapshotEvaluator, CP], None]
    ddl_concurrent_tasks: int

    def serialized_command_payload(self, context: Context) -> str:
        """Returns the serialized command payload for the Spark application.

        Args:
            context: Airflow task context.

        Returns:
            The serialized command payload.
        """
        return self._get_command_payload_or_skip(context).json()

    def execute(self, context: Context, connection: t.Any, dialect: str) -> None:
        """Executes this target.

        Args:
            context: Airflow task context.
            connection: Database API compliant connection. The connection will be
                lazily established if a callable that returns a connection is passed in.
            dialect: The dialect with which this adapter is associated.
        """
        payload = self._get_command_payload_or_skip(context)
        snapshot_evaluator = SnapshotEvaluator(
            EngineAdapter(connection, dialect),
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
        )
        self.command_handler(snapshot_evaluator, payload)
        self.post_hook(context)

    def post_hook(self, context: Context, **kwargs) -> None:
        """The hook that should be invoked once the processing of this target
        is complete.

        Args:
            context: Airflow task context.
        """

    @abc.abstractmethod
    def _get_command_payload(self, context: Context) -> t.Optional[CP]:
        """Constructs the command payload.

        Args:
            context: Airflow task context.

        Returns:
            The command payload or None if there is no command to execute
            and the target must be skipped.
        """

    def _get_command_payload_or_skip(self, context: Context) -> CP:
        payload = self._get_command_payload(context)
        if not payload:
            self.post_hook(context)
            raise AirflowSkipException
        return payload


class SnapshotEvaluationTarget(
    BaseTarget[commands.EvaluateCommandPayload], PydanticModel
):
    """The target which contains attributes necessary to evaluate a given snapshot.

    Args:
        snapshot: The snapshot which should be evaluated.
        table_mapping: The mapping from table names referenced in model queries to physical tables.
        start: The start of the interval to evaluate.
        end: The end of the interval to evaluate.
        latest: The latest time used for non incremental datasets.
    """

    command_type: commands.CommandType = commands.CommandType.EVALUATE
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.EvaluateCommandPayload], None
    ] = commands.evaluate
    ddl_concurrent_tasks: int = 1

    snapshot: Snapshot
    table_mapping: t.Dict[str, str]
    start: t.Optional[TimeLike]
    end: t.Optional[TimeLike]
    latest: t.Optional[TimeLike]

    @provide_session
    def post_hook(
        self, context: Context, session: Session = util.PROVIDED_SESSION, **kwargs
    ) -> None:
        XComStateSync(session).add_interval(
            self.snapshot.snapshot_id,
            self._get_start(context),
            self._get_end(context),
        )

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.EvaluateCommandPayload]:
        return commands.EvaluateCommandPayload(
            snapshot=self.snapshot,
            table_mapping=self.table_mapping,
            start=self._get_start(context),
            end=self._get_end(context),
            latest=self._get_latest(context),
        )

    def _get_start(self, context: Context) -> TimeLike:
        return self.start or context["dag_run"].data_interval_start

    def _get_end(self, context: Context) -> TimeLike:
        return self.end or context["dag_run"].data_interval_end

    def _get_latest(self, context: Context) -> TimeLike:
        return self.latest or context["dag_run"].logical_date


class SnapshotPromotionTarget(
    BaseTarget[commands.PromoteCommandPayload], PydanticModel
):
    """The target which contains attributes necessary to perform snapshot promotion in a given environment.

    The promotion means creation of views associated with the environment which target physical tables
    associated with the given list of snapshots.

    Args:
        snapshots: The list of snapshots that should be promoted in the target environment.
        environment: The target environment.
    """

    command_type: commands.CommandType = commands.CommandType.PROMOTE
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.PromoteCommandPayload], None
    ] = commands.promote

    snapshots: t.List[SnapshotTableInfo]
    environment: str
    ddl_concurrent_tasks: int

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.PromoteCommandPayload]:
        return commands.PromoteCommandPayload(
            snapshots=self.snapshots,
            environment=self.environment,
        )


class SnapshotDemotionTarget(BaseTarget[commands.DemoteCommandPayload], PydanticModel):
    """The target which contains attributes necessary to perform snapshot demotion in a given environment.

    The demotion means deletion of views that match names of provided snapshots in the target environment.

    Args:
        snapshots: The list of snapshots that should be demoted in the target environment.
        environment: The target environment.
    """

    command_type: commands.CommandType = commands.CommandType.DEMOTE
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.DemoteCommandPayload], None
    ] = commands.demote

    snapshots: t.List[SnapshotTableInfo]
    environment: str
    ddl_concurrent_tasks: int

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.DemoteCommandPayload]:
        return commands.DemoteCommandPayload(
            snapshots=self.snapshots,
            environment=self.environment,
        )


class SnapshotTableCleanupTarget(
    BaseTarget[commands.CleanupCommandPayload], PydanticModel
):
    """The target which contains attributes necessary to perform table cleanup of expired snapshots"""

    command_type: commands.CommandType = commands.CommandType.CLEANUP
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.CleanupCommandPayload], None
    ] = commands.cleanup
    ddl_concurrent_tasks: int = 1

    @provide_session
    def post_hook(
        self, context: Context, session: Session = util.PROVIDED_SESSION, **kwargs
    ) -> None:
        _delete_xcom(
            common.SNAPSHOT_TABLE_CLEANUP_XCOM_KEY,
            common.JANITOR_TASK_ID,
            context,
            session,
        )

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.CleanupCommandPayload]:
        snapshots = self._get_snapshots(context)
        if not snapshots:
            return None
        return commands.CleanupCommandPayload(snapshots=snapshots)

    def _get_snapshots(self, context: Context) -> t.List[SnapshotTableInfo]:
        return [
            SnapshotTableInfo.parse_obj(s)
            for s in json.loads(
                context["ti"].xcom_pull(key=common.SNAPSHOT_TABLE_CLEANUP_XCOM_KEY)
            )
        ]


class SnapshotCreateTableTarget(
    BaseTarget[commands.CreateTablesCommandPayload], PydanticModel
):
    """The target which creates physical tables for the given set of new snapshots."""

    command_type: commands.CommandType = commands.CommandType.CREATE_TABLES
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.CreateTablesCommandPayload], None
    ] = commands.create_tables

    new_snapshots: t.List[Snapshot]
    ddl_concurrent_tasks: int

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.CreateTablesCommandPayload]:
        if not self.new_snapshots:
            return None

        parent_snapshot_ids = {
            p_sid for snapshot in self.new_snapshots for p_sid in snapshot.parents
        }
        stored_snapshots = self._get_stored_snapshots(parent_snapshot_ids)

        return commands.CreateTablesCommandPayload(
            target_snapshot_ids=[s.snapshot_id for s in self.new_snapshots],
            snapshots=stored_snapshots + self.new_snapshots,
        )

    @provide_session
    def _get_stored_snapshots(
        self, snapshot_ids: t.Set[SnapshotId], session: Session = util.PROVIDED_SESSION
    ) -> t.List[Snapshot]:
        return list(XComStateSync(session).get_snapshots(snapshot_ids).values())


def _delete_xcom(key: str, task_id: str, context: Context, session: Session) -> None:
    ti = context["ti"]
    util.delete_xcoms(
        ti.dag_id,
        {key},
        task_id=task_id,
        run_id=ti.run_id,
        session=session,
    )
