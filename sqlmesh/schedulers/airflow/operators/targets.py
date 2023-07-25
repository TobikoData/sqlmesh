import abc
import typing as t
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.utils.context import Context
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

from sqlmesh.core.engine_adapter import create_engine_adapter
from sqlmesh.core.model import SeedModel
from sqlmesh.core.snapshot import Snapshot, SnapshotEvaluator, SnapshotTableInfo
from sqlmesh.engines import commands
from sqlmesh.schedulers.airflow import common, util
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

    def execute(
        self,
        context: Context,
        connection_factory: t.Callable[[], t.Any],
        dialect: str,
        **kwargs: t.Any,
    ) -> None:
        """Executes this target.

        Args:
            context: Airflow task context.
            connection_factory: a callable which produces a new Database API compliant
                connection on every call.
            dialect: The dialect with which this adapter is associated.
        """
        payload = self._get_command_payload_or_skip(context)
        snapshot_evaluator = SnapshotEvaluator(
            create_engine_adapter(
                connection_factory,
                dialect,
                multithreaded=self.ddl_concurrent_tasks > 1,
                **kwargs,
            ),
            ddl_concurrent_tasks=self.ddl_concurrent_tasks,
        )
        try:
            self.command_handler(snapshot_evaluator, payload)
            self.post_hook(context)
        finally:
            snapshot_evaluator.close()

    def post_hook(self, context: Context, **kwargs: t.Any) -> None:
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


class SnapshotEvaluationTarget(BaseTarget[commands.EvaluateCommandPayload], PydanticModel):
    """The target which contains attributes necessary to evaluate a given snapshot.

    Args:
        snapshot: The snapshot which should be evaluated.
        parent_snapshots: All upstream snapshots (by model name) to use for expansion and mapping of physical locations.
        start: The start of the interval to evaluate.
        end: The end of the interval to evaluate.
        execution_time: The date/time time reference to use for execution time. Defaults to now.
        is_dev: Indicates whether the evaluation happens in the development mode and temporary
            tables / table clones should be used where applicable.
    """

    command_type: commands.CommandType = commands.CommandType.EVALUATE
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.EvaluateCommandPayload], None
    ] = commands.evaluate
    ddl_concurrent_tasks: int = 1

    snapshot: Snapshot
    parent_snapshots: t.Dict[str, Snapshot]
    start: t.Optional[TimeLike]
    end: t.Optional[TimeLike]
    execution_time: t.Optional[TimeLike]
    is_dev: bool

    def post_hook(
        self,
        context: Context,
        **kwargs: t.Any,
    ) -> None:
        with util.scoped_state_sync() as state_sync:
            state_sync.add_interval(
                self.snapshot,
                self._get_start(context),
                self._get_end(context),
                is_dev=self.is_dev,
            )

    def _get_command_payload(self, context: Context) -> t.Optional[commands.EvaluateCommandPayload]:
        snapshot = self.snapshot
        if isinstance(snapshot.model, SeedModel) and not snapshot.model.is_hydrated:
            with util.scoped_state_sync() as state_sync:
                snapshot = state_sync.get_snapshots([snapshot], hydrate_seeds=True)[
                    snapshot.snapshot_id
                ]

        return commands.EvaluateCommandPayload(
            snapshot=snapshot,
            parent_snapshots=self.parent_snapshots,
            start=self._get_start(context),
            end=self._get_end(context),
            execution_time=self._get_execution_time(context),
            is_dev=self.is_dev,
        )

    def _get_start(self, context: Context) -> TimeLike:
        return self.start or self.snapshot.model.lookback_start(
            t.cast(datetime, context["dag_run"].data_interval_start)
        )

    def _get_end(self, context: Context) -> TimeLike:
        return self.end or context["dag_run"].data_interval_end

    def _get_execution_time(self, context: Context) -> TimeLike:
        return self.execution_time or context["dag_run"].logical_date


class SnapshotPromotionTarget(BaseTarget[commands.PromoteCommandPayload], PydanticModel):
    """The target which contains attributes necessary to perform snapshot promotion in a given environment.

    The promotion means creation of views associated with the environment which target physical tables
    associated with the given list of snapshots.

    Args:
        snapshots: The list of snapshots that should be promoted in the target environment.
        environment: The target environment.
        ddl_concurrent_tasks: The number of concurrent tasks used for DDL
            operations (table / view creation, deletion, etc). Default: 1.
        is_dev: Indicates whether the promotion happens in the development mode and temporary
            tables / table clones should be used where applicable.
    """

    command_type: commands.CommandType = commands.CommandType.PROMOTE
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.PromoteCommandPayload], None
    ] = commands.promote

    snapshots: t.List[SnapshotTableInfo]
    environment: str
    ddl_concurrent_tasks: int
    is_dev: bool

    def _get_command_payload(self, context: Context) -> t.Optional[commands.PromoteCommandPayload]:
        return commands.PromoteCommandPayload(
            snapshots=self.snapshots,
            environment=self.environment,
            is_dev=self.is_dev,
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

    def _get_command_payload(self, context: Context) -> t.Optional[commands.DemoteCommandPayload]:
        return commands.DemoteCommandPayload(
            snapshots=self.snapshots,
            environment=self.environment,
        )


class SnapshotCleanupTarget(BaseTarget[commands.CleanupCommandPayload], PydanticModel):
    """The target which contains attributes necessary to perform table cleanup of expired snapshots"""

    command_type: commands.CommandType = commands.CommandType.CLEANUP
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.CleanupCommandPayload], None
    ] = commands.cleanup
    ddl_concurrent_tasks: int = 1

    @provide_session
    def post_hook(
        self,
        context: Context,
        session: Session = util.PROVIDED_SESSION,
        **kwargs: t.Any,
    ) -> None:
        _delete_xcom(
            common.SNAPSHOT_CLEANUP_COMMAND_XCOM_KEY,
            common.JANITOR_TASK_ID,
            context,
            session,
        )

    def _get_command_payload(self, context: Context) -> t.Optional[commands.CleanupCommandPayload]:
        command = commands.CleanupCommandPayload.parse_raw(
            context["ti"].xcom_pull(key=common.SNAPSHOT_CLEANUP_COMMAND_XCOM_KEY)
        )
        if not command.snapshots and not command.environments:
            return None
        return command


class SnapshotCreateTablesTarget(BaseTarget[commands.CreateTablesCommandPayload], PydanticModel):
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

        return commands.CreateTablesCommandPayload(
            target_snapshot_ids=[s.snapshot_id for s in self.new_snapshots],
            snapshots=_get_snapshots_with_parents(self.new_snapshots),
        )


class SnapshotMigrateTablesTarget(BaseTarget[commands.MigrateTablesCommandPayload], PydanticModel):
    """The target which updates schemas of existing physical tables to bring them in correspondance
    with schemas of target snapshots.
    """

    command_type: commands.CommandType = commands.CommandType.MIGRATE_TABLES
    command_handler: t.Callable[
        [SnapshotEvaluator, commands.MigrateTablesCommandPayload], None
    ] = commands.migrate_tables

    snapshots: t.List[Snapshot]
    ddl_concurrent_tasks: int

    def _get_command_payload(
        self, context: Context
    ) -> t.Optional[commands.MigrateTablesCommandPayload]:
        if not self.snapshots:
            return None

        return commands.MigrateTablesCommandPayload(
            target_snapshot_ids=[s.snapshot_id for s in self.snapshots],
            snapshots=_get_snapshots_with_parents(self.snapshots),
        )


def _get_snapshots_with_parents(snapshots: t.Iterable[Snapshot]) -> t.List[Snapshot]:
    snapshots_by_id = {s.snapshot_id: s for s in snapshots}

    parent_snapshot_ids = {p_sid for snapshot in snapshots for p_sid in snapshot.parents}
    missing_parent_ids = parent_snapshot_ids - set(snapshots_by_id.keys())

    existing_snapshots = list(snapshots_by_id.values())

    if not missing_parent_ids:
        return existing_snapshots

    with util.scoped_state_sync() as state_sync:
        return existing_snapshots + list(state_sync.get_snapshots(missing_parent_ids).values())


def _delete_xcom(key: str, task_id: str, context: Context, session: Session) -> None:
    ti = context["ti"]
    util.delete_xcoms(
        ti.dag_id,
        {key},
        task_id=task_id,
        run_id=ti.run_id,
        session=session,
    )
