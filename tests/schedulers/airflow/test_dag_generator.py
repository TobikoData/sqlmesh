import typing as t
from pytest_mock.plugin import MockerFixture

from sqlglot import parse_one
from airflow.models import BaseOperator
from airflow.utils.context import Context

from sqlmesh.core.config import EnvironmentSuffixTarget
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import IncrementalByUniqueKeyKind, SqlModel
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
)
from sqlmesh.schedulers.airflow.dag_generator import SnapshotDagGenerator
from sqlmesh.schedulers.airflow import common
from sqlmesh.schedulers.airflow.operators.targets import BaseTarget, SnapshotEvaluationTarget
from sqlmesh.schedulers.airflow.operators.sensor import HighWaterMarkSensor
from sqlmesh.utils.date import to_datetime, to_timestamp


class TestSubmitOperator(BaseOperator):
    __test__ = False  # prevent pytest trying to collect this as a test class

    def __init__(
        self,
        *,
        target: BaseTarget,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(**kwargs)
        self.target = target


def test_generate_plan_application_dag__batch_index_populated(mocker: MockerFixture, make_snapshot):
    model = SqlModel(
        name="test_model",
        kind=IncrementalByUniqueKeyKind(unique_key="item_id", batch_size=1),
        cron="@daily",
        start="2020-01-01",
        end="2020-01-07",
        storage_format="ICEBERG",
        query=parse_one("""
        SELECT item_id::int AS item_id, event_date::date AS event_date
        FROM (
            VALUES
                (2, '2020-01-01'),
                (1, '2020-01-01'),
                (3, '2020-01-03'),
                (1, '2020-01-04'),
                (1, '2020-01-05'),
                (1, '2020-01-06'),
                (1, '2020-01-07')
        ) AS t(item_id, event_date)
        WHERE event_date BETWEEN @start_date AND @end_date
        """),
    )

    snapshot: Snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_snapshots.return_value = {}

    generator = SnapshotDagGenerator(
        engine_operator=TestSubmitOperator,
        engine_operator_args={},
        ddl_engine_operator=TestSubmitOperator,
        ddl_engine_operator_args={},
        external_table_sensor_factory=None,
        sensor_mode="reschedule",
        state_reader=state_reader_mock,
        external_sensor_args=None,
        high_water_mark_sensor_args=None,
    )

    environment_name = "test_env"
    new_environment = Environment(
        name=environment_name,
        snapshots=[],
        start_at="2020-01-01",
        end_at="2020-01-10",
        plan_id="test_plan_id",
        suffix_target=EnvironmentSuffixTarget.TABLE,
        catalog_name_override="test_catalog",
    )

    dag_plan = common.PlanDagSpec(
        request_id="test_request_id",
        environment=new_environment,
        new_snapshots=[snapshot],
        backfill_intervals_per_snapshot=[
            common.BackfillIntervalsPerSnapshot(
                snapshot_id=snapshot.snapshot_id,
                intervals=[
                    (to_datetime("2020-01-01"), to_datetime("2020-01-02")),
                    (to_datetime("2020-01-02"), to_datetime("2020-01-03")),
                    (to_datetime("2020-01-03"), to_datetime("2020-01-04")),
                ],
            )
        ],
        demoted_snapshots=[],
        no_gaps=True,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
        is_dev=False,
        allow_destructive_snapshots=set(),
        execution_time=to_datetime("2024-01-01"),
    )

    dag = generator.generate_plan_application_dag(dag_plan)
    assert dag is not None

    backfill_tasks = [
        t
        for t in dag.tasks
        if "backfill__test_model" in t.task_id
        and not t.task_id.endswith("__start")
        and not t.task_id.endswith("__end")
    ]
    assert len(backfill_tasks) == 3

    for batch_idx, task in enumerate(backfill_tasks):
        target: SnapshotEvaluationTarget = task.target  # type: ignore
        assert target is not None
        command = target._get_command_payload(context=t.cast(Context, None))
        assert command is not None
        assert target.batch_index == batch_idx
        assert command.batch_index == batch_idx


def test_sensor_mode_override(mocker: MockerFixture, make_snapshot):
    snapshot_a = make_snapshot(
        SqlModel(name="a", kind=dict(name="FULL"), query=parse_one("select 1 as a, ds")),
    )
    snapshot_a.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_a.unpaused_ts = to_timestamp("2024-01-01")

    snapshot_b = make_snapshot(
        SqlModel(name="b", kind=dict(name="FULL"), query=parse_one("select a, ds from a")),
        nodes={snapshot_a.name: snapshot_a.node},
    )
    snapshot_b.categorize_as(SnapshotChangeCategory.BREAKING)
    snapshot_b.unpaused_ts = to_timestamp("2024-01-01")

    state_reader_mock = mocker.Mock()
    state_reader_mock.get_snapshots.return_value = {
        snapshot_a.snapshot_id: snapshot_a,
        snapshot_b.snapshot_id: snapshot_b,
    }

    generator = SnapshotDagGenerator(
        engine_operator=TestSubmitOperator,
        engine_operator_args={},
        ddl_engine_operator=TestSubmitOperator,
        ddl_engine_operator_args={},
        external_table_sensor_factory=None,
        sensor_mode="poke",
        state_reader=state_reader_mock,
        external_sensor_args=None,
        high_water_mark_sensor_args=None,
    )

    dags = generator.generate_cadence_dags([snapshot_a, snapshot_b])
    assert len(dags) == 2

    assert len(dags[0].tasks) == 1
    assert isinstance(dags[0].tasks[0], TestSubmitOperator)

    assert len(dags[1].tasks) == 2
    assert isinstance(dags[1].tasks[0], HighWaterMarkSensor)
    assert isinstance(dags[1].tasks[1], TestSubmitOperator)
    assert dags[1].tasks[0].mode == "poke"
