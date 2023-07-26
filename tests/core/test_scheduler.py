import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.core.model.kind import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    TimeColumn,
)
from sqlmesh.core.scheduler import Scheduler, compute_interval_params
from sqlmesh.core.snapshot import Snapshot, SnapshotEvaluator
from sqlmesh.utils.date import to_datetime


@pytest.fixture
def scheduler(sushi_context_fixed_date: Context) -> Scheduler:
    return sushi_context_fixed_date.scheduler()


@pytest.fixture
def orders(sushi_context_fixed_date: Context) -> Snapshot:
    return sushi_context_fixed_date.snapshots["sushi.orders"]


def test_interval_params(scheduler: Scheduler, sushi_context_fixed_date: Context, orders: Snapshot):
    waiter_revenue = sushi_context_fixed_date.snapshots["sushi.waiter_revenue_by_day"]
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    assert compute_interval_params(
        [orders, waiter_revenue],
        start=start_ds,
        end=end_ds,
        is_dev=False,
    ) == {
        orders: [
            (to_datetime(start_ds), to_datetime("2022-01-31")),
            (to_datetime("2022-01-31"), to_datetime("2022-02-06")),
        ],
        waiter_revenue: [
            (to_datetime(start_ds), to_datetime("2022-01-11")),
            (to_datetime("2022-01-11"), to_datetime("2022-01-21")),
            (to_datetime("2022-01-21"), to_datetime("2022-01-31")),
            (to_datetime("2022-01-31"), to_datetime("2022-02-06")),
        ],
    }


def test_interval_params_nonconsecutive(scheduler: Scheduler, orders: Snapshot):
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    orders.add_interval("2022-01-10", "2022-01-15")

    assert compute_interval_params([orders], start=start_ds, end=end_ds, is_dev=False) == {
        orders: [
            (to_datetime(start_ds), to_datetime("2022-01-10")),
            (to_datetime("2022-01-16"), to_datetime("2022-02-06")),
        ]
    }


def test_interval_params_missing(scheduler: Scheduler, sushi_context_fixed_date: Context):
    waiters = sushi_context_fixed_date.snapshots["sushi.waiter_as_customer_by_day"]

    start_ds = "2022-01-01"
    end_ds = "2022-03-01"
    assert compute_interval_params([waiters], start=start_ds, end=end_ds, is_dev=False) == {
        waiters: [
            (to_datetime(start_ds), to_datetime("2022-03-02")),
        ]
    }


def test_run(sushi_context_fixed_date: Context, scheduler: Scheduler):
    adapter = sushi_context_fixed_date.engine_adapter
    snapshot = sushi_context_fixed_date.snapshots["sushi.items"]
    scheduler.run(
        c.PROD,
        "2022-01-01",
        "2022-01-03",
        "2022-01-30",
    )

    assert (
        adapter.fetchone(
            f"""
        SELECT id, name, price FROM sqlmesh__sushi.sushi__items__{snapshot.version} ORDER BY ds LIMIT 1
    """
        )
        == (0, "Hotate", 5.99)
    )


def test_incremental_by_unique_key_kind_dag(mocker: MockerFixture, make_snapshot):
    """
    Test that when given a week of data that it batches dates together when possible but also makes sure to only
    run future intervals when the past ones are complete.
    """
    start = to_datetime("2023-01-01")
    end = to_datetime("2023-01-07")
    unique_by_key_snapshot: Snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByUniqueKeyKind(unique_key=["id"]),
            owner="owner",
            dialect="",
            cron="@daily",
            start=start,
            query=parse_one("SELECT id FROM VALUES (1), (2) AS t(id)"),
        ),
    )
    snapshot_evaluator = SnapshotEvaluator(adapter=mocker.MagicMock(), ddl_concurrent_tasks=1)
    mock_state_sync = mocker.MagicMock()
    mock_state_sync.get_snapshots.return_value = {
        unique_by_key_snapshot.snapshot_id: unique_by_key_snapshot
    }
    unique_by_key_snapshot.add_interval("2023-01-02", "2023-01-02")
    unique_by_key_snapshot.add_interval("2023-01-05", "2023-01-05")
    scheduler = Scheduler(
        snapshots=[],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=2,
    )
    batches = scheduler.batches(start, end, end, is_dev=False)
    dag = scheduler._dag(batches)
    assert dag.graph == {
        # Depends on no one
        (unique_by_key_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02"))): set(),
        # Batches multiple days and depends on previous interval
        (unique_by_key_snapshot, (to_datetime("2023-01-03"), to_datetime("2023-01-05"))): {
            (unique_by_key_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02")))
        },
        # Depends on last two intervals
        (unique_by_key_snapshot, (to_datetime("2023-01-06"), to_datetime("2023-01-07"))): {
            (unique_by_key_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02"))),
            (unique_by_key_snapshot, (to_datetime("2023-01-03"), to_datetime("2023-01-05"))),
        },
    }


def test_incremental_time_self_reference_dag(mocker: MockerFixture, make_snapshot):
    """
    Test that we always process a day at a time and all future days rely on the previous day
    """
    start = to_datetime("2023-01-01")
    end = to_datetime("2023-01-07")
    incremental_self_snapshot: Snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="ds"), batch_size=1),
            owner="owner",
            dialect="",
            cron="@daily",
            start=start,
            query=parse_one("SELECT id, @end_ds as ds FROM name"),
        ),
    )
    snapshot_evaluator = SnapshotEvaluator(adapter=mocker.MagicMock(), ddl_concurrent_tasks=1)
    mock_state_sync = mocker.MagicMock()
    mock_state_sync.get_snapshots.return_value = {
        incremental_self_snapshot.snapshot_id: incremental_self_snapshot
    }
    incremental_self_snapshot.add_interval("2023-01-02", "2023-01-02")
    incremental_self_snapshot.add_interval("2023-01-05", "2023-01-05")
    scheduler = Scheduler(
        snapshots=[],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=2,
    )
    batches = scheduler.batches(start, end, end, is_dev=False)
    dag = scheduler._dag(batches)
    assert dag.graph == {
        # Only run one day at a time and each day relies on the previous days
        (incremental_self_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02"))): set(),
        (incremental_self_snapshot, (to_datetime("2023-01-03"), to_datetime("2023-01-04"))): {
            (incremental_self_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02")))
        },
        (incremental_self_snapshot, (to_datetime("2023-01-04"), to_datetime("2023-01-05"))): {
            (incremental_self_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02"))),
            (incremental_self_snapshot, (to_datetime("2023-01-03"), to_datetime("2023-01-04"))),
        },
        (incremental_self_snapshot, (to_datetime("2023-01-06"), to_datetime("2023-01-07"))): {
            (incremental_self_snapshot, (to_datetime("2023-01-01"), to_datetime("2023-01-02"))),
            (incremental_self_snapshot, (to_datetime("2023-01-03"), to_datetime("2023-01-04"))),
            (incremental_self_snapshot, (to_datetime("2023-01-04"), to_datetime("2023-01-05"))),
        },
    }
