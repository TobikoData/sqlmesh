import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.core.model.kind import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    TimeColumn,
)
from sqlmesh.core.scheduler import Scheduler, compute_interval_params
from sqlmesh.core.snapshot import Snapshot, SnapshotEvaluator
from sqlmesh.utils.date import to_datetime
from sqlmesh.utils.errors import CircuitBreakerError


@pytest.fixture
def scheduler(sushi_context_fixed_date: Context) -> Scheduler:
    return sushi_context_fixed_date.scheduler()


@pytest.fixture
def orders(sushi_context_fixed_date: Context) -> Snapshot:
    return sushi_context_fixed_date.get_snapshot("sushi.orders", raise_if_missing=True)


@pytest.mark.slow
def test_interval_params(scheduler: Scheduler, sushi_context_fixed_date: Context, orders: Snapshot):
    waiter_revenue = sushi_context_fixed_date.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    assert compute_interval_params([orders, waiter_revenue], start=start_ds, end=end_ds) == {
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

    assert compute_interval_params([orders], start=start_ds, end=end_ds) == {
        orders: [
            (to_datetime(start_ds), to_datetime("2022-01-10")),
            (to_datetime("2022-01-16"), to_datetime("2022-02-06")),
        ]
    }


@pytest.mark.slow
def test_interval_params_missing(scheduler: Scheduler, sushi_context_fixed_date: Context):
    waiters = sushi_context_fixed_date.get_snapshot(
        "sushi.waiter_as_customer_by_day", raise_if_missing=True
    )

    start_ds = "2022-01-01"
    end_ds = "2022-03-01"
    assert compute_interval_params(
        sushi_context_fixed_date.snapshots.values(), start=start_ds, end=end_ds
    )[waiters] == [
        (to_datetime(start_ds), to_datetime("2022-03-02")),
    ]


@pytest.mark.slow
def test_run(sushi_context_fixed_date: Context, scheduler: Scheduler):
    adapter = sushi_context_fixed_date.engine_adapter
    snapshot = sushi_context_fixed_date.get_snapshot("sushi.items", raise_if_missing=True)
    scheduler.run(
        EnvironmentNamingInfo(),
        "2022-01-01",
        "2022-01-03",
        "2022-01-30",
    )

    assert (
        adapter.fetchone(
            f"""
        SELECT id, name, price FROM sqlmesh__sushi.sushi__items__{snapshot.version} ORDER BY event_date LIMIT 1
    """
        )
        == (0, "Hotate", 5.99)
    )


def test_incremental_by_unique_key_kind_dag(mocker: MockerFixture, make_snapshot):
    """
    Test that when given a week of data that it batches dates together.
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
    scheduler = Scheduler(
        snapshots=[unique_by_key_snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=2,
        default_catalog=None,
    )
    batches = scheduler.batches(start, end, end)
    dag = scheduler._dag(batches)
    assert dag.graph == {
        (
            unique_by_key_snapshot.name,
            ((to_datetime("2023-01-01"), to_datetime("2023-01-07")), 0),
        ): set(),
    }
    mock_state_sync.refresh_snapshot_intervals.assert_called_once()


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
    incremental_self_snapshot.add_interval("2023-01-02", "2023-01-02")
    incremental_self_snapshot.add_interval("2023-01-05", "2023-01-05")

    snapshot_evaluator = SnapshotEvaluator(adapter=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[incremental_self_snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )
    batches = scheduler.batches(start, end, end)
    dag = scheduler._dag(batches)

    assert dag.graph == {
        # Only run one day at a time and each day relies on the previous days
        (
            incremental_self_snapshot.name,
            ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0),
        ): set(),
        (
            incremental_self_snapshot.name,
            ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 1),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0),
            )
        },
        (
            incremental_self_snapshot.name,
            ((to_datetime("2023-01-04"), to_datetime("2023-01-05")), 2),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 1),
            ),
        },
        (
            incremental_self_snapshot.name,
            ((to_datetime("2023-01-06"), to_datetime("2023-01-07")), 3),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_datetime("2023-01-04"), to_datetime("2023-01-05")), 2),
            ),
        },
        (
            incremental_self_snapshot.name,
            ((to_datetime(0), to_datetime(0)), -1),
        ): set(
            [
                (
                    incremental_self_snapshot.name,
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 1),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_datetime("2023-01-04"), to_datetime("2023-01-05")), 2),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_datetime("2023-01-06"), to_datetime("2023-01-07")), 3),
                ),
            ]
        ),
    }


def test_circuit_breaker(scheduler: Scheduler):
    with pytest.raises(CircuitBreakerError):
        scheduler.run(
            EnvironmentNamingInfo(),
            "2022-01-01",
            "2022-01-03",
            "2022-01-30",
            circuit_breaker=lambda: True,
        )
