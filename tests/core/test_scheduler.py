import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one, parse

from sqlmesh.core.context import Context
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.model.definition import SqlModel
from sqlmesh.core.model.kind import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    TimeColumn,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.scheduler import Scheduler, compute_interval_params
from sqlmesh.core.snapshot import Snapshot, SnapshotEvaluator, SnapshotChangeCategory
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

    assert adapter.fetchone(
        f"""
        SELECT id, name, price FROM sqlmesh__sushi.sushi__items__{snapshot.version} ORDER BY event_date LIMIT 1
    """
    ) == (0, "Hotate", 5.99)


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


@pytest.mark.parametrize(
    "batch_size, batch_concurrency, expected_graph",
    [
        (
            2,
            2,
            {
                (
                    '"test_model"',
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-03")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-03"), to_datetime("2023-01-05")), 1),
                ): set(),
                ('"test_model"', ((to_datetime("2023-01-05"), to_datetime("2023-01-07")), 2)): {
                    ('"test_model"', ((to_datetime("2023-01-01"), to_datetime("2023-01-03")), 0)),
                },
            },
        ),
        (
            1,
            3,
            {
                (
                    '"test_model"',
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-02"), to_datetime("2023-01-03")), 1),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 2),
                ): set(),
                ('"test_model"', ((to_datetime("2023-01-04"), to_datetime("2023-01-05")), 3)): {
                    ('"test_model"', ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0)),
                },
                ('"test_model"', ((to_datetime("2023-01-05"), to_datetime("2023-01-06")), 4)): {
                    ('"test_model"', ((to_datetime("2023-01-02"), to_datetime("2023-01-03")), 1)),
                },
                ('"test_model"', ((to_datetime("2023-01-06"), to_datetime("2023-01-07")), 5)): {
                    ('"test_model"', ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 2)),
                },
            },
        ),
        (
            1,
            10,
            {
                (
                    '"test_model"',
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-02")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-02"), to_datetime("2023-01-03")), 1),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-03"), to_datetime("2023-01-04")), 2),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-04"), to_datetime("2023-01-05")), 3),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-05"), to_datetime("2023-01-06")), 4),
                ): set(),
                (
                    '"test_model"',
                    ((to_datetime("2023-01-06"), to_datetime("2023-01-07")), 5),
                ): set(),
            },
        ),
        (
            10,
            10,
            {
                (
                    '"test_model"',
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-07")), 0),
                ): set(),
            },
        ),
        (
            10,
            1,
            {
                (
                    '"test_model"',
                    ((to_datetime("2023-01-01"), to_datetime("2023-01-07")), 0),
                ): set(),
            },
        ),
    ],
)
def test_incremental_batch_concurrency(
    mocker: MockerFixture,
    make_snapshot,
    batch_size: int,
    batch_concurrency: int,
    expected_graph: t.Dict[str, t.Any],
):
    start = to_datetime("2023-01-01")
    end = to_datetime("2023-01-07")
    snapshot: Snapshot = make_snapshot(
        SqlModel(
            name="test_model",
            kind=IncrementalByTimeRangeKind(
                time_column="ds", batch_size=batch_size, batch_concurrency=batch_concurrency
            ),
            cron="@daily",
            start=start,
            query=parse_one("SELECT 1, ds FROM source"),
        ),
    )

    snapshot_evaluator = SnapshotEvaluator(adapter=mocker.MagicMock(), ddl_concurrent_tasks=1)
    mock_state_sync = mocker.MagicMock()
    scheduler = Scheduler(
        snapshots=[snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=4,
        default_catalog=None,
    )

    batches = scheduler.batches(start, end, end)
    dag = scheduler._dag(batches)
    graph = {k: v for k, v in dag.graph.items() if k[1][1] != -1}  # exclude the terminal node.}
    assert graph == expected_graph


def test_circuit_breaker(scheduler: Scheduler):
    with pytest.raises(CircuitBreakerError):
        scheduler.run(
            EnvironmentNamingInfo(),
            "2022-01-01",
            "2022-01-03",
            "2022-01-30",
            circuit_breaker=lambda: True,
        )


def test_intervals_with_end_date_on_model(mocker: MockerFixture, make_snapshot):
    snapshot: Snapshot = make_snapshot(
        SqlModel(
            name="name",
            kind=IncrementalByTimeRangeKind(time_column="ds", batch_size=1),
            interval_unit=IntervalUnit.DAY,
            start="2023-01-01",
            end="2023-01-31",
            query=parse_one("SELECT ds FROM parent.tbl"),
        )
    )

    snapshot_evaluator = SnapshotEvaluator(adapter=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )

    # generate for 1 year to show that the returned batches should only cover
    # the range defined on the model itself
    batches = scheduler.batches(start="2023-01-01", end="2024-01-01")[snapshot]

    assert len(batches) == 31  # days in Jan 2023
    assert batches[0] == (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    assert batches[-1] == (to_datetime("2023-01-31"), to_datetime("2023-02-01"))

    # generate for less than 1 month to ensure that the scheduler end date
    # takes precedence over the model end date
    batches = scheduler.batches(start="2023-01-01", end="2023-01-10")[snapshot]

    assert len(batches) == 10
    assert batches[0] == (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    assert batches[-1] == (to_datetime("2023-01-10"), to_datetime("2023-01-11"))

    # generate for the last day of range
    batches = scheduler.batches(start="2023-01-31", end="2023-01-31")[snapshot]
    assert len(batches) == 1
    assert batches[0] == (to_datetime("2023-01-31"), to_datetime("2023-02-01"))

    # generate for future days to ensure no future batches are loaded
    snapshot_to_batches = scheduler.batches(start="2023-02-01", end="2023-02-28")
    assert len(snapshot_to_batches) == 0


def test_external_model_audit(mocker, make_snapshot):
    model = load_sql_based_model(
        parse(  # type: ignore
            """
            MODEL (
                name test_schema.test_model,
                kind EXTERNAL,
                columns (id int),
                audits not_null(columns := id)
            );

            SELECT 1;
            """
        ),
    )

    snapshot = make_snapshot(model)
    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    evaluator = SnapshotEvaluator(adapter=mocker.MagicMock())
    spy = mocker.spy(evaluator, "_audit")

    scheduler = Scheduler(
        snapshots=[snapshot],
        snapshot_evaluator=evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )

    scheduler.run(
        EnvironmentNamingInfo(),
        "2022-01-01",
        "2022-01-01",
        "2022-01-30",
    )

    spy.assert_called_once()


def test_contiguous_intervals():
    from sqlmesh.core.scheduler import _contiguous_intervals as ci

    assert ci([]) == []
    assert ci([(0, 1)]) == [[(0, 1)]]
    assert ci([(0, 1), (1, 2), (2, 3)]) == [[(0, 1), (1, 2), (2, 3)]]
    assert ci([(0, 1), (3, 4), (4, 5), (6, 7)]) == [
        [(0, 1)],
        [(3, 4), (4, 5)],
        [(6, 7)],
    ]


def test_check_ready_intervals(mocker: MockerFixture):
    from sqlmesh.core.scheduler import _check_ready_intervals, Interval

    def const_signal(const):
        signal_mock = mocker.Mock()
        signal_mock.check_intervals = mocker.MagicMock(return_value=const)
        return signal_mock

    def assert_always_signal(intervals):
        _check_ready_intervals(const_signal(True), intervals) == intervals

    assert_always_signal([])
    assert_always_signal([(0, 1)])
    assert_always_signal([(0, 1), (1, 2)])
    assert_always_signal([(0, 1), (2, 3)])

    def assert_never_signal(intervals):
        _check_ready_intervals(const_signal(False), intervals) == []

    assert_never_signal([])
    assert_never_signal([(0, 1)])
    assert_never_signal([(0, 1), (1, 2)])
    assert_never_signal([(0, 1), (2, 3)])

    def to_intervals(values: t.List[t.Tuple[int, int]]) -> t.List[Interval]:
        return [(to_datetime(s), to_datetime(e)) for s, e in values]

    def assert_check_intervals(
        intervals: t.List[t.Tuple[int, int]],
        ready: t.List[t.List[t.Tuple[int, int]]],
        expected: t.List[t.Tuple[int, int]],
    ):
        signal_mock = mocker.Mock()
        signal_mock.check_intervals = mocker.MagicMock(side_effect=[to_intervals(r) for r in ready])
        _check_ready_intervals(signal_mock, intervals) == expected

    assert_check_intervals([], [], [])
    assert_check_intervals([(0, 1)], [[]], [])
    assert_check_intervals(
        [(0, 1)],
        [[(0, 1)]],
        [(0, 1)],
    )
    assert_check_intervals(
        [(0, 1), (1, 2)],
        [[(0, 1)]],
        [(0, 1)],
    )
    assert_check_intervals(
        [(0, 1), (1, 2)],
        [[(1, 2)]],
        [(1, 2)],
    )
    assert_check_intervals(
        [(0, 1), (1, 2)],
        [[(0, 1), (1, 2)]],
        [(0, 1), (1, 2)],
    )
    assert_check_intervals(
        [(0, 1), (1, 2), (3, 4)],
        [[], []],
        [],
    )
    assert_check_intervals(
        [(0, 1), (1, 2), (3, 4)],
        [[(0, 1)], []],
        [(0, 1)],
    )
    assert_check_intervals(
        [(0, 1), (1, 2), (3, 4)],
        [[(0, 1)], [(3, 4)]],
        [(0, 1), (3, 4)],
    )
