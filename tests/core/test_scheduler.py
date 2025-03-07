import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one, parse
from sqlglot.helper import first

from sqlmesh.core.context import Context
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.model.definition import AuditResult, SqlModel
from sqlmesh.core.model.kind import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    TimeColumn,
)
from sqlmesh.core.node import IntervalUnit
from sqlmesh.core.scheduler import (
    Scheduler,
    interval_diff,
    compute_interval_params,
    SnapshotToIntervals,
)
from sqlmesh.core.signal import signal
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotEvaluator,
    SnapshotChangeCategory,
    DeployabilityIndex,
)
from sqlmesh.utils.date import to_datetime, to_timestamp, DatetimeRanges, TimeLike
from sqlmesh.utils.errors import CircuitBreakerError, NodeAuditsErrors


@pytest.fixture
def scheduler(sushi_context_fixed_date: Context) -> Scheduler:
    return sushi_context_fixed_date.scheduler()


@pytest.fixture
def orders(sushi_context_fixed_date: Context) -> Snapshot:
    return sushi_context_fixed_date.get_snapshot("sushi.orders", raise_if_missing=True)


@pytest.fixture
def waiter_names(sushi_context_fixed_date: Context) -> Snapshot:
    return sushi_context_fixed_date.get_snapshot("sushi.waiter_names", raise_if_missing=True)


@pytest.mark.slow
def test_interval_params(scheduler: Scheduler, sushi_context_fixed_date: Context, orders: Snapshot):
    waiter_revenue = sushi_context_fixed_date.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    assert compute_interval_params([orders, waiter_revenue], start=start_ds, end=end_ds) == {
        orders: [
            (to_timestamp(start_ds), to_timestamp("2022-02-06")),
        ],
        waiter_revenue: [
            (to_timestamp(start_ds), to_timestamp("2022-02-06")),
        ],
    }


@pytest.fixture
def get_batched_missing_intervals() -> (
    t.Callable[[Scheduler, TimeLike, TimeLike, t.Optional[TimeLike]], SnapshotToIntervals]
):
    def _get_batched_missing_intervals(
        scheduler: Scheduler,
        start: TimeLike,
        end: TimeLike,
        execution_time: t.Optional[TimeLike] = None,
    ) -> SnapshotToIntervals:
        merged_intervals = scheduler.merged_missing_intervals(start, end, execution_time)
        return scheduler.batch_intervals(merged_intervals)

    return _get_batched_missing_intervals


def test_interval_params_nonconsecutive(scheduler: Scheduler, orders: Snapshot):
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    orders.add_interval("2022-01-10", "2022-01-15")

    assert compute_interval_params([orders], start=start_ds, end=end_ds) == {
        orders: [
            (to_timestamp(start_ds), to_timestamp("2022-01-10")),
            (to_timestamp("2022-01-16"), to_timestamp("2022-02-06")),
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
        (to_timestamp(start_ds), to_timestamp("2022-03-02")),
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


def test_incremental_by_unique_key_kind_dag(
    mocker: MockerFixture, make_snapshot, get_batched_missing_intervals
):
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
    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    mock_state_sync = mocker.MagicMock()
    scheduler = Scheduler(
        snapshots=[unique_by_key_snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=2,
        default_catalog=None,
    )
    batches = get_batched_missing_intervals(scheduler, start, end, end)
    dag = scheduler._dag(batches)
    assert dag.graph == {
        (
            unique_by_key_snapshot.name,
            ((to_timestamp("2023-01-01"), to_timestamp("2023-01-07")), 0),
        ): set(),
    }


def test_incremental_time_self_reference_dag(
    mocker: MockerFixture, make_snapshot, get_batched_missing_intervals
):
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

    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[incremental_self_snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )
    batches = get_batched_missing_intervals(scheduler, start, end, end)
    dag = scheduler._dag(batches)

    assert dag.graph == {
        # Only run one day at a time and each day relies on the previous days
        (
            incremental_self_snapshot.name,
            ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0),
        ): set(),
        (
            incremental_self_snapshot.name,
            ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 1),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0),
            )
        },
        (
            incremental_self_snapshot.name,
            ((to_timestamp("2023-01-04"), to_timestamp("2023-01-05")), 2),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 1),
            ),
        },
        (
            incremental_self_snapshot.name,
            ((to_timestamp("2023-01-06"), to_timestamp("2023-01-07")), 3),
        ): {
            (
                incremental_self_snapshot.name,
                ((to_timestamp("2023-01-04"), to_timestamp("2023-01-05")), 2),
            ),
        },
        (
            incremental_self_snapshot.name,
            ((to_timestamp(0), to_timestamp(0)), -1),
        ): set(
            [
                (
                    incremental_self_snapshot.name,
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 1),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_timestamp("2023-01-04"), to_timestamp("2023-01-05")), 2),
                ),
                (
                    incremental_self_snapshot.name,
                    ((to_timestamp("2023-01-06"), to_timestamp("2023-01-07")), 3),
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
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-03")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-03"), to_timestamp("2023-01-05")), 1),
                ): set(),
                ('"test_model"', ((to_timestamp("2023-01-05"), to_timestamp("2023-01-07")), 2)): {
                    ('"test_model"', ((to_timestamp("2023-01-01"), to_timestamp("2023-01-03")), 0)),
                },
            },
        ),
        (
            1,
            3,
            {
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-02"), to_timestamp("2023-01-03")), 1),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 2),
                ): set(),
                ('"test_model"', ((to_timestamp("2023-01-04"), to_timestamp("2023-01-05")), 3)): {
                    ('"test_model"', ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0)),
                },
                ('"test_model"', ((to_timestamp("2023-01-05"), to_timestamp("2023-01-06")), 4)): {
                    ('"test_model"', ((to_timestamp("2023-01-02"), to_timestamp("2023-01-03")), 1)),
                },
                ('"test_model"', ((to_timestamp("2023-01-06"), to_timestamp("2023-01-07")), 5)): {
                    ('"test_model"', ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 2)),
                },
            },
        ),
        (
            1,
            10,
            {
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-02")), 0),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-02"), to_timestamp("2023-01-03")), 1),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-03"), to_timestamp("2023-01-04")), 2),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-04"), to_timestamp("2023-01-05")), 3),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-05"), to_timestamp("2023-01-06")), 4),
                ): set(),
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-06"), to_timestamp("2023-01-07")), 5),
                ): set(),
            },
        ),
        (
            10,
            10,
            {
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-07")), 0),
                ): set(),
            },
        ),
        (
            10,
            1,
            {
                (
                    '"test_model"',
                    ((to_timestamp("2023-01-01"), to_timestamp("2023-01-07")), 0),
                ): set(),
            },
        ),
    ],
)
def test_incremental_batch_concurrency(
    mocker: MockerFixture,
    make_snapshot,
    get_batched_missing_intervals,
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

    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    mock_state_sync = mocker.MagicMock()
    scheduler = Scheduler(
        snapshots=[snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mock_state_sync,
        max_workers=4,
        default_catalog=None,
    )

    batches = get_batched_missing_intervals(scheduler, start, end, end)
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


def test_intervals_with_end_date_on_model(
    mocker: MockerFixture, make_snapshot, get_batched_missing_intervals
):
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

    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[snapshot],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )

    # generate for 1 year to show that the returned batches should only cover
    # the range defined on the model itself
    batches = get_batched_missing_intervals(scheduler, start="2023-01-01", end="2024-01-01")[
        snapshot
    ]

    assert len(batches) == 31  # days in Jan 2023
    assert batches[0] == (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert batches[-1] == (to_timestamp("2023-01-31"), to_timestamp("2023-02-01"))

    # generate for less than 1 month to ensure that the scheduler end date
    # takes precedence over the model end date
    batches = get_batched_missing_intervals(scheduler, start="2023-01-01", end="2023-01-10")[
        snapshot
    ]

    assert len(batches) == 10
    assert batches[0] == (to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))
    assert batches[-1] == (to_timestamp("2023-01-10"), to_timestamp("2023-01-11"))

    # generate for the last day of range
    batches = get_batched_missing_intervals(scheduler, start="2023-01-31", end="2023-01-31")[
        snapshot
    ]
    assert len(batches) == 1
    assert batches[0] == (to_timestamp("2023-01-31"), to_timestamp("2023-02-01"))

    # generate for future days to ensure no future batches are loaded
    snapshot_to_batches = get_batched_missing_intervals(
        scheduler, start="2023-02-01", end="2023-02-28"
    )
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

    evaluator = SnapshotEvaluator(adapters=mocker.MagicMock())
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


def test_audit_failure_notifications(
    scheduler: Scheduler, waiter_names: Snapshot, mocker: MockerFixture
):
    evaluator_evaluate_mock = mocker.Mock()
    mocker.patch("sqlmesh.core.scheduler.SnapshotEvaluator.evaluate", evaluator_evaluate_mock)
    evaluator_audit_mock = mocker.Mock()
    mocker.patch("sqlmesh.core.scheduler.SnapshotEvaluator.audit", evaluator_audit_mock)
    notify_user_mock = mocker.Mock()
    mocker.patch(
        "sqlmesh.core.notification_target.NotificationTargetManager.notify_user", notify_user_mock
    )
    notify_mock = mocker.Mock()
    mocker.patch("sqlmesh.core.notification_target.NotificationTargetManager.notify", notify_mock)

    audit = first(waiter_names.model.audit_definitions.values())
    query = waiter_names.model.render_query()

    def _evaluate():
        scheduler.evaluate(
            waiter_names,
            to_datetime("2022-01-01"),
            to_datetime("2022-01-02"),
            to_datetime("2022-01-03"),
            DeployabilityIndex.all_deployable(),
            0,
        )

    evaluator_audit_mock.return_value = [
        AuditResult(
            audit=audit,
            audit_args={},
            model=waiter_names.model,
            query=query,
            count=0,
            skipped=False,
        )
    ]
    _evaluate()
    assert notify_user_mock.call_count == 0
    assert notify_mock.call_count == 0

    evaluator_audit_mock.return_value = [
        AuditResult(
            audit=audit,
            audit_args={},
            model=waiter_names.model,
            query=query,
            count=None,
            skipped=True,
        )
    ]
    _evaluate()
    assert notify_user_mock.call_count == 0
    assert notify_mock.call_count == 0

    evaluator_audit_mock.return_value = [
        AuditResult(
            audit=audit,
            audit_args={},
            model=waiter_names.model,
            query=query,
            count=1,
            skipped=False,
            blocking=False,
        )
    ]
    _evaluate()
    assert notify_user_mock.call_count == 1
    assert notify_mock.call_count == 1
    notify_user_mock.reset_mock()
    notify_mock.reset_mock()

    evaluator_audit_mock.return_value = [
        AuditResult(
            audit=audit,
            audit_args={},
            model=waiter_names.model,
            query=query,
            count=1,
            skipped=False,
        )
    ]
    with pytest.raises(NodeAuditsErrors):
        _evaluate()
    assert notify_user_mock.call_count == 1
    assert notify_mock.call_count == 1


def test_interval_diff():
    assert interval_diff([(1, 2)], []) == [(1, 2)]
    assert interval_diff([(1, 2)], [(1, 2)]) == []
    assert interval_diff([(1, 2)], [(0, 2)]) == []
    assert interval_diff([(1, 2)], [(2, 3)]) == [(1, 2)]
    assert interval_diff([(1, 2)], [(0, 1)]) == [(1, 2)]
    assert interval_diff([(1, 2), (2, 3), (3, 4)], [(1, 4)]) == []
    assert interval_diff([(1, 2), (2, 3), (3, 4)], [(1, 2)]) == [(2, 3), (3, 4)]
    assert interval_diff([(4, 5)], [(1, 2), (2, 3)]) == [(4, 5)]
    assert interval_diff(
        [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)],
        [(2, 3), (4, 6)],
    ) == [(1, 2), (3, 4)]

    assert interval_diff(
        [(1, 2), (2, 3), (3, 4)],
        [(1, 3)],
    ) == [(3, 4)]

    assert interval_diff(
        [(1, 3), (3, 4)],
        [(1, 2), (2, 3)],
    ) == [(3, 4)]

    assert interval_diff([(1, 2), (2, 3)], [(1, 2)], uninterrupted=True) == []
    assert interval_diff([(1, 2), (2, 3)], [(3, 4)], uninterrupted=True) == [(1, 2), (2, 3)]
    assert interval_diff([(1, 2), (2, 3)], [(2, 3)], uninterrupted=True) == [(1, 2)]


def test_signal_intervals(mocker: MockerFixture, make_snapshot, get_batched_missing_intervals):
    @signal()
    def signal_a(batch: DatetimeRanges):
        return [batch[0], batch[1]]

    @signal()
    def signal_b(batch: DatetimeRanges):
        return batch[-49:]

    signals = signal.get_registry()

    a = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name a,
                    kind FULL,
                    start '2023-01-01',
                    signals SIGNAL_A(),
                );

                SELECT 1 x;
                """
            ),
            signal_definitions=signals,
        ),
    )

    b = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name b,
                    kind FULL,
                    cron '@hourly',
                    start '2023-01-01',
                    signals SIGNAL_B(),
                );

                SELECT 2 x;
                """
            ),
            signal_definitions=signals,
        ),
        nodes={a.name: a.model},
    )

    c = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name c,
                    kind FULL,
                    start '2023-01-01',
                );

                SELECT * FROM a UNION SELECT * FROM b
                """
            ),
            signal_definitions=signals,
        ),
        nodes={a.name: a.model, b.name: b.model},
    )
    d = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name d,
                    kind FULL,
                    start '2023-01-01',
                );

                SELECT * FROM c UNION SELECT * FROM d
                """
            ),
            signal_definitions=signals,
        ),
        nodes={a.name: a.model, b.name: b.model, c.name: c.model},
    )

    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[a, b, c, d],
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )

    batches = get_batched_missing_intervals(scheduler, "2023-01-01", "2023-01-03", None)

    assert batches == {
        a: [(to_timestamp("2023-01-01"), to_timestamp("2023-01-03"))],
        b: [(to_timestamp("2023-01-01 23:00:00"), to_timestamp("2023-01-04"))],
        # Full models and models that depend on past can't run for a discontinuous range
        c: [],
        d: [],
    }


def test_signals_snapshots_out_of_order(
    mocker: MockerFixture, make_snapshot, get_batched_missing_intervals
):
    @signal()
    def signal_base(batch: DatetimeRanges):
        return [batch[0]]

    signals = signal.get_registry()

    snapshot_a = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name a,
                    kind INCREMENTAL_BY_TIME_RANGE(
                      lookback 1,
                      time_column dt,
                    ),
                    start '2023-01-01',
                    signals SIGNAL_BASE(),
                );
                SELECT @start_date AS dt;
                """
            ),
            signal_definitions=signals,
        ),
    )

    snapshot_b = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name b,
                    kind INCREMENTAL_BY_TIME_RANGE(
                      lookback 1,
                      time_column dt,
                    ),
                    start '2023-01-01'
                );
                SELECT @start_date AS dt;
                """
            ),
            signal_definitions=signals,
        )
    )

    snapshot_c = make_snapshot(
        load_sql_based_model(
            parse(  # type: ignore
                """
                MODEL (
                    name c,
                    kind INCREMENTAL_BY_TIME_RANGE(
                      lookback 1,
                      time_column dt,
                    ),
                    start '2023-01-01',
                );
                SELECT * FROM a UNION SELECT * FROM b
                """
            ),
            signal_definitions=signals,
        ),
        nodes={snapshot_a.name: snapshot_a.model, snapshot_b.name: snapshot_b.model},
    )

    snapshot_evaluator = SnapshotEvaluator(adapters=mocker.MagicMock(), ddl_concurrent_tasks=1)
    scheduler = Scheduler(
        snapshots=[snapshot_c, snapshot_b, snapshot_a],  # reverse order
        snapshot_evaluator=snapshot_evaluator,
        state_sync=mocker.MagicMock(),
        max_workers=2,
        default_catalog=None,
    )

    batches = get_batched_missing_intervals(scheduler, "2023-01-01", "2023-01-03", None)

    assert batches == {
        snapshot_a: [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))],
        snapshot_b: [(to_timestamp("2023-01-01"), to_timestamp("2023-01-04"))],
        snapshot_c: [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))],
    }
