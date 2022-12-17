import pytest
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils.date import to_datetime


@pytest.fixture
def scheduler(sushi_context_pre_scheduling: Context) -> Scheduler:
    return sushi_context_pre_scheduling.scheduler()


@pytest.fixture
def orders(sushi_context_pre_scheduling: Context) -> Snapshot:
    return sushi_context_pre_scheduling.snapshots["sushi.orders"]


def test_interval_params(
    scheduler: Scheduler, sushi_context_pre_scheduling: Context, orders: Snapshot
):
    waiter_revenue = sushi_context_pre_scheduling.snapshots[
        "sushi.waiter_revenue_by_day"
    ]
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"
    assert scheduler.interval_params([orders, waiter_revenue], start_ds, end_ds) == [
        (
            orders,
            [
                (to_datetime(start_ds), to_datetime("2022-01-31")),
                (to_datetime("2022-01-31"), to_datetime("2022-02-06")),
            ],
        ),
        (
            waiter_revenue,
            [
                (to_datetime(start_ds), to_datetime("2022-01-11")),
                (to_datetime("2022-01-11"), to_datetime("2022-01-21")),
                (to_datetime("2022-01-21"), to_datetime("2022-01-31")),
                (to_datetime("2022-01-31"), to_datetime("2022-02-06")),
            ],
        ),
    ]


def test_interval_params_nonconsecutive(scheduler: Scheduler, orders: Snapshot):
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    orders.add_interval("2022-01-10", "2022-01-15")
    scheduler.state_sync.add_interval(orders.snapshot_id, "2022-01-10", "2022-01-15")

    assert scheduler.interval_params([orders], start_ds, end_ds) == [
        (
            orders,
            [
                (to_datetime(start_ds), to_datetime("2022-01-10")),
                (to_datetime("2022-01-16"), to_datetime("2022-02-06")),
            ],
        ),
    ]


def test_interval_params_missing(
    scheduler: Scheduler, sushi_context_pre_scheduling: Context
):
    waiters = sushi_context_pre_scheduling.snapshots["sushi.waiters"]

    start_ds = "2022-01-01"
    end_ds = "2022-03-01"
    assert scheduler.interval_params([waiters], start_ds, end_ds) == [
        (
            waiters,
            [
                (to_datetime(start_ds), to_datetime("2022-03-02")),
            ],
        ),
    ]


def test_multi_version_snapshots(
    sushi_context_pre_scheduling: Context, scheduler: Scheduler, make_snapshot
):
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    model = sushi_context_pre_scheduling.models["sushi.items"]

    items_a = make_snapshot(
        model,
        models=sushi_context_pre_scheduling.models,
        version="1",
    )
    items_a.fingerprint = 123
    items_a.add_interval("2022-01-10", "2022-01-15")
    sushi_context_pre_scheduling.state_sync.push_snapshots([items_a])

    model = sushi_context_pre_scheduling.upsert_model(
        model,
        query=parse_one("SELECT 1::INT, '2022-01-01'::TEXT AS ds"),
    )

    items_b = make_snapshot(
        model,
        models=sushi_context_pre_scheduling.models,
        version="1",
    )
    items_b.add_interval("2022-01-20", "2022-01-25")
    sushi_context_pre_scheduling.state_sync.push_snapshots([items_b])

    interval_params = scheduler.interval_params([items_a], start_ds, end_ds)
    assert len(interval_params) == 1
    assert interval_params[0][1] == [
        (to_datetime(start_ds), to_datetime("2022-01-10")),
        (to_datetime("2022-01-16"), to_datetime("2022-01-20")),
        (to_datetime("2022-01-26"), to_datetime("2022-02-06")),
    ]


def test_run(sushi_context_pre_scheduling: Context, scheduler: Scheduler):
    adapter = sushi_context_pre_scheduling.engine_adapter
    snapshot = sushi_context_pre_scheduling.snapshots["sushi.items"]
    scheduler.run(
        "2022-01-01",
        "2022-01-03",
        "2022-01-30",
    )

    assert (
        adapter.fetchone(
            f"""
        SELECT id, name, price FROM sqlmesh.sushi__items__{snapshot.version} ORDER BY ds LIMIT 1
    """
        )
        == (0, "Hotate", 5.99)
    )
