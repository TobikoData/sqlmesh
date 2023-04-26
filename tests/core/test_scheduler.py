import pytest
from sqlglot import parse_one

from sqlmesh.core import constants as c
from sqlmesh.core.context import Context
from sqlmesh.core.scheduler import Scheduler
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory, SnapshotFingerprint
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
    assert scheduler._interval_params([orders, waiter_revenue], start_ds, end_ds) == {
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
    scheduler.state_sync.add_interval(orders.snapshot_id, "2022-01-10", "2022-01-15")

    assert scheduler._interval_params([orders], start_ds, end_ds) == {
        orders: [
            (to_datetime(start_ds), to_datetime("2022-01-10")),
            (to_datetime("2022-01-16"), to_datetime("2022-02-06")),
        ]
    }


def test_interval_params_missing(scheduler: Scheduler, sushi_context_fixed_date: Context):
    waiters = sushi_context_fixed_date.snapshots["sushi.waiter_as_customer_by_day"]

    start_ds = "2022-01-01"
    end_ds = "2022-03-01"
    assert scheduler._interval_params([waiters], start_ds, end_ds) == {
        waiters: [
            (to_datetime(start_ds), to_datetime("2022-03-02")),
        ]
    }


def test_multi_version_snapshots(
    sushi_context_fixed_date: Context, scheduler: Scheduler, make_snapshot
):
    start_ds = "2022-01-01"
    end_ds = "2022-02-05"

    model = sushi_context_fixed_date.models["sushi.waiter_as_customer_by_day"]

    items_a = make_snapshot(
        model,
        models=sushi_context_fixed_date.models,
        version="1",
    )
    items_a.fingerprint = SnapshotFingerprint(data_hash="data", metadata_hash="metadata")
    items_a.add_interval("2022-01-10", "2022-01-15")
    sushi_context_fixed_date.state_sync.push_snapshots([items_a])

    model = sushi_context_fixed_date.upsert_model(
        model,
        query=parse_one("SELECT 1::INT, '2022-01-01'::TEXT AS ds"),
    )

    items_b = make_snapshot(
        model,
        models=sushi_context_fixed_date.models,
        version="1",
    )
    items_b.change_category = SnapshotChangeCategory.FORWARD_ONLY
    items_b.add_interval("2022-01-20", "2022-01-25")
    sushi_context_fixed_date.state_sync.push_snapshots([items_b])

    interval_params = scheduler._interval_params([items_a], start_ds, end_ds)
    assert len(interval_params) == 1
    assert list(interval_params.values())[0] == [
        (to_datetime(start_ds), to_datetime("2022-01-10")),
        (to_datetime("2022-01-16"), to_datetime("2022-01-20")),
        (to_datetime("2022-01-26"), to_datetime("2022-02-06")),
    ]

    # Make sure that intervals of items_a are ignored in development mode.
    scheduler.snapshots[items_b.snapshot_id] = items_b
    interval_params_dev_mode = scheduler._interval_params([items_b], start_ds, end_ds, is_dev=True)
    assert len(interval_params_dev_mode) == 1
    assert list(interval_params_dev_mode.values())[0] == [
        (to_datetime(start_ds), to_datetime("2022-01-20")),
        (to_datetime("2022-01-26"), to_datetime("2022-02-06")),
    ]


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
        SELECT id, name, price FROM sqlmesh.sushi__items__{snapshot.version} ORDER BY ds LIMIT 1
    """
        )
        == (0, "Hotate", 5.99)
    )
