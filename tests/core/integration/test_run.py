from __future__ import annotations

import typing as t
import pytest
import time_machine
from pytest_mock.plugin import MockerFixture

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.config.categorizer import CategorizerConfig
from sqlmesh.core.model import (
    SqlModel,
    PythonModel,
    load_sql_based_model,
)
from sqlmesh.utils.date import to_timestamp

if t.TYPE_CHECKING:
    pass

pytestmark = pytest.mark.slow


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_run_with_select_models(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    with time_machine.travel("2023-01-09 00:00:00 UTC"):
        assert context.run(select_models=["*waiter_revenue_by_day"])

        snapshots = context.state_sync.state_sync.get_snapshots(context.snapshots.values())
        # Only waiter_revenue_by_day and its parents should be backfilled up to 2023-01-09.
        assert {s.name: s.intervals[0][1] for s in snapshots.values() if s.intervals} == {
            '"memory"."sushi"."waiter_revenue_by_day"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."order_items"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."orders"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."items"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."customer_revenue_lifetime"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."customer_revenue_by_day"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."latest_order"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."waiter_names"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."raw_marketing"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."marketing"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."waiter_as_customer_by_day"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."top_waiters"': to_timestamp("2023-01-08"),
            '"memory"."raw"."demographics"': to_timestamp("2023-01-08"),
            "assert_item_price_above_zero": to_timestamp("2023-01-08"),
            '"memory"."sushi"."active_customers"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."customers"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."count_customers_active"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."count_customers_inactive"': to_timestamp("2023-01-08"),
        }


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_run_with_select_models_no_auto_upstream(
    init_and_plan_context: t.Callable,
):
    context, _ = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.waiter_revenue_by_day")
    model = SqlModel.parse_obj({**model.dict(), "audits": []})
    context.upsert_model(model)

    context.plan("prod", no_prompts=True, skip_tests=True, auto_apply=True)

    with time_machine.travel("2023-01-09 00:00:00 UTC"):
        assert context.run(select_models=["*waiter_revenue_by_day"], no_auto_upstream=True)

        snapshots = context.state_sync.state_sync.get_snapshots(context.snapshots.values())
        # Only waiter_revenue_by_day should be backfilled up to 2023-01-09.
        assert {s.name: s.intervals[0][1] for s in snapshots.values() if s.intervals} == {
            '"memory"."sushi"."waiter_revenue_by_day"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."order_items"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."orders"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."items"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."customer_revenue_lifetime"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."customer_revenue_by_day"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."latest_order"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."waiter_names"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."raw_marketing"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."marketing"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."waiter_as_customer_by_day"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."top_waiters"': to_timestamp("2023-01-08"),
            '"memory"."raw"."demographics"': to_timestamp("2023-01-08"),
            "assert_item_price_above_zero": to_timestamp("2023-01-08"),
            '"memory"."sushi"."active_customers"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."customers"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."count_customers_active"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."count_customers_inactive"': to_timestamp("2023-01-08"),
        }


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_run_respects_excluded_transitive_dependencies(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    # Graph: C <- B <- A
    # B is a transitive dependency linking A and C
    # Note that the alphabetical ordering of the model names is intentional and helps
    # surface the problem
    expressions_a = d.parse(
        f"""
        MODEL (
            name memory.sushi.test_model_c,
            kind FULL,
            allow_partials true,
            cron '@hourly',
            start '2023-01-01',
        );

        SELECT @execution_ts AS execution_ts
        """
    )
    model_c = load_sql_based_model(expressions_a)
    context.upsert_model(model_c)

    # A VIEW model with no partials allowed and a daily cron instead of hourly.
    expressions_b = d.parse(
        f"""
        MODEL (
            name memory.sushi.test_model_b,
            kind VIEW,
            allow_partials false,
            cron '@daily',
        );

        SELECT * FROM memory.sushi.test_model_c
        """
    )
    model_b = load_sql_based_model(expressions_b)
    context.upsert_model(model_b)

    expressions_a = d.parse(
        f"""
        MODEL (
            name memory.sushi.test_model_a,
            kind FULL,
            allow_partials true,
            cron '@hourly',
        );

        SELECT * FROM memory.sushi.test_model_b
        """
    )
    model_a = load_sql_based_model(expressions_a)
    context.upsert_model(model_a)

    context.plan("prod", skip_tests=True, auto_apply=True, no_prompts=True)
    assert (
        context.fetchdf("SELECT execution_ts FROM memory.sushi.test_model_c")["execution_ts"].iloc[
            0
        ]
        == "2023-01-08 15:00:00"
    )

    with time_machine.travel("2023-01-08 17:00:00 UTC", tick=False):
        context.run(
            "prod",
            select_models=["*test_model_c", "*test_model_a"],
            no_auto_upstream=True,
            ignore_cron=True,
        )
        assert (
            context.fetchdf("SELECT execution_ts FROM memory.sushi.test_model_a")[
                "execution_ts"
            ].iloc[0]
            == "2023-01-08 17:00:00"
        )


@time_machine.travel("2023-01-08 00:00:00 UTC")
def test_snapshot_triggers(init_and_plan_context: t.Callable, mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # auto-restatement triggers
    orders = context.get_model("sushi.orders")
    orders_kind = {
        **orders.kind.dict(),
        "auto_restatement_cron": "@hourly",
    }
    orders_kwargs = {
        **orders.dict(),
        "kind": orders_kind,
    }
    context.upsert_model(PythonModel.parse_obj(orders_kwargs))

    order_items = context.get_model("sushi.order_items")
    order_items_kind = {
        **order_items.kind.dict(),
        "auto_restatement_cron": "@hourly",
    }
    order_items_kwargs = {
        **order_items.dict(),
        "kind": order_items_kind,
    }
    context.upsert_model(PythonModel.parse_obj(order_items_kwargs))

    waiter_revenue_by_day = context.get_model("sushi.waiter_revenue_by_day")
    waiter_revenue_by_day_kind = {
        **waiter_revenue_by_day.kind.dict(),
        "auto_restatement_cron": "@hourly",
    }
    waiter_revenue_by_day_kwargs = {
        **waiter_revenue_by_day.dict(),
        "kind": waiter_revenue_by_day_kind,
    }
    context.upsert_model(SqlModel.parse_obj(waiter_revenue_by_day_kwargs))

    context.plan(auto_apply=True, no_prompts=True, categorizer_config=CategorizerConfig.all_full())

    scheduler = context.scheduler()

    import sqlmesh

    spy = mocker.spy(sqlmesh.core.scheduler.Scheduler, "run_merged_intervals")

    with time_machine.travel("2023-01-09 00:00:01 UTC"):
        scheduler.run(
            environment=c.PROD,
            start="2023-01-01",
            auto_restatement_enabled=True,
        )

    assert spy.called

    actual_triggers = spy.call_args.kwargs["auto_restatement_triggers"]
    actual_triggers = {k: v for k, v in actual_triggers.items() if v}
    assert len(actual_triggers) == 12

    for id, trigger in actual_triggers.items():
        model_name = id.name.replace('"memory"."sushi".', "").replace('"', "")
        auto_restatement_triggers = [
            t.name.replace('"memory"."sushi".', "").replace('"', "") for t in trigger
        ]

        if model_name in ("orders", "order_items", "waiter_revenue_by_day"):
            assert auto_restatement_triggers == [model_name]
        elif model_name in ("customer_revenue_lifetime", "customer_revenue_by_day"):
            assert sorted(auto_restatement_triggers) == sorted(["orders", "order_items"])
        elif model_name == "top_waiters":
            assert auto_restatement_triggers == ["waiter_revenue_by_day"]
        else:
            assert auto_restatement_triggers == ["orders"]
