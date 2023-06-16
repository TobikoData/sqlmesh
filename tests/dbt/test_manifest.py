from __future__ import annotations

from pathlib import Path

from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.utils.jinja import MacroReference


def test_manifest_helper():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    helper = ManifestHelper(project_path, project_path, "sushi")

    assert helper.models()["top_waiters"].dependencies == Dependencies(
        refs={"sushi.waiter_revenue_by_day", "waiter_revenue_by_day"},
    )
    assert helper.models()["top_waiters"].materialized == "view"

    assert helper.models()["waiters"].dependencies == Dependencies(
        macros={MacroReference(name="incremental_by_time")},
        sources={"streaming.orders"},
    )
    assert helper.models()["waiters"].materialized == "ephemeral"

    waiter_as_customer_by_day_config = helper.models()["waiter_as_customer_by_day"]
    assert waiter_as_customer_by_day_config.dependencies == Dependencies(
        refs={"waiters", "waiter_names", "customers"},
    )
    assert waiter_as_customer_by_day_config.materialized == "incremental"
    assert waiter_as_customer_by_day_config.incremental_strategy == "delete+insert"
    assert waiter_as_customer_by_day_config.cluster_by == ["ds"]
    assert waiter_as_customer_by_day_config.time_column == "ds"

    waiter_revenue_by_day_config = helper.models()["waiter_revenue_by_day"]
    assert waiter_revenue_by_day_config.dependencies == Dependencies(
        macros={
            MacroReference(name="log_value"),
            MacroReference(package="customers", name="duckdb__current_engine"),
            MacroReference(package="dbt", name="run_query"),
            MacroReference(package="dbt", name="is_incremental"),
        },
        sources={"streaming.items", "streaming.orders", "streaming.order_items"},
    )
    assert waiter_revenue_by_day_config.materialized == "incremental"
    assert waiter_revenue_by_day_config.incremental_strategy == "delete+insert"
    assert waiter_revenue_by_day_config.cluster_by == ["ds"]
    assert waiter_revenue_by_day_config.time_column == "ds"

    assert helper.models("customers")["customers"].dependencies == Dependencies(
        sources={"raw.orders"},
    )

    assert set(helper.macros()["incremental_by_time"].info.depends_on) == {
        MacroReference(package=None, name="incremental_dates_by_time_type"),
        MacroReference(package="dbt", name="is_incremental"),
    }

    assert helper.seeds()["waiter_names"].path == Path("seeds/waiter_names.csv")

    assert helper.sources()["streaming.items"].sql_name == "raw.items"
    assert helper.sources()["streaming.orders"].sql_name == "raw.orders"
    assert helper.sources()["streaming.order_items"].sql_name == "raw.order_items"
