from __future__ import annotations

from pathlib import Path

import pytest

from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.profile import Profile
from sqlmesh.utils.jinja import MacroReference

pytestmark = pytest.mark.dbt


@pytest.mark.xdist_group("dbt_manifest")
def test_manifest_helper(caplog):
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))
    helper = ManifestHelper(project_path, project_path, "sushi", profile.target)

    assert helper.models()["top_waiters"].dependencies == Dependencies(
        refs={"sushi.waiter_revenue_by_day", "waiter_revenue_by_day"},
        variables={"top_waiters:revenue", "top_waiters:limit"},
    )
    assert helper.models()["top_waiters"].materialized == "view"

    assert helper.models()["waiters"].dependencies == Dependencies(
        macros={MacroReference(name="incremental_by_time")},
        sources={"streaming.orders"},
    )
    assert helper.models()["waiters"].materialized == "ephemeral"
    assert helper.models()["items_snapshot"].materialized == "snapshot"
    assert helper.models()["items_snapshot"].updated_at == "ds"
    assert helper.models()["items_snapshot"].unique_key == ["id"]
    assert helper.models()["items_snapshot"].strategy == "timestamp"
    assert helper.models()["items_snapshot"].table_schema == "snapshots"
    assert helper.models()["items_snapshot"].invalidate_hard_deletes is True
    assert helper.models()["items_check_snapshot"].materialized == "snapshot"
    assert helper.models()["items_check_snapshot"].check_cols == ["ds"]
    assert helper.models()["items_check_snapshot"].unique_key == ["id"]
    assert helper.models()["items_check_snapshot"].strategy == "check"
    assert helper.models()["items_check_snapshot"].table_schema == "snapshots"
    assert helper.models()["items_check_snapshot"].invalidate_hard_deletes is True
    assert helper.models()["items_no_hard_delete_snapshot"].materialized == "snapshot"
    assert helper.models()["items_no_hard_delete_snapshot"].unique_key == ["id"]
    assert helper.models()["items_no_hard_delete_snapshot"].strategy == "timestamp"
    assert helper.models()["items_no_hard_delete_snapshot"].table_schema == "snapshots"
    assert helper.models()["items_no_hard_delete_snapshot"].invalidate_hard_deletes is False

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
            MacroReference(name="test_dependencies"),
            MacroReference(package="customers", name="duckdb__current_engine"),
            MacroReference(package="dbt", name="run_query"),
        },
        sources={"streaming.items", "streaming.orders", "streaming.order_items"},
        variables={"yet_another_var"},
    )
    assert waiter_revenue_by_day_config.materialized == "incremental"
    assert waiter_revenue_by_day_config.incremental_strategy == "delete+insert"
    assert waiter_revenue_by_day_config.cluster_by == ["ds"]
    assert waiter_revenue_by_day_config.time_column == "ds"

    assert helper.models("customers")["customers"].dependencies == Dependencies(
        sources={"raw.orders"},
        variables={"customers:customer_id"},
    )

    assert set(helper.macros()["incremental_by_time"].info.depends_on) == {
        MacroReference(package=None, name="incremental_dates_by_time_type"),
    }

    assert helper.seeds()["waiter_names"].path == Path("seeds/waiter_names.csv")

    assert helper.sources()["streaming.items"].table_name == "items"
    assert helper.sources()["streaming.items"].schema_ == "raw"
    assert helper.sources()["streaming.orders"].table_name == "orders"
    assert helper.sources()["streaming.orders"].schema_ == "raw"
    assert helper.sources()["streaming.order_items"].table_name == "order_items"
    assert helper.sources()["streaming.order_items"].schema_ == "raw"


@pytest.mark.xdist_group("dbt_manifest")
def test_tests_referencing_disabled_models():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))
    helper = ManifestHelper(project_path, project_path, "sushi", profile.target)

    assert "disabled_model" not in helper.models()
    assert "not_null_disabled_model_one" not in helper.tests()


@pytest.mark.xdist_group("dbt_manifest")
def test_variable_override():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(project_path, project_path, "sushi", profile.target)
    assert helper.models()["top_waiters"].limit_value == 10

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"top_waiters:limit": 1},
    )
    assert helper.models()["top_waiters"].limit_value == 1
