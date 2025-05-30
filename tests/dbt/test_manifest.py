from __future__ import annotations

from pathlib import Path

import pytest

from sqlmesh.dbt.basemodel import Dependencies
from sqlmesh.dbt.context import DbtContext
from sqlmesh.dbt.manifest import ManifestHelper
from sqlmesh.dbt.profile import Profile
from sqlmesh.dbt.builtin import Api, _relation_info_to_relation
from sqlmesh.dbt.util import DBT_VERSION
from sqlmesh.utils.jinja import MacroReference

pytestmark = pytest.mark.dbt


@pytest.mark.xdist_group("dbt_manifest")
def test_manifest_helper(caplog):
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))
    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )

    models = helper.models()

    assert models["top_waiters"].dependencies == Dependencies(
        refs={"sushi.waiter_revenue_by_day", "waiter_revenue_by_day"},
        variables={"top_waiters:revenue", "top_waiters:limit"},
        model_attrs={"columns", "config"},
        macros=[MacroReference(name="ref"), MacroReference(name="var")],
    )
    assert models["top_waiters"].materialized == "view"
    assert models["top_waiters"].dialect_ == "postgres"

    assert models["waiters"].dependencies == Dependencies(
        macros={MacroReference(name="incremental_by_time"), MacroReference(name="source")},
        sources={"streaming.orders"},
    )
    assert models["waiters"].materialized == "ephemeral"
    assert models["items_snapshot"].materialized == "snapshot"
    assert models["items_snapshot"].updated_at == "ds"
    assert models["items_snapshot"].unique_key == ["id"]
    assert models["items_snapshot"].strategy == "timestamp"
    assert models["items_snapshot"].table_schema == "snapshots"
    assert models["items_snapshot"].invalidate_hard_deletes is True
    assert models["items_check_snapshot"].materialized == "snapshot"
    assert models["items_check_snapshot"].check_cols == ["ds"]
    assert models["items_check_snapshot"].unique_key == ["id"]
    assert models["items_check_snapshot"].strategy == "check"
    assert models["items_check_snapshot"].table_schema == "snapshots"
    assert models["items_check_snapshot"].invalidate_hard_deletes is True
    assert models["items_no_hard_delete_snapshot"].materialized == "snapshot"
    assert models["items_no_hard_delete_snapshot"].unique_key == ["id"]
    assert models["items_no_hard_delete_snapshot"].strategy == "timestamp"
    assert models["items_no_hard_delete_snapshot"].table_schema == "snapshots"
    assert models["items_no_hard_delete_snapshot"].invalidate_hard_deletes is False

    # Test versioned models
    assert models["waiter_revenue_by_day_v1"].version == 1
    assert models["waiter_revenue_by_day_v2"].version == 2
    assert "waiter_revenue_by_day" not in models

    waiter_as_customer_by_day_config = models["waiter_as_customer_by_day"]
    assert waiter_as_customer_by_day_config.dependencies == Dependencies(
        refs={"waiters", "waiter_names", "customers"},
        macros=[MacroReference(name="ref")],
    )
    assert waiter_as_customer_by_day_config.materialized == "incremental"
    assert waiter_as_customer_by_day_config.incremental_strategy == "delete+insert"
    assert waiter_as_customer_by_day_config.cluster_by == ["ds"]
    assert waiter_as_customer_by_day_config.time_column == "ds"

    waiter_revenue_by_day_config = models["waiter_revenue_by_day_v2"]
    assert waiter_revenue_by_day_config.dependencies == Dependencies(
        macros={
            MacroReference(name="log_value"),
            MacroReference(name="test_dependencies"),
            MacroReference(package="customers", name="duckdb__current_engine"),
            MacroReference(package="dbt", name="run_query"),
            MacroReference(name="source"),
        },
        sources={"streaming.items", "streaming.orders", "streaming.order_items"},
        variables={"yet_another_var", "nested_vars"},
    )
    assert waiter_revenue_by_day_config.materialized == "incremental"
    assert waiter_revenue_by_day_config.incremental_strategy == "delete+insert"
    assert waiter_revenue_by_day_config.cluster_by == ["ds"]
    assert waiter_revenue_by_day_config.time_column == "ds"
    assert waiter_revenue_by_day_config.dialect_ == "bigquery"

    assert helper.models("customers")["customers"].dependencies == Dependencies(
        sources={"raw.orders"},
        variables={"customers:customer_id"},
        macros=[MacroReference(name="source"), MacroReference(name="var")],
    )

    assert set(helper.macros()["incremental_by_time"].info.depends_on) == {
        MacroReference(package=None, name="incremental_dates_by_time_type"),
    }

    assert helper.seeds()["waiter_names"].path == Path("seeds/waiter_names.csv")

    sources = helper.sources()

    assert sources["streaming.items"].table_name == "items"
    assert sources["streaming.items"].schema_ == "raw"
    assert sources["streaming.orders"].table_name == "orders"
    assert sources["streaming.orders"].schema_ == "raw"
    assert sources["streaming.order_items"].table_name == "order_items"
    assert sources["streaming.order_items"].schema_ == "raw"

    assert sources["streaming.order_items"].freshness == {
        "warn_after": {"count": 10 if DBT_VERSION < (1, 9, 5) else 12, "period": "hour"},
        "error_after": {"count": 11 if DBT_VERSION < (1, 9, 5) else 13, "period": "hour"},
        "filter": None,
    }


@pytest.mark.xdist_group("dbt_manifest")
def test_tests_referencing_disabled_models():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))
    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )

    assert "disabled_model" not in helper.models()
    assert "not_null_disabled_model_one" not in helper.tests()


@pytest.mark.xdist_group("dbt_manifest")
def test_call_cache():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))
    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )

    unused = "0000"
    helper._call_cache.put("", value={unused: "unused"})
    helper._load_all()
    calls = set(helper._call_cache.get("").keys())
    assert len(calls) >= 300
    assert unused not in calls


@pytest.mark.xdist_group("dbt_manifest")
def test_variable_override():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )
    assert helper.models()["top_waiters"].limit_value == 10

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"top_waiters:limit": 1, "start": "2020-01-01"},
    )
    assert helper.models()["top_waiters"].limit_value == 1


@pytest.mark.xdist_group("dbt_manifest")
def test_source_meta_external_location():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )

    sources = helper.sources()
    parquet_orders = sources["parquet_file.orders"]
    assert parquet_orders.source_meta == {
        "external_location": "read_parquet('path/to/external/{name}.parquet')"
    }
    assert (
        parquet_orders.relation_info.external == "read_parquet('path/to/external/orders.parquet')"
    )

    api = Api("duckdb")
    relation_info = sources["parquet_file.items"].relation_info
    assert relation_info.external == "read_parquet('path/to/external/items.parquet')"

    relation = _relation_info_to_relation(
        sources["parquet_file.items"].relation_info, api.Relation, api.quote_policy
    )
    assert relation.identifier == "items"
    assert relation.render() == "read_parquet('path/to/external/items.parquet')"


@pytest.mark.xdist_group("dbt_manifest")
def test_top_level_dbt_adapter_macros():
    project_path = Path("tests/fixtures/dbt/sushi_test")
    profile = Profile.load(DbtContext(project_path))

    helper = ManifestHelper(
        project_path,
        project_path,
        "sushi",
        profile.target,
        variable_overrides={"start": "2020-01-01"},
    )

    # Adapter macros must be marked as top-level
    dbt_macros = helper.macros("dbt")
    dbt_duckdb_macros = helper.macros("dbt_duckdb")
    assert dbt_macros["default__dateadd"].info.is_top_level
    assert dbt_macros["default__datediff"].info.is_top_level
    assert dbt_duckdb_macros["duckdb__datediff"].info.is_top_level
    assert dbt_duckdb_macros["duckdb__dateadd"].info.is_top_level

    # Project dispatch macros should not be marked as top-level
    customers_macros = helper.macros("customers")
    assert not customers_macros["default__current_engine"].info.is_top_level
    assert not customers_macros["duckdb__current_engine"].info.is_top_level
