from __future__ import annotations

import typing as t
import pytest
from sqlmesh.core.model.common import ParsableSql
import time_machine

from sqlmesh.core.context import Context
from sqlmesh.core.model import (
    IncrementalUnmanagedKind,
)
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    SnapshotChangeCategory,
)

if t.TYPE_CHECKING:
    pass

pytestmark = pytest.mark.slow


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_select_star_is_directly_modified(sushi_test_dbt_context: Context):
    context = sushi_test_dbt_context

    model = context.get_model("sushi.simple_model_a")
    context.upsert_model(
        model,
        query_=ParsableSql(sql="SELECT 1 AS a, 2 AS b"),
    )

    snapshot_a_id = context.get_snapshot("sushi.simple_model_a").snapshot_id  # type: ignore
    snapshot_b_id = context.get_snapshot("sushi.simple_model_b").snapshot_id  # type: ignore

    plan = context.plan_builder("dev", skip_tests=True).build()
    assert plan.directly_modified == {snapshot_a_id, snapshot_b_id}
    assert {i.snapshot_id for i in plan.missing_intervals} == {snapshot_a_id, snapshot_b_id}

    assert plan.snapshots[snapshot_a_id].change_category == SnapshotChangeCategory.NON_BREAKING
    assert plan.snapshots[snapshot_b_id].change_category == SnapshotChangeCategory.NON_BREAKING


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_is_incremental_table_is_missing(sushi_test_dbt_context: Context):
    context = sushi_test_dbt_context

    model = context.get_model("sushi.waiter_revenue_by_day_v2")
    model = model.copy(update={"kind": IncrementalUnmanagedKind(), "start": "2023-01-01"})
    context.upsert_model(model)
    context._standalone_audits["sushi.test_top_waiters"].start = "2023-01-01"

    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)

    snapshot = context.get_snapshot("sushi.waiter_revenue_by_day_v2")
    assert snapshot

    # Manually drop the table
    context.engine_adapter.drop_table(snapshot.table_name())

    context.snapshot_evaluator.evaluate(
        snapshot,
        start="2023-01-01",
        end="2023-01-08",
        execution_time="2023-01-08 15:00:00",
        snapshots={s.name: s for s in context.snapshots.values()},
        deployability_index=DeployabilityIndex.all_deployable(),
    )

    # Make sure the table was recreated
    assert context.engine_adapter.table_exists(snapshot.table_name())


def test_model_attr(sushi_test_dbt_context: Context, assert_exp_eq):
    context = sushi_test_dbt_context
    model = context.get_model("sushi.top_waiters")
    assert_exp_eq(
        model.render_query(),
        """
        SELECT
          CAST("waiter_id" AS INT) AS "waiter_id",
          CAST("revenue" AS DOUBLE) AS "revenue",
          3 AS "model_columns"
        FROM "memory"."sushi"."waiter_revenue_by_day_v2" AS "waiter_revenue_by_day_v2"
        WHERE
          "ds" = (
             SELECT
               MAX("ds")
             FROM "memory"."sushi"."waiter_revenue_by_day_v2" AS "waiter_revenue_by_day_v2"
           )
        ORDER BY
          "revenue" DESC NULLS FIRST
        LIMIT 10
        """,
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_requirements(sushi_dbt_context: Context):
    assert set(sushi_dbt_context.requirements) == {"dbt-core", "dbt-duckdb"}
    assert sushi_dbt_context.requirements["dbt-core"].startswith("1.")
    assert sushi_dbt_context.requirements["dbt-duckdb"].startswith("1.")


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_dialect_with_normalization_strategy(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context(
        "tests/fixtures/dbt/sushi_test", config="test_config_with_normalization_strategy"
    )
    assert context.default_dialect == "duckdb,normalization_strategy=LOWERCASE"


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_before_all_with_var_ref_source(init_and_plan_context: t.Callable):
    _, plan = init_and_plan_context(
        "tests/fixtures/dbt/sushi_test", config="test_config_with_normalization_strategy"
    )
    environment_statements = plan.to_evaluatable().environment_statements
    assert environment_statements
    rendered_statements = [e.render_before_all(dialect="duckdb") for e in environment_statements]
    assert rendered_statements[0] == [
        "CREATE TABLE IF NOT EXISTS analytic_stats (physical_table TEXT, evaluation_time TEXT)",
        "CREATE TABLE IF NOT EXISTS to_be_executed_last (col TEXT)",
        "SELECT 1 AS var, 'items' AS src, 'waiters' AS ref",
    ]
