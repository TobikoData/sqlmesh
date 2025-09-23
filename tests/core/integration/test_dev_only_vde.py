from __future__ import annotations

import typing as t
import pytest
from sqlmesh.core.model.common import ParsableSql
import time_machine

from sqlmesh.core import dialect as d
from sqlmesh.core.config.common import VirtualEnvironmentMode
from sqlmesh.core.model import (
    FullKind,
    IncrementalUnmanagedKind,
    SqlModel,
    ViewKind,
    load_sql_based_model,
)
from sqlmesh.core.plan import SnapshotIntervals
from sqlmesh.core.snapshot import (
    SnapshotChangeCategory,
)
from sqlmesh.utils.date import to_date, to_timestamp
from tests.core.integration.utils import add_projection_to_model

pytestmark = pytest.mark.slow


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )

    assert all(
        s.virtual_environment_mode.is_dev_only or not s.is_model or s.is_symbolic
        for s in context.snapshots.values()
    )

    # Init prod
    context.plan("prod", auto_apply=True, no_prompts=True)

    # Make a change in dev
    original_model = context.get_model("sushi.waiter_revenue_by_day")
    original_fingerprint = context.get_snapshot(original_model.name).fingerprint
    model = original_model.copy(
        update={
            "query_": ParsableSql(
                sql=original_model.query.order_by("waiter_id").sql(dialect=original_model.dialect)
            )
        }
    )
    model = add_projection_to_model(t.cast(SqlModel, model))
    context.upsert_model(model)

    plan_dev = context.plan_builder("dev").build()
    assert to_timestamp(plan_dev.start) == to_timestamp("2023-01-07")
    assert plan_dev.requires_backfill
    assert plan_dev.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=context.get_snapshot("sushi.top_waiters").snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot("sushi.waiter_revenue_by_day").snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
    ]
    assert plan_dev.context_diff.snapshots[context.get_snapshot(model.name).snapshot_id].intervals
    assert plan_dev.context_diff.snapshots[
        context.get_snapshot("sushi.top_waiters").snapshot_id
    ].intervals
    assert plan_dev.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].dev_intervals
    assert plan_dev.context_diff.snapshots[
        context.get_snapshot("sushi.top_waiters").snapshot_id
    ].dev_intervals
    context.apply(plan_dev)

    # Make sure the waiter_revenue_by_day model is a table in prod and a view in dev
    table_types_df = context.engine_adapter.fetchdf(
        "SELECT table_schema, table_type FROM INFORMATION_SCHEMA.TABLES WHERE table_name = 'waiter_revenue_by_day'"
    )
    assert table_types_df.to_dict("records") == [
        {"table_schema": "sushi", "table_type": "BASE TABLE"},
        {"table_schema": "sushi__dev", "table_type": "VIEW"},
    ]

    # Check that the specified dates were backfilled
    min_event_date = context.engine_adapter.fetchone(
        "SELECT MIN(event_date) FROM sushi__dev.waiter_revenue_by_day"
    )[0]
    assert min_event_date == to_date("2023-01-07")

    # Make sure the changes are applied without backfill in prod
    plan_prod = context.plan_builder("prod").build()
    assert not plan_prod.requires_backfill
    assert not plan_prod.missing_intervals
    context.apply(plan_prod)
    assert "one" in context.engine_adapter.columns("sushi.waiter_revenue_by_day")

    # Make sure the revert of a breaking changes results in a full rebuild
    context.upsert_model(original_model)
    assert context.get_snapshot(original_model.name).fingerprint == original_fingerprint

    plan_prod = context.plan_builder(
        "prod", allow_destructive_models=["sushi.waiter_revenue_by_day"]
    ).build()
    assert not plan_prod.requires_backfill
    assert not plan_prod.missing_intervals
    context.apply(plan_prod)
    assert "one" not in context.engine_adapter.columns("sushi.waiter_revenue_by_day")


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_kind_change(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.apply(plan)

    # Change to full kind
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": FullKind()})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.missing_intervals
    assert prod_plan.requires_backfill
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "table"

    # Change back to view
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": ViewKind()})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "view"

    # Change to incremental
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": IncrementalUnmanagedKind()})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "table"

    # Change back to full
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": FullKind()})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "table"


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_kind_change_incremental(
    init_and_plan_context: t.Callable,
):
    context, _ = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )

    forward_only_model_name = "memory.sushi.test_forward_only_model"
    forward_only_model_expressions = d.parse(
        f"""
        MODEL (
            name {forward_only_model_name},
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only true,
            ),
        );

        SELECT '2023-01-01' AS ds, 'value' AS value;
        """
    )
    forward_only_model = load_sql_based_model(forward_only_model_expressions)
    forward_only_model = forward_only_model.copy(
        update={"virtual_environment_mode": VirtualEnvironmentMode.DEV_ONLY}
    )
    context.upsert_model(forward_only_model)

    context.plan("prod", auto_apply=True, no_prompts=True)

    # Change to view
    model = context.get_model(forward_only_model_name)
    original_kind = model.kind
    model = model.copy(update={"kind": ViewKind()})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"test_forward_only_model"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "view"

    model = model.copy(update={"kind": original_kind})
    context.upsert_model(model)
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"test_forward_only_model"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "table"


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_kind_change_with_follow_up_changes_in_dev(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.apply(plan)

    # Make sure the initial state is a view
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "view"

    # Change to incremental unmanaged kind
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": IncrementalUnmanagedKind()})
    context.upsert_model(model)
    dev_plan = context.plan_builder("dev", skip_tests=True).build()
    assert dev_plan.missing_intervals
    assert dev_plan.requires_backfill
    context.apply(dev_plan)

    # Make a follow-up forward-only change
    model = add_projection_to_model(t.cast(SqlModel, model))
    context.upsert_model(model)
    dev_plan = context.plan_builder("dev", skip_tests=True, forward_only=True).build()
    context.apply(dev_plan)

    # Deploy to prod
    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals
    assert not prod_plan.context_diff.snapshots[
        context.get_snapshot(model.name).snapshot_id
    ].intervals
    context.apply(prod_plan)
    data_objects = context.engine_adapter.get_data_objects("sushi", {"top_waiters"})
    assert len(data_objects) == 1
    assert data_objects[0].type == "table"


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_kind_change_manual_categorization(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.apply(plan)

    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": FullKind()})
    context.upsert_model(model)
    dev_plan_builder = context.plan_builder("dev", skip_tests=True, no_auto_categorization=True)
    dev_plan_builder.set_choice(
        dev_plan_builder._context_diff.snapshots[context.get_snapshot(model.name).snapshot_id],
        SnapshotChangeCategory.NON_BREAKING,
    )
    dev_plan = dev_plan_builder.build()
    assert dev_plan.requires_backfill
    assert len(dev_plan.missing_intervals) == 1
    context.apply(dev_plan)

    prod_plan = context.plan_builder("prod", skip_tests=True).build()
    assert prod_plan.requires_backfill
    assert prod_plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=context.get_snapshot("sushi.top_waiters").snapshot_id,
            intervals=[
                (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
                (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
                (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_seed_model_change(
    init_and_plan_context: t.Callable,
):
    context, _ = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.load()
    context.plan("prod", auto_apply=True, no_prompts=True)

    seed_model = context.get_model("sushi.waiter_names")
    with open(seed_model.seed_path, "a") as fd:
        fd.write("\n123,New Test Name")

    context.load()
    seed_model_snapshot = context.get_snapshot("sushi.waiter_names")
    plan = context.plan_builder("dev").build()
    assert plan.directly_modified == {seed_model_snapshot.snapshot_id}
    assert len(plan.missing_intervals) == 2
    context.apply(plan)

    actual_seed_df_in_dev = context.fetchdf("SELECT * FROM sushi__dev.waiter_names WHERE id = 123")
    assert actual_seed_df_in_dev.to_dict("records") == [{"id": 123, "name": "New Test Name"}]
    actual_seed_df_in_prod = context.fetchdf("SELECT * FROM sushi.waiter_names WHERE id = 123")
    assert actual_seed_df_in_prod.empty

    plan = context.plan_builder("prod").build()
    assert plan.directly_modified == {seed_model_snapshot.snapshot_id}
    assert len(plan.missing_intervals) == 1
    assert plan.missing_intervals[0].snapshot_id == seed_model_snapshot.snapshot_id
    context.apply(plan)

    actual_seed_df_in_prod = context.fetchdf("SELECT * FROM sushi.waiter_names WHERE id = 123")
    assert actual_seed_df_in_prod.to_dict("records") == [{"id": 123, "name": "New Test Name"}]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_change_downstream_of_seed(
    init_and_plan_context: t.Callable,
):
    """This test covers a scenario when a model downstream of a seed model is modified and explicitly selected
    causing an (unhydrated) seed model to sourced from the state. If SQLMesh attempts to create
    a table for the unchanged seed model, it will fail because the seed model is not hydrated.
    """
    context, _ = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.load()
    context.plan("prod", auto_apply=True, no_prompts=True)

    # Make sure that a different version of the seed model is loaded
    seed_model = context.get_model("sushi.waiter_names")
    seed_model = seed_model.copy(update={"stamp": "force new version"})
    context.upsert_model(seed_model)

    # Make a change to the downstream model
    model = context.get_model("sushi.waiter_as_customer_by_day")
    model = model.copy(update={"stamp": "force new version"})
    context.upsert_model(model)

    # It is important to clear the cache so that the hydrated seed model is not sourced from the cache
    context.clear_caches()

    # Make sure to use the selector so that the seed model is sourced from the state
    plan = context.plan_builder("dev", select_models=[model.name]).build()
    assert len(plan.directly_modified) == 1
    assert list(plan.directly_modified)[0].name == model.fqn
    assert len(plan.missing_intervals) == 1
    assert plan.missing_intervals[0].snapshot_id.name == model.fqn

    # Make sure there's no error when applying the plan
    context.apply(plan)


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_model_change_standalone_audit(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.apply(plan)

    # Change a model upstream from a standalone audit
    model = context.get_model("sushi.items")
    model = model.copy(update={"stamp": "force new version"})
    context.upsert_model(model)

    plan = context.plan_builder("prod", skip_tests=True).build()

    # Make sure the standalone audit is among modified
    assert (
        context.get_snapshot("assert_item_price_above_zero").snapshot_id
        in plan.indirectly_modified[context.get_snapshot("sushi.items").snapshot_id]
    )

    # Make sure there's no error when applying the plan
    context.apply(plan)


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_virtual_environment_mode_dev_only_seed_model_change_schema(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context(
        "examples/sushi", config="test_config_virtual_environment_mode_dev_only"
    )
    context.apply(plan)

    new_csv = []
    with open(context.path / "seeds" / "waiter_names.csv", "r") as fd:
        is_header = True
        for idx, line in enumerate(fd):
            line = line.strip()
            if not line:
                continue
            if is_header:
                new_csv.append(line + ",new_column")
                is_header = False
            else:
                new_csv.append(line + f",v{idx}")

    with open(context.path / "seeds" / "waiter_names.csv", "w") as fd:
        fd.write("\n".join(new_csv))

    context.load()

    downstream_model = context.get_model("sushi.waiter_as_customer_by_day")
    downstream_model_kind = downstream_model.kind.dict()
    downstream_model_kwargs = {
        **downstream_model.dict(),
        "kind": {
            **downstream_model_kind,
            "on_destructive_change": "allow",
        },
        "audits": [],
        # Use the new column
        "query": "SELECT '2023-01-07' AS event_date, new_column AS new_column FROM sushi.waiter_names",
    }
    context.upsert_model(SqlModel.parse_obj(downstream_model_kwargs))

    context.plan("dev", auto_apply=True, no_prompts=True, skip_tests=True, enable_preview=True)

    assert (
        context.engine_adapter.fetchone(
            "SELECT COUNT(*) FROM sushi__dev.waiter_as_customer_by_day"
        )[0]
        == len(new_csv) - 1
    )

    # Deploy to prod
    context.clear_caches()
    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)
    assert "new_column" in context.engine_adapter.columns("sushi.waiter_as_customer_by_day")
