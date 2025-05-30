from __future__ import annotations

import typing as t
import json
from collections import Counter
from datetime import timedelta
from unittest import mock
from unittest.mock import patch

import os
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from sqlmesh.core.config.naming import NameInferenceConfig
from sqlmesh.utils.concurrency import NodeExecutionFailedError
import time_machine
from pytest_mock.plugin import MockerFixture
from sqlglot import exp
from sqlglot.expressions import DataType
import re
from IPython.utils.capture import capture_output


from sqlmesh import CustomMaterialization
from sqlmesh.cli.example_project import init_example_project
from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.config import (
    AutoCategorizationMode,
    Config,
    GatewayConfig,
    ModelDefaultsConfig,
    DuckDBConnectionConfig,
)
from sqlmesh.core.console import Console, get_console
from sqlmesh.core.context import Context
from sqlmesh.core.config.categorizer import CategorizerConfig
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.macros import macro
from sqlmesh.core.model import (
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    Model,
    ModelKind,
    ModelKindName,
    SqlModel,
    PythonModel,
    ViewKind,
    CustomKind,
    TimeColumn,
    load_sql_based_model,
)
from sqlmesh.core.model.kind import model_kind_type_from_name
from sqlmesh.core.plan import Plan, PlanBuilder, SnapshotIntervals
from sqlmesh.core.snapshot import (
    DeployabilityIndex,
    Snapshot,
    SnapshotChangeCategory,
    SnapshotId,
    SnapshotInfoLike,
    SnapshotTableInfo,
)
from sqlmesh.utils.date import TimeLike, now, to_date, to_datetime, to_timestamp
from sqlmesh.utils.errors import NoChangesPlanError, SQLMeshError, PlanError, ConfigError
from sqlmesh.utils.pydantic import validate_string
from tests.conftest import DuckDBMetadata, SushiDataValidator
from tests.utils.test_helpers import use_terminal_console
from tests.utils.test_filesystem import create_temp_file

if t.TYPE_CHECKING:
    from sqlmesh import QueryOrDF

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def mock_choices(mocker: MockerFixture):
    mocker.patch("sqlmesh.core.console.TerminalConsole._get_snapshot_change_category")
    mocker.patch("sqlmesh.core.console.TerminalConsole._prompt_backfill")


def plan_choice(plan_builder: PlanBuilder, choice: SnapshotChangeCategory) -> None:
    for snapshot in plan_builder.build().snapshots.values():
        if not snapshot.version:
            plan_builder.set_choice(snapshot, choice)


@time_machine.travel("2023-01-08 15:00:00 UTC")
@pytest.mark.parametrize(
    "context_fixture",
    ["sushi_context", "sushi_no_default_catalog"],
)
def test_forward_only_plan_with_effective_date(context_fixture: Context, request):
    context = request.getfixturevalue(context_fixture)
    model_name = "sushi.waiter_revenue_by_day"
    model = context.get_model(model_name)
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)), start="2023-01-01")
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan_builder = context.plan_builder("dev", skip_tests=True, forward_only=True)
    plan = plan_builder.build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert to_timestamp(plan.start) == to_timestamp("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
    ]

    plan = plan_builder.set_effective_from("2023-01-05").build()
    # Default start should be set to effective_from
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    plan = plan_builder.set_start("2023-01-06").build()
    # Start override should take precedence
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    plan = plan_builder.set_effective_from("2023-01-04").build()
    # Start should remain unchanged
    assert plan.start == "2023-01-06"
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert dev_df["event_date"].tolist() == [
        pd.to_datetime("2023-01-06"),
        pd.to_datetime("2023-01-07"),
    ]

    prod_plan = context.plan_builder(skip_tests=True).build()
    # Make sure that the previously set effective_from is respected
    assert prod_plan.start == to_timestamp("2023-01-04")
    assert prod_plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(prod_plan)

    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY event_date"
    )
    assert prod_df["event_date"].tolist() == [
        pd.to_datetime(x) for x in ["2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07"]
    ]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_forward_only_model_regular_plan(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.get_model(model_name)
    model = add_projection_to_model(t.cast(SqlModel, model))
    forward_only_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"kind": forward_only_kind})

    context.upsert_model(model)
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_datetime("2023-01-01")
    assert not plan.missing_intervals

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert not dev_df["event_date"].tolist()

    # Run a restatement plan to preview changes
    plan_builder = context.plan_builder(
        "dev", skip_tests=True, restate_models=[model_name], enable_preview=False
    )
    plan_builder.set_start("2023-01-06")
    assert plan_builder.build().missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    # Make sure that changed start is reflected in missing intervals
    plan_builder.set_start("2023-01-07")
    assert plan_builder.build().missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan_builder.build())

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert dev_df["event_date"].tolist() == [pd.to_datetime("2023-01-07")]

    # Promote changes to prod
    prod_plan = context.plan_builder(skip_tests=True).build()
    assert not prod_plan.missing_intervals

    context.apply(prod_plan)

    # The change was applied in a forward-only manner so no values in the new column should be populated
    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY event_date"
    )
    assert not prod_df["event_date"].tolist()


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_forward_only_model_regular_plan_preview_enabled(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.get_model(model_name)
    model = add_projection_to_model(t.cast(SqlModel, model))
    forward_only_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"kind": forward_only_kind})

    context.upsert_model(model)
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=True).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert to_timestamp(plan.start) == to_timestamp("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert dev_df["event_date"].tolist() == [pd.to_datetime("2023-01-07")]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_forward_only_model_restate_full_history_in_dev(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    model_name = "memory.sushi.customer_max_revenue"
    expressions = d.parse(
        f"""
        MODEL (
            name {model_name},
            kind INCREMENTAL_BY_UNIQUE_KEY (
                unique_key customer_id,
                forward_only true,
            ),
        );

        SELECT
          customer_id, MAX(revenue) AS max_revenue
        FROM memory.sushi.customer_revenue_lifetime
        GROUP BY 1;
        """
    )

    model = load_sql_based_model(expressions)
    assert model.forward_only
    assert model.kind.full_history_restatement_only
    context.upsert_model(model)

    context.plan("prod", skip_tests=True, auto_apply=True, enable_preview=False)

    model_kwargs = {
        **model.dict(),
        # Make a breaking change.
        "query": model.query.order_by("customer_id"),  # type: ignore
    }
    context.upsert_model(SqlModel.parse_obj(model_kwargs))

    # Apply the model change in dev
    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert not plan.missing_intervals
    context.apply(plan)

    snapshot = context.get_snapshot(model, raise_if_missing=True)
    snapshot_table_name = snapshot.table_name(False)

    # Manually insert a dummy value to check that the table is recreated during the restatement
    context.engine_adapter.insert_append(
        snapshot_table_name,
        pd.DataFrame({"customer_id": [-1], "max_revenue": [100]}),
    )
    df = context.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM sushi__dev.customer_max_revenue WHERE customer_id = -1"
    )
    assert df["cnt"][0] == 1

    # Apply a restatement plan in dev
    plan = context.plan("dev", restate_models=[model.name], auto_apply=True, enable_preview=False)
    assert len(plan.missing_intervals) == 1

    # Check that the dummy value is not present
    df = context.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM sushi__dev.customer_max_revenue WHERE customer_id = -1"
    )
    assert df["cnt"][0] == 0

    # Check that the table is not empty
    df = context.engine_adapter.fetchdf(
        "SELECT COUNT(*) AS cnt FROM sushi__dev.customer_max_revenue"
    )
    assert df["cnt"][0] > 0


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_full_history_restatement_model_regular_plan_preview_enabled(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model_name = "sushi.marketing"  # SCD2 model

    model = context.get_model(model_name)
    model = add_projection_to_model(t.cast(SqlModel, model))

    context.upsert_model(model)
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    customers_snapshot = context.get_snapshot("sushi.customers", raise_if_missing=True)
    active_customers_snapshot = context.get_snapshot(
        "sushi.active_customers", raise_if_missing=True
    )
    waiter_as_customer_snapshot = context.get_snapshot(
        "sushi.waiter_as_customer_by_day", raise_if_missing=True
    )
    count_customers_active_snapshot = context.get_snapshot(
        "sushi.count_customers_active", raise_if_missing=True
    )
    count_customers_inactive_snapshot = context.get_snapshot(
        "sushi.count_customers_inactive", raise_if_missing=True
    )

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=True).build()

    assert len(plan.new_snapshots) == 6
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[customers_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[active_customers_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[waiter_as_customer_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )

    assert to_timestamp(plan.start) == to_timestamp("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=active_customers_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=count_customers_active_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=count_customers_inactive_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=customers_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=waiter_as_customer_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan)


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_metadata_changed_regular_plan_preview_enabled(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.get_model(model_name)
    model = model.copy(update={"owner": "new_owner"})

    context.upsert_model(model)
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=True).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.METADATA
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.METADATA
    )
    assert not plan.missing_intervals
    assert not plan.restatements


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_hourly_model_with_lookback_no_backfill_in_dev(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")

    model_name = "sushi.waiter_revenue_by_day"

    model = context.get_model(model_name)
    model = SqlModel.parse_obj(
        {
            **model.dict(),
            "kind": model.kind.copy(update={"lookback": 1}),
            "cron": "@hourly",
            "audits": [],
        }
    )
    context.upsert_model(model)

    plan = context.plan_builder("prod", skip_tests=True).build()
    context.apply(plan)

    top_waiters_model = context.get_model("sushi.top_waiters")
    top_waiters_model = add_projection_to_model(t.cast(SqlModel, top_waiters_model), literal=True)
    context.upsert_model(top_waiters_model)

    context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    with time_machine.travel(now() + timedelta(hours=2)):
        plan = context.plan_builder("dev", skip_tests=True).build()
        # Make sure the waiter_revenue_by_day model is not backfilled.
        assert plan.missing_intervals == [
            SnapshotIntervals(
                snapshot_id=top_waiters_snapshot.snapshot_id,
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


@time_machine.travel("2023-01-08 00:00:00 UTC", tick=False)
def test_parent_cron_after_child(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.waiter_revenue_by_day")
    model = SqlModel.parse_obj(
        {
            **model.dict(),
            "cron": "50 23 * * *",
        }
    )
    context.upsert_model(model)

    plan = context.plan_builder("prod", skip_tests=True).build()
    context.apply(plan)

    waiter_revenue_by_day_snapshot = context.get_snapshot(model.name, raise_if_missing=True)
    assert waiter_revenue_by_day_snapshot.intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-07"))
    ]

    top_waiters_model = context.get_model("sushi.top_waiters")
    top_waiters_model = add_projection_to_model(t.cast(SqlModel, top_waiters_model), literal=True)
    context.upsert_model(top_waiters_model)

    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    with time_machine.travel("2023-01-08 23:55:00 UTC"):  # Past parent's cron, but before child's
        plan = context.plan_builder("dev", skip_tests=True).build()
        # Make sure the waiter_revenue_by_day model is not backfilled.
        assert plan.missing_intervals == [
            SnapshotIntervals(
                snapshot_id=top_waiters_snapshot.snapshot_id,
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


@time_machine.travel("2023-01-08 00:00:00 UTC")
@pytest.mark.parametrize(
    "forward_only, expected_intervals",
    [
        (
            False,
            [
                (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
                (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
                (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
            ],
        ),
        (
            True,
            [
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
            ],
        ),
    ],
)
def test_cron_not_aligned_with_day_boundary(
    init_and_plan_context: t.Callable,
    forward_only: bool,
    expected_intervals: t.List[t.Tuple[int, int]],
):
    context, plan = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.waiter_revenue_by_day")
    model = SqlModel.parse_obj(
        {
            **model.dict(),
            "kind": model.kind.copy(update={"forward_only": forward_only}),
            "cron": "0 12 * * *",
        }
    )
    context.upsert_model(model)

    plan = context.plan_builder("prod", skip_tests=True).build()
    context.apply(plan)

    waiter_revenue_by_day_snapshot = context.get_snapshot(model.name, raise_if_missing=True)
    assert waiter_revenue_by_day_snapshot.intervals == [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-07"))
    ]

    model = add_projection_to_model(t.cast(SqlModel, model), literal=True)
    context.upsert_model(model)

    waiter_revenue_by_day_snapshot = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )

    with time_machine.travel("2023-01-08 00:10:00 UTC"):  # Past model's cron.
        plan = context.plan_builder(
            "dev", select_models=[model.name], skip_tests=True, enable_preview=True
        ).build()
        assert plan.missing_intervals == [
            SnapshotIntervals(
                snapshot_id=waiter_revenue_by_day_snapshot.snapshot_id,
                intervals=expected_intervals,
            ),
        ]


@time_machine.travel("2023-01-08 00:00:00 UTC")
def test_cron_not_aligned_with_day_boundary_new_model(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    existing_model = context.get_model("sushi.waiter_revenue_by_day")
    existing_model = SqlModel.parse_obj(
        {
            **existing_model.dict(),
            "kind": existing_model.kind.copy(update={"forward_only": True}),
        }
    )
    context.upsert_model(existing_model)

    plan = context.plan_builder("prod", skip_tests=True).build()
    context.apply(plan)

    # Add a new model and make a change to a forward-only model.
    # The cron of the new model is not aligned with the day boundary.
    new_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name memory.sushi.new_model,
            kind FULL,
            cron '0 8 * * *',
            start '2023-01-01',
        );

        SELECT 1 AS one;
        """
        )
    )
    context.upsert_model(new_model)

    existing_model = add_projection_to_model(t.cast(SqlModel, existing_model), literal=True)
    context.upsert_model(existing_model)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=True).build()
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "memory.sushi.new_model", raise_if_missing=True
            ).snapshot_id,
            intervals=[(to_timestamp("2023-01-06"), to_timestamp("2023-01-07"))],
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "sushi.top_waiters", raise_if_missing=True
            ).snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "sushi.waiter_revenue_by_day", raise_if_missing=True
            ).snapshot_id,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]


@time_machine.travel("2023-01-08 00:00:00 UTC")
def test_forward_only_preview_child_that_runs_before_parent(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    # This model runs at minute 30 of every hour
    upstream_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name memory.sushi.upstream_model,
            kind FULL,
            cron '30 * * * *',
            start '2023-01-01',
        );

        SELECT 1 AS a;
        """
        )
    )
    context.upsert_model(upstream_model)

    # This model runs at minute 0 of every hour, so it runs before the upstream model
    downstream_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name memory.sushi.downstream_model,
            kind INCREMENTAL_BY_TIME_RANGE(
               time_column event_date,
               forward_only True,
            ),
            cron '0 * * * *',
            start '2023-01-01',
        );

        SELECT a, '2023-01-06' AS event_date FROM memory.sushi.upstream_model;
        """
        )
    )
    context.upsert_model(downstream_model)

    context.plan("prod", skip_tests=True, auto_apply=True)

    with time_machine.travel("2023-01-08 00:05:00 UTC"):
        # The downstream model runs but not the upstream model
        context.run("prod")

    # Now it's time for the upstream model to run but it hasn't run yet
    with time_machine.travel("2023-01-08 00:35:00 UTC"):
        # Make a change to the downstream model.
        downstream_model = add_projection_to_model(t.cast(SqlModel, downstream_model), literal=True)
        context.upsert_model(downstream_model)

        # The plan should only backfill the downstream model despite upstream missing intervals
        plan = context.plan_builder("dev", skip_tests=True, enable_preview=True).build()
        assert plan.missing_intervals == [
            SnapshotIntervals(
                snapshot_id=context.get_snapshot(
                    downstream_model.name, raise_if_missing=True
                ).snapshot_id,
                intervals=[
                    (to_timestamp("2023-01-07 23:00:00"), to_timestamp("2023-01-08 00:00:00"))
                ],
            ),
        ]


@time_machine.travel("2023-01-08 00:00:00 UTC")
def test_forward_only_monthly_model(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.waiter_revenue_by_day")
    model = SqlModel.parse_obj(
        {
            **model.dict(),
            "kind": model.kind.copy(update={"forward_only": True}),
            "cron": "0 0 1 * *",
            "start": "2022-01-01",
            "audits": [],
        }
    )
    context.upsert_model(model)

    plan = context.plan_builder("prod", skip_tests=True).build()
    context.apply(plan)

    waiter_revenue_by_day_snapshot = context.get_snapshot(model.name, raise_if_missing=True)
    assert waiter_revenue_by_day_snapshot.intervals == [
        (to_timestamp("2022-01-01"), to_timestamp("2023-01-01"))
    ]

    model = add_projection_to_model(t.cast(SqlModel, model), literal=True)
    context.upsert_model(model)

    waiter_revenue_by_day_snapshot = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )

    plan = context.plan_builder(
        "dev", select_models=[model.name], skip_tests=True, enable_preview=True
    ).build()
    assert to_timestamp(plan.start) == to_timestamp("2022-12-01")
    assert to_timestamp(plan.end) == to_timestamp("2023-01-08")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot.snapshot_id,
            intervals=[(to_timestamp("2022-12-01"), to_timestamp("2023-01-01"))],
        ),
    ]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_forward_only_parent_created_in_dev_child_created_in_prod(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    waiter_revenue_by_day_model = context.get_model("sushi.waiter_revenue_by_day")
    waiter_revenue_by_day_model = add_projection_to_model(
        t.cast(SqlModel, waiter_revenue_by_day_model)
    )
    forward_only_kind = waiter_revenue_by_day_model.kind.copy(update={"forward_only": True})
    waiter_revenue_by_day_model = waiter_revenue_by_day_model.copy(
        update={"kind": forward_only_kind}
    )
    context.upsert_model(waiter_revenue_by_day_model)

    waiter_revenue_by_day_snapshot = context.get_snapshot(
        waiter_revenue_by_day_model, raise_if_missing=True
    )
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[waiter_revenue_by_day_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_datetime("2023-01-01")
    assert not plan.missing_intervals

    context.apply(plan)

    # Update the child to refer to a newly added column.
    top_waiters_model = context.get_model("sushi.top_waiters")
    top_waiters_model = add_projection_to_model(t.cast(SqlModel, top_waiters_model), literal=False)
    context.upsert_model(top_waiters_model)

    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("prod", skip_tests=True, enable_preview=False).build()
    assert len(plan.new_snapshots) == 1
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )

    context.apply(plan)


@time_machine.travel("2023-01-08 00:00:00 UTC")
def test_new_forward_only_model(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    context.plan("dev", skip_tests=True, no_prompts=True, auto_apply=True, enable_preview=False)

    snapshot = context.get_snapshot("sushi.marketing")

    # The deployable table should not exist yet
    assert not context.engine_adapter.table_exists(snapshot.table_name())
    assert context.engine_adapter.table_exists(snapshot.table_name(is_deployable=False))

    context.plan("prod", skip_tests=True, no_prompts=True, auto_apply=True)

    assert context.engine_adapter.table_exists(snapshot.table_name())
    assert context.engine_adapter.table_exists(snapshot.table_name(is_deployable=False))


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_plan_set_choice_is_reflected_in_missing_intervals(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.get_model(model_name)
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))
    snapshot = context.get_snapshot(model, raise_if_missing=True)
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan_builder = context.plan_builder("dev", skip_tests=True)
    plan = plan_builder.build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
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

    # Change the category to BREAKING
    plan = plan_builder.set_choice(
        plan.context_diff.snapshots[snapshot.snapshot_id], SnapshotChangeCategory.BREAKING
    ).build()
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_BREAKING
    )
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
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

    # Change the category back to NON_BREAKING
    plan = plan_builder.set_choice(
        plan.context_diff.snapshots[snapshot.snapshot_id], SnapshotChangeCategory.NON_BREAKING
    ).build()
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=snapshot.snapshot_id,
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

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert dev_df["event_date"].tolist() == [
        pd.to_datetime(x)
        for x in [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
        ]
    ]

    # Promote changes to prod
    prod_plan = context.plan_builder(skip_tests=True).build()
    assert not prod_plan.missing_intervals

    context.apply(prod_plan)
    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY event_date"
    )
    assert prod_df["event_date"].tolist() == [
        pd.to_datetime(x)
        for x in [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
        ]
    ]


@time_machine.travel("2023-01-08 15:00:00 UTC", tick=True)
@pytest.mark.parametrize("has_view_binding", [False, True])
def test_non_breaking_change_after_forward_only_in_dev(
    init_and_plan_context: t.Callable, has_view_binding: bool
):
    context, plan = init_and_plan_context("examples/sushi")
    context.snapshot_evaluator.adapter.HAS_VIEW_BINDING = has_view_binding
    context.apply(plan)

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))
    waiter_revenue_by_day_snapshot = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, forward_only=True).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[waiter_revenue_by_day_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert to_timestamp(plan.start) == to_timestamp("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot.snapshot_id,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
    ]

    # Apply the forward-only changes first.
    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert dev_df["event_date"].tolist() == [pd.to_datetime("2023-01-07")]

    # Make a non-breaking change to a model downstream.
    model = context.get_model("sushi.top_waiters")
    # Select 'one' column from the updated upstream model.
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model), literal=False))
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True).build()
    assert len(plan.new_snapshots) == 1
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert to_timestamp(plan.start) == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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

    # Apply the non-breaking changes.
    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT waiter_id FROM sushi__dev.top_waiters WHERE one IS NOT NULL"
    )
    assert not dev_df.empty

    prod_df = context.engine_adapter.fetchdf("DESCRIBE sushi.top_waiters")
    assert "one" not in prod_df["column_name"].tolist()

    # Deploy both changes to prod.
    plan = context.plan_builder("prod", skip_tests=True).build()
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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

    context.apply(plan)

    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY event_date"
    )
    assert prod_df.empty

    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT waiter_id FROM sushi.top_waiters WHERE one IS NOT NULL"
    )
    assert prod_df.empty


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_indirect_non_breaking_change_after_forward_only_in_dev(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")
    # Make sure that the most downstream model is a materialized model.
    model = context.get_model("sushi.top_waiters")
    model = model.copy(update={"kind": FullKind()})
    context.upsert_model(model)
    context.plan("prod", skip_tests=True, auto_apply=True, no_prompts=True)

    # Make sushi.orders a forward-only model.
    model = context.get_model("sushi.orders")
    updated_model_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"stamp": "force new version", "kind": updated_model_kind})
    context.upsert_model(model)
    snapshot = context.get_snapshot(model, raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert (
        plan.context_diff.snapshots[snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert not plan.requires_backfill
    context.apply(plan)

    # Make a non-breaking change to a model.
    model = context.get_model("sushi.top_waiters")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert len(plan.new_snapshots) == 1
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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

    # Apply the non-breaking changes.
    context.apply(plan)

    # Make a non-breaking change upstream from the previously modified model.
    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))
    waiter_revenue_by_day_snapshot = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[waiter_revenue_by_day_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiters_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot.snapshot_id,
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

    # Apply the upstream non-breaking changes.
    context.apply(plan)
    assert not context.plan_builder("dev", skip_tests=True).build().requires_backfill

    # Deploy everything to prod.
    plan = context.plan_builder("prod", skip_tests=True, enable_preview=False).build()
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=top_waiters_snapshot.snapshot_id,
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
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot.snapshot_id,
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

    context.apply(plan)
    assert (
        not context.plan_builder("prod", skip_tests=True, enable_preview=False)
        .build()
        .requires_backfill
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_forward_only_precedence_over_indirect_non_breaking(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # Make sushi.orders a forward-only model.
    forward_only_model = context.get_model("sushi.orders")
    updated_model_kind = forward_only_model.kind.copy(update={"forward_only": True})
    forward_only_model = forward_only_model.copy(
        update={"stamp": "force new version", "kind": updated_model_kind}
    )
    context.upsert_model(forward_only_model)
    forward_only_snapshot = context.get_snapshot(forward_only_model, raise_if_missing=True)

    non_breaking_model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, non_breaking_model)))
    non_breaking_snapshot = context.get_snapshot(non_breaking_model, raise_if_missing=True)
    top_waiter_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan = context.plan_builder("dev", skip_tests=True, enable_preview=False).build()
    assert (
        plan.context_diff.snapshots[forward_only_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots[non_breaking_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiter_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=non_breaking_snapshot.snapshot_id,
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

    context.apply(plan)
    assert (
        not context.plan_builder("dev", skip_tests=True, enable_preview=False)
        .build()
        .requires_backfill
    )

    # Deploy everything to prod.
    plan = context.plan_builder("prod", skip_tests=True).build()
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=non_breaking_snapshot.snapshot_id,
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

    context.apply(plan)
    assert (
        not context.plan_builder("prod", skip_tests=True, enable_preview=False)
        .build()
        .requires_backfill
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_breaking_only_impacts_immediate_children(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    breaking_model = context.get_model("sushi.orders")
    breaking_model = breaking_model.copy(update={"stamp": "force new version"})
    context.upsert_model(breaking_model)
    breaking_snapshot = context.get_snapshot(breaking_model, raise_if_missing=True)

    non_breaking_model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, non_breaking_model)))
    non_breaking_snapshot = context.get_snapshot(non_breaking_model, raise_if_missing=True)
    top_waiter_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)

    plan_builder = context.plan_builder("dev", skip_tests=True, enable_preview=False)
    plan_builder.set_choice(breaking_snapshot, SnapshotChangeCategory.BREAKING)
    plan = plan_builder.build()
    assert (
        plan.context_diff.snapshots[breaking_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )
    assert (
        plan.context_diff.snapshots[non_breaking_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots[top_waiter_snapshot.snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert not any(i.snapshot_id == top_waiter_snapshot.snapshot_id for i in plan.missing_intervals)

    context.apply(plan)
    assert (
        not context.plan_builder("dev", skip_tests=True, enable_preview=False)
        .build()
        .requires_backfill
    )

    # Deploy everything to prod.
    plan = context.plan_builder("prod", skip_tests=True).build()
    assert not plan.missing_intervals

    context.apply(plan)
    assert (
        not context.plan_builder("prod", skip_tests=True, enable_preview=False)
        .build()
        .requires_backfill
    )


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
def test_plan_with_run(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    with time_machine.travel("2023-01-09 00:00:00 UTC"):
        plan = context.plan(run=True)
        assert plan.has_changes
        assert plan.missing_intervals

        context.apply(plan)

        snapshots = context.state_sync.state_sync.get_snapshots(context.snapshots.values())
        assert {s.name: s.intervals[0][1] for s in snapshots.values() if s.intervals} == {
            '"memory"."sushi"."waiter_revenue_by_day"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."order_items"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."orders"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."items"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."customer_revenue_lifetime"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."customer_revenue_by_day"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."latest_order"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."waiter_names"': to_timestamp("2023-01-08"),
            '"memory"."sushi"."raw_marketing"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."marketing"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."waiter_as_customer_by_day"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."top_waiters"': to_timestamp("2023-01-09"),
            '"memory"."raw"."demographics"': to_timestamp("2023-01-09"),
            "assert_item_price_above_zero": to_timestamp("2023-01-09"),
            '"memory"."sushi"."active_customers"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."customers"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."count_customers_active"': to_timestamp("2023-01-09"),
            '"memory"."sushi"."count_customers_inactive"': to_timestamp("2023-01-09"),
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
def test_select_models(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # Modify 2 models.
    model = context.get_model("sushi.waiter_revenue_by_day")
    kwargs = {
        **model.dict(),
        # Make a breaking change.
        "query": model.query.order_by("waiter_id"),  # type: ignore
    }
    context.upsert_model(SqlModel.parse_obj(kwargs))

    model = context.get_model("sushi.customer_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    expected_intervals = [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
        (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
    ]

    waiter_revenue_by_day_snapshot_id = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    ).snapshot_id

    # Select one of the modified models.
    plan_builder = context.plan_builder(
        "dev", select_models=["*waiter_revenue_by_day"], skip_tests=True
    )
    snapshot = plan_builder._context_diff.snapshots[waiter_revenue_by_day_snapshot_id]
    plan_builder.set_choice(snapshot, SnapshotChangeCategory.BREAKING)
    plan = plan_builder.build()

    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot_id,
            intervals=expected_intervals,
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert len(dev_df) == 7

    # Make sure that we only create a view for the selected model.
    schema_objects = context.engine_adapter.get_data_objects("sushi__dev")
    assert len(schema_objects) == 1
    assert schema_objects[0].name == "waiter_revenue_by_day"

    # Validate the other modified model.
    assert not context.get_snapshot("sushi.customer_revenue_by_day").change_category
    assert not context.get_snapshot("sushi.customer_revenue_by_day").version

    # Validate the downstream model.
    assert not context.engine_adapter.table_exists(
        context.get_snapshot("sushi.top_waiters").table_name()
    )
    assert not context.engine_adapter.table_exists(
        context.get_snapshot("sushi.top_waiters").table_name(False)
    )

    # Make sure that tables are created when deploying to prod.
    plan = context.plan("prod", skip_tests=True)
    context.apply(plan)
    assert context.engine_adapter.table_exists(
        context.get_snapshot("sushi.top_waiters").table_name()
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_select_unchanged_model_for_backfill(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # Modify 2 models.
    model = context.get_model("sushi.waiter_revenue_by_day")
    kwargs = {
        **model.dict(),
        # Make a breaking change.
        "query": d.parse_one(
            f"{model.query.sql(dialect='duckdb')} ORDER BY waiter_id", dialect="duckdb"
        ),
    }
    context.upsert_model(SqlModel.parse_obj(kwargs))

    model = context.get_model("sushi.customer_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    expected_intervals = [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
        (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
    ]

    waiter_revenue_by_day_snapshot_id = context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    ).snapshot_id

    # Select one of the modified models.
    plan_builder = context.plan_builder(
        "dev", select_models=["*waiter_revenue_by_day"], skip_tests=True
    )
    snapshot = plan_builder._context_diff.snapshots[waiter_revenue_by_day_snapshot_id]
    plan_builder.set_choice(snapshot, SnapshotChangeCategory.BREAKING)
    plan = plan_builder.build()

    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=waiter_revenue_by_day_snapshot_id,
            intervals=expected_intervals,
        ),
    ]

    context.apply(plan)

    # Make sure that we only create a view for the selected model.
    schema_objects = context.engine_adapter.get_data_objects("sushi__dev")
    assert {o.name for o in schema_objects} == {"waiter_revenue_by_day"}

    # Now select a model downstream from the previously modified one in order to backfill it.
    plan = context.plan_builder("dev", select_models=["*top_waiters"], skip_tests=True).build()

    assert not plan.has_changes
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "sushi.top_waiters", raise_if_missing=True
            ).snapshot_id,
            intervals=expected_intervals,
        ),
    ]

    context.apply(plan)

    # Make sure that a view has been created for the downstream selected model.
    schema_objects = context.engine_adapter.get_data_objects("sushi__dev")
    assert {o.name for o in schema_objects} == {"waiter_revenue_by_day", "top_waiters"}


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_max_interval_end_per_model_not_applied_when_end_is_provided(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    with time_machine.travel("2023-01-09 00:00:00 UTC"):
        context.run()

        plan = context.plan_builder(
            restate_models=["*"], start="2023-01-09", end="2023-01-09"
        ).build()
        context.apply(plan)


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_select_models_for_backfill(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    expected_intervals = [
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
    ]

    plan = context.plan_builder(
        "dev", backfill_models=["+*waiter_revenue_by_day"], skip_tests=True
    ).build()

    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=context.get_snapshot("sushi.items", raise_if_missing=True).snapshot_id,
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "sushi.order_items", raise_if_missing=True
            ).snapshot_id,
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot("sushi.orders", raise_if_missing=True).snapshot_id,
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_id=context.get_snapshot(
                "sushi.waiter_revenue_by_day", raise_if_missing=True
            ).snapshot_id,
            intervals=expected_intervals,
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT event_date FROM sushi__dev.waiter_revenue_by_day ORDER BY event_date"
    )
    assert len(dev_df) == 1

    schema_objects = context.engine_adapter.get_data_objects("sushi__dev")
    assert {o.name for o in schema_objects} == {
        "items",
        "order_items",
        "orders",
        "waiter_revenue_by_day",
    }

    assert not context.engine_adapter.table_exists(
        context.get_snapshot("sushi.customer_revenue_by_day").table_name()
    )

    # Make sure that tables are created when deploying to prod.
    plan = context.plan("prod")
    context.apply(plan)
    assert context.engine_adapter.table_exists(
        context.get_snapshot("sushi.customer_revenue_by_day").table_name()
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dbt_select_star_is_directly_modified(sushi_test_dbt_context: Context):
    context = sushi_test_dbt_context

    model = context.get_model("sushi.simple_model_a")
    context.upsert_model(
        model,
        query=d.parse_one("SELECT 1 AS a, 2 AS b"),
    )

    snapshot_a_id = context.get_snapshot("sushi.simple_model_a").snapshot_id  # type: ignore
    snapshot_b_id = context.get_snapshot("sushi.simple_model_b").snapshot_id  # type: ignore

    plan = context.plan_builder("dev", skip_tests=True).build()
    assert plan.directly_modified == {snapshot_a_id, snapshot_b_id}
    assert {i.snapshot_id for i in plan.missing_intervals} == {snapshot_a_id, snapshot_b_id}

    assert plan.snapshots[snapshot_a_id].change_category == SnapshotChangeCategory.NON_BREAKING
    assert plan.snapshots[snapshot_b_id].change_category == SnapshotChangeCategory.NON_BREAKING


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
def test_incremental_by_partition(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    source_name = "raw.test_incremental_by_partition"
    model_name = "memory.sushi.test_incremental_by_partition"

    expressions = d.parse(
        f"""
        MODEL (
            name {model_name},
            kind INCREMENTAL_BY_PARTITION (disable_restatement false),
            partitioned_by [key],
            allow_partials true,
            start '2023-01-07',
        );

        SELECT key, value FROM {source_name};
        """
    )
    model = load_sql_based_model(expressions)
    context.upsert_model(model)

    context.engine_adapter.ctas(
        source_name,
        d.parse_one("SELECT 'key_a' AS key, 1 AS value"),
    )

    context.plan(auto_apply=True, no_prompts=True)
    assert context.engine_adapter.fetchall(f"SELECT * FROM {model_name}") == [
        ("key_a", 1),
    ]

    context.engine_adapter.replace_query(
        source_name,
        d.parse_one("SELECT 'key_b' AS key, 1 AS value"),
    )
    context.run(ignore_cron=True)
    assert context.engine_adapter.fetchall(f"SELECT * FROM {model_name}") == [
        ("key_a", 1),
        ("key_b", 1),
    ]

    context.engine_adapter.replace_query(
        source_name,
        d.parse_one("SELECT 'key_a' AS key, 2 AS value"),
    )
    # Run 1 minute later.
    with time_machine.travel("2023-01-08 15:01:00 UTC"):
        context.run(ignore_cron=True)
    assert context.engine_adapter.fetchall(f"SELECT * FROM {model_name}") == [
        ("key_b", 1),
        ("key_a", 2),
    ]

    # model should fully refresh on restatement
    context.engine_adapter.replace_query(
        source_name,
        d.parse_one("SELECT 'key_c' AS key, 3 AS value"),
    )
    context.plan(auto_apply=True, no_prompts=True, restate_models=[model_name])
    assert context.engine_adapter.fetchall(f"SELECT * FROM {model_name}") == [
        ("key_c", 3),
    ]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_custom_materialization(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    custom_insert_called = False

    class CustomFullMaterialization(CustomMaterialization):
        NAME = "test_custom_full"

        def insert(
            self,
            table_name: str,
            query_or_df: QueryOrDF,
            model: Model,
            is_first_insert: bool,
            **kwargs: t.Any,
        ) -> None:
            nonlocal custom_insert_called
            custom_insert_called = True

            self._replace_query_for_model(model, table_name, query_or_df)

    model = context.get_model("sushi.top_waiters")
    kwargs = {
        **model.dict(),
        # Make a breaking change.
        "kind": dict(name="CUSTOM", materialization="test_custom_full"),
    }
    context.upsert_model(SqlModel.parse_obj(kwargs))

    context.plan(auto_apply=True, no_prompts=True)

    assert custom_insert_called


# needs to be defined at the top level. If its defined within the test body,
# adding to the snapshot cache fails with: AttributeError: Can't pickle local object
class TestCustomKind(CustomKind):
    __test__ = False  # prevent pytest warning since this isnt a class containing tests

    @property
    def custom_property(self) -> str:
        return validate_string(self.materialization_properties.get("custom_property"))


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_custom_materialization_with_custom_kind(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    custom_insert_calls = []

    class CustomFullMaterialization(CustomMaterialization[TestCustomKind]):
        NAME = "test_custom_full_with_custom_kind"

        def insert(
            self,
            table_name: str,
            query_or_df: QueryOrDF,
            model: Model,
            is_first_insert: bool,
            **kwargs: t.Any,
        ) -> None:
            assert isinstance(model.kind, TestCustomKind)

            nonlocal custom_insert_calls
            custom_insert_calls.append(model.kind.custom_property)

            self._replace_query_for_model(model, table_name, query_or_df)

    model = context.get_model("sushi.top_waiters")
    kwargs = {
        **model.dict(),
        # Make a breaking change.
        "kind": dict(
            name="CUSTOM",
            materialization="test_custom_full_with_custom_kind",
            materialization_properties={"custom_property": "pytest"},
        ),
    }
    context.upsert_model(SqlModel.parse_obj(kwargs))

    context.plan(auto_apply=True)

    assert custom_insert_calls == ["pytest"]

    # no changes
    context.plan(auto_apply=True)

    assert custom_insert_calls == ["pytest"]

    # change a property on the custom kind, breaking change
    kwargs["kind"]["materialization_properties"]["custom_property"] = "some value"
    context.upsert_model(SqlModel.parse_obj(kwargs))
    context.plan(auto_apply=True)

    assert custom_insert_calls == ["pytest", "some value"]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_indirect_non_breaking_view_model_non_representative_snapshot(
    init_and_plan_context: t.Callable,
):
    context, _ = init_and_plan_context("examples/sushi")

    # Forward-only parent
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
    assert forward_only_model.forward_only
    context.upsert_model(forward_only_model)

    # FULL downstream model.
    full_downstream_model_name = "memory.sushi.test_full_downstream_model"
    full_downstream_model_expressions = d.parse(
        f"""
        MODEL (
            name {full_downstream_model_name},
            kind FULL,
        );

        SELECT ds, value FROM {forward_only_model_name};
        """
    )
    full_downstream_model = load_sql_based_model(full_downstream_model_expressions)
    context.upsert_model(full_downstream_model)

    # VIEW downstream of the previous FULL model.
    view_downstream_model_name = "memory.sushi.test_view_downstream_model"
    view_downstream_model_expressions = d.parse(
        f"""
        MODEL (
            name {view_downstream_model_name},
            kind VIEW,
        );

        SELECT ds, value FROM {full_downstream_model_name};
        """
    )
    view_downstream_model = load_sql_based_model(view_downstream_model_expressions)
    context.upsert_model(view_downstream_model)

    # Apply the initial plan with all 3 models.
    context.plan(auto_apply=True, no_prompts=True)

    # Make a change to the forward-only model and apply it in dev.
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, forward_only_model)))
    forward_only_model_snapshot_id = context.get_snapshot(forward_only_model_name).snapshot_id
    full_downstream_model_snapshot_id = context.get_snapshot(full_downstream_model_name).snapshot_id
    full_downstream_model_2_snapshot_id = context.get_snapshot(
        view_downstream_model_name
    ).snapshot_id
    dev_plan = context.plan("dev", auto_apply=True, no_prompts=True, enable_preview=False)
    assert (
        dev_plan.snapshots[forward_only_model_snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        dev_plan.snapshots[full_downstream_model_snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        dev_plan.snapshots[full_downstream_model_2_snapshot_id].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert not dev_plan.missing_intervals

    # Make a follow-up breaking change to the downstream full model.
    new_full_downstream_model_expressions = d.parse(
        f"""
        MODEL (
            name {full_downstream_model_name},
            kind FULL,
        );

        SELECT ds, 'new_value' AS value FROM {forward_only_model_name};
        """
    )
    new_full_downstream_model = load_sql_based_model(new_full_downstream_model_expressions)
    context.upsert_model(new_full_downstream_model)
    full_downstream_model_snapshot_id = context.get_snapshot(full_downstream_model_name).snapshot_id
    full_downstream_model_2_snapshot_id = context.get_snapshot(
        view_downstream_model_name
    ).snapshot_id
    dev_plan = context.plan(
        "dev",
        categorizer_config=CategorizerConfig.all_full(),
        auto_apply=True,
        no_prompts=True,
        enable_preview=False,
    )
    assert (
        dev_plan.snapshots[full_downstream_model_snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )
    assert (
        dev_plan.snapshots[full_downstream_model_2_snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_BREAKING
    )
    assert len(dev_plan.missing_intervals) == 2
    assert dev_plan.missing_intervals[0].snapshot_id == full_downstream_model_snapshot_id
    assert dev_plan.missing_intervals[1].snapshot_id == full_downstream_model_2_snapshot_id

    # Check that the representative view hasn't been created yet.
    assert not context.engine_adapter.table_exists(
        context.get_snapshot(view_downstream_model_name).table_name()
    )

    # Now promote the very first change to prod without promoting the 2nd breaking change.
    context.upsert_model(full_downstream_model)
    context.plan(auto_apply=True, no_prompts=True, categorizer_config=CategorizerConfig.all_full())

    # Finally, make a non-breaking change to the full model in the same dev environment.
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, new_full_downstream_model)))
    full_downstream_model_snapshot_id = context.get_snapshot(full_downstream_model_name).snapshot_id
    full_downstream_model_2_snapshot_id = context.get_snapshot(
        view_downstream_model_name
    ).snapshot_id
    dev_plan = context.plan(
        "dev",
        categorizer_config=CategorizerConfig.all_full(),
        auto_apply=True,
        no_prompts=True,
        enable_preview=False,
    )
    assert (
        dev_plan.snapshots[full_downstream_model_snapshot_id].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        dev_plan.snapshots[full_downstream_model_2_snapshot_id].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )

    # Check that the representative view has been created.
    assert context.engine_adapter.table_exists(
        context.get_snapshot(view_downstream_model_name).table_name()
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_indirect_non_breaking_view_model_non_representative_snapshot_migration(
    init_and_plan_context: t.Callable,
):
    context, _ = init_and_plan_context("examples/sushi")

    forward_only_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.forward_only_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only TRUE,
                on_destructive_change 'allow',
            ),
        );

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    forward_only_model = load_sql_based_model(forward_only_model_expr)
    context.upsert_model(forward_only_model)

    downstream_view_a_expr = d.parse(
        """
        MODEL (
            name memory.sushi.downstream_view_a,
            kind VIEW,
        );

        SELECT a from memory.sushi.forward_only_model;
        """
    )
    downstream_view_a = load_sql_based_model(downstream_view_a_expr)
    context.upsert_model(downstream_view_a)

    downstream_view_b_expr = d.parse(
        """
        MODEL (
            name memory.sushi.downstream_view_b,
            kind VIEW,
        );

        SELECT a from memory.sushi.downstream_view_a;
        """
    )
    downstream_view_b = load_sql_based_model(downstream_view_b_expr)
    context.upsert_model(downstream_view_b)

    context.plan(auto_apply=True, no_prompts=True, skip_tests=True)

    # Make a forward-only change
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, forward_only_model)))
    # Make a non-breaking change downstream
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, downstream_view_a)))

    context.plan(auto_apply=True, no_prompts=True, skip_tests=True)

    # Make sure the downstrean indirect non-breaking view is available in prod
    count = context.engine_adapter.fetchone("SELECT COUNT(*) FROM memory.sushi.downstream_view_b")[
        0
    ]
    assert count > 0


@time_machine.travel("2023-01-08 15:00:00 UTC")
@pytest.mark.parametrize(
    "parent_a_category,parent_b_category,expected_child_category",
    [
        (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        ),
        (
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        ),
        (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        ),
        (
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        ),
        (
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.METADATA,
        ),
        (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.METADATA,
        ),
        (
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        ),
        (
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.NON_BREAKING,
            SnapshotChangeCategory.INDIRECT_NON_BREAKING,
        ),
        (
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.METADATA,
            SnapshotChangeCategory.METADATA,
        ),
        (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.INDIRECT_BREAKING,
        ),
        (
            SnapshotChangeCategory.BREAKING,
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.FORWARD_ONLY,
        ),
        (
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.FORWARD_ONLY,
            SnapshotChangeCategory.FORWARD_ONLY,
        ),
    ],
)
def test_rebase_two_changed_parents(
    init_and_plan_context: t.Callable,
    parent_a_category: SnapshotChangeCategory,  # This change is deployed to prod first
    parent_b_category: SnapshotChangeCategory,  # This change is deployed to prod second
    expected_child_category: SnapshotChangeCategory,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    initial_model_a = context.get_model("sushi.orders")
    initial_model_b = context.get_model("sushi.items")

    # Make change A and deploy it to dev_a
    context.upsert_model(initial_model_a.name, stamp="1")
    plan_builder = context.plan_builder("dev_a", skip_tests=True)
    plan_builder.set_choice(context.get_snapshot(initial_model_a.name), parent_a_category)
    context.apply(plan_builder.build())

    # Make change B and deploy it to dev_b
    context.upsert_model(initial_model_a)
    context.upsert_model(initial_model_b.name, stamp="1")
    plan_builder = context.plan_builder("dev_b", skip_tests=True)
    plan_builder.set_choice(context.get_snapshot(initial_model_b.name), parent_b_category)
    context.apply(plan_builder.build())

    # Deploy change A to prod
    context.upsert_model(initial_model_a.name, stamp="1")
    context.upsert_model(initial_model_b)
    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)

    # Apply change B in addition to A and plan against prod
    context.upsert_model(initial_model_b.name, stamp="1")
    plan = context.plan_builder("prod", skip_tests=True).build()

    # Validate the category of child snapshots
    direct_child_snapshot = plan.snapshots[context.get_snapshot("sushi.order_items").snapshot_id]
    assert direct_child_snapshot.change_category == expected_child_category

    indirect_child_snapshot = plan.snapshots[context.get_snapshot("sushi.top_waiters").snapshot_id]
    assert indirect_child_snapshot.change_category == expected_child_category


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_unaligned_start_snapshot_with_non_deployable_downstream(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    downstream_model_name = "memory.sushi.customer_max_revenue"

    expressions = d.parse(
        f"""
        MODEL (
            name {downstream_model_name},
            kind INCREMENTAL_BY_UNIQUE_KEY (
                unique_key customer_id,
                forward_only true,
            ),
        );

        SELECT
          customer_id, MAX(revenue) AS max_revenue
        FROM memory.sushi.customer_revenue_lifetime
        GROUP BY 1;
        """
    )

    downstream_model = load_sql_based_model(expressions)
    assert downstream_model.forward_only
    context.upsert_model(downstream_model)

    context.plan(auto_apply=True, no_prompts=True)

    customer_revenue_lifetime_model = context.get_model("sushi.customer_revenue_lifetime")
    kwargs = {
        **customer_revenue_lifetime_model.dict(),
        "name": "memory.sushi.customer_revenue_lifetime_new",
        "kind": dict(
            name="INCREMENTAL_UNMANAGED"
        ),  # Make it incremental unmanaged to ensure the depends_on_past behavior.
    }
    context.upsert_model(SqlModel.parse_obj(kwargs))
    context.upsert_model(
        downstream_model_name,
        query=d.parse_one(
            "SELECT customer_id, MAX(revenue) AS max_revenue FROM memory.sushi.customer_revenue_lifetime_new GROUP BY 1"
        ),
    )

    plan = context.plan_builder("dev", enable_preview=True).build()
    assert {s.name for s in plan.new_snapshots} == {
        '"memory"."sushi"."customer_revenue_lifetime_new"',
        '"memory"."sushi"."customer_max_revenue"',
    }
    for snapshot_interval in plan.missing_intervals:
        assert not plan.deployability_index.is_deployable(snapshot_interval.snapshot_id)
        assert snapshot_interval.intervals[0][0] == to_timestamp("2023-01-07")


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_restatement_plan_ignores_changes(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    restated_snapshot = context.get_snapshot("sushi.top_waiters")

    # Simulate a change.
    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    plan = context.plan_builder(restate_models=["sushi.top_waiters"]).build()
    assert plan.snapshots != context.snapshots

    assert not plan.directly_modified
    assert not plan.has_changes
    assert not plan.new_snapshots
    assert plan.requires_backfill
    assert plan.restatements == {
        restated_snapshot.snapshot_id: (to_timestamp("2023-01-01"), to_timestamp("2023-01-09"))
    }
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_id=restated_snapshot.snapshot_id,
            intervals=[
                (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
                (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
                (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        )
    ]

    context.apply(plan)


def test_restatement_plan_hourly_with_downstream_daily_restates_correct_intervals(tmp_path: Path):
    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@hourly'
    );

    select account_id, ts from test.external_table;
    """

    model_b = """
    MODEL (
        name test.b,
        kind FULL,
        cron '@daily'
    );

    select account_id, ts from test.a;
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    for path, defn in {"a.sql": model_a, "b.sql": model_b}.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply
    ctx.plan(auto_apply=True, no_prompts=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    # verify initial state
    for tbl in ["test.a", "test.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]

    # restate A
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-01 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01 01:00:00",
        end="2024-01-01 02:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    # verify result
    for tbl in ["test.a", "test.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ], f"Table {tbl} wasnt cleared"

    # Put some data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 01:30:00",
                "2024-01-01 23:30:00",
                "2024-01-02 03:30:00",
                "2024-01-03 12:30:00",
            ],
        }
    )
    engine_adapter.replace_query(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # Restate A across a day boundary with the expectation that two day intervals in B are affected
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01 02:00:00",
        end="2024-01-02 04:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    for tbl in ["test.a", "test.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",  # present already
            # "2024-01-01 02:30:00", #removed in last restatement
            "2024-01-01 23:30:00",  # added in last restatement
            "2024-01-02 03:30:00",  # added in last restatement
        ], f"Table {tbl} wasnt cleared"


def test_restatement_plan_respects_disable_restatements(tmp_path: Path):
    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01',
        cron '@daily'
    );

    select account_id, ts from test.external_table;
    """

    model_b = """
    MODEL (
        name test.b,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts",
            disable_restatement true,
        ),
        start '2024-01-01',
        cron '@daily'
    );

    select account_id, ts from test.a;
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    for path, defn in {"a.sql": model_a, "b.sql": model_b}.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply
    ctx.plan(auto_apply=True, no_prompts=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    def get_snapshot_intervals(snapshot_id):
        return list(ctx.state_sync.get_snapshots([snapshot_id]).values())[0].intervals

    # verify initial state
    for tbl in ["test.a", "test.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]

    # restate A and expect b to be ignored
    starting_b_intervals = get_snapshot_intervals(ctx.snapshots['"memory"."test"."b"'].snapshot_id)
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-01 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01",
        end="2024-01-02",
        auto_apply=True,
        no_prompts=True,
    )

    # verify A was changed and not b
    assert _dates_in_table("test.a") == [
        "2024-01-01 00:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]
    assert _dates_in_table("test.b") == [
        "2024-01-01 00:30:00",
        "2024-01-01 01:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]

    # Verify B intervals were not touched
    b_intervals = get_snapshot_intervals(ctx.snapshots['"memory"."test"."b"'].snapshot_id)
    assert starting_b_intervals == b_intervals


def test_restatement_plan_clears_correct_intervals_across_environments(tmp_path: Path):
    model1 = """
    MODEL (
        name test.incremental_model,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "date"
        ),
        start '2024-01-01',
        cron '@daily'
    );

    select account_id, date from test.external_table;
    """

    model2 = """
    MODEL (
        name test.downstream_of_incremental,
        kind FULL
    );

    select account_id, date from test.incremental_model;
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    with open(models_dir / "model1.sql", "w") as f:
        f.write(model1)

    with open(models_dir / "model2.sql", "w") as f:
        f.write(model2)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004, 1005],
            "name": ["foo", "bar", "baz", "bing", "bong"],
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "name": exp.DataType.build("varchar"),
        "date": exp.DataType.build("date"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # first, create the prod models
    ctx.plan(auto_apply=True, no_prompts=True)
    assert engine_adapter.fetchone("select count(*) from test.incremental_model") == (5,)
    assert engine_adapter.fetchone("select count(*) from test.downstream_of_incremental") == (5,)
    assert not engine_adapter.table_exists("test__dev.incremental_model")

    # then, make a dev version
    model1 = """
    MODEL (
        name test.incremental_model,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "date"
        ),
        start '2024-01-01',
        cron '@daily'
    );

    select account_id, name, date from test.external_table;
    """
    with open(models_dir / "model1.sql", "w") as f:
        f.write(model1)
    ctx.load()

    ctx.plan(environment="dev", auto_apply=True, no_prompts=True)
    assert engine_adapter.table_exists("test__dev.incremental_model")
    assert engine_adapter.fetchone("select count(*) from test__dev.incremental_model") == (5,)

    # drop some source data so when we restate the interval it essentially clears it which is easy to verify
    engine_adapter.execute("delete from test.external_table where date = '2024-01-01'")
    assert engine_adapter.fetchone("select count(*) from test.external_table") == (4,)

    # now, restate intervals in dev and verify prod is NOT affected
    ctx.plan(
        environment="dev",
        start="2024-01-01",
        end="2024-01-02",
        restate_models=["test.incremental_model"],
        auto_apply=True,
        no_prompts=True,
    )
    assert engine_adapter.fetchone("select count(*) from test.incremental_model") == (5,)
    assert engine_adapter.fetchone(
        "select count(*) from test.incremental_model where date = '2024-01-01'"
    ) == (1,)
    assert engine_adapter.fetchone("select count(*) from test__dev.incremental_model") == (4,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-01'"
    ) == (0,)

    # prod still should not be affected by a run because the restatement only happened in dev
    ctx.run()
    assert engine_adapter.fetchone("select count(*) from test.incremental_model") == (5,)
    assert engine_adapter.fetchone(
        "select count(*) from test.incremental_model where date = '2024-01-01'"
    ) == (1,)

    # drop another interval from the source data
    engine_adapter.execute("delete from test.external_table where date = '2024-01-02'")

    # now, restate intervals in prod and verify that dev IS affected
    ctx.plan(
        start="2024-01-01",
        end="2024-01-03",
        restate_models=["test.incremental_model"],
        auto_apply=True,
        no_prompts=True,
    )
    assert engine_adapter.fetchone("select count(*) from test.incremental_model") == (3,)
    assert engine_adapter.fetchone(
        "select count(*) from test.incremental_model where date = '2024-01-01'"
    ) == (0,)
    assert engine_adapter.fetchone(
        "select count(*) from test.incremental_model where date = '2024-01-02'"
    ) == (0,)
    assert engine_adapter.fetchone(
        "select count(*) from test.incremental_model where date = '2024-01-03'"
    ) == (1,)

    # dev not affected yet until `sqlmesh run` is run
    assert engine_adapter.fetchone("select count(*) from test__dev.incremental_model") == (4,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-01'"
    ) == (0,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-02'"
    ) == (1,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-03'"
    ) == (1,)

    # the restatement plan for prod should have cleared dev intervals too, which means this `sqlmesh run` re-runs 2024-01-01 and 2024-01-02
    ctx.run(environment="dev")
    assert engine_adapter.fetchone("select count(*) from test__dev.incremental_model") == (3,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-01'"
    ) == (0,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-02'"
    ) == (0,)
    assert engine_adapter.fetchone(
        "select count(*) from test__dev.incremental_model where date = '2024-01-03'"
    ) == (1,)

    # the downstream full model should always reflect whatever the incremental model is showing
    assert engine_adapter.fetchone("select count(*) from test.downstream_of_incremental") == (3,)
    assert engine_adapter.fetchone("select count(*) from test__dev.downstream_of_incremental") == (
        3,
    )


def test_prod_restatement_plan_clears_correct_intervals_in_derived_dev_tables(tmp_path: Path):
    """
    Scenario:
        I have models A[hourly] <- B[daily] <- C in prod
        I create dev and add 2 new models D and E so that my dev DAG looks like A <- B <- C <- D[daily] <- E
        I prod, I restate *one hour* of A
    Outcome:
        D and E should be restated in dev despite not being a part of prod
        since B and D are daily, the whole day should be restated even though only 1hr of the upstream model was restated
    """

    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@hourly'
    );

    select account_id, ts from test.external_table;
    """

    def _derived_full_model_def(name: str, upstream: str) -> str:
        return f"""
        MODEL (
            name test.{name},
            kind FULL
        );

        select account_id, ts from test.{upstream};
        """

    def _derived_incremental_model_def(name: str, upstream: str) -> str:
        return f"""
        MODEL (
            name test.{name},
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts
            ),
            cron '@daily'
        );

        select account_id, ts from test.{upstream} where ts between @start_ts and @end_ts;
        """

    model_b = _derived_incremental_model_def("b", upstream="a")
    model_c = _derived_full_model_def("c", upstream="b")

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    for path, defn in {"a.sql": model_a, "b.sql": model_b, "c.sql": model_c}.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply A, B, C in prod
    ctx.plan(auto_apply=True, no_prompts=True)

    # add D[daily], E in dev
    model_d = _derived_incremental_model_def("d", upstream="c")
    model_e = _derived_full_model_def("e", upstream="d")

    for path, defn in {
        "d.sql": model_d,
        "e.sql": model_e,
    }.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    # plan + apply dev
    ctx.load()
    ctx.plan(environment="dev", auto_apply=True, no_prompts=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    # verify initial state
    for tbl in ["test.a", "test.b", "test.c", "test__dev.d", "test__dev.e"]:
        assert engine_adapter.table_exists(tbl)
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]

    for tbl in ["test.d", "test.e"]:
        assert not engine_adapter.table_exists(tbl)

    # restate A in prod
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-01 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01 01:00:00",
        end="2024-01-01 02:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    # verify result
    for tbl in ["test.a", "test.b", "test.c"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ], f"Table {tbl} wasnt cleared"

    # dev shouldnt have been affected yet
    for tbl in ["test__dev.d", "test__dev.e"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ], f"Table {tbl} was prematurely cleared"

    # run dev to trigger the processing of the prod restatement
    ctx.run(environment="dev")

    # data should now be cleared from dev
    # note that D is a daily model, so clearing an hour interval from A should have triggered the full day in D
    for tbl in ["test__dev.d", "test__dev.e"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ], f"Table {tbl} wasnt cleared"


def test_prod_restatement_plan_clears_unaligned_intervals_in_derived_dev_tables(tmp_path: Path):
    """
    Scenario:
        I have a model A[hourly] in prod
        I create dev and add a model B[daily]
        I prod, I restate *one hour* of A

    Outcome:
        The whole day for B should be restated. The restatement plan for prod has no hints about B's cadence because
        B only exists in dev and there are no other downstream models in prod that would cause the restatement intervals
        to be widened.

        Therefore, this test checks that SQLMesh does the right thing when an interval is partially cleared
    """

    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@hourly'
    );

    select account_id, ts from test.external_table;
    """

    model_b = """
        MODEL (
            name test.b,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts
            ),
            cron '@daily'
        );

        select account_id, ts from test.a where ts between @start_ts and @end_ts;
        """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    with open(models_dir / "a.sql", "w") as f:
        f.write(model_a)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply A[hourly] in prod
    ctx.plan(auto_apply=True, no_prompts=True)

    # add B[daily] in dev
    with open(models_dir / "b.sql", "w") as f:
        f.write(model_b)

    # plan + apply dev
    ctx.load()
    ctx.plan(environment="dev", auto_apply=True, no_prompts=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    # verify initial state
    for tbl in ["test.a", "test__dev.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]

    # restate A in prod
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-01 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01 01:00:00",
        end="2024-01-01 02:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    # verify result
    assert _dates_in_table("test.a") == [
        "2024-01-01 00:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]

    # dev shouldnt have been affected yet
    assert _dates_in_table("test__dev.b") == [
        "2024-01-01 00:30:00",
        "2024-01-01 01:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]

    # mess with A independently of SQLMesh to prove a whole day gets restated for B instead of just 1hr
    snapshot_table_name = ctx.table_name("test.a", "dev")
    engine_adapter.execute(
        f"delete from {snapshot_table_name} where cast(ts as date) == '2024-01-01'"
    )
    engine_adapter.execute(
        f"insert into {snapshot_table_name} (account_id, ts) values (1007, '2024-01-02 01:30:00')"
    )

    assert _dates_in_table("test.a") == ["2024-01-02 00:30:00", "2024-01-02 01:30:00"]

    # run dev to trigger the processing of the prod restatement
    ctx.run(environment="dev")

    # B should now have no data for 2024-01-01
    # To prove a single day was restated vs the whole model, it also shouldnt have the '2024-01-02 01:30:00' record
    assert _dates_in_table("test__dev.b") == ["2024-01-02 00:30:00"]


def test_prod_restatement_plan_causes_dev_intervals_to_be_processed_in_next_dev_plan(
    tmp_path: Path,
):
    """
    Scenario:
        I have a model A[hourly] in prod
        I create dev and add a model B[daily]
        I prod, I restate *one hour* of A
        In dev, I run a normal plan instead of a cadence run

    Outcome:
        The whole day for B should be restated as part of a normal plan
    """

    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@hourly'
    );

    select account_id, ts from test.external_table;
    """

    model_b = """
        MODEL (
            name test.b,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts
            ),
            cron '@daily'
        );

        select account_id, ts from test.a where ts between @start_ts and @end_ts;
        """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    with open(models_dir / "a.sql", "w") as f:
        f.write(model_a)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply A[hourly] in prod
    ctx.plan(auto_apply=True, no_prompts=True)

    # add B[daily] in dev
    with open(models_dir / "b.sql", "w") as f:
        f.write(model_b)

    # plan + apply dev
    ctx.load()
    ctx.plan(environment="dev", auto_apply=True, no_prompts=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    # verify initial state
    for tbl in ["test.a", "test__dev.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 01:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]

    # restate A in prod
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-01 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-01 01:00:00",
        end="2024-01-01 02:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    # verify result
    assert _dates_in_table("test.a") == [
        "2024-01-01 00:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]

    # dev shouldnt have been affected yet
    assert _dates_in_table("test__dev.b") == [
        "2024-01-01 00:30:00",
        "2024-01-01 01:30:00",
        "2024-01-01 02:30:00",
        "2024-01-02 00:30:00",
    ]

    # plan dev which should trigger the missing intervals to get repopulated
    ctx.plan(environment="dev", auto_apply=True, no_prompts=True)

    # dev should have the restated data
    for tbl in ["test.a", "test__dev.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-01 02:30:00",
            "2024-01-02 00:30:00",
        ]


def test_prod_restatement_plan_causes_dev_intervals_to_be_widened_on_full_restatement_only_model(
    tmp_path,
):
    """
    Scenario:
        I have am INCREMENTAL_BY_TIME_RANGE model A[daily] in prod
        I create dev and add a INCREMENTAL_BY_UNIQUE_KEY model B (which supports full restatement only)
        I prod, I restate one day of A which should cause intervals in dev to be cleared (but not processed)
        In dev, I run a plan

    Outcome:
        In the dev plan, the entire model for B should be rebuilt because it does not support partial restatement
    """

    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@daily'
    );

    select account_id, ts from test.external_table where ts between @start_ts and @end_ts;
    """

    model_b = """
        MODEL (
            name test.b,
            kind INCREMENTAL_BY_UNIQUE_KEY (
                unique_key (account_id, ts)
            ),
            cron '@daily'
        );

        select account_id, ts from test.a where ts between @start_ts and @end_ts;
        """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    with open(models_dir / "a.sql", "w") as f:
        f.write(model_a)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-02 01:30:00",
                "2024-01-03 02:30:00",
                "2024-01-04 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply A[daily] in prod
    ctx.plan(auto_apply=True)

    # add B[daily] in dev
    with open(models_dir / "b.sql", "w") as f:
        f.write(model_b)

    # plan + apply dev
    ctx.load()
    ctx.plan(environment="dev", auto_apply=True)

    def _dates_in_table(table_name: str) -> t.List[str]:
        return [
            str(r[0]) for r in engine_adapter.fetchall(f"select ts from {table_name} order by ts")
        ]

    # verify initial state
    for tbl in ["test.a", "test__dev.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-02 01:30:00",
            "2024-01-03 02:30:00",
            "2024-01-04 00:30:00",
        ]

    # restate A in prod
    engine_adapter.execute("delete from test.external_table where ts = '2024-01-02 01:30:00'")
    ctx.plan(
        restate_models=["test.a"],
        start="2024-01-02 00:00:00",
        end="2024-01-03 00:00:00",
        auto_apply=True,
        no_prompts=True,
    )

    # verify result
    assert _dates_in_table("test.a") == [
        "2024-01-01 00:30:00",
        "2024-01-03 02:30:00",
        "2024-01-04 00:30:00",
    ]

    # dev shouldnt have been affected yet
    assert _dates_in_table("test__dev.b") == [
        "2024-01-01 00:30:00",
        "2024-01-02 01:30:00",
        "2024-01-03 02:30:00",
        "2024-01-04 00:30:00",
    ]

    # plan dev which should trigger the missing intervals to get repopulated
    ctx.plan(environment="dev", auto_apply=True)

    # dev should have fully refreshed
    # this is proven by the fact that INCREMENTAL_BY_UNIQUE_KEY cant propagate deletes, so if the
    # model was not fully rebuilt, the deleted record would still be present
    for tbl in ["test.a", "test__dev.b"]:
        assert _dates_in_table(tbl) == [
            "2024-01-01 00:30:00",
            "2024-01-03 02:30:00",
            "2024-01-04 00:30:00",
        ]


def test_prod_restatement_plan_missing_model_in_dev(
    tmp_path: Path,
):
    """
    Scenario:
        I have a model B in prod but only model A in dev
        I restate B in prod

    Outcome:
        The A model should be ignore and the plan shouldn't fail
    """

    model_a = """
    MODEL (
        name test.a,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column "ts"
        ),
        start '2024-01-01 00:00:00',
        cron '@hourly'
    );

    select account_id, ts from test.external_table;
    """

    model_b = """
        MODEL (
            name test.b,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts
            ),
            cron '@daily'
        );

        select account_id, ts from test.external_table where ts between @start_ts and @end_ts;
        """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    with open(models_dir / "a.sql", "w") as f:
        f.write(model_a)

    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    ctx = Context(paths=[tmp_path], config=config)

    engine_adapter = ctx.engine_adapter
    engine_adapter.create_schema("test")

    # source data
    df = pd.DataFrame(
        {
            "account_id": [1001, 1002, 1003, 1004],
            "ts": [
                "2024-01-01 00:30:00",
                "2024-01-01 01:30:00",
                "2024-01-01 02:30:00",
                "2024-01-02 00:30:00",
            ],
        }
    )
    columns_to_types = {
        "account_id": exp.DataType.build("int"),
        "ts": exp.DataType.build("timestamp"),
    }
    external_table = exp.table_(table="external_table", db="test", quoted=True)
    engine_adapter.create_table(table_name=external_table, columns_to_types=columns_to_types)
    engine_adapter.insert_append(
        table_name=external_table, query_or_df=df, columns_to_types=columns_to_types
    )

    # plan + apply A[hourly] in dev
    ctx.plan("dev", auto_apply=True, no_prompts=True)

    # add B[daily] in prod and remove A
    with open(models_dir / "b.sql", "w") as f:
        f.write(model_b)
    Path(models_dir / "a.sql").unlink()

    # plan + apply dev
    ctx.load()
    ctx.plan(auto_apply=True, no_prompts=True)

    # restate B in prod
    ctx.plan(
        restate_models=["test.b"],
        start="2024-01-01",
        end="2024-01-02",
        auto_apply=True,
        no_prompts=True,
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_dev_restatement_of_prod_model(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    context.plan("dev", auto_apply=True, no_prompts=True, skip_tests=True)

    restatement_plan = context.plan_builder("dev", restate_models=["*"]).build()
    assert set(restatement_plan.restatements) == {
        context.get_snapshot("sushi.waiter_revenue_by_day").snapshot_id,
        context.get_snapshot("sushi.top_waiters").snapshot_id,
    }


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_plan_snapshot_table_exists_for_promoted_snapshot(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    context.plan("dev", auto_apply=True, no_prompts=True, skip_tests=True)

    # Drop the views and make sure SQLMesh recreates them later
    top_waiters_snapshot = context.get_snapshot("sushi.top_waiters", raise_if_missing=True)
    context.engine_adapter.drop_view(top_waiters_snapshot.table_name())
    context.engine_adapter.drop_view(top_waiters_snapshot.table_name(False))

    # Make the environment unfinalized to force recreation of all views in the virtual layer
    context.state_sync.state_sync.engine_adapter.execute(
        "UPDATE sqlmesh._environments SET finalized_ts = NULL WHERE name = 'dev'"
    )

    model = context.get_model("sushi.customers")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    context.plan(
        "dev", select_models=["sushi.customers"], auto_apply=True, no_prompts=True, skip_tests=True
    )
    assert context.engine_adapter.table_exists(top_waiters_snapshot.table_name())


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_plan_against_expired_environment(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    modified_models = {model.fqn, context.get_model("sushi.top_waiters").fqn}

    plan = context.plan_builder("dev").build()
    assert plan.has_changes
    assert set(plan.context_diff.modified_snapshots) == modified_models
    assert plan.missing_intervals
    context.apply(plan)

    # Make sure there are no changes when comparing against the existing environment.
    plan = context.plan_builder("dev").build()
    assert not plan.has_changes
    assert not plan.context_diff.modified_snapshots
    assert not plan.missing_intervals

    # Invalidate the environment and make sure that the plan detects the changes.
    context.invalidate_environment("dev")
    plan = context.plan_builder("dev").build()
    assert plan.has_changes
    assert set(plan.context_diff.modified_snapshots) == modified_models
    assert not plan.missing_intervals
    context.apply(plan)


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_new_forward_only_model_concurrent_versions(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    new_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only TRUE,
                on_destructive_change 'allow',
            ),
        );

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    new_model = load_sql_based_model(new_model_expr)

    # Add the first version of the model and apply it to dev_a.
    context.upsert_model(new_model)
    snapshot_a = context.get_snapshot(new_model.name)
    plan_a = context.plan_builder("dev_a").build()
    snapshot_a = plan_a.snapshots[snapshot_a.snapshot_id]

    assert snapshot_a.snapshot_id in plan_a.context_diff.new_snapshots
    assert snapshot_a.snapshot_id in plan_a.context_diff.added
    assert snapshot_a.change_category == SnapshotChangeCategory.BREAKING

    context.apply(plan_a)

    new_model_alt_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only TRUE,
                on_destructive_change 'allow',
            ),
        );

        SELECT '2023-01-07' AS ds, 1 AS b;
        """
    )
    new_model_alt = load_sql_based_model(new_model_alt_expr)

    # Add the second version of the model but don't apply it yet
    context.upsert_model(new_model_alt)
    snapshot_b = context.get_snapshot(new_model_alt.name)
    plan_b = context.plan_builder("dev_b").build()
    snapshot_b = plan_b.snapshots[snapshot_b.snapshot_id]

    assert snapshot_b.snapshot_id in plan_b.context_diff.new_snapshots
    assert snapshot_b.snapshot_id in plan_b.context_diff.added
    assert snapshot_b.change_category == SnapshotChangeCategory.BREAKING

    assert snapshot_b.fingerprint != snapshot_a.fingerprint
    assert snapshot_b.version == snapshot_a.version

    # Apply the 1st version to prod
    context.upsert_model(new_model)
    plan_prod_a = context.plan_builder("prod").build()
    assert snapshot_a.snapshot_id in plan_prod_a.snapshots
    assert (
        plan_prod_a.snapshots[snapshot_a.snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )
    context.apply(plan_prod_a)

    df = context.fetchdf("SELECT * FROM memory.sushi.new_model")
    assert df.to_dict() == {"ds": {0: "2023-01-07"}, "a": {0: 1}}

    # Modify the 1st version in prod to trigger a forward-only change
    new_model = add_projection_to_model(t.cast(SqlModel, new_model))
    context.upsert_model(new_model)
    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)

    # Apply the 2nd version to dev_b.
    # At this point the snapshot of the 2nd version has already been categorized but not
    # persisted in the state. This means that when the snapshot of the 1st version was
    # being unpaused during promotion to prod, the state of the 2nd version snapshot was not updated
    context.apply(plan_b)

    # Apply the 2nd version to prod
    context.upsert_model(new_model_alt)
    plan_prod_b = context.plan_builder("prod").build()
    assert (
        plan_prod_b.snapshots[snapshot_b.snapshot_id].change_category
        == SnapshotChangeCategory.BREAKING
    )
    assert not plan_prod_b.requires_backfill
    context.apply(plan_prod_b)

    df = context.fetchdf("SELECT * FROM memory.sushi.new_model").replace({np.nan: None})
    assert df.to_dict() == {"ds": {0: "2023-01-07"}, "b": {0: None}}


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_new_forward_only_model_same_dev_environment(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    new_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only TRUE,
                on_destructive_change 'allow',
            ),
        );

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    new_model = load_sql_based_model(new_model_expr)

    # Add the first version of the model and apply it to dev.
    context.upsert_model(new_model)
    snapshot_a = context.get_snapshot(new_model.name)
    plan_a = context.plan_builder("dev").build()
    snapshot_a = plan_a.snapshots[snapshot_a.snapshot_id]

    assert snapshot_a.snapshot_id in plan_a.context_diff.new_snapshots
    assert snapshot_a.snapshot_id in plan_a.context_diff.added
    assert snapshot_a.change_category == SnapshotChangeCategory.BREAKING

    context.apply(plan_a)

    df = context.fetchdf("SELECT * FROM memory.sushi__dev.new_model")
    assert df.to_dict() == {"ds": {0: "2023-01-07"}, "a": {0: 1}}

    new_model_alt_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                forward_only TRUE,
                on_destructive_change 'allow',
            ),
        );

        SELECT '2023-01-07' AS ds, 1 AS b;
        """
    )
    new_model_alt = load_sql_based_model(new_model_alt_expr)

    # Add the second version of the model and apply it to the same environment.
    context.upsert_model(new_model_alt)
    snapshot_b = context.get_snapshot(new_model_alt.name)

    context.invalidate_environment("dev", sync=True)
    plan_b = context.plan_builder("dev").build()
    snapshot_b = plan_b.snapshots[snapshot_b.snapshot_id]

    context.apply(plan_b)

    df = context.fetchdf("SELECT * FROM memory.sushi__dev.new_model").replace({np.nan: None})
    assert df.to_dict() == {"ds": {0: "2023-01-07"}, "b": {0: 1}}


@time_machine.travel("2023-01-08 01:00:00 UTC")
def test_run_auto_restatement(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    context.engine_adapter.execute(
        "CREATE TABLE _test_auto_restatement_intervals (name STRING, start_ds STRING, end_ds STRING)"
    )

    @macro()
    def record_intervals(
        evaluator, name: exp.Expression, start: exp.Expression, end: exp.Expression, **kwargs: t.Any
    ) -> None:
        if evaluator.runtime_stage == "evaluating":
            evaluator.engine_adapter.insert_append(
                "_test_auto_restatement_intervals",
                pd.DataFrame({"name": [name.name], "start_ds": [start.name], "end_ds": [end.name]}),
            )

    new_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                auto_restatement_cron '0 6 * * 7',  -- At 6am every Sunday
                auto_restatement_intervals 3,
            ),
            start '2023-01-01',
        );

        @record_intervals('new_model', @start_ds, @end_ds);

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    new_model = load_sql_based_model(new_model_expr)
    context.upsert_model(new_model)

    new_model_downstream_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model_downstream,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
            ),
            cron '@hourly',
        );

        @record_intervals('new_model_downstream', @start_ts, @end_ts);

        SELECT * FROM memory.sushi.new_model;
        """
    )
    new_model_downstream = load_sql_based_model(new_model_downstream_expr)
    context.upsert_model(new_model_downstream)

    plan = context.plan_builder("prod").build()
    context.apply(plan)

    with time_machine.travel("2023-01-08 06:01:00 UTC"):
        assert context.run()

        recorded_intervals_df = context.engine_adapter.fetchdf(
            "SELECT start_ds, end_ds FROM _test_auto_restatement_intervals WHERE name = 'new_model'"
        )
        # The first interval is the first backfill and the second interval should be the 3 auto restated intervals
        assert recorded_intervals_df.to_dict() == {
            "start_ds": {0: "2023-01-01", 1: "2023-01-05"},
            "end_ds": {0: "2023-01-07", 1: "2023-01-07"},
        }
        recorded_intervals_downstream_df = context.engine_adapter.fetchdf(
            "SELECT start_ds, end_ds FROM _test_auto_restatement_intervals WHERE name = 'new_model_downstream'"
        )
        # The first interval is the first backfill, the second interval should be the 3 days of restated intervals, and
        # the third interval should catch up to the current hour
        assert recorded_intervals_downstream_df.to_dict() == {
            "start_ds": {
                0: "2023-01-01 00:00:00",
                1: "2023-01-05 00:00:00",
                2: "2023-01-08 01:00:00",
            },
            "end_ds": {
                0: "2023-01-08 00:59:59.999999",
                1: "2023-01-07 23:59:59.999999",
                2: "2023-01-08 05:59:59.999999",
            },
        }

        snapshot = context.get_snapshot(new_model.name)
        snapshot = context.state_sync.state_sync.get_snapshots([snapshot.snapshot_id])[
            snapshot.snapshot_id
        ]
        assert snapshot.next_auto_restatement_ts == to_timestamp("2023-01-15 06:00:00")
        assert not snapshot.pending_restatement_intervals

        snapshot_downstream = context.get_snapshot(new_model_downstream.name)
        snapshot_downstream = context.state_sync.state_sync.get_snapshots(
            [snapshot_downstream.snapshot_id]
        )[snapshot_downstream.snapshot_id]
        assert not snapshot_downstream.next_auto_restatement_ts
        assert not snapshot_downstream.pending_restatement_intervals


@time_machine.travel("2023-01-08 01:00:00 UTC")
def test_run_auto_restatement_plan_preview(init_and_plan_context: t.Callable):
    context, init_plan = init_and_plan_context("examples/sushi")
    context.apply(init_plan)

    new_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                auto_restatement_cron '0 6 * * 7',
            ),
            start '2023-01-01',
        );

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    new_model = load_sql_based_model(new_model_expr)
    context.upsert_model(new_model)
    snapshot = context.get_snapshot(new_model.name)

    plan_dev = context.plan_builder("dev").build()
    # Make sure that a limited preview is computed by default
    assert to_timestamp(plan_dev.start) == to_timestamp("2023-01-07")
    assert plan_dev.missing_intervals == [
        SnapshotIntervals(
            snapshot.snapshot_id,
            [(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        )
    ]
    assert not plan_dev.deployability_index.is_deployable(snapshot.snapshot_id)
    context.apply(plan_dev)

    plan_prod = context.plan_builder("prod").build()
    assert plan_prod.missing_intervals == [
        SnapshotIntervals(
            context.get_snapshot(new_model.name).snapshot_id,
            [
                (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
                (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
                (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        )
    ]
    context.apply(plan_prod)


@time_machine.travel("2023-01-08 01:00:00 UTC")
def test_run_auto_restatement_failure(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    @macro()
    def fail_auto_restatement(evaluator, start: exp.Expression, **kwargs: t.Any) -> None:
        if evaluator.runtime_stage == "evaluating" and start.name != "2023-01-01":
            raise Exception("Failed")

    new_model_expr = d.parse(
        """
        MODEL (
            name memory.sushi.new_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ds,
                auto_restatement_cron '0 6 * * 7',  -- At 6am every Sunday
                auto_restatement_intervals 3,
            ),
            start '2023-01-01',
        );

        @fail_auto_restatement(@start_ds);

        SELECT '2023-01-07' AS ds, 1 AS a;
        """
    )
    new_model = load_sql_based_model(new_model_expr)
    context.upsert_model(new_model)

    plan = context.plan_builder("prod").build()
    context.apply(plan)

    with time_machine.travel("2023-01-08 06:01:00 UTC"):
        run_status = context.run()
        assert run_status.is_failure

        snapshot = context.get_snapshot(new_model.name)
        snapshot = context.state_sync.state_sync.get_snapshots([snapshot.snapshot_id])[
            snapshot.snapshot_id
        ]
        assert snapshot.next_auto_restatement_ts == to_timestamp("2023-01-15 06:00:00")
        assert snapshot.pending_restatement_intervals == [
            (to_timestamp("2023-01-05"), to_timestamp("2023-01-08"))
        ]


def test_plan_twice_with_star_macro_yields_no_diff(tmp_path: Path):
    init_example_project(tmp_path, dialect="duckdb")

    star_model_definition = """
        MODEL (
          name sqlmesh_example.star_model,
          kind FULL
        );

        SELECT @STAR(sqlmesh_example.full_model) FROM sqlmesh_example.full_model
    """

    star_model_path = tmp_path / "models" / "star_model.sql"
    star_model_path.write_text(star_model_definition)

    db_path = str(tmp_path / "db.db")
    config = Config(
        gateways={"main": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path))},
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    context = Context(paths=tmp_path, config=config)
    context.plan(auto_apply=True, no_prompts=True)

    # Instantiate new context to remove caches etc
    new_context = Context(paths=tmp_path, config=config)

    star_model = new_context.get_model("sqlmesh_example.star_model")
    assert (
        star_model.render_query_or_raise().sql()
        == 'SELECT CAST("full_model"."item_id" AS INT) AS "item_id", CAST("full_model"."num_orders" AS BIGINT) AS "num_orders" FROM "db"."sqlmesh_example"."full_model" AS "full_model"'
    )

    new_plan = new_context.plan_builder().build()
    assert not new_plan.has_changes
    assert not new_plan.new_snapshots


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_create_environment_no_changes_with_selector(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    with pytest.raises(NoChangesPlanError):
        context.plan_builder("dev").build()

    plan = context.plan_builder("dev", select_models=["*top_waiters"]).build()
    assert not plan.missing_intervals
    context.apply(plan)

    schema_objects = context.engine_adapter.get_data_objects("sushi__dev")
    assert {o.name for o in schema_objects} == {"top_waiters"}


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_empty_backfill(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    plan = context.plan_builder("prod", skip_tests=True, empty_backfill=True).build()
    assert plan.missing_intervals
    assert plan.empty_backfill
    assert not plan.requires_backfill

    context.apply(plan)

    for model in context.models.values():
        if model.is_seed or model.kind.is_symbolic:
            continue
        row_num = context.engine_adapter.fetchone(f"SELECT COUNT(*) FROM {model.name}")[0]
        assert row_num == 0

    plan = context.plan_builder("prod", skip_tests=True).build()
    assert not plan.requires_backfill
    assert not plan.has_changes
    assert not plan.missing_intervals

    snapshots = plan.snapshots
    for snapshot in snapshots.values():
        if not snapshot.intervals:
            continue
        assert snapshot.intervals[-1][1] <= to_timestamp("2023-01-08")


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_empty_backfill_new_model(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    new_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name memory.sushi.new_model,
            kind FULL,
            cron '0 8 * * *',
            start '2023-01-01',
        );

        SELECT 1 AS one;
        """
        )
    )
    new_model_name = context.upsert_model(new_model).fqn

    with time_machine.travel("2023-01-09 00:00:00 UTC"):
        plan = context.plan_builder("dev", skip_tests=True, empty_backfill=True).build()
        assert plan.end == to_datetime("2023-01-09")
        assert plan.missing_intervals
        assert plan.empty_backfill
        assert not plan.requires_backfill

        context.apply(plan)

        for model in context.models.values():
            if model.is_seed or model.kind.is_symbolic:
                continue
            row_num = context.engine_adapter.fetchone(f"SELECT COUNT(*) FROM sushi__dev.new_model")[
                0
            ]
            assert row_num == 0

        plan = context.plan_builder("prod", skip_tests=True).build()
        assert not plan.requires_backfill
        assert not plan.missing_intervals

        snapshots = plan.snapshots
        for snapshot in snapshots.values():
            if not snapshot.intervals:
                continue
            elif snapshot.name == new_model_name:
                assert snapshot.intervals[-1][1] == to_timestamp("2023-01-09")
            else:
                assert snapshot.intervals[-1][1] <= to_timestamp("2023-01-08")


@time_machine.travel("2023-01-08 15:00:00 UTC")
@pytest.mark.parametrize("forward_only", [False, True])
def test_plan_repairs_unrenderable_snapshot_state(
    init_and_plan_context: t.Callable, forward_only: bool
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    target_snapshot = context.get_snapshot("sushi.waiter_revenue_by_day")
    assert target_snapshot

    # Manually corrupt the snapshot's query
    raw_snapshot = context.state_sync.state_sync.engine_adapter.fetchone(
        f"SELECT snapshot FROM sqlmesh._snapshots WHERE name = '{target_snapshot.name}' AND identifier = '{target_snapshot.identifier}'"
    )[0]  # type: ignore
    parsed_snapshot = json.loads(raw_snapshot)
    parsed_snapshot["node"]["query"] = "SELECT @missing_macro()"
    context.state_sync.state_sync.engine_adapter.update_table(
        "sqlmesh._snapshots",
        {"snapshot": json.dumps(parsed_snapshot)},
        f"name = '{target_snapshot.name}' AND identifier = '{target_snapshot.identifier}'",
    )

    with pytest.raises(Exception):
        context_copy = context.copy()
        context_copy.clear_caches()
        target_snapshot_in_state = context_copy.state_sync.get_snapshots(
            [target_snapshot.snapshot_id]
        )[target_snapshot.snapshot_id]
        target_snapshot_in_state.model.render_query_or_raise()

    # Repair the snapshot by creating a new version of it
    context.upsert_model(target_snapshot.model.name, stamp="repair")
    target_snapshot = context.get_snapshot(target_snapshot.name)

    plan_builder = context.plan_builder("prod", forward_only=forward_only)
    plan = plan_builder.build()
    assert plan.directly_modified == {target_snapshot.snapshot_id}
    if not forward_only:
        assert {i.snapshot_id for i in plan.missing_intervals} == {target_snapshot.snapshot_id}
        plan_builder.set_choice(target_snapshot, SnapshotChangeCategory.NON_BREAKING)
        plan = plan_builder.build()

    context.apply(plan)

    context.clear_caches()
    assert context.get_snapshot(target_snapshot.name).model.render_query_or_raise()
    target_snapshot_in_state = context.state_sync.get_snapshots([target_snapshot.snapshot_id])[
        target_snapshot.snapshot_id
    ]
    assert target_snapshot_in_state.model.render_query_or_raise()


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_no_backfill_for_model_downstream_of_metadata_change(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    # Make sushi.waiter_revenue_by_day a forward-only model.
    forward_only_model = context.get_model("sushi.waiter_revenue_by_day")
    updated_model_kind = forward_only_model.kind.copy(update={"forward_only": True})
    forward_only_model = forward_only_model.copy(update={"kind": updated_model_kind})
    context.upsert_model(forward_only_model)

    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)

    # Make a metadata change upstream of the forward-only model.
    context.upsert_model("sushi.orders", owner="new_owner")

    plan = context.plan_builder("test_dev").build()
    assert plan.has_changes
    assert not plan.directly_modified
    assert not plan.indirectly_modified
    assert not plan.missing_intervals
    assert all(
        snapshot.change_category == SnapshotChangeCategory.METADATA
        for snapshot in plan.new_snapshots
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_evaluate_uncategorized_snapshot(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # Add a new projection
    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    # Downstream model references the new projection
    downstream_model = context.get_model("sushi.top_waiters")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, downstream_model), literal=False))

    df = context.evaluate(
        "sushi.top_waiters", start="2023-01-05", end="2023-01-06", execution_time=now()
    )
    assert set(df["one"].tolist()) == {1}


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_table_name(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    snapshot = context.get_snapshot("sushi.waiter_revenue_by_day")
    assert snapshot
    assert (
        context.table_name("sushi.waiter_revenue_by_day", "prod")
        == f"memory.sqlmesh__sushi.sushi__waiter_revenue_by_day__{snapshot.version}"
    )

    with pytest.raises(SQLMeshError, match="Environment 'dev' was not found."):
        context.table_name("sushi.waiter_revenue_by_day", "dev")

    with pytest.raises(
        SQLMeshError, match="Model 'sushi.missing' was not found in environment 'prod'."
    ):
        context.table_name("sushi.missing", "prod")

    # Add a new projection
    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    context.plan("dev_a", auto_apply=True, no_prompts=True, skip_tests=True)

    new_snapshot = context.get_snapshot("sushi.waiter_revenue_by_day")
    assert new_snapshot.version != snapshot.version

    assert (
        context.table_name("sushi.waiter_revenue_by_day", "dev_a")
        == f"memory.sqlmesh__sushi.sushi__waiter_revenue_by_day__{new_snapshot.version}"
    )

    # Make a forward-only change
    context.upsert_model(model, stamp="forward_only")

    context.plan("dev_b", auto_apply=True, no_prompts=True, skip_tests=True, forward_only=True)

    forward_only_snapshot = context.get_snapshot("sushi.waiter_revenue_by_day")
    assert forward_only_snapshot.version == snapshot.version
    assert forward_only_snapshot.dev_version != snapshot.version

    assert (
        context.table_name("sushi.waiter_revenue_by_day", "dev_b")
        == f"memory.sqlmesh__sushi.sushi__waiter_revenue_by_day__{forward_only_snapshot.dev_version}__dev"
    )

    assert (
        context.table_name("sushi.waiter_revenue_by_day", "dev_b", prod=True)
        == f"memory.sqlmesh__sushi.sushi__waiter_revenue_by_day__{snapshot.version}"
    )


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_full_model_change_with_plan_start_not_matching_model_start(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    model = context.get_model("sushi.top_waiters")
    context.upsert_model(model, kind=model_kind_type_from_name("FULL")())  # type: ignore

    # Apply the change with --skip-backfill first and no plan start
    context.plan("dev", skip_tests=True, skip_backfill=True, no_prompts=True, auto_apply=True)

    # Apply the plan again but this time don't skip backfill and set start
    # to be later than the model start
    context.plan("dev", skip_tests=True, no_prompts=True, auto_apply=True, start="1 day ago")

    # Check that the number of rows is not 0
    row_num = context.engine_adapter.fetchone(f"SELECT COUNT(*) FROM sushi__dev.top_waiters")[0]
    assert row_num > 0


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_indirect_non_breaking_view_is_updated_with_new_table_references(
    init_and_plan_context: t.Callable,
):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    # Add a new projection to the base model
    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)

    # Run the janitor to delete the old snapshot record
    context.run_janitor(ignore_ttl=True)

    # Check the downstream view and make sure it's still queryable
    assert context.get_model("sushi.top_waiters").kind.is_view
    row_num = context.engine_adapter.fetchone(f"SELECT COUNT(*) FROM sushi.top_waiters")[0]
    assert row_num > 0


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


@pytest.mark.parametrize(
    "context_fixture",
    ["sushi_context", "sushi_dbt_context", "sushi_test_dbt_context", "sushi_no_default_catalog"],
)
def test_model_add(context_fixture: Context, request):
    initial_add(request.getfixturevalue(context_fixture), "dev")


def test_model_removed(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)

    top_waiters_snapshot_id = sushi_context.get_snapshot(
        "sushi.top_waiters", raise_if_missing=True
    ).snapshot_id

    sushi_context._models.pop('"memory"."sushi"."top_waiters"')

    def _validate_plan(context, plan):
        validate_plan_changes(plan, removed=[top_waiters_snapshot_id])
        assert not plan.missing_intervals

    def _validate_apply(context):
        assert not sushi_context.get_snapshot("sushi.top_waiters", raise_if_missing=False)
        assert sushi_context.state_reader.get_snapshots([top_waiters_snapshot_id])
        env = sushi_context.state_reader.get_environment(environment)
        assert env
        assert all(snapshot.name != '"memory"."sushi"."top_waiters"' for snapshot in env.snapshots)

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


def test_non_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING, False)


def test_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.BREAKING, False)


def test_forward_only(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.FORWARD_ONLY, False)


def test_logical_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    previous_sushi_items_version = sushi_context.get_snapshot(
        "sushi.items", raise_if_missing=True
    ).version

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING)

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.FLOAT,
        DataType.Type.DOUBLE,
    )
    apply_to_environment(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING)

    assert (
        sushi_context.get_snapshot("sushi.items", raise_if_missing=True).version
        == previous_sushi_items_version
    )


def validate_query_change(
    context: Context,
    environment: str,
    change_category: SnapshotChangeCategory,
    logical: bool,
):
    versions = snapshots_to_versions(context.snapshots.values())

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )

    directly_modified = ['"memory"."sushi"."items"']
    indirectly_modified = [
        '"memory"."sushi"."order_items"',
        '"memory"."sushi"."waiter_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_lifetime"',
        '"memory"."sushi"."top_waiters"',
        "assert_item_price_above_zero",
    ]
    not_modified = [
        snapshot.name
        for snapshot in context.snapshots.values()
        if snapshot.name not in directly_modified and snapshot.name not in indirectly_modified
    ]

    if change_category == SnapshotChangeCategory.BREAKING and not logical:
        models_same = not_modified
        models_different = directly_modified + indirectly_modified
    elif change_category == SnapshotChangeCategory.FORWARD_ONLY:
        models_same = not_modified + directly_modified + indirectly_modified
        models_different = []
    else:
        models_same = not_modified + indirectly_modified
        models_different = directly_modified

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert bool(plan.missing_intervals) != logical

    def _validate_apply(context):
        current_versions = snapshots_to_versions(context.snapshots.values())
        validate_versions_same(models_same, versions, current_versions)
        validate_versions_different(models_different, versions, current_versions)

    apply_to_environment(
        context,
        environment,
        change_category,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


@pytest.mark.parametrize(
    "from_, to",
    [
        (ModelKindName.INCREMENTAL_BY_TIME_RANGE, ModelKindName.FULL),
        (ModelKindName.FULL, ModelKindName.INCREMENTAL_BY_TIME_RANGE),
    ],
)
def test_model_kind_change(from_: ModelKindName, to: ModelKindName, sushi_context: Context):
    environment = f"test_model_kind_change__{from_.value.lower()}__{to.value.lower()}"
    incremental_snapshot = sushi_context.get_snapshot("sushi.items", raise_if_missing=True).copy()

    if from_ != ModelKindName.INCREMENTAL_BY_TIME_RANGE:
        change_model_kind(sushi_context, from_)
        apply_to_environment(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING)

    if to == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
        sushi_context.upsert_model(incremental_snapshot.model)
    else:
        change_model_kind(sushi_context, to)

    logical = to in (ModelKindName.INCREMENTAL_BY_TIME_RANGE, ModelKindName.EMBEDDED)
    validate_model_kind_change(to, sushi_context, environment, logical=logical)


def change_model_kind(context: Context, kind: ModelKindName):
    if kind in (ModelKindName.VIEW, ModelKindName.EMBEDDED, ModelKindName.FULL):
        context.upsert_model(
            "sushi.items",
            partitioned_by=[],
        )
    context.upsert_model("sushi.items", kind=model_kind_type_from_name(kind)())  # type: ignore


def validate_model_kind_change(
    kind_name: ModelKindName,
    context: Context,
    environment: str,
    *,
    logical: bool,
):
    directly_modified = ['"memory"."sushi"."items"']
    indirectly_modified = [
        '"memory"."sushi"."order_items"',
        '"memory"."sushi"."waiter_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_lifetime"',
        '"memory"."sushi"."top_waiters"',
        "assert_item_price_above_zero",
    ]
    if kind_name == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
        kind: ModelKind = IncrementalByTimeRangeKind(time_column=TimeColumn(column="event_date"))
    elif kind_name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
        kind = IncrementalByUniqueKeyKind(unique_key="id")
    else:
        kind = model_kind_type_from_name(kind_name)()  # type: ignore

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert (
            next(
                snapshot
                for snapshot in plan.snapshots.values()
                if snapshot.name == '"memory"."sushi"."items"'
            ).model.kind.name
            == kind.name
        )
        assert bool(plan.missing_intervals) != logical

    apply_to_environment(
        context,
        environment,
        SnapshotChangeCategory.NON_BREAKING,
        plan_validators=[_validate_plan],
    )


def test_environment_isolation(sushi_context: Context):
    prod_snapshots = sushi_context.snapshots.values()

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    directly_modified = ['"memory"."sushi"."items"']
    indirectly_modified = [
        '"memory"."sushi"."order_items"',
        '"memory"."sushi"."waiter_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_by_day"',
        '"memory"."sushi"."customer_revenue_lifetime"',
        '"memory"."sushi"."top_waiters"',
        "assert_item_price_above_zero",
    ]

    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    # Verify prod unchanged
    validate_apply_basics(sushi_context, "prod", prod_snapshots)

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert not plan.missing_intervals

    apply_to_environment(
        sushi_context,
        "prod",
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
    )


def test_environment_promotion(sushi_context: Context):
    initial_add(sushi_context, "dev")

    # Simulate prod "ahead"
    change_data_type(sushi_context, "sushi.items", DataType.Type.DOUBLE, DataType.Type.FLOAT)
    apply_to_environment(sushi_context, "prod", SnapshotChangeCategory.BREAKING)

    # Simulate rebase
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    # Make changes in dev
    change_data_type(sushi_context, "sushi.items", DataType.Type.FLOAT, DataType.Type.DECIMAL)
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.NON_BREAKING)

    change_data_type(sushi_context, "sushi.top_waiters", DataType.Type.DOUBLE, DataType.Type.INT)
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.BREAKING)

    change_data_type(
        sushi_context,
        "sushi.customer_revenue_by_day",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(
        sushi_context,
        "dev",
        SnapshotChangeCategory.FORWARD_ONLY,
        allow_destructive_models=['"memory"."sushi"."customer_revenue_by_day"'],
    )

    # Promote to prod
    def _validate_plan(context, plan):
        sushi_items_snapshot = context.get_snapshot("sushi.items", raise_if_missing=True)
        sushi_top_waiters_snapshot = context.get_snapshot(
            "sushi.top_waiters", raise_if_missing=True
        )
        sushi_customer_revenue_by_day_snapshot = context.get_snapshot(
            "sushi.customer_revenue_by_day", raise_if_missing=True
        )

        assert (
            plan.context_diff.modified_snapshots[sushi_items_snapshot.name][0].change_category
            == SnapshotChangeCategory.NON_BREAKING
        )
        assert (
            plan.context_diff.modified_snapshots[sushi_top_waiters_snapshot.name][0].change_category
            == SnapshotChangeCategory.BREAKING
        )
        assert (
            plan.context_diff.modified_snapshots[sushi_customer_revenue_by_day_snapshot.name][
                0
            ].change_category
            == SnapshotChangeCategory.FORWARD_ONLY
        )

    apply_to_environment(
        sushi_context,
        "prod",
        SnapshotChangeCategory.NON_BREAKING,
        plan_validators=[_validate_plan],
        allow_destructive_models=['"memory"."sushi"."customer_revenue_by_day"'],
    )


def test_no_override(sushi_context: Context) -> None:
    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )

    change_data_type(
        sushi_context,
        "sushi.order_items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )

    plan_builder = sushi_context.plan_builder("prod")
    plan = plan_builder.build()

    sushi_items_snapshot = sushi_context.get_snapshot("sushi.items", raise_if_missing=True)
    sushi_order_items_snapshot = sushi_context.get_snapshot(
        "sushi.order_items", raise_if_missing=True
    )
    sushi_water_revenue_by_day_snapshot = sushi_context.get_snapshot(
        "sushi.waiter_revenue_by_day", raise_if_missing=True
    )

    items = plan.context_diff.snapshots[sushi_items_snapshot.snapshot_id]
    order_items = plan.context_diff.snapshots[sushi_order_items_snapshot.snapshot_id]
    waiter_revenue = plan.context_diff.snapshots[sushi_water_revenue_by_day_snapshot.snapshot_id]

    plan_builder.set_choice(items, SnapshotChangeCategory.BREAKING).set_choice(
        order_items, SnapshotChangeCategory.NON_BREAKING
    )
    plan_builder.build()
    assert items.is_new_version
    assert waiter_revenue.is_new_version
    plan_builder.set_choice(items, SnapshotChangeCategory.NON_BREAKING)
    plan_builder.build()
    assert not waiter_revenue.is_new_version


@pytest.mark.parametrize(
    "change_categories, expected",
    [
        ([SnapshotChangeCategory.NON_BREAKING], SnapshotChangeCategory.BREAKING),
        ([SnapshotChangeCategory.BREAKING], SnapshotChangeCategory.BREAKING),
        (
            [SnapshotChangeCategory.NON_BREAKING, SnapshotChangeCategory.NON_BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.NON_BREAKING, SnapshotChangeCategory.BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.NON_BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
        (
            [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.BREAKING],
            SnapshotChangeCategory.BREAKING,
        ),
    ],
)
def test_revert(
    sushi_context: Context,
    change_categories: t.List[SnapshotChangeCategory],
    expected: SnapshotChangeCategory,
):
    environment = "prod"
    original_snapshot_id = sushi_context.get_snapshot("sushi.items", raise_if_missing=True)

    types = (DataType.Type.DOUBLE, DataType.Type.FLOAT, DataType.Type.DECIMAL)
    assert len(change_categories) < len(types)

    for i, category in enumerate(change_categories):
        change_data_type(sushi_context, "sushi.items", *types[i : i + 2])
        apply_to_environment(sushi_context, environment, category)
        assert (
            sushi_context.get_snapshot("sushi.items", raise_if_missing=True) != original_snapshot_id
        )

    change_data_type(sushi_context, "sushi.items", types[len(change_categories)], types[0])

    def _validate_plan(_, plan):
        snapshot = next(s for s in plan.snapshots.values() if s.name == '"memory"."sushi"."items"')
        assert snapshot.change_category == expected
        assert not plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        change_categories[-1],
        plan_validators=[_validate_plan],
    )
    assert sushi_context.get_snapshot("sushi.items", raise_if_missing=True) == original_snapshot_id


def test_revert_after_downstream_change(sushi_context: Context):
    environment = "prod"
    change_data_type(sushi_context, "sushi.items", DataType.Type.DOUBLE, DataType.Type.FLOAT)
    apply_to_environment(sushi_context, environment, SnapshotChangeCategory.BREAKING)

    change_data_type(
        sushi_context,
        "sushi.waiter_revenue_by_day",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    apply_to_environment(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING)

    change_data_type(sushi_context, "sushi.items", DataType.Type.FLOAT, DataType.Type.DOUBLE)

    def _validate_plan(_, plan):
        snapshot = next(s for s in plan.snapshots.values() if s.name == '"memory"."sushi"."items"')
        assert snapshot.change_category == SnapshotChangeCategory.BREAKING
        assert plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
    )


def test_auto_categorization(sushi_context: Context):
    environment = "dev"
    for config in sushi_context.configs.values():
        config.plan.auto_categorize_changes.sql = AutoCategorizationMode.FULL
    initial_add(sushi_context, environment)

    version = sushi_context.get_snapshot(
        "sushi.waiter_as_customer_by_day", raise_if_missing=True
    ).version
    fingerprint = sushi_context.get_snapshot(
        "sushi.waiter_as_customer_by_day", raise_if_missing=True
    ).fingerprint

    model = t.cast(SqlModel, sushi_context.get_model("sushi.customers", raise_if_missing=True))
    sushi_context.upsert_model("sushi.customers", query=model.query.select("'foo' AS foo"))  # type: ignore
    apply_to_environment(sushi_context, environment)

    assert (
        sushi_context.get_snapshot(
            "sushi.waiter_as_customer_by_day", raise_if_missing=True
        ).change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert (
        sushi_context.get_snapshot(
            "sushi.waiter_as_customer_by_day", raise_if_missing=True
        ).fingerprint
        != fingerprint
    )
    assert (
        sushi_context.get_snapshot("sushi.waiter_as_customer_by_day", raise_if_missing=True).version
        == version
    )


@use_terminal_console
def test_multi(mocker):
    context = Context(paths=["examples/multi/repo_1", "examples/multi/repo_2"], gateway="memory")

    with patch.object(get_console(), "log_warning") as mock_logger:
        context.plan_builder(environment="dev")
        warnings = mock_logger.call_args[0][0]
        repo1_path, repo2_path = context.configs.keys()
        assert f"Linter warnings for {repo1_path}" in warnings
        assert f"Linter warnings for {repo2_path}" not in warnings

    assert (
        context.render("bronze.a").sql()
        == '''SELECT 1 AS "col_a", 'b' AS "col_b", 1 AS "one", 'repo_1' AS "dup"'''
    )
    assert (
        context.render("silver.d").sql()
        == '''SELECT "c"."col_a" AS "col_a", 2 AS "two", 'repo_2' AS "dup" FROM "memory"."silver"."c" AS "c"'''
    )
    context._new_state_sync().reset(default_catalog=context.default_catalog)
    plan = context.plan_builder().build()
    assert len(plan.new_snapshots) == 5
    context.apply(plan)

    # Ensure before_all, after_all statements for multiple repos have executed
    environment_statements = context.state_reader.get_environment_statements(c.PROD)
    assert len(environment_statements) == 2
    assert context.fetchdf("select * from before_1").to_dict()["1"][0] == 1
    assert context.fetchdf("select * from before_2").to_dict()["2"][0] == 2
    assert context.fetchdf("select * from after_1").to_dict()["repo_1"][0] == "repo_1"
    assert context.fetchdf("select * from after_2").to_dict()["repo_2"][0] == "repo_2"

    context = Context(
        paths=["examples/multi/repo_1"],
        state_sync=context.state_sync,
        gateway="memory",
    )

    model = context.get_model("bronze.a")
    assert model.project == "repo_1"
    context.upsert_model(model.copy(update={"query": model.query.select("'c' AS c")}))
    plan = context.plan_builder().build()

    assert set(snapshot.name for snapshot in plan.directly_modified) == {
        '"memory"."bronze"."a"',
        '"memory"."bronze"."b"',
        '"memory"."silver"."e"',
    }
    assert sorted([x.name for x in list(plan.indirectly_modified.values())[0]]) == [
        '"memory"."silver"."c"',
        '"memory"."silver"."d"',
    ]
    assert len(plan.missing_intervals) == 3
    context.apply(plan)
    validate_apply_basics(context, c.PROD, plan.snapshots.values())

    # Ensure only repo_1's environment statements have executed in this context
    environment_statements = context.state_reader.get_environment_statements(c.PROD)
    assert len(environment_statements) == 1
    assert environment_statements[0].before_all == [
        "CREATE TABLE IF NOT EXISTS before_1 AS select @one()"
    ]
    assert environment_statements[0].after_all == [
        "CREATE TABLE IF NOT EXISTS after_1 AS select @dup()"
    ]


@use_terminal_console
def test_multi_virtual_layer(copy_to_temp_path):
    paths = copy_to_temp_path("tests/fixtures/multi_virtual_layer")
    path = Path(paths[0])
    first_db_path = str(path / "db_1.db")
    second_db_path = str(path / "db_2.db")

    config = Config(
        gateways={
            "first": GatewayConfig(
                connection=DuckDBConnectionConfig(database=first_db_path),
                variables={"overriden_var": "gateway_1"},
            ),
            "second": GatewayConfig(
                connection=DuckDBConnectionConfig(database=second_db_path),
                variables={"overriden_var": "gateway_2"},
            ),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        model_naming=NameInferenceConfig(infer_names=True),
        default_gateway="first",
        gateway_managed_virtual_layer=True,
        variables={"overriden_var": "global", "global_one": 88},
    )

    context = Context(paths=paths, config=config)
    assert context.default_catalog_per_gateway == {"first": "db_1", "second": "db_2"}
    assert len(context.engine_adapters) == 2

    # For the model without gateway the default should be used and the gateway variable should overide the global
    assert (
        context.render("first_schema.model_one").sql()
        == 'SELECT \'gateway_1\' AS "item_id", 88 AS "global_one", 1 AS "macro_one"'
    )

    # For model with gateway specified the appropriate variable should be used to overide
    assert (
        context.render("db_2.second_schema.model_one").sql()
        == 'SELECT \'gateway_2\' AS "item_id", 88 AS "global_one", 1 AS "macro_one"'
    )

    plan = context.plan_builder().build()
    assert len(plan.new_snapshots) == 4
    context.apply(plan)

    # Validate the tables that source from the first tables are correct as well with evaluate
    assert (
        context.evaluate(
            "first_schema.model_two", start=now(), end=now(), execution_time=now()
        ).to_string()
        == "     item_id  global_one\n0  gateway_1          88"
    )
    assert (
        context.evaluate(
            "db_2.second_schema.model_two", start=now(), end=now(), execution_time=now()
        ).to_string()
        == "     item_id  global_one\n0  gateway_2          88"
    )

    assert sorted(set(snapshot.name for snapshot in plan.directly_modified)) == [
        '"db_1"."first_schema"."model_one"',
        '"db_1"."first_schema"."model_two"',
        '"db_2"."second_schema"."model_one"',
        '"db_2"."second_schema"."model_two"',
    ]

    model = context.get_model("db_1.first_schema.model_one")

    context.upsert_model(model.copy(update={"query": model.query.select("'c' AS extra")}))
    plan = context.plan_builder().build()
    context.apply(plan)

    state_environments = context.state_reader.get_environments()
    state_snapshots = context.state_reader.get_snapshots(context.snapshots.values())

    assert state_environments[0].gateway_managed
    assert len(state_snapshots) == len(state_environments[0].snapshots)
    assert [snapshot.name for snapshot in plan.directly_modified] == [
        '"db_1"."first_schema"."model_one"'
    ]
    assert [x.name for x in list(plan.indirectly_modified.values())[0]] == [
        '"db_1"."first_schema"."model_two"'
    ]

    assert len(plan.missing_intervals) == 1
    assert (
        context.evaluate(
            "db_1.first_schema.model_one", start=now(), end=now(), execution_time=now()
        ).to_string()
        == "     item_id  global_one  macro_one extra\n0  gateway_1          88          1     c"
    )

    # Create dev environment with changed models
    model = context.get_model("db_2.second_schema.model_one")
    context.upsert_model(model.copy(update={"query": model.query.select("'d' AS extra")}))
    model = context.get_model("first_schema.model_two")
    context.upsert_model(model.copy(update={"query": model.query.select("'d2' AS col")}))
    plan = context.plan_builder("dev").build()
    context.apply(plan)

    dev_environment = context.state_sync.get_environment("dev")
    assert dev_environment is not None

    metadata_engine_1 = DuckDBMetadata.from_context(context)
    start_schemas_1 = set(metadata_engine_1.schemas)
    assert sorted(start_schemas_1) == sorted(
        {"first_schema__dev", "sqlmesh", "first_schema", "sqlmesh__first_schema"}
    )

    metadata_engine_2 = DuckDBMetadata(context._get_engine_adapter("second"))
    start_schemas_2 = set(metadata_engine_2.schemas)
    assert sorted(start_schemas_2) == sorted(
        {"sqlmesh__second_schema", "second_schema", "second_schema__dev"}
    )

    # Invalidate dev environment
    context.invalidate_environment("dev")
    invalidate_environment = context.state_sync.get_environment("dev")
    assert invalidate_environment is not None
    assert invalidate_environment.expiration_ts < dev_environment.expiration_ts  # type: ignore
    assert sorted(start_schemas_1) == sorted(set(metadata_engine_1.schemas))
    assert sorted(start_schemas_2) == sorted(set(metadata_engine_2.schemas))

    # Run janitor
    context._run_janitor()
    assert context.state_sync.get_environment("dev") is None
    removed_schemas = start_schemas_1 - set(metadata_engine_1.schemas)
    assert removed_schemas == {"first_schema__dev"}
    removed_schemas = start_schemas_2 - set(metadata_engine_2.schemas)
    assert removed_schemas == {"second_schema__dev"}
    prod_environment = context.state_sync.get_environment("prod")

    # Remove the second gateway's second model and apply plan
    second_model = path / "models/second_schema/model_two.sql"
    os.remove(second_model)
    assert not second_model.exists()
    context = Context(paths=paths, config=config)
    plan = context.plan_builder().build()
    context.apply(plan)
    prod_environment = context.state_sync.get_environment("prod")
    assert len(prod_environment.snapshots_) == 3

    # Changing the flag should show a diff
    context.config.gateway_managed_virtual_layer = False
    plan = context.plan_builder().build()
    assert not plan.requires_backfill
    assert (
        plan.context_diff.previous_gateway_managed_virtual_layer
        != plan.context_diff.gateway_managed_virtual_layer
    )
    assert plan.context_diff.has_changes

    # This should error since the default_gateway won't have access to create the view on a non-shared catalog
    with pytest.raises(NodeExecutionFailedError, match=r"Execution failed for node SnapshotId*"):
        context.apply(plan)


def test_multi_dbt(mocker):
    context = Context(paths=["examples/multi_dbt/bronze", "examples/multi_dbt/silver"])
    context._new_state_sync().reset(default_catalog=context.default_catalog)
    plan = context.plan_builder().build()
    assert len(plan.new_snapshots) == 4
    context.apply(plan)
    validate_apply_basics(context, c.PROD, plan.snapshots.values())

    environment_statements = context.state_sync.get_environment_statements(c.PROD)
    assert len(environment_statements) == 2
    bronze_statements = environment_statements[0]
    assert bronze_statements.before_all == [
        "JINJA_STATEMENT_BEGIN;\nCREATE TABLE IF NOT EXISTS analytic_stats (physical_table VARCHAR, evaluation_time VARCHAR);\nJINJA_END;"
    ]
    assert not bronze_statements.after_all
    silver_statements = environment_statements[1]
    assert not silver_statements.before_all
    assert silver_statements.after_all == [
        "JINJA_STATEMENT_BEGIN;\n{{ store_schemas(schemas) }}\nJINJA_END;"
    ]
    assert "store_schemas" in silver_statements.jinja_macros.root_macros
    analytics_table = context.fetchdf("select * from analytic_stats;")
    assert sorted(analytics_table.columns) == sorted(["physical_table", "evaluation_time"])
    schema_table = context.fetchdf("select * from schema_table;")
    assert sorted(schema_table.all_schemas[0]) == sorted(["bronze", "silver"])


def test_multi_hybrid(mocker):
    context = Context(
        paths=["examples/multi_hybrid/dbt_repo", "examples/multi_hybrid/sqlmesh_repo"]
    )
    context._new_state_sync().reset(default_catalog=context.default_catalog)
    plan = context.plan_builder().build()

    assert len(plan.new_snapshots) == 5
    assert context.dag.roots == {'"memory"."dbt_repo"."e"'}
    assert context.dag.graph['"memory"."dbt_repo"."c"'] == {'"memory"."sqlmesh_repo"."b"'}
    assert context.dag.graph['"memory"."sqlmesh_repo"."b"'] == {'"memory"."sqlmesh_repo"."a"'}
    assert context.dag.graph['"memory"."sqlmesh_repo"."a"'] == {'"memory"."dbt_repo"."e"'}
    assert context.dag.downstream('"memory"."dbt_repo"."e"') == [
        '"memory"."sqlmesh_repo"."a"',
        '"memory"."sqlmesh_repo"."b"',
        '"memory"."dbt_repo"."c"',
        '"memory"."dbt_repo"."d"',
    ]

    sqlmesh_model_a = context.get_model("sqlmesh_repo.a")
    dbt_model_c = context.get_model("dbt_repo.c")
    assert sqlmesh_model_a.project == "sqlmesh_repo"

    sqlmesh_rendered = (
        'SELECT "e"."col_a" AS "col_a", "e"."col_b" AS "col_b" FROM "memory"."dbt_repo"."e" AS "e"'
    )
    dbt_rendered = 'SELECT DISTINCT ROUND(CAST(("b"."col_a" / NULLIF(100, 0)) AS DECIMAL(16, 2)), 2) AS "rounded_col_a" FROM "memory"."sqlmesh_repo"."b" AS "b"'
    assert sqlmesh_model_a.render_query().sql() == sqlmesh_rendered
    assert dbt_model_c.render_query().sql() == dbt_rendered

    context.apply(plan)
    validate_apply_basics(context, c.PROD, plan.snapshots.values())


def test_incremental_time_self_reference(
    mocker: MockerFixture, sushi_context: Context, sushi_data_validator: SushiDataValidator
):
    start_ts = to_timestamp("1 week ago")
    start_date, end_date = to_date("1 week ago"), to_date("yesterday")
    if to_timestamp(start_date) < start_ts:
        # The start date must be aligned by the interval unit.
        start_date += timedelta(days=1)

    df = sushi_context.engine_adapter.fetchdf(
        "SELECT MIN(event_date) FROM sushi.customer_revenue_lifetime"
    )
    assert df.iloc[0, 0] == pd.to_datetime(start_date)
    df = sushi_context.engine_adapter.fetchdf(
        "SELECT MAX(event_date) FROM sushi.customer_revenue_lifetime"
    )
    assert df.iloc[0, 0] == pd.to_datetime(end_date)
    results = sushi_data_validator.validate("sushi.customer_revenue_lifetime", start_date, end_date)
    plan = sushi_context.plan_builder(
        restate_models=["sushi.customer_revenue_lifetime", "sushi.customer_revenue_by_day"],
        start=start_date,
        end="5 days ago",
    ).build()
    revenue_lifeteime_snapshot = sushi_context.get_snapshot(
        "sushi.customer_revenue_lifetime", raise_if_missing=True
    )
    revenue_by_day_snapshot = sushi_context.get_snapshot(
        "sushi.customer_revenue_by_day", raise_if_missing=True
    )
    assert sorted(plan.missing_intervals, key=lambda x: x.snapshot_id) == sorted(
        [
            SnapshotIntervals(
                snapshot_id=revenue_lifeteime_snapshot.snapshot_id,
                intervals=[
                    (to_timestamp(to_date("7 days ago")), to_timestamp(to_date("6 days ago"))),
                    (to_timestamp(to_date("6 days ago")), to_timestamp(to_date("5 days ago"))),
                    (to_timestamp(to_date("5 days ago")), to_timestamp(to_date("4 days ago"))),
                    (to_timestamp(to_date("4 days ago")), to_timestamp(to_date("3 days ago"))),
                    (to_timestamp(to_date("3 days ago")), to_timestamp(to_date("2 days ago"))),
                    (to_timestamp(to_date("2 days ago")), to_timestamp(to_date("1 days ago"))),
                    (to_timestamp(to_date("1 day ago")), to_timestamp(to_date("today"))),
                ],
            ),
            SnapshotIntervals(
                snapshot_id=revenue_by_day_snapshot.snapshot_id,
                intervals=[
                    (to_timestamp(to_date("7 days ago")), to_timestamp(to_date("6 days ago"))),
                    (to_timestamp(to_date("6 days ago")), to_timestamp(to_date("5 days ago"))),
                ],
            ),
        ],
        key=lambda x: x.snapshot_id,
    )
    sushi_context.console = mocker.Mock(spec=Console)
    sushi_context.apply(plan)
    num_batch_calls = Counter(
        [x[0][0] for x in sushi_context.console.update_snapshot_evaluation_progress.call_args_list]  # type: ignore
    )
    # Validate that we made 7 calls to the customer_revenue_lifetime snapshot and 1 call to the customer_revenue_by_day snapshot
    assert num_batch_calls == {
        sushi_context.get_snapshot("sushi.customer_revenue_lifetime", raise_if_missing=True): 7,
        sushi_context.get_snapshot("sushi.customer_revenue_by_day", raise_if_missing=True): 1,
    }
    # Validate that the results are the same as before the restate
    assert results == sushi_data_validator.validate(
        "sushi.customer_revenue_lifetime", start_date, end_date
    )


def test_invalidating_environment(sushi_context: Context):
    apply_to_environment(sushi_context, "dev")
    start_environment = sushi_context.state_sync.get_environment("dev")
    assert start_environment is not None
    metadata = DuckDBMetadata.from_context(sushi_context)
    start_schemas = set(metadata.schemas)
    assert "sushi__dev" in start_schemas
    sushi_context.invalidate_environment("dev")
    invalidate_environment = sushi_context.state_sync.get_environment("dev")
    assert invalidate_environment is not None
    schemas_prior_to_janitor = set(metadata.schemas)
    assert invalidate_environment.expiration_ts < start_environment.expiration_ts  # type: ignore
    assert start_schemas == schemas_prior_to_janitor
    sushi_context._run_janitor()
    schemas_after_janitor = set(metadata.schemas)
    assert sushi_context.state_sync.get_environment("dev") is None
    assert start_schemas - schemas_after_janitor == {"sushi__dev"}


def test_environment_suffix_target_table(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi", config="environment_suffix_config")
    context.apply(plan)
    metadata = DuckDBMetadata.from_context(context)
    environments_schemas = {"sushi"}
    internal_schemas = {"sqlmesh", "sqlmesh__sushi"}
    starting_schemas = environments_schemas | internal_schemas
    # Make sure no new schemas are created
    assert set(metadata.schemas) - starting_schemas == {"raw"}
    prod_views = {x for x in metadata.qualified_views if x.db in environments_schemas}
    # Make sure that all models are present
    assert len(prod_views) == 16
    apply_to_environment(context, "dev")
    # Make sure no new schemas are created
    assert set(metadata.schemas) - starting_schemas == {"raw"}
    dev_views = {
        x for x in metadata.qualified_views if x.db in environments_schemas and "__dev" in x.name
    }
    # Make sure that there is a view with `__dev` for each view that exists in prod
    assert len(dev_views) == len(prod_views)
    assert {x.name.replace("__dev", "") for x in dev_views} - {x.name for x in prod_views} == set()
    context.invalidate_environment("dev")
    context._run_janitor()
    views_after_janitor = metadata.qualified_views
    # Make sure that the number of views after the janitor is the same as when you subtract away dev views
    assert len(views_after_janitor) == len(
        {x.sql(dialect="duckdb") for x in views_after_janitor}
        - {x.sql(dialect="duckdb") for x in dev_views}
    )
    # Double check there are no dev views
    assert len({x for x in views_after_janitor if "__dev" in x.name}) == 0
    # Make sure prod views were not removed
    assert {x.sql(dialect="duckdb") for x in prod_views} - {
        x.sql(dialect="duckdb") for x in views_after_janitor
    } == set()


def test_environment_catalog_mapping(init_and_plan_context: t.Callable):
    environments_schemas = {"raw", "sushi"}

    def get_prod_dev_views(metadata: DuckDBMetadata) -> t.Tuple[t.Set[exp.Table], t.Set[exp.Table]]:
        views = metadata.qualified_views
        prod_views = {
            x for x in views if x.catalog == "prod_catalog" if x.db in environments_schemas
        }
        dev_views = {x for x in views if x.catalog == "dev_catalog" if x.db in environments_schemas}
        return prod_views, dev_views

    def get_default_catalog_and_non_tables(
        metadata: DuckDBMetadata, default_catalog: t.Optional[str]
    ) -> t.Tuple[t.Set[exp.Table], t.Set[exp.Table]]:
        tables = metadata.qualified_tables
        user_default_tables = {
            x for x in tables if x.catalog == default_catalog and x.db != "sqlmesh"
        }
        non_default_tables = {x for x in tables if x.catalog != default_catalog}
        return user_default_tables, non_default_tables

    context, plan = init_and_plan_context(
        "examples/sushi", config="environment_catalog_mapping_config"
    )
    context.apply(plan)
    metadata = DuckDBMetadata(context.engine_adapter)
    state_metadata = DuckDBMetadata.from_context(context.state_sync.state_sync)
    prod_views, dev_views = get_prod_dev_views(metadata)
    (
        user_default_tables,
        non_default_tables,
    ) = get_default_catalog_and_non_tables(metadata, context.default_catalog)
    assert len(prod_views) == 16
    assert len(dev_views) == 0
    assert len(user_default_tables) == 21
    assert state_metadata.schemas == ["sqlmesh"]
    assert {x.sql() for x in state_metadata.qualified_tables}.issuperset(
        {
            "physical.sqlmesh._environments",
            "physical.sqlmesh._intervals",
            "physical.sqlmesh._plan_dags",
            "physical.sqlmesh._snapshots",
            "physical.sqlmesh._versions",
        }
    )
    apply_to_environment(context, "dev")
    prod_views, dev_views = get_prod_dev_views(metadata)
    (
        user_default_tables,
        non_default_tables,
    ) = get_default_catalog_and_non_tables(metadata, context.default_catalog)
    assert len(prod_views) == 16
    assert len(dev_views) == 16
    assert len(user_default_tables) == 21
    assert len(non_default_tables) == 0
    assert state_metadata.schemas == ["sqlmesh"]
    assert {x.sql() for x in state_metadata.qualified_tables}.issuperset(
        {
            "physical.sqlmesh._environments",
            "physical.sqlmesh._intervals",
            "physical.sqlmesh._plan_dags",
            "physical.sqlmesh._snapshots",
            "physical.sqlmesh._versions",
        }
    )
    apply_to_environment(context, "prodnot")
    prod_views, dev_views = get_prod_dev_views(metadata)
    (
        user_default_tables,
        non_default_tables,
    ) = get_default_catalog_and_non_tables(metadata, context.default_catalog)
    assert len(prod_views) == 16
    assert len(dev_views) == 32
    assert len(user_default_tables) == 21
    assert len(non_default_tables) == 0
    assert state_metadata.schemas == ["sqlmesh"]
    assert {x.sql() for x in state_metadata.qualified_tables}.issuperset(
        {
            "physical.sqlmesh._environments",
            "physical.sqlmesh._intervals",
            "physical.sqlmesh._plan_dags",
            "physical.sqlmesh._snapshots",
            "physical.sqlmesh._versions",
        }
    )
    context.invalidate_environment("dev")
    context._run_janitor()
    prod_views, dev_views = get_prod_dev_views(metadata)
    (
        user_default_tables,
        non_default_tables,
    ) = get_default_catalog_and_non_tables(metadata, context.default_catalog)
    assert len(prod_views) == 16
    assert len(dev_views) == 16
    assert len(user_default_tables) == 21
    assert len(non_default_tables) == 0
    assert state_metadata.schemas == ["sqlmesh"]
    assert {x.sql() for x in state_metadata.qualified_tables}.issuperset(
        {
            "physical.sqlmesh._environments",
            "physical.sqlmesh._intervals",
            "physical.sqlmesh._plan_dags",
            "physical.sqlmesh._snapshots",
            "physical.sqlmesh._versions",
        }
    )


@pytest.mark.parametrize(
    "context_fixture",
    ["sushi_context", "sushi_no_default_catalog"],
)
def test_unaligned_start_snapshots(context_fixture: Context, request):
    context = request.getfixturevalue(context_fixture)
    environment = "dev"
    apply_to_environment(context, environment)
    # Make breaking change to model upstream of a depends_on_self model
    context.upsert_model("sushi.order_items", stamp="1")
    # Apply the change starting at a date later then the beginning of the downstream depends_on_self model
    plan = apply_to_environment(
        context,
        environment,
        choice=SnapshotChangeCategory.BREAKING,
        plan_start="2 days ago",
        enable_preview=True,
    )
    revenue_lifetime_snapshot = context.get_snapshot(
        "sushi.customer_revenue_lifetime", raise_if_missing=True
    )
    # Validate that the depends_on_self model is non-deployable
    assert not plan.deployability_index.is_deployable(revenue_lifetime_snapshot)


class OldPythonModel(PythonModel):
    kind: ModelKind = ViewKind()


def test_python_model_default_kind_change(init_and_plan_context: t.Callable):
    """
    Around 2024-07-17 Python models had their default Kind changed from VIEW to FULL in order to
    avoid some edge cases where the views might not get updated in certain situations.

    This test ensures that if a user had a Python `kind: VIEW` model stored in state,
    it can still be loaded without error and just show as a breaking change from `kind: VIEW`
    to `kind: FULL`
    """

    # note: we deliberately dont specify a Kind here to allow the defaults to be picked up
    python_model_file = """import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model

@model(
    "sushi.python_view_model",
    columns={
        "id": "int",
    }
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    return pd.DataFrame([
        {"id": 1}
    ])
"""

    context: Context
    context, _ = init_and_plan_context("examples/sushi")

    with open(context.path / "models" / "python_view_model.py", mode="w", encoding="utf8") as f:
        f.write(python_model_file)

    # monkey-patch PythonModel to default to kind: View again
    # and ViewKind to allow python models again
    with (
        mock.patch.object(ViewKind, "supports_python_models", return_value=True),
        mock.patch("sqlmesh.core.model.definition.PythonModel", OldPythonModel),
    ):
        context.load()

    # check the monkey-patching worked
    model = context.get_model("sushi.python_view_model")
    assert model.kind.name == ModelKindName.VIEW
    assert model.source_type == "python"

    # apply plan
    plan: Plan = context.plan(auto_apply=True)

    # check that run() still works even though we have a Python model with kind: View in the state
    snapshot_ids = [s for s in plan.directly_modified if "python_view_model" in s.name]
    snapshot_from_state = list(context.state_sync.get_snapshots(snapshot_ids).values())[0]
    assert snapshot_from_state.model.kind.name == ModelKindName.VIEW
    assert snapshot_from_state.model.source_type == "python"
    context.run()

    # reload context to load model with new defaults
    # this also shows the earlier monkey-patching is no longer in effect
    context.load()
    model = context.get_model("sushi.python_view_model")
    assert model.kind.name == ModelKindName.FULL
    assert model.source_type == "python"

    plan = context.plan(
        categorizer_config=CategorizerConfig.all_full()
    )  # the default categorizer_config doesnt auto-categorize python models

    assert plan.has_changes
    assert not plan.indirectly_modified

    assert len(plan.directly_modified) == 1
    snapshot_id = list(plan.directly_modified)[0]
    assert snapshot_id.name == '"memory"."sushi"."python_view_model"'
    assert plan.modified_snapshots[snapshot_id].change_category == SnapshotChangeCategory.BREAKING

    context.apply(plan)

    df = context.engine_adapter.fetchdf("SELECT id FROM sushi.python_view_model")
    assert df["id"].to_list() == [1]


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_restatement_of_full_model_with_start(init_and_plan_context: t.Callable):
    context, plan = init_and_plan_context("examples/sushi")
    context.apply(plan)

    restatement_plan = context.plan(
        restate_models=["sushi.customers"],
        start="2023-01-07",
        auto_apply=True,
        no_prompts=True,
    )

    sushi_customer_interval = restatement_plan.restatements[
        context.get_snapshot("sushi.customers").snapshot_id
    ]
    assert sushi_customer_interval == (to_timestamp("2023-01-01"), to_timestamp("2023-01-09"))
    waiter_by_day_interval = restatement_plan.restatements[
        context.get_snapshot("sushi.waiter_as_customer_by_day").snapshot_id
    ]
    assert waiter_by_day_interval == (to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))


@time_machine.travel("2023-01-08 15:00:00 UTC")
def test_restatement_shouldnt_backfill_beyond_prod_intervals(init_and_plan_context: t.Callable):
    context, _ = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.top_waiters")
    context.upsert_model(SqlModel.parse_obj({**model.dict(), "cron": "@hourly"}))

    context.plan("prod", auto_apply=True, no_prompts=True, skip_tests=True)
    context.run()

    with time_machine.travel("2023-01-09 02:00:00 UTC"):
        # It's time to backfill the waiter_revenue_by_day model but it hasn't run yet
        restatement_plan = context.plan(
            restate_models=["sushi.waiter_revenue_by_day"],
            no_prompts=True,
            skip_tests=True,
        )
        intervals_by_id = {i.snapshot_id: i for i in restatement_plan.missing_intervals}
        # Make sure the intervals don't go beyond the prod intervals
        assert intervals_by_id[context.get_snapshot("sushi.top_waiters").snapshot_id].intervals[-1][
            1
        ] == to_timestamp("2023-01-08 15:00:00 UTC")
        assert intervals_by_id[
            context.get_snapshot("sushi.waiter_revenue_by_day").snapshot_id
        ].intervals[-1][1] == to_timestamp("2023-01-08 00:00:00 UTC")


def initial_add(context: Context, environment: str):
    assert not context.state_reader.get_environment(environment)

    plan = context.plan(environment, start=start(context), create_from="nonexistent_env")
    validate_plan_changes(plan, added={x.snapshot_id for x in context.snapshots.values()})

    context.apply(plan)
    validate_apply_basics(context, environment, plan.snapshots.values())


def test_plan_production_environment_statements(tmp_path: Path):
    model_a = """
    MODEL (
        name test_schema.a,
        kind FULL,
    );

    @IF(
        @runtime_stage = 'creating',
        INSERT INTO schema_names_for_prod (physical_schema_name) VALUES (@resolve_template('@{schema_name}'))
    );

    SELECT 1 AS account_id
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    for path, defn in {"a.sql": model_a}.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    before_all = [
        "CREATE TABLE IF NOT EXISTS schema_names_for_@this_env (physical_schema_name VARCHAR)",
        "@IF(@runtime_stage = 'before_all', CREATE TABLE IF NOT EXISTS should_create AS SELECT @runtime_stage)",
    ]
    after_all = [
        "@IF(@this_env = 'prod', CREATE TABLE IF NOT EXISTS after_t AS SELECT @var_5)",
        "@IF(@runtime_stage = 'before_all', CREATE TABLE IF NOT EXISTS not_create AS SELECT @runtime_stage)",
    ]
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        before_all=before_all,
        after_all=after_all,
        variables={"var_5": 5},
    )
    ctx = Context(paths=[tmp_path], config=config)
    ctx.plan(auto_apply=True, no_prompts=True)

    before_t = ctx.fetchdf("select * from schema_names_for_prod").to_dict()
    assert before_t["physical_schema_name"][0] == "sqlmesh__test_schema"

    after_t = ctx.fetchdf("select * from after_t").to_dict()
    assert after_t["5"][0] == 5

    environment_statements = ctx.state_reader.get_environment_statements(c.PROD)
    assert environment_statements[0].before_all == before_all
    assert environment_statements[0].after_all == after_all
    assert environment_statements[0].python_env.keys() == {"__sqlmesh__vars__"}
    assert environment_statements[0].python_env["__sqlmesh__vars__"].payload == "{'var_5': 5}"

    should_create = ctx.fetchdf("select * from should_create").to_dict()
    assert should_create["before_all"][0] == "before_all"

    with pytest.raises(
        Exception, match=r"Catalog Error: Table with name not_create does not exist!"
    ):
        ctx.fetchdf("select * from not_create")


def test_environment_statements_error_handling(tmp_path: Path):
    model_a = """
    MODEL (
        name test_schema.a,
        kind FULL,
    );

    SELECT 1 AS account_id
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    for path, defn in {"a.sql": model_a}.items():
        with open(models_dir / path, "w") as f:
            f.write(defn)

    before_all = [
        "CREATE TABLE identical_table (physical_schema_name VARCHAR)",
        "CREATE TABLE identical_table (physical_schema_name VARCHAR)",
    ]

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        before_all=before_all,
    )
    ctx = Context(paths=[tmp_path], config=config)

    expected_error_message = re.escape(
        """An error occurred during execution of the following 'before_all' statement:

CREATE TABLE identical_table (physical_schema_name TEXT)

Catalog Error: Table with name "identical_table" already exists!"""
    )

    with pytest.raises(SQLMeshError, match=expected_error_message):
        ctx.plan(auto_apply=True, no_prompts=True)

    after_all = [
        "@bad_macro()",
    ]

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        after_all=after_all,
    )
    ctx = Context(paths=[tmp_path], config=config)

    expected_error_message = re.escape(
        """An error occurred during rendering of the 'after_all' statements:

Failed to resolve macros for

@bad_macro()

Macro 'bad_macro' does not exist."""
    )

    with pytest.raises(SQLMeshError, match=expected_error_message):
        ctx.plan(auto_apply=True, no_prompts=True)


@time_machine.travel("2025-03-08 00:00:00 UTC")
def test_tz(init_and_plan_context):
    context, _ = init_and_plan_context("examples/sushi")

    model = context.get_model("sushi.waiter_revenue_by_day")
    context.upsert_model(
        SqlModel.parse_obj(
            {**model.dict(), "cron_tz": "America/Los_Angeles", "start": "2025-03-07"}
        )
    )

    def assert_intervals(plan, intervals):
        assert (
            next(
                intervals.intervals
                for intervals in plan.missing_intervals
                if intervals.snapshot_id.name == model.fqn
            )
            == intervals
        )

    plan = context.plan_builder("prod", skip_tests=True).build()

    # we have missing intervals but not waiter_revenue_by_day because it's not midnight pacific yet
    assert plan.missing_intervals

    with pytest.raises(StopIteration):
        assert_intervals(plan, [])

    # now we're ready 8AM UTC == midnight PST
    with time_machine.travel("2025-03-08 08:00:00 UTC"):
        plan = context.plan_builder("prod", skip_tests=True).build()
        assert_intervals(plan, [(to_timestamp("2025-03-07"), to_timestamp("2025-03-08"))])

    with time_machine.travel("2025-03-09 07:00:00 UTC"):
        plan = context.plan_builder("prod", skip_tests=True).build()

        assert_intervals(
            plan,
            [
                (to_timestamp("2025-03-07"), to_timestamp("2025-03-08")),
            ],
        )

    with time_machine.travel("2025-03-09 08:00:00 UTC"):
        plan = context.plan_builder("prod", skip_tests=True).build()

        assert_intervals(
            plan,
            [
                (to_timestamp("2025-03-07"), to_timestamp("2025-03-08")),
                (to_timestamp("2025-03-08"), to_timestamp("2025-03-09")),
            ],
        )

        context.apply(plan)

        plan = context.plan_builder("prod", skip_tests=True).build()
        assert not plan.missing_intervals


def apply_to_environment(
    context: Context,
    environment: str,
    choice: t.Optional[SnapshotChangeCategory] = None,
    plan_validators: t.Optional[t.Iterable[t.Callable]] = None,
    apply_validators: t.Optional[t.Iterable[t.Callable]] = None,
    plan_start: t.Optional[TimeLike] = None,
    allow_destructive_models: t.Optional[t.List[str]] = None,
    enable_preview: bool = False,
):
    plan_validators = plan_validators or []
    apply_validators = apply_validators or []

    plan_builder = context.plan_builder(
        environment,
        start=plan_start or start(context) if environment != c.PROD else None,
        forward_only=choice == SnapshotChangeCategory.FORWARD_ONLY,
        include_unmodified=True,
        allow_destructive_models=allow_destructive_models if allow_destructive_models else [],
        enable_preview=enable_preview,
    )
    if environment != c.PROD:
        plan_builder.set_start(plan_start or start(context))

    if choice:
        plan_choice(plan_builder, choice)
    for validator in plan_validators:
        validator(context, plan_builder.build())

    plan = plan_builder.build()
    context.apply(plan)

    validate_apply_basics(context, environment, plan.snapshots.values(), plan.deployability_index)
    for validator in apply_validators:
        validator(context)
    return plan


def change_data_type(
    context: Context, model_name: str, old_type: DataType.Type, new_type: DataType.Type
) -> None:
    model = context.get_model(model_name)
    assert model is not None

    if isinstance(model, SqlModel):
        data_types = model.query.find_all(DataType)
        for data_type in data_types:
            if data_type.this == old_type:
                data_type.set("this", new_type)
        context.upsert_model(model_name, query=model.query)
    elif model.columns_to_types_ is not None:
        for k, v in model.columns_to_types_.items():
            if v.this == old_type:
                model.columns_to_types_[k] = DataType.build(new_type)
        context.upsert_model(model_name, columns=model.columns_to_types_)


def validate_plan_changes(
    plan: Plan,
    *,
    added: t.Optional[t.Iterable[SnapshotId]] = None,
    modified: t.Optional[t.Iterable[str]] = None,
    removed: t.Optional[t.Iterable[SnapshotId]] = None,
) -> None:
    added = added or []
    modified = modified or []
    removed = removed or []
    assert set(added) == plan.context_diff.added
    assert set(modified) == set(plan.context_diff.modified_snapshots)
    assert set(removed) == set(plan.context_diff.removed_snapshots)


def validate_versions_same(
    model_names: t.List[str],
    versions: t.Dict[str, str],
    other_versions: t.Dict[str, str],
) -> None:
    for name in model_names:
        assert versions[name] == other_versions[name]


def validate_versions_different(
    model_names: t.List[str],
    versions: t.Dict[str, str],
    other_versions: t.Dict[str, str],
) -> None:
    for name in model_names:
        assert versions[name] != other_versions[name]


def validate_apply_basics(
    context: Context,
    environment: str,
    snapshots: t.Iterable[Snapshot],
    deployability_index: t.Optional[DeployabilityIndex] = None,
) -> None:
    validate_snapshots_in_state_sync(snapshots, context)
    validate_state_sync_environment(snapshots, environment, context)
    validate_tables(snapshots, context, deployability_index)
    validate_environment_views(snapshots, environment, context, deployability_index)


def validate_snapshots_in_state_sync(snapshots: t.Iterable[Snapshot], context: Context) -> None:
    snapshot_infos = map(to_snapshot_info, snapshots)
    state_sync_table_infos = map(
        to_snapshot_info, context.state_reader.get_snapshots(snapshots).values()
    )
    assert set(snapshot_infos) == set(state_sync_table_infos)


def validate_state_sync_environment(
    snapshots: t.Iterable[Snapshot], env: str, context: Context
) -> None:
    environment = context.state_reader.get_environment(env)
    assert environment
    snapshot_infos = map(to_snapshot_info, snapshots)
    environment_table_infos = map(to_snapshot_info, environment.snapshots)
    assert set(snapshot_infos) == set(environment_table_infos)


def validate_tables(
    snapshots: t.Iterable[Snapshot],
    context: Context,
    deployability_index: t.Optional[DeployabilityIndex] = None,
) -> None:
    adapter = context.engine_adapter
    deployability_index = deployability_index or DeployabilityIndex.all_deployable()
    for snapshot in snapshots:
        is_deployable = deployability_index.is_representative(snapshot)
        if not snapshot.is_model or snapshot.is_external:
            continue
        table_should_exist = not snapshot.is_embedded
        assert adapter.table_exists(snapshot.table_name(is_deployable)) == table_should_exist
        if table_should_exist:
            assert select_all(snapshot.table_name(is_deployable), adapter)


def validate_environment_views(
    snapshots: t.Iterable[Snapshot],
    environment: str,
    context: Context,
    deployability_index: t.Optional[DeployabilityIndex] = None,
) -> None:
    adapter = context.engine_adapter
    deployability_index = deployability_index or DeployabilityIndex.all_deployable()
    for snapshot in snapshots:
        is_deployable = deployability_index.is_representative(snapshot)
        if not snapshot.is_model or snapshot.is_symbolic:
            continue
        view_name = snapshot.qualified_view_name.for_environment(
            EnvironmentNamingInfo.from_environment_catalog_mapping(
                context.config.environment_catalog_mapping,
                name=environment,
                suffix_target=context.config.environment_suffix_target,
            )
        )

        assert adapter.table_exists(view_name)
        assert select_all(snapshot.table_name(is_deployable), adapter) == select_all(
            view_name, adapter
        )


def select_all(table: str, adapter: EngineAdapter) -> t.Iterable:
    return adapter.fetchall(f"select * from {table} order by 1")


def snapshots_to_versions(snapshots: t.Iterable[Snapshot]) -> t.Dict[str, str]:
    return {snapshot.name: snapshot.version or "" for snapshot in snapshots}


def to_snapshot_info(snapshot: SnapshotInfoLike) -> SnapshotTableInfo:
    return snapshot.table_info


def start(context: Context) -> TimeLike:
    env = context.state_sync.get_environment("prod")
    assert env
    return env.start_at


def add_projection_to_model(model: SqlModel, literal: bool = True) -> SqlModel:
    one_expr = exp.Literal.number(1).as_("one") if literal else exp.column("one")
    kwargs = {
        **model.dict(),
        "query": model.query.select(one_expr),  # type: ignore
    }
    return SqlModel.parse_obj(kwargs)


def test_plan_environment_statements_doesnt_cause_extra_diff(tmp_path: Path):
    model_a = """
    MODEL (
        name test_schema.a,
        kind FULL,
    );

    SELECT 1;
    """

    models_dir = tmp_path / "models"
    models_dir.mkdir()

    (models_dir / "a.sql").write_text(model_a)

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        before_all=["select 1 as before_all"],
        after_all=["select 2 as after_all"],
    )
    ctx = Context(paths=[tmp_path], config=config)

    # first plan - should apply changes
    assert ctx.plan(auto_apply=True, no_prompts=True).has_changes

    # second plan - nothing has changed so should report no changes
    assert not ctx.plan(auto_apply=True, no_prompts=True).has_changes


def test_janitor_cleanup_order(mocker: MockerFixture, tmp_path: Path):
    def setup_scenario():
        models_dir = tmp_path / "models"

        if not models_dir.exists():
            models_dir.mkdir()

        model1_path = models_dir / "model1.sql"

        with open(model1_path, "w") as f:
            f.write("MODEL(name test.model1, kind FULL); SELECT 1 AS col")

        config = Config(
            model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        )
        ctx = Context(paths=[tmp_path], config=config)

        ctx.plan("dev", no_prompts=True, auto_apply=True)

        model1_snapshot = ctx.get_snapshot("test.model1")

        # Delete the model file to cause a snapshot expiration
        model1_path.unlink()

        ctx.load()

        ctx.plan("dev", no_prompts=True, auto_apply=True)

        # Invalidate the environment to cause an environment cleanup
        ctx.invalidate_environment("dev")

        try:
            ctx._run_janitor(ignore_ttl=True)
        except:
            pass

        return ctx, model1_snapshot

    # Case 1: Assume that the snapshot cleanup yields an error, the snapshot records
    # should still exist in the state sync so the next janitor can retry
    mocker.patch(
        "sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.cleanup",
        side_effect=Exception("snapshot cleanup error"),
    )
    ctx, model1_snapshot = setup_scenario()

    # - Check that the snapshot record exists in the state sync
    state_snapshot = ctx.state_sync.state_sync.get_snapshots([model1_snapshot.snapshot_id])
    assert state_snapshot

    # - Run the janitor again, this time it should succeed
    mocker.patch("sqlmesh.core.snapshot.evaluator.SnapshotEvaluator.cleanup")
    ctx._run_janitor(ignore_ttl=True)

    # - Check that the snapshot record does not exist in the state sync anymore
    state_snapshot = ctx.state_sync.state_sync.get_snapshots([model1_snapshot.snapshot_id])
    assert not state_snapshot

    # Case 2: Assume that the view cleanup yields an error, the enviroment
    # record should still exist
    mocker.patch(
        "sqlmesh.core.context.cleanup_expired_views", side_effect=Exception("view cleanup error")
    )
    ctx, model1_snapshot = setup_scenario()

    views = ctx.fetchdf("FROM duckdb_views() SELECT * EXCLUDE(sql) WHERE NOT internal")
    assert views.empty

    # - Check that the environment record exists in the state sync
    assert ctx.state_sync.get_environment("dev")

    # - Run the janitor again, this time it should succeed
    mocker.patch("sqlmesh.core.context.cleanup_expired_views")
    ctx._run_janitor(ignore_ttl=True)

    # - Check that the environment record does not exist in the state sync anymore
    assert not ctx.state_sync.get_environment("dev")


@use_terminal_console
def test_destroy(copy_to_temp_path):
    # Testing project with two gateways to verify cleanup is performed across engines
    paths = copy_to_temp_path("tests/fixtures/multi_virtual_layer")
    path = Path(paths[0])
    first_db_path = str(path / "db_1.db")
    second_db_path = str(path / "db_2.db")

    config = Config(
        gateways={
            "first": GatewayConfig(
                connection=DuckDBConnectionConfig(database=first_db_path),
                variables={"overriden_var": "gateway_1"},
            ),
            "second": GatewayConfig(
                connection=DuckDBConnectionConfig(database=second_db_path),
                variables={"overriden_var": "gateway_2"},
            ),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        model_naming=NameInferenceConfig(infer_names=True),
        default_gateway="first",
        gateway_managed_virtual_layer=True,
        variables={"overriden_var": "global", "global_one": 88},
    )

    context = Context(paths=paths, config=config)
    plan = context.plan_builder().build()
    assert len(plan.new_snapshots) == 4
    context.apply(plan)

    # Confirm cache exists
    cache_path = Path(path) / ".cache"
    assert cache_path.exists()
    assert len(list(cache_path.iterdir())) > 0

    model = context.get_model("db_1.first_schema.model_one")

    context.upsert_model(model.copy(update={"query": model.query.select("'c' AS extra")}))
    plan = context.plan_builder().build()
    context.apply(plan)

    state_environments = context.state_reader.get_environments()
    state_snapshots = context.state_reader.get_snapshots(context.snapshots.values())

    assert len(state_snapshots) == len(state_environments[0].snapshots)

    # Create dev environment with changed models
    model = context.get_model("db_2.second_schema.model_one")
    context.upsert_model(model.copy(update={"query": model.query.select("'d' AS extra")}))
    model = context.get_model("first_schema.model_two")
    context.upsert_model(model.copy(update={"query": model.query.select("'d2' AS col")}))
    plan = context.plan_builder("dev").build()
    context.apply(plan)

    dev_environment = context.state_sync.get_environment("dev")
    assert dev_environment is not None

    state_environments = context.state_reader.get_environments()
    state_snapshots = context.state_reader.get_snapshots(context.snapshots.values())
    assert (
        len(state_snapshots)
        == len(state_environments[0].snapshots)
        == len(state_environments[1].snapshots)
    )

    # The state tables at this point should be able to be retrieved
    state_tables = {
        "_environments",
        "_snapshots",
        "_intervals",
        "_auto_restatements",
        "_environment_statements",
        "_intervals",
        "_plan_dags",
        "_versions",
    }
    for table_name in state_tables:
        context.fetchdf(f"SELECT * FROM db_1.sqlmesh.{table_name}")

    # The actual tables as well
    context.engine_adapters["second"].fetchdf(f"SELECT * FROM db_2.second_schema.model_one")
    context.engine_adapters["second"].fetchdf(f"SELECT * FROM db_2.second_schema.model_two")
    context.fetchdf(f"SELECT * FROM db_1.first_schema.model_one")
    context.fetchdf(f"SELECT * FROM db_1.first_schema.model_two")

    # Use the destroy command to remove all data objects and state
    context._destroy()

    # Ensure all tables have been removed
    for table_name in state_tables:
        with pytest.raises(
            Exception, match=f"Catalog Error: Table with name {table_name} does not exist!"
        ):
            context.fetchdf(f"SELECT * FROM db_1.sqlmesh.{table_name}")

    # Validate tables have been deleted as well
    with pytest.raises(
        Exception, match=r"Catalog Error: Table with name model_two does not exist!"
    ):
        context.fetchdf("SELECT * FROM db_1.first_schema.model_two")
    with pytest.raises(
        Exception, match=r"Catalog Error: Table with name model_one does not exist!"
    ):
        context.fetchdf("SELECT * FROM db_1.first_schema.model_one")

    with pytest.raises(
        Exception, match=r"Catalog Error: Table with name model_two does not exist!"
    ):
        context.engine_adapters["second"].fetchdf("SELECT * FROM db_2.second_schema.model_two")
    with pytest.raises(
        Exception, match=r"Catalog Error: Table with name model_one does not exist!"
    ):
        context.engine_adapters["second"].fetchdf("SELECT * FROM db_2.second_schema.model_one")

    # Ensure the cache has been removed
    assert not cache_path.exists()


@use_terminal_console
def test_audits_running_on_metadata_changes(tmp_path: Path):
    def setup_senario(model_before: str, model_after: str):
        models_dir = Path("models")
        create_temp_file(tmp_path, models_dir / "test.sql", model_before)

        # Create first snapshot
        context = Context(paths=tmp_path, config=Config())
        context.plan("prod", no_prompts=True, auto_apply=True)

        # Create second (metadata) snapshot
        create_temp_file(tmp_path, models_dir / "test.sql", model_after)
        context.load()

        with capture_output() as output:
            with pytest.raises(PlanError):
                context.plan("prod", no_prompts=True, auto_apply=True)

        assert 'Failed models\n\n  "model"' in output.stdout

        return output

    # Ensure incorrect audits (bad data, incorrect definition etc) are evaluated immediately
    output = setup_senario(
        "MODEL (name model); SELECT NULL AS col",
        "MODEL (name model, audits (not_null(columns=[col]))); SELECT NULL AS col",
    )
    assert "'not_null' audit error: 1 row failed" in output.stdout

    output = setup_senario(
        "MODEL (name model); SELECT NULL AS col",
        "MODEL (name model, audits (not_null(columns=[this_col_does_not_exist]))); SELECT NULL AS col",
    )
    assert (
        'Binder Error: Referenced column "this_col_does_not_exist" not found in \nFROM clause!'
        in output.stdout
    )


@pytest.mark.set_default_connection(disable=True)
def test_missing_connection_config():
    # This is testing the actual implementation of Config.get_connection
    # To make writing tests easier, it's patched by the autouse fixture provide_sqlmesh_default_connection
    # Case 1: No default_connection or gateways specified should raise a ConfigError
    with pytest.raises(ConfigError):
        ctx = Context(config=Config())

    # Case 2: No connection specified in the gateway should raise a ConfigError
    with pytest.raises(ConfigError):
        ctx = Context(config=Config(gateways={"incorrect": GatewayConfig()}))

    # Case 3: Specifying a default_connection or connection in the gateway should work
    ctx = Context(config=Config(default_connection=DuckDBConnectionConfig()))
    ctx = Context(
        config=Config(gateways={"default": GatewayConfig(connection=DuckDBConnectionConfig())})
    )


@use_terminal_console
def test_render_path_instead_of_model(tmp_path: Path):
    create_temp_file(tmp_path, Path("models/test.sql"), "MODEL (name test_model); SELECT 1 AS col")
    ctx = Context(paths=tmp_path, config=Config())

    # Case 1: Fail gracefully when the user is passing in a path instead of a model name
    for test_model in ["models/test.sql", "models/test.py"]:
        with pytest.raises(
            SQLMeshError,
            match="Resolving models by path is not supported, please pass in the model name instead.",
        ):
            ctx.render(test_model)

    # Case 2: Fail gracefully when the model name is not found
    with pytest.raises(SQLMeshError, match="Cannot find model with name 'incorrect_model'"):
        ctx.render("incorrect_model")

    # Case 3: Render the model successfully
    assert ctx.render("test_model").sql() == 'SELECT 1 AS "col"'
