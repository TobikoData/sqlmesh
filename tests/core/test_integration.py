import pathlib
import shutil
import typing as t
from collections import Counter
from datetime import timedelta

import numpy as np
import pandas as pd
import pytest
from freezegun import freeze_time
from pytest_mock.plugin import MockerFixture
from sqlglot import exp
from sqlglot.expressions import DataType

from sqlmesh.core import constants as c
from sqlmesh.core.config import AutoCategorizationMode
from sqlmesh.core.console import Console
from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter import EngineAdapter
from sqlmesh.core.environment import EnvironmentNamingInfo
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalByUniqueKeyKind,
    ModelKind,
    ModelKindName,
    SqlModel,
    TimeColumn,
)
from sqlmesh.core.model.kind import model_kind_type_from_name
from sqlmesh.core.plan import Plan, SnapshotIntervals
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotChangeCategory,
    SnapshotInfoLike,
    SnapshotTableInfo,
)
from sqlmesh.utils.date import (
    TimeLike,
    to_date,
    to_datetime,
    to_ds,
    to_timestamp,
    to_ts,
)
from tests.conftest import DuckDBMetadata, SushiDataValidator, init_and_plan_context


@pytest.fixture(autouse=True)
def mock_choices(mocker: MockerFixture):
    mocker.patch("sqlmesh.core.console.TerminalConsole._get_snapshot_change_category")
    mocker.patch("sqlmesh.core.console.TerminalConsole._prompt_backfill")


def plan_choice(plan: Plan, choice: SnapshotChangeCategory) -> None:
    for snapshot in plan.snapshots:
        if not snapshot.version:
            plan.set_choice(snapshot, choice)


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_forward_only_plan_with_effective_date(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"
    model = context.models[model_name]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)), start="2023-01-01")

    plan = context.plan("dev", no_prompts=True, skip_tests=True, forward_only=True)
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[model_name].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_date("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
    ]

    plan.effective_from = "2023-01-05"
    # Default start should be set to effective_from
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    plan.start = "2023-01-06"
    # Start override should take precedence
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    plan.effective_from = "2023-01-04"
    # Start should remain unchanged
    assert plan.start == "2023-01-06"
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert dev_df["ds"].tolist() == ["2023-01-06", "2023-01-07"]

    prod_plan = context.plan(no_prompts=True, skip_tests=True)
    # Make sure that the previously set effective_from is respected
    assert prod_plan.start == to_timestamp("2023-01-04")
    assert prod_plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
                (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
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
        "SELECT DISTINCT ds FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY ds"
    )
    assert prod_df["ds"].tolist() == ["2023-01-04", "2023-01-05", "2023-01-06", "2023-01-07"]


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_forward_only_model_regular_plan(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.models[model_name]
    model = add_projection_to_model(t.cast(SqlModel, model))
    forward_only_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"kind": forward_only_kind})

    context.upsert_model(model)

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[model_name].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_datetime("2023-01-01")
    assert not plan.missing_intervals

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert not dev_df["ds"].tolist()

    # Run a restatement plan to preview changes
    plan = context.plan("dev", no_prompts=True, skip_tests=True, restate_models=[model_name])
    plan.start = "2023-01-06"
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[
                (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    # Make sure that changed start is reflected in missing intervals
    plan.start = "2023-01-07"
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[
                (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
            ],
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert dev_df["ds"].tolist() == ["2023-01-07"]

    # Promote changes to prod
    prod_plan = context.plan(no_prompts=True, skip_tests=True)
    assert not prod_plan.missing_intervals

    context.apply(prod_plan)

    # The change was applied in a forward-only manner so no values in the new column should be populated
    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY ds"
    )
    assert not prod_df["ds"].tolist()


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_plan_set_choice_is_reflected_in_missing_intervals(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    model_name = "sushi.waiter_revenue_by_day"

    model = context.models[model_name]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots[model_name].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
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
    plan.set_choice(plan.context_diff.snapshots[model_name], SnapshotChangeCategory.BREAKING)
    assert (
        plan.context_diff.snapshots[model_name].change_category == SnapshotChangeCategory.BREAKING
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.INDIRECT_BREAKING
    )
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
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
            snapshot_name="sushi.top_waiters",
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
    plan.set_choice(plan.context_diff.snapshots[model_name], SnapshotChangeCategory.NON_BREAKING)
    assert (
        plan.context_diff.snapshots[model_name].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name=model_name,
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
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert dev_df["ds"].tolist() == [
        "2023-01-01",
        "2023-01-02",
        "2023-01-03",
        "2023-01-04",
        "2023-01-05",
        "2023-01-06",
        "2023-01-07",
    ]

    # Promote changes to prod
    prod_plan = context.plan(no_prompts=True, skip_tests=True)
    assert not prod_plan.missing_intervals

    context.apply(prod_plan)
    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY ds"
    )
    assert prod_df["ds"].tolist() == [
        "2023-01-01",
        "2023-01-02",
        "2023-01-03",
        "2023-01-04",
        "2023-01-05",
        "2023-01-06",
        "2023-01-07",
    ]


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_non_breaking_change_after_forward_only_in_dev(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    model = context.models["sushi.waiter_revenue_by_day"]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    plan = context.plan("dev", no_prompts=True, skip_tests=True, forward_only=True)
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots["sushi.waiter_revenue_by_day"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_date("2023-01-07")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
            intervals=[(to_timestamp("2023-01-07"), to_timestamp("2023-01-08"))],
        ),
    ]

    # Apply the forward-only changes first.
    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert dev_df["ds"].tolist() == ["2023-01-07"]

    # FIXME: Due to freezgun freezing the time, all inteval records have the same creation timestamp.
    # As a result removal records are always being applied after any addition records. Running the plan repeatadly
    # to make sure there are no missing intervals.
    context._run_janitor()
    context.plan("dev", no_prompts=True, skip_tests=True, auto_apply=True)

    # Make a non-breaking change to a model downstream.
    model = context.models["sushi.top_waiters"]
    # Select 'one' column from the updated upstream model.
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model), literal=False))

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert len(plan.new_snapshots) == 1
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
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
    plan = context.plan("prod", no_prompts=True, skip_tests=True)
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
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
        "SELECT DISTINCT ds FROM sushi.waiter_revenue_by_day WHERE one IS NOT NULL ORDER BY ds"
    )
    assert prod_df.empty

    prod_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT waiter_id FROM sushi.top_waiters WHERE one IS NOT NULL"
    )
    assert prod_df.empty


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_indirect_non_breaking_change_after_forward_only_in_dev(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    # Make sushi.orders a forward-only model.
    model = context.models["sushi.orders"]
    updated_model_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"stamp": "force new version", "kind": updated_model_kind})
    context.upsert_model(model)

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert (
        plan.context_diff.snapshots["sushi.orders"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert not plan.requires_backfill
    context.apply(plan)

    # Make a non-breaking change to a model.
    model = context.models["sushi.top_waiters"]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert len(plan.new_snapshots) == 1
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.top_waiters",
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
    model = context.models["sushi.waiter_revenue_by_day"]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, model)))

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert len(plan.new_snapshots) == 2
    assert (
        plan.context_diff.snapshots["sushi.waiter_revenue_by_day"].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
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
    assert not context.plan("dev", no_prompts=True, skip_tests=True).requires_backfill

    # Deploy everything to prod.
    plan = context.plan("prod", no_prompts=True, skip_tests=True)
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
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
            snapshot_name="sushi.top_waiters",
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
    assert not context.plan("prod", no_prompts=True, skip_tests=True).requires_backfill


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_forward_only_precedence_over_indirect_non_breaking(mocker: MockerFixture):
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)

    # Make sushi.orders a forward-only model.
    forward_only_model = context.models["sushi.orders"]
    updated_model_kind = forward_only_model.kind.copy(update={"forward_only": True})
    forward_only_model = forward_only_model.copy(
        update={"stamp": "force new version", "kind": updated_model_kind}
    )
    context.upsert_model(forward_only_model)

    non_breaking_model = context.models["sushi.waiter_revenue_by_day"]
    context.upsert_model(add_projection_to_model(t.cast(SqlModel, non_breaking_model)))

    plan = context.plan("dev", no_prompts=True, skip_tests=True)
    assert (
        plan.context_diff.snapshots["sushi.orders"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert (
        plan.context_diff.snapshots["sushi.waiter_revenue_by_day"].change_category
        == SnapshotChangeCategory.NON_BREAKING
    )
    assert (
        plan.context_diff.snapshots["sushi.top_waiters"].change_category
        == SnapshotChangeCategory.FORWARD_ONLY
    )
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
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
    assert not context.plan("dev", no_prompts=True, skip_tests=True).requires_backfill

    # Deploy everything to prod.
    plan = context.plan("prod", no_prompts=True, skip_tests=True)
    assert plan.start == to_timestamp("2023-01-01")
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
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
    assert not context.plan("prod", no_prompts=True, skip_tests=True).requires_backfill


@freeze_time("2023-01-08 15:00:00")
@pytest.mark.integration
@pytest.mark.core_integration
def test_select_models_for_backfill(mocker: MockerFixture):
    context, _ = init_and_plan_context("examples/sushi", mocker)

    expected_intervals = [
        (to_timestamp("2023-01-01"), to_timestamp("2023-01-02")),
        (to_timestamp("2023-01-02"), to_timestamp("2023-01-03")),
        (to_timestamp("2023-01-03"), to_timestamp("2023-01-04")),
        (to_timestamp("2023-01-04"), to_timestamp("2023-01-05")),
        (to_timestamp("2023-01-05"), to_timestamp("2023-01-06")),
        (to_timestamp("2023-01-06"), to_timestamp("2023-01-07")),
        (to_timestamp("2023-01-07"), to_timestamp("2023-01-08")),
    ]

    plan = context.plan(
        "dev", backfill_models=["*waiter_revenue_by_day"], no_prompts=True, skip_tests=True
    )
    assert plan.missing_intervals == [
        SnapshotIntervals(
            snapshot_name="sushi.items",
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_name="sushi.orders",
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_name="sushi.order_items",
            intervals=expected_intervals,
        ),
        SnapshotIntervals(
            snapshot_name="sushi.waiter_revenue_by_day",
            intervals=expected_intervals,
        ),
    ]

    context.apply(plan)

    dev_df = context.engine_adapter.fetchdf(
        "SELECT DISTINCT ds FROM sushi__dev.waiter_revenue_by_day ORDER BY ds"
    )
    assert len(dev_df) == 7

    dev_df = context.engine_adapter.fetchdf("SELECT * FROM sushi__dev.customer_revenue_by_day")
    assert dev_df.empty


@pytest.mark.integration
@pytest.mark.core_integration
@pytest.mark.parametrize(
    "context_fixture",
    ["sushi_context", "sushi_dbt_context", "sushi_test_dbt_context", "sushi_default_catalog"],
)
def test_model_add(context_fixture: Context, request):
    initial_add(request.getfixturevalue(context_fixture), "dev")


@pytest.mark.integration
@pytest.mark.core_integration
def test_model_removed(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)

    top_waiters_snapshot_id = sushi_context.snapshots["sushi.top_waiters"].snapshot_id

    sushi_context._models.pop("sushi.top_waiters")
    removed = ["sushi.top_waiters"]

    def _validate_plan(context, plan):
        validate_plan_changes(plan, removed=removed)
        assert not plan.missing_intervals

    def _validate_apply(context):
        assert not sushi_context.snapshots.get("sushi.top_waiters")
        assert sushi_context.state_reader.get_snapshots([top_waiters_snapshot_id])
        env = sushi_context.state_reader.get_environment(environment)
        assert env
        assert all(snapshot.name != "sushi.top_waiters" for snapshot in env.snapshots)

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_non_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.NON_BREAKING, False)


@pytest.mark.integration
@pytest.mark.core_integration
def test_breaking_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.BREAKING, False)


@pytest.mark.integration
@pytest.mark.core_integration
def test_forward_only(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    validate_query_change(sushi_context, environment, SnapshotChangeCategory.FORWARD_ONLY, False)


@pytest.mark.integration
@pytest.mark.core_integration
def test_logical_change(sushi_context: Context):
    environment = "dev"
    initial_add(sushi_context, environment)
    previous_sushi_items_version = sushi_context.snapshots["sushi.items"].version

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

    assert sushi_context.snapshots["sushi.items"].version == previous_sushi_items_version


def validate_query_change(
    context: Context,
    environment: str,
    change_category: SnapshotChangeCategory,
    logical: bool,
):
    versions = snapshots_to_versions(context.snapshots)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )

    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.order_items",
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.customer_revenue_lifetime",
        "sushi.top_waiters",
        "assert_item_price_above_zero",
    ]
    not_modified = [
        key
        for key in context.snapshots
        if key not in directly_modified and key not in indirectly_modified
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
        current_versions = snapshots_to_versions(context.snapshots)
        validate_versions_same(models_same, versions, current_versions)
        validate_versions_different(models_different, versions, current_versions)

    apply_to_environment(
        context,
        environment,
        change_category,
        plan_validators=[_validate_plan],
        apply_validators=[_validate_apply],
    )


@pytest.mark.integration
@pytest.mark.core_integration
@pytest.mark.parametrize(
    "from_, to",
    [
        (ModelKindName.INCREMENTAL_BY_TIME_RANGE, ModelKindName.VIEW),
        (ModelKindName.INCREMENTAL_BY_TIME_RANGE, ModelKindName.EMBEDDED),
        (ModelKindName.INCREMENTAL_BY_TIME_RANGE, ModelKindName.FULL),
        (ModelKindName.VIEW, ModelKindName.EMBEDDED),
        (ModelKindName.VIEW, ModelKindName.FULL),
        (ModelKindName.VIEW, ModelKindName.INCREMENTAL_BY_TIME_RANGE),
        (ModelKindName.EMBEDDED, ModelKindName.VIEW),
        (ModelKindName.EMBEDDED, ModelKindName.FULL),
        (ModelKindName.EMBEDDED, ModelKindName.INCREMENTAL_BY_TIME_RANGE),
        (ModelKindName.FULL, ModelKindName.VIEW),
        (ModelKindName.FULL, ModelKindName.EMBEDDED),
        (ModelKindName.FULL, ModelKindName.INCREMENTAL_BY_TIME_RANGE),
    ],
)
def test_model_kind_change(from_: ModelKindName, to: ModelKindName, sushi_context: Context):
    environment = f"test_model_kind_change__{from_.value.lower()}__{to.value.lower()}"
    incremental_snapshot = sushi_context.snapshots["sushi.items"].copy()

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
            audits=[],
        )
    context.upsert_model("sushi.items", kind=model_kind_type_from_name(kind)())  # type: ignore


def validate_model_kind_change(
    kind_name: ModelKindName,
    context: Context,
    environment: str,
    *,
    logical: bool,
):
    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.order_items",
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.customer_revenue_lifetime",
        "sushi.top_waiters",
        "assert_item_price_above_zero",
    ]
    if kind_name == ModelKindName.INCREMENTAL_BY_TIME_RANGE:
        kind: ModelKind = IncrementalByTimeRangeKind(
            time_column=TimeColumn(column="ds", format="%Y-%m-%d")
        )
    elif kind_name == ModelKindName.INCREMENTAL_BY_UNIQUE_KEY:
        kind = IncrementalByUniqueKeyKind(unique_key="id")
    else:
        kind = model_kind_type_from_name(kind_name)()  # type: ignore

    def _validate_plan(context, plan):
        validate_plan_changes(plan, modified=directly_modified + indirectly_modified)
        assert (
            next(
                snapshot for snapshot in plan.snapshots if snapshot.name == "sushi.items"
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


@pytest.mark.integration
@pytest.mark.core_integration
def test_environment_isolation(sushi_context: Context):
    prod_snapshots = sushi_context.snapshots.values()

    change_data_type(
        sushi_context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    directly_modified = ["sushi.items"]
    indirectly_modified = [
        "sushi.order_items",
        "sushi.waiter_revenue_by_day",
        "sushi.customer_revenue_by_day",
        "sushi.customer_revenue_lifetime",
        "sushi.top_waiters",
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


@pytest.mark.integration
@pytest.mark.core_integration
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
    apply_to_environment(sushi_context, "dev", SnapshotChangeCategory.FORWARD_ONLY)

    # Promote to prod
    def _validate_plan(context, plan):
        assert (
            plan.context_diff.modified_snapshots["sushi.items"][0].change_category
            == SnapshotChangeCategory.NON_BREAKING
        )
        assert (
            plan.context_diff.modified_snapshots["sushi.top_waiters"][0].change_category
            == SnapshotChangeCategory.BREAKING
        )
        assert (
            plan.context_diff.modified_snapshots["sushi.customer_revenue_by_day"][0].change_category
            == SnapshotChangeCategory.FORWARD_ONLY
        )

    apply_to_environment(
        sushi_context,
        "prod",
        SnapshotChangeCategory.NON_BREAKING,
        plan_validators=[_validate_plan],
    )


@pytest.mark.integration
@pytest.mark.core_integration
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
    plan = sushi_context.plan("prod")

    items = plan.context_diff.snapshots["sushi.items"]
    order_items = plan.context_diff.snapshots["sushi.order_items"]
    waiter_revenue = plan.context_diff.snapshots["sushi.waiter_revenue_by_day"]
    plan.set_choice(items, SnapshotChangeCategory.BREAKING)
    plan.set_choice(order_items, SnapshotChangeCategory.NON_BREAKING)
    assert items.is_new_version
    assert waiter_revenue.is_new_version
    plan.set_choice(items, SnapshotChangeCategory.NON_BREAKING)
    assert not waiter_revenue.is_new_version


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_remote_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.BREAKING,
        SnapshotChangeCategory.NON_BREAKING,
        version_kind="remote",
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_local_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.NON_BREAKING,
        SnapshotChangeCategory.BREAKING,
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_no_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.NON_BREAKING,
        SnapshotChangeCategory.NON_BREAKING,
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_rebase_break(sushi_context: Context):
    setup_rebase(
        sushi_context,
        SnapshotChangeCategory.BREAKING,
        SnapshotChangeCategory.BREAKING,
        "new",
    )


def setup_rebase(
    context: Context,
    remote_choice: SnapshotChangeCategory,
    local_choice: SnapshotChangeCategory,
    version_kind: str = "local",
) -> None:
    initial_add(context, "dev")

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    plan = context.plan("prod")

    plan_choice(plan, remote_choice)
    remote_versions = {snapshot.name: snapshot.version for snapshot in plan.snapshots}
    context.apply(plan)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.FLOAT,
        DataType.Type.DOUBLE,
    )
    change_data_type(
        context,
        "sushi.order_items",
        DataType.Type.INT,
        DataType.Type.BIGINT,
    )
    plan = context.plan("dev", start=start(context))
    plan_choice(plan, local_choice)
    local_versions = {snapshot.name: snapshot.version for snapshot in plan.snapshots}
    context.apply(plan)

    change_data_type(
        context,
        "sushi.items",
        DataType.Type.DOUBLE,
        DataType.Type.FLOAT,
    )
    plan = context.plan("dev", start=start(context))

    assert plan.categorized == [context.snapshots["sushi.items"]]
    assert plan.indirectly_modified == {
        "sushi.items": {
            "sushi.order_items",
            "sushi.waiter_revenue_by_day",
            "sushi.top_waiters",
            "sushi.customer_revenue_by_day",
            "sushi.customer_revenue_lifetime",
            "assert_item_price_above_zero",
        }
    }
    context.apply(plan)
    validate_apply_basics(context, "dev", plan.snapshots)

    if version_kind == "new":
        for versions in [remote_versions, local_versions]:
            assert (
                context.snapshots["sushi.waiter_revenue_by_day"].version
                != versions["sushi.waiter_revenue_by_day"]
            )
            assert context.snapshots["sushi.top_waiters"].version != versions["sushi.top_waiters"]
            assert (
                context.snapshots["sushi.customer_revenue_by_day"].version
                != versions["sushi.customer_revenue_by_day"]
            )
    else:
        if version_kind == "remote":
            versions = remote_versions
        else:
            versions = local_versions
        assert (
            context.snapshots["sushi.waiter_revenue_by_day"].version
            == versions["sushi.waiter_revenue_by_day"]
        )
        assert context.snapshots["sushi.top_waiters"].version == versions["sushi.top_waiters"]
        assert (
            context.snapshots["sushi.customer_revenue_by_day"].version
            == versions["sushi.customer_revenue_by_day"]
        )


@pytest.mark.integration
@pytest.mark.core_integration
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
    original_snapshot_id = sushi_context.snapshots["sushi.items"]

    types = (DataType.Type.DOUBLE, DataType.Type.FLOAT, DataType.Type.DECIMAL)
    assert len(change_categories) < len(types)

    for i, category in enumerate(change_categories):
        change_data_type(sushi_context, "sushi.items", *types[i : i + 2])
        apply_to_environment(sushi_context, environment, category)
        assert sushi_context.snapshots["sushi.items"] != original_snapshot_id

    change_data_type(sushi_context, "sushi.items", types[len(change_categories)], types[0])

    def _validate_plan(_, plan):
        snapshot = next(s for s in plan.snapshots if s.name == "sushi.items")
        assert snapshot.change_category == expected
        assert not plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        change_categories[-1],
        plan_validators=[_validate_plan],
    )
    assert sushi_context.snapshots["sushi.items"] == original_snapshot_id


@pytest.mark.integration
@pytest.mark.core_integration
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
        snapshot = next(s for s in plan.snapshots if s.name == "sushi.items")
        assert snapshot.change_category == SnapshotChangeCategory.BREAKING
        assert plan.missing_intervals

    apply_to_environment(
        sushi_context,
        environment,
        SnapshotChangeCategory.BREAKING,
        plan_validators=[_validate_plan],
    )


@pytest.mark.integration
@pytest.mark.core_integration
def test_auto_categorization(sushi_context: Context):
    environment = "dev"
    for config in sushi_context.configs.values():
        config.auto_categorize_changes.sql = AutoCategorizationMode.FULL
    initial_add(sushi_context, environment)

    version = sushi_context.snapshots["sushi.waiter_as_customer_by_day"].version
    fingerprint = sushi_context.snapshots["sushi.waiter_as_customer_by_day"].fingerprint

    model = t.cast(SqlModel, sushi_context.models["sushi.customers"])
    sushi_context.upsert_model("sushi.customers", query=model.query.select("'foo' AS foo"))  # type: ignore
    apply_to_environment(sushi_context, environment)

    assert (
        sushi_context.snapshots["sushi.waiter_as_customer_by_day"].change_category
        == SnapshotChangeCategory.INDIRECT_NON_BREAKING
    )
    assert sushi_context.snapshots["sushi.waiter_as_customer_by_day"].fingerprint != fingerprint
    assert sushi_context.snapshots["sushi.waiter_as_customer_by_day"].version == version


@pytest.mark.integration
@pytest.mark.core_integration
def test_multi(mocker):
    context = Context(paths=["examples/multi/repo_1", "examples/multi/repo_2"], gateway="memory")
    context._new_state_sync().reset()
    plan = context.plan()
    assert len(plan.new_snapshots) == 4
    context.apply(plan)

    context = Context(
        paths=["examples/multi/repo_1"], engine_adapter=context.engine_adapter, gateway="memory"
    )
    model = context.models["bronze.a"]
    assert model.project == "repo_1"
    context.upsert_model(model.copy(update={"query": model.query.select("'c' AS c")}))
    plan = context.plan()
    assert set(snapshot.name for snapshot in plan.directly_modified) == {"bronze.a", "bronze.b"}
    assert list(plan.indirectly_modified.values())[0] == {"silver.c", "silver.d"}
    assert len(plan.missing_intervals) == 2
    context.apply(plan)
    validate_apply_basics(context, c.PROD, plan.snapshots)


@pytest.mark.integration
@pytest.mark.core_integration
def test_incremental_time_self_reference(
    mocker: MockerFixture, sushi_context: Context, sushi_data_validator: SushiDataValidator
):
    start_ts = to_timestamp("1 week ago")
    start_date, end_date = to_date("1 week ago"), to_date("yesterday")
    if to_timestamp(start_date) < start_ts:
        # The start date must be aligned by the interval unit.
        start_date += timedelta(days=1)

    df = sushi_context.engine_adapter.fetchdf("SELECT MIN(ds) FROM sushi.customer_revenue_lifetime")
    assert df.iloc[0, 0] == to_ds(start_date)
    df = sushi_context.engine_adapter.fetchdf("SELECT MAX(ds) FROM sushi.customer_revenue_lifetime")
    assert df.iloc[0, 0] == to_ds(end_date)
    results = sushi_data_validator.validate("sushi.customer_revenue_lifetime", start_date, end_date)
    plan = sushi_context.plan(
        restate_models=["sushi.customer_revenue_lifetime", "sushi.customer_revenue_by_day"],
        no_prompts=True,
        start=start_date,
        end="5 days ago",
    )
    assert sorted(plan.missing_intervals, key=lambda x: x.snapshot_name) == sorted(
        [
            SnapshotIntervals(
                snapshot_name="sushi.customer_revenue_lifetime",
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
                snapshot_name="sushi.customer_revenue_by_day",
                intervals=[
                    (to_timestamp(to_date("7 days ago")), to_timestamp(to_date("6 days ago"))),
                    (to_timestamp(to_date("6 days ago")), to_timestamp(to_date("5 days ago"))),
                ],
            ),
        ],
        key=lambda x: x.snapshot_name,
    )
    sushi_context.console = mocker.Mock(spec=Console)
    plan.apply()
    num_batch_calls = Counter(
        [x[0][0] for x in sushi_context.console.update_snapshot_evaluation_progress.call_args_list]  # type: ignore
    )
    # Validate that we made 7 calls to the customer_revenue_lifetime snapshot and 1 call to the customer_revenue_by_day snapshot
    assert num_batch_calls == {
        sushi_context.snapshots["sushi.customer_revenue_lifetime"]: 7,
        sushi_context.snapshots["sushi.customer_revenue_by_day"]: 1,
    }
    # Validate that the results are the same as before the restate
    assert results == sushi_data_validator.validate(
        "sushi.customer_revenue_lifetime", start_date, end_date
    )


@pytest.mark.integration
@pytest.mark.core_integration
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


@pytest.mark.integration
@pytest.mark.core_integration
def test_environment_suffix_target_table(mocker: MockerFixture):
    context, plan = init_and_plan_context(
        "examples/sushi", mocker, config="environment_suffix_config"
    )
    context.apply(plan)
    metadata = DuckDBMetadata.from_context(context)
    environments_schemas = {"raw", "sushi"}
    internal_schemas = {"sqlmesh", "sqlmesh__sushi"}
    starting_schemas = environments_schemas | internal_schemas
    # Make sure no new schemas are created
    assert set(metadata.schemas) - starting_schemas == set()
    prod_views = {x for x in metadata.qualified_views if x.db in environments_schemas}
    # Make sure that all models are present
    assert len(prod_views) == 12
    apply_to_environment(context, "dev")
    # Make sure no new schemas are created
    assert set(metadata.schemas) - starting_schemas == set()
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


@pytest.mark.integration
@pytest.mark.core_integration
def test_ignored_snapshots(sushi_context: Context):
    environment = "dev"
    apply_to_environment(sushi_context, environment)
    # Make breaking change to model upstream of a depends_on_past model
    sushi_context.upsert_model("sushi.order_items", stamp="1")
    # Apply the change starting at a date later then the beginning of the downstream depends_on_past model
    plan = apply_to_environment(
        sushi_context, environment, choice=SnapshotChangeCategory.BREAKING, plan_start="2 days ago"
    )
    # Validate that the depends_on_past model is ignored
    assert plan.ignored_snapshot_names == {"sushi.customer_revenue_lifetime"}
    # Validate that the table was really ignored
    metadata = DuckDBMetadata.from_context(sushi_context)
    # Make sure prod view exists
    assert exp.to_table("sushi.customer_revenue_lifetime") in metadata.qualified_views
    # Make sure dev view doesn't exist since it was ignored
    assert exp.to_table("sushi__dev.customer_revenue_lifetime") not in metadata.qualified_views
    # Make sure that dev view for order items was created
    assert exp.to_table("sushi__dev.order_items") in metadata.qualified_views


@pytest.mark.integration
@pytest.mark.core_integration
def test_scd_type_2(tmp_path: pathlib.Path):
    def create_source_dataframe(values: t.List[t.Tuple[int, str, str]]) -> pd.DataFrame:
        return pd.DataFrame(
            np.array(
                values,
                [
                    ("customer_id", "int32"),
                    ("status", "object"),
                    ("updated_at", "datetime64[us]"),
                ],
            ),
        )

    def create_target_dataframe(
        values: t.List[t.Tuple[int, str, str, str, t.Optional[str]]]
    ) -> pd.DataFrame:
        return pd.DataFrame(
            np.array(
                values,
                [
                    ("customer_id", "int32"),
                    ("status", "object"),
                    ("updated_at", "datetime64[us]"),
                    ("valid_from", "datetime64[us]"),
                    ("valid_to", "datetime64[us]"),
                ],
            ),
        )

    def replace_source_table(
        context: Context,
        values: t.List[t.Tuple[int, str, str]],
    ):
        df = create_source_dataframe(values)
        context.engine_adapter.replace_query(
            "sushi.raw_marketing",
            df,
            columns_to_types={
                "customer_id": exp.DataType.build("int"),
                "status": exp.DataType.build("STRING"),
                "updated_at": exp.DataType.build("TIMESTAMP"),
            },
        )

    def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
        df1 = df1.sort_values(by=["customer_id"]).reset_index(drop=True)
        df2 = df2.sort_values(by=["customer_id"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(df1, df2)

    def get_current_df(context: Context):
        return context.engine_adapter.fetchdf("SELECT * FROM sushi.marketing")

    sushi_root = pathlib.Path("examples/sushi")
    shutil.copy(str(pathlib.Path(sushi_root / "config.py")), str(tmp_path / "config.py"))
    (tmp_path / "models").mkdir()
    shutil.copy(
        str(pathlib.Path(sushi_root / "models" / "marketing.sql")),
        str(tmp_path / "models" / "marketing.sql"),
    )

    context = Context(paths=[str(tmp_path)], config="test_config")
    context.engine_adapter.create_schema("sushi")
    replace_source_table(
        context,
        [
            (1, "a", "2020-01-01 00:00:00"),
            (2, "b", "2020-01-01 00:00:00"),
            (3, "c", "2020-01-01 00:00:00"),
        ],
    )
    plan = context.plan("prod")
    plan.apply()
    df_actual = get_current_df(context)
    df_expected = create_target_dataframe(
        [
            (1, "a", "2020-01-01 00:00:00", "1970-01-01 00:00:00", None),
            (2, "b", "2020-01-01 00:00:00", "1970-01-01 00:00:00", None),
            (3, "c", "2020-01-01 00:00:00", "1970-01-01 00:00:00", None),
        ]
    )
    compare_dataframes(df_actual, df_expected)

    replace_source_table(
        context,
        [
            # Update to "x"
            (1, "x", "2020-01-02 00:00:00"),
            # No Change
            (2, "b", "2020-01-01 00:00:00"),
            # Deleted 3
            # (3, "c", "2020-01-01 00:00:00"),
            # Added 4
            (4, "d", "2020-01-02 00:00:00"),
        ],
    )
    tomorrow = to_datetime("tomorrow")
    context.run("prod", start=to_date("today"), end=tomorrow, execution_time=tomorrow)
    df_actual = get_current_df(context)
    df_expected = create_target_dataframe(
        [
            (1, "a", "2020-01-01 00:00:00", "1970-01-01 00:00:00", "2020-01-02 00:00:00"),
            (1, "x", "2020-01-02 00:00:00", "2020-01-02 00:00:00", None),
            (2, "b", "2020-01-01 00:00:00", "1970-01-01 00:00:00", None),
            (3, "c", "2020-01-01 00:00:00", "1970-01-01 00:00:00", to_ts(tomorrow)),
            (4, "d", "2020-01-02 00:00:00", "1970-01-01 00:00:00", None),
        ]
    )
    compare_dataframes(df_actual, df_expected)

    replace_source_table(
        context,
        [
            # Update to "y"
            (1, "y", "2020-01-03 00:00:00"),
            # Delete 2
            # (2, "b", "2020-01-01 00:00:00"),
            # Add back 3
            (3, "c", "2099-01-01 00:00:00"),
            # No Change
            (4, "d", "2020-01-02 00:00:00"),
            # Added 5
            (5, "e", "2020-01-03 00:00:00"),
        ],
    )
    two_days_from_now = to_datetime("in 2 days")
    context.run(
        "prod",
        start=to_date("tomorrow"),
        end=two_days_from_now,
        execution_time=two_days_from_now,
    )
    df_actual = get_current_df(context)
    df_expected = create_target_dataframe(
        [
            (1, "a", "2020-01-01 00:00:00", "1970-01-01 00:00:00", "2020-01-02 00:00:00"),
            (1, "x", "2020-01-02 00:00:00", "2020-01-02 00:00:00", "2020-01-03 00:00:00"),
            (1, "y", "2020-01-03 00:00:00", "2020-01-03 00:00:00", None),
            (2, "b", "2020-01-01 00:00:00", "1970-01-01 00:00:00", to_ts(two_days_from_now)),
            (3, "c", "2020-01-01 00:00:00", "1970-01-01 00:00:00", to_ts(tomorrow)),
            # Since 3 was deleted and came back and the updated at time when it came back
            # is greater than the execution time when it was deleted, we have the valid_from
            # match the updated_at time. If it was less then the valid_from would match the
            # execution time when it was deleted.
            (3, "c", "2099-01-01 00:00:00", "2099-01-01 00:00:00", None),
            # What the result would be if the updated_at time was `2020-01-03`
            # (3, "c", "2020-01-03 00:00:00", to_ts(tomorrow), None),
            (4, "d", "2020-01-02 00:00:00", "1970-01-01 00:00:00", None),
            (5, "e", "2020-01-03 00:00:00", "1970-01-01 00:00:00", None),
        ]
    )
    compare_dataframes(df_actual, df_expected)


def initial_add(context: Context, environment: str):
    assert not context.state_reader.get_environment(environment)

    plan = context.plan(environment, start=start(context), create_from="nonexistent_env")
    validate_plan_changes(plan, added=set(context.models) | set(context.standalone_audits))

    context.apply(plan)
    validate_apply_basics(context, environment, plan.snapshots)


def apply_to_environment(
    context: Context,
    environment: str,
    choice: t.Optional[SnapshotChangeCategory] = None,
    plan_validators: t.Optional[t.Iterable[t.Callable]] = None,
    apply_validators: t.Optional[t.Iterable[t.Callable]] = None,
    plan_start: t.Optional[TimeLike] = None,
):
    plan_validators = plan_validators or []
    apply_validators = apply_validators or []

    plan = context.plan(
        environment,
        start=plan_start or start(context) if environment != c.PROD else None,
        forward_only=choice == SnapshotChangeCategory.FORWARD_ONLY,
        no_prompts=True,
        include_unmodified=True,
    )
    if environment != c.PROD:
        plan.start = plan_start or start(context)

    if choice:
        plan_choice(plan, choice)
    for validator in plan_validators:
        validator(context, plan)

    context.apply(plan)
    validate_apply_basics(context, environment, plan.snapshots)
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
    added: t.Optional[t.Iterable[str]] = None,
    modified: t.Optional[t.Iterable[str]] = None,
    removed: t.Optional[t.Iterable[str]] = None,
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
    context: Context, environment: str, snapshots: t.Iterable[Snapshot]
) -> None:
    validate_snapshots_in_state_sync(snapshots, context)
    validate_state_sync_environment(snapshots, environment, context)
    validate_tables(snapshots, context)
    validate_environment_views(snapshots, environment, context)


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


def validate_tables(snapshots: t.Iterable[Snapshot], context: Context) -> None:
    adapter = context.engine_adapter
    for snapshot in snapshots:
        if not snapshot.is_model:
            continue
        table_should_exist = not snapshot.is_symbolic
        assert adapter.table_exists(snapshot.table_name()) == table_should_exist
        if table_should_exist:
            assert select_all(snapshot.table_name(), adapter)


def validate_environment_views(
    snapshots: t.Iterable[Snapshot], environment: str, context: Context
) -> None:
    adapter = context.engine_adapter
    for snapshot in snapshots:
        if not snapshot.is_model or snapshot.is_symbolic:
            continue
        view_name = snapshot.qualified_view_name.for_environment(
            EnvironmentNamingInfo(
                name=environment, suffix_target=context.config.environment_suffix_target
            )
        )

        is_deployable = environment == c.PROD or not snapshot.is_paused_forward_only

        assert adapter.table_exists(view_name)
        assert select_all(snapshot.table_name(is_deployable), adapter) == select_all(
            view_name, adapter
        )


def select_all(table: str, adapter: EngineAdapter) -> t.Iterable:
    return adapter.fetchall(f"select * from {table}")


def snapshots_to_versions(snapshots: t.Dict[str, Snapshot]) -> t.Dict[str, str]:
    return {k: v.version or "" for k, v in snapshots.items()}


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
