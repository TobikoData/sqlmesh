import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.model import FullKind, SqlModel, ViewKind
from sqlmesh.core.plan import (
    BuiltInPlanEvaluator,
    Plan,
    PlanBuilder,
    update_intervals_for_new_snapshots,
)
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.date import to_timestamp


@pytest.fixture
def sushi_plan(sushi_context: Context, mocker: MockerFixture) -> Plan:
    mock_prompt = mocker.Mock()
    mock_prompt.ask.return_value = "2022-01-01"
    mocker.patch("sqlmesh.core.console.Prompt", mock_prompt)

    return PlanBuilder(
        sushi_context._context_diff("dev"),
        sushi_context.engine_adapter.SCHEMA_DIFFER,
        is_dev=True,
        include_unmodified=True,
    ).build()


@pytest.mark.slow
def test_builtin_evaluator_push(sushi_context: Context, make_snapshot):
    new_model = SqlModel(
        name="sushi.new_test_model",
        kind=FullKind(),
        owner="jen",
        cron="@daily",
        start="2020-01-01",
        query=parse_one("SELECT 1::INT AS one"),
        default_catalog="memory",
    )
    new_view_model = SqlModel(
        name="sushi.new_test_view_model",
        kind=ViewKind(),
        owner="jen",
        start="2020-01-01",
        query=parse_one("SELECT 1::INT AS one FROM sushi.new_test_model, sushi.waiters"),
        default_catalog="memory",
    )

    sushi_context.upsert_model(new_model)
    sushi_context.upsert_model(new_view_model)

    new_model_snapshot = sushi_context.get_snapshot(new_model, raise_if_missing=True)
    new_view_model_snapshot = sushi_context.get_snapshot(new_view_model, raise_if_missing=True)

    new_model_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)
    new_view_model_snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    plan = PlanBuilder(
        sushi_context._context_diff("prod"), sushi_context.engine_adapter.SCHEMA_DIFFER
    ).build()

    evaluator = BuiltInPlanEvaluator(
        sushi_context.state_sync,
        sushi_context.snapshot_evaluator,
        sushi_context.create_scheduler,
        sushi_context.default_catalog,
        console=sushi_context.console,
    )
    evaluator._push(plan.to_evaluatable(), plan.snapshots)

    assert (
        len(sushi_context.state_sync.get_snapshots([new_model_snapshot, new_view_model_snapshot]))
        == 2
    )
    assert sushi_context.engine_adapter.table_exists(new_model_snapshot.table_name())
    assert sushi_context.engine_adapter.table_exists(new_view_model_snapshot.table_name())


@pytest.mark.parametrize(
    "change_category", [SnapshotChangeCategory.BREAKING, SnapshotChangeCategory.FORWARD_ONLY]
)
def test_update_intervals_for_new_snapshots(
    sushi_context: Context,
    mocker: MockerFixture,
    change_category: SnapshotChangeCategory,
    make_snapshot,
):
    model = SqlModel(
        name="sushi.new_test_model",
        query=parse_one("SELECT 1::INT AS one"),
    )
    snapshot = make_snapshot(model)
    snapshot.categorize_as(change_category)

    snapshot.add_interval("2023-01-01", "2023-01-01")

    state_sync_mock = mocker.Mock()
    state_sync_mock.refresh_snapshot_intervals.return_value = [snapshot]

    update_intervals_for_new_snapshots([snapshot], state_sync_mock)

    state_sync_mock.refresh_snapshot_intervals.assert_called_once_with([snapshot])

    if change_category == SnapshotChangeCategory.FORWARD_ONLY:
        assert snapshot.dev_intervals == [(to_timestamp("2023-01-01"), to_timestamp("2023-01-02"))]
        expected_intervals = snapshot.snapshot_intervals
        expected_intervals.intervals.clear()
        state_sync_mock.add_snapshots_intervals.assert_called_once_with([expected_intervals])
    else:
        assert not snapshot.dev_intervals
        state_sync_mock.add_snapshots_intervals.assert_not_called()
