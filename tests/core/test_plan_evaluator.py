import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

from sqlmesh.core.context import Context
from sqlmesh.core.model import FullKind, SqlModel, ViewKind
from sqlmesh.core.plan import (
    AirflowPlanEvaluator,
    BuiltInPlanEvaluator,
    MWAAPlanEvaluator,
    Plan,
    PlanBuilder,
    update_intervals_for_new_snapshots,
)
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.date import to_timestamp
from sqlmesh.utils.errors import SQLMeshError


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


def test_airflow_evaluator(sushi_plan: Plan, mocker: MockerFixture):
    airflow_client_mock = mocker.Mock()
    airflow_client_mock.wait_for_dag_run_completion.return_value = True
    airflow_client_mock.wait_for_first_dag_run.return_value = "test_plan_application_dag_run_id"

    evaluatable_plan = sushi_plan.to_evaluatable()

    evaluator = AirflowPlanEvaluator(airflow_client_mock)
    evaluator.evaluate(evaluatable_plan)

    airflow_client_mock.apply_plan.assert_called_once_with(
        evaluatable_plan,
        notification_targets=[],
        backfill_concurrent_tasks=1,
        ddl_concurrent_tasks=1,
        users=[],
    )

    airflow_client_mock.wait_for_dag_run_completion.assert_called_once()
    airflow_client_mock.wait_for_first_dag_run.assert_called_once()


def test_airflow_evaluator_plan_application_dag_fails(sushi_plan: Plan, mocker: MockerFixture):
    airflow_client_mock = mocker.Mock()
    airflow_client_mock.wait_for_dag_run_completion.return_value = False
    airflow_client_mock.wait_for_first_dag_run.return_value = "test_plan_application_dag_run_id"

    evaluator = AirflowPlanEvaluator(airflow_client_mock)

    with pytest.raises(SQLMeshError):
        evaluator.evaluate(sushi_plan.to_evaluatable())

    airflow_client_mock.apply_plan.assert_called_once()
    airflow_client_mock.wait_for_dag_run_completion.assert_called_once()
    airflow_client_mock.wait_for_first_dag_run.assert_called_once()


def test_mwaa_evaluator(sushi_plan: Plan, mocker: MockerFixture):
    mwaa_client_mock = mocker.Mock()
    mwaa_client_mock.wait_for_dag_run_completion.return_value = True
    mwaa_client_mock.wait_for_first_dag_run.return_value = "test_plan_application_dag_run_id"
    mwaa_client_mock.set_variable.return_value = "", ""

    state_sync_mock = mocker.Mock()

    plan_dag_spec_mock = mocker.Mock()

    create_plan_dag_spec_mock = mocker.patch("sqlmesh.schedulers.airflow.plan.create_plan_dag_spec")
    create_plan_dag_spec_mock.return_value = plan_dag_spec_mock

    plan_dag_state_mock = mocker.Mock()
    mocker.patch(
        "sqlmesh.schedulers.airflow.plan.PlanDagState.from_state_sync",
        return_value=plan_dag_state_mock,
    )

    evaluator = MWAAPlanEvaluator(mwaa_client_mock, state_sync_mock)
    evaluator.evaluate(sushi_plan.to_evaluatable())

    plan_dag_state_mock.add_dag_spec.assert_called_once_with(plan_dag_spec_mock)

    mwaa_client_mock.wait_for_dag_run_completion.assert_called_once()
    mwaa_client_mock.wait_for_first_dag_run.assert_called_once()


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


def test_state_based_airflow_evaluator_with_restatements(
    sushi_context: Context, mocker: MockerFixture
):
    model_fqn = sushi_context.get_model("sushi.waiter_revenue_by_day").fqn
    downstream_model_fqn = sushi_context.get_model("sushi.top_waiters").fqn

    plan = PlanBuilder(
        sushi_context._context_diff("prod"),
        sushi_context.engine_adapter.SCHEMA_DIFFER,
        restate_models=[sushi_context.get_model("sushi.waiter_revenue_by_day").fqn],
    ).build()

    mwaa_client_mock = mocker.Mock()
    mwaa_client_mock.wait_for_dag_run_completion.return_value = True
    mwaa_client_mock.wait_for_first_dag_run.return_value = "test_plan_application_dag_run_id"
    mwaa_client_mock.set_variable.return_value = "", ""

    state_sync_mock = mocker.Mock()

    plan_dag_spec_mock = mocker.Mock()

    create_plan_dag_spec_mock = mocker.patch("sqlmesh.schedulers.airflow.plan.create_plan_dag_spec")
    create_plan_dag_spec_mock.return_value = plan_dag_spec_mock

    plan_dag_state_mock = mocker.Mock()
    mocker.patch(
        "sqlmesh.schedulers.airflow.plan.PlanDagState.from_state_sync",
        return_value=plan_dag_state_mock,
    )

    evaluator = MWAAPlanEvaluator(mwaa_client_mock, state_sync_mock)
    evaluator.evaluate(plan.to_evaluatable())

    plan_application_request = create_plan_dag_spec_mock.call_args[0][0]
    assert plan_application_request.plan.restatements.keys() == {model_fqn, downstream_model_fqn}
