from __future__ import annotations

import typing as t
from shutil import rmtree

import duckdb
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, maybe_parse
from sqlglot.helper import ensure_list

from sqlmesh.core.context import Context
from sqlmesh.core.model import Model
from sqlmesh.core.plan import BuiltInPlanEvaluator, Plan
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils import random_id
from sqlmesh.utils.date import TimeLike

pytest_plugins = ["tests.common_fixtures"]


@pytest.fixture
def context(tmpdir) -> Context:
    return Context(paths=str(tmpdir))


@pytest.fixture
def duck_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def push_plan(context: Context, plan: Plan) -> None:
    plan_evaluator = BuiltInPlanEvaluator(context.state_sync, context.snapshot_evaluator)
    plan_evaluator._push(plan)
    plan_evaluator._promote(plan)


@pytest.fixture()
def sushi_context_pre_scheduling(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi", mocker)
    push_plan(context, plan)
    return context


@pytest.fixture()
def sushi_context_fixed_date(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi", mocker)

    for model in context.models.values():
        if model.start:
            context.upsert_model(model.name, start="2022-01-01")

    plan = context.plan("prod")
    plan.set_start("1 week ago")
    push_plan(context, plan)
    return context


@pytest.fixture()
def sushi_context(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)
    return context


@pytest.fixture()
def sushi_dbt_context(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi_dbt", mocker, "Jan 1 2022")

    context.apply(plan)
    return context


@pytest.fixture()
def sushi_test_dbt_context(mocker: MockerFixture) -> Context:
    from tests.fixtures.dbt.sushi_test.seed_sources import init_raw_schema

    context, plan = init_and_plan_context("tests/fixtures/dbt/sushi_test", mocker, "Jan 1 2022")
    init_raw_schema(context.engine_adapter)

    context.apply(plan)
    return context


def init_and_plan_context(
    paths: str | t.List[str], mocker: MockerFixture, start: TimeLike = "1 week ago"
) -> t.Tuple[Context, Plan]:
    delete_cache(paths)
    sushi_context = Context(paths=paths, config="test_config")
    confirm = mocker.patch("sqlmesh.core.console.Confirm")
    confirm.ask.return_value = False

    plan = sushi_context.plan("prod")
    plan.set_start(start)

    return (sushi_context, plan)


@pytest.fixture
def assert_exp_eq() -> t.Callable:
    def _assert_exp_eq(source: exp.Expression | str, expected: exp.Expression | str) -> None:
        source_exp = maybe_parse(source)
        expected_exp = maybe_parse(expected)

        if not source_exp:
            raise ValueError(f"Could not parse {source}")
        if not expected_exp:
            raise ValueError(f"Could not parse {expected}")

        if source_exp != expected_exp:
            assert source_exp.sql(pretty=True) == expected_exp.sql(pretty=True)
            assert source_exp == expected_exp

    return _assert_exp_eq


@pytest.fixture
def make_snapshot() -> t.Callable:
    def _make_function(model: Model, version: t.Optional[str] = None, **kwargs) -> Snapshot:
        return Snapshot.from_model(
            model,
            **{  # type: ignore
                "physical_schema": "sqlmesh",
                "models": {},
                "ttl": "in 1 week",
                **kwargs,
            },
            version=version,
        )

    return _make_function


@pytest.fixture
def random_name() -> t.Callable:
    return lambda: f"generated_{random_id()}"


def delete_cache(project_paths: str | t.List[str]) -> None:
    for path in ensure_list(project_paths):
        try:
            rmtree(path + "/.cache")
        except FileNotFoundError:
            pass
