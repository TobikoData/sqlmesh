from __future__ import annotations

import datetime
import typing as t
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from unittest import mock

import duckdb
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, maybe_parse
from sqlglot.helper import ensure_list

from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.plan import BuiltInPlanEvaluator, Plan
from sqlmesh.core.snapshot import Node, Snapshot
from sqlmesh.utils import random_id
from sqlmesh.utils.date import TimeLike, to_date, to_ds

pytest_plugins = ["tests.common_fixtures"]

T = t.TypeVar("T", bound=EngineAdapter)


class DuckDBMetadata:
    def __init__(self, engine_adapter: EngineAdapter):
        assert engine_adapter.dialect == "duckdb"
        self.engine_adapter = engine_adapter

    @classmethod
    def from_context(cls, context: Context):
        return cls(engine_adapter=context.engine_adapter)

    @property
    def tables(self) -> t.List[exp.Table]:
        qualified_tables = self.qualified_tables
        for table in qualified_tables:
            table.set("db", None)
        return qualified_tables

    @property
    def qualified_tables(self) -> t.List[exp.Table]:
        return [
            exp.to_table(x, dialect="duckdb")
            for x in self._get_single_col(
                f"SELECT table_schema || '.' || table_name as qualified_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND {self._system_schema_filter('table_schema')}",
                "qualified_name",
            )
        ]

    @property
    def views(self) -> t.List[exp.Table]:
        qualified_views = self.qualified_views
        for view in qualified_views:
            view.set("db", None)
        return qualified_views

    @property
    def qualified_views(self) -> t.List[exp.Table]:
        return [
            exp.to_table(x, dialect="duckdb")
            for x in self._get_single_col(
                f"SELECT table_schema || '.' || table_name as qualified_name FROM information_schema.tables WHERE table_type = 'VIEW' AND {self._system_schema_filter('table_schema')}",
                "qualified_name",
            )
        ]

    @property
    def schemas(self) -> t.List[str]:
        return self._get_single_col(
            f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'memory' and {self._system_schema_filter('schema_name')}",
            "schema_name",
        )

    def _system_schema_filter(self, col: str) -> str:
        return f"{col} not in ('information_schema', 'pg_catalog', 'main')"

    def _get_single_col(self, query: str, col: str) -> t.List[t.Any]:
        return list(self.engine_adapter.fetchdf(query)[col].to_dict().values())


class SushiDataValidator:
    def __init__(self, engine_adapter: EngineAdapter):
        self.engine_adapter = engine_adapter

    @classmethod
    def from_context(cls, context: Context):
        return cls(engine_adapter=context.engine_adapter)

    def validate(
        self, model_name: str, start: TimeLike, end: TimeLike, *, env_name: t.Optional[str] = None
    ) -> t.Dict[t.Any, t.Any]:
        """
        Both start and end are inclusive.
        """
        if model_name == "sushi.customer_revenue_lifetime":
            env_name = f"__{env_name}" if env_name else ""
            full_table_path = f"sushi{env_name}.customer_revenue_lifetime"
            query = f"SELECT ds, count(*) AS the_count FROM {full_table_path} group by 1 order by 2 desc, 1 desc"
            results = self.engine_adapter.fetchdf(query).to_dict()
            start_date, end_date = to_date(start), to_date(end)
            num_days_diff = (end_date - start_date).days + 1
            assert len(results["ds"]) == num_days_diff
            assert list(results["ds"].values()) == [
                to_ds(end_date - datetime.timedelta(days=x)) for x in range(num_days_diff)
            ]
            return results
        else:
            raise NotImplementedError(f"Unknown model_name: {model_name}")


# Ignore all local config files
@pytest.fixture(scope="session", autouse=True)
def ignore_local_config_files():
    mock_home = mock.Mock()
    mock_home.return_value = Path(TemporaryDirectory().name)
    with mock.patch("pathlib.Path.home", mock_home):
        yield


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
    push_plan(context, plan)
    return context


@pytest.fixture()
def sushi_context(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi", mocker)
    context.apply(plan)
    return context


@pytest.fixture()
def sushi_dbt_context(mocker: MockerFixture) -> Context:
    context, plan = init_and_plan_context("examples/sushi_dbt", mocker)

    context.apply(plan)
    return context


@pytest.fixture()
def sushi_test_dbt_context(mocker: MockerFixture) -> Context:
    from tests.fixtures.dbt.sushi_test.seed_sources import init_raw_schema

    context, plan = init_and_plan_context("tests/fixtures/dbt/sushi_test", mocker)
    init_raw_schema(context.engine_adapter)

    context.apply(plan)
    return context


def init_and_plan_context(
    paths: str | t.List[str],
    mocker: MockerFixture,
    config="test_config",
) -> t.Tuple[Context, Plan]:
    delete_cache(paths)
    sushi_context = Context(paths=paths, config=config)
    confirm = mocker.patch("sqlmesh.core.console.Confirm")
    confirm.ask.return_value = False

    plan = sushi_context.plan("prod")

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
    def _make_function(node: Node, version: t.Optional[str] = None, **kwargs) -> Snapshot:
        return Snapshot.from_node(
            node,
            **{  # type: ignore
                "nodes": {},
                "ttl": "in 1 week",
                **kwargs,
            },
            version=version,
        )

    return _make_function


@pytest.fixture
def random_name() -> t.Callable:
    return lambda: f"generated_{random_id()}"


@pytest.fixture
def sushi_data_validator(sushi_context: Context) -> SushiDataValidator:
    return SushiDataValidator.from_context(sushi_context)


@pytest.fixture
def sushi_fixed_date_data_validator(sushi_context_fixed_date: Context) -> SushiDataValidator:
    return SushiDataValidator.from_context(sushi_context_fixed_date)


@pytest.fixture
def make_mocked_engine_adapter(mocker: MockerFixture) -> t.Callable:
    def _make_function(klass: t.Type[T], dialect: t.Optional[str] = None) -> T:
        connection_mock = mocker.NonCallableMock()
        cursor_mock = mocker.Mock()
        connection_mock.cursor.return_value = cursor_mock
        cursor_mock.connection.return_value = connection_mock
        return klass(lambda: connection_mock, dialect=dialect or klass.DIALECT)

    return _make_function


def delete_cache(project_paths: str | t.List[str]) -> None:
    for path in ensure_list(project_paths):
        try:
            rmtree(path + "/.cache")
        except FileNotFoundError:
            pass
