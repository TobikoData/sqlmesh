from __future__ import annotations

import datetime
import typing as t
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import PropertyMock

import duckdb
import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, maybe_parse, parse_one
from sqlglot.helper import ensure_list

from sqlmesh.core.context import Context
from sqlmesh.core.engine_adapter.base import EngineAdapter
from sqlmesh.core.macros import macro
from sqlmesh.core.model import model
from sqlmesh.core.plan import BuiltInPlanEvaluator, Plan
from sqlmesh.core.snapshot import Node, Snapshot
from sqlmesh.utils import random_id
from sqlmesh.utils.date import TimeLike, to_date

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
            table.set("catalog", None)
        return qualified_tables

    @property
    def qualified_tables(self) -> t.List[exp.Table]:
        return [
            exp.to_table(x, dialect="duckdb")
            for x in self._get_single_col(
                f"SELECT table_catalog || '.' || table_schema || '.' || table_name as qualified_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND {self._system_schema_filter('table_schema')}",
                "qualified_name",
            )
        ]

    @property
    def views(self) -> t.List[exp.Table]:
        qualified_views = self.qualified_views
        for view in qualified_views:
            view.set("db", None)
            view.set("catalog", None)
        return qualified_views

    @property
    def qualified_views(self) -> t.List[exp.Table]:
        return [
            exp.to_table(x, dialect="duckdb")
            for x in self._get_single_col(
                f"SELECT table_catalog || '.' || table_schema || '.' || table_name as qualified_name FROM information_schema.tables WHERE table_type = 'VIEW' AND {self._system_schema_filter('table_schema')}",
                "qualified_name",
            )
        ]

    @property
    def schemas(self) -> t.List[str]:
        return self._get_single_col(
            f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{self.engine_adapter.get_current_catalog()}' and {self._system_schema_filter('schema_name')}",
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
        self,
        model_name: str,
        start: TimeLike,
        end: TimeLike,
        *,
        env_name: t.Optional[str] = None,
        dialect: t.Optional[str] = None,
    ) -> t.Dict[t.Any, t.Any]:
        """
        Both start and end are inclusive.
        """
        if model_name == "sushi.customer_revenue_lifetime":
            env_name = f"__{env_name}" if env_name else ""
            full_table_path = f"sushi{env_name}.customer_revenue_lifetime"
            query = f"SELECT event_date, count(*) AS the_count FROM {full_table_path} group by event_date order by 2 desc, 1 desc"
            results = self.engine_adapter.fetchdf(
                parse_one(query), quote_identifiers=True
            ).to_dict()
            start_date, end_date = to_date(start), to_date(end)
            num_days_diff = (end_date - start_date).days + 1
            assert len(results["event_date"]) == num_days_diff

            # this creates Pandas Timestamp objects
            expected_dates = [
                pd.to_datetime(end_date - datetime.timedelta(days=x)) for x in range(num_days_diff)
            ]
            # all engines but duckdb fetch dates as datetime.date objects
            if dialect and dialect != "duckdb":
                expected_dates = [x.date() for x in expected_dates]  # type: ignore
            assert list(results["event_date"].values()) == expected_dates

            return results
        else:
            raise NotImplementedError(f"Unknown model_name: {model_name}")


def pytest_collection_modifyitems(items, *args, **kwargs):
    test_type_markers = {"unit", "integration", "docker", "remote"}
    for item in items:
        for marker in item.iter_markers():
            if marker.name in test_type_markers:
                break
        else:
            # if no test type marker is found, assume unit test
            item.add_marker("unit")


# Ignore all local config files
@pytest.fixture(scope="session", autouse=True)
def ignore_local_config_files():
    with mock.patch("sqlmesh.core.constants.SQLMESH_PATH", Path(TemporaryDirectory().name)):
        yield


@pytest.fixture(scope="module", autouse=True)
def rescope_global_macros(request):
    existing_registry = macro.get_registry().copy()
    yield
    macro.set_registry(existing_registry)


@pytest.fixture(scope="module", autouse=True)
def rescope_global_models(request):
    existing_registry = model.get_registry().copy()
    yield
    model.set_registry(existing_registry)


@pytest.fixture
def duck_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def push_plan(context: Context, plan: Plan) -> None:
    plan_evaluator = BuiltInPlanEvaluator(
        context.state_sync, context.snapshot_evaluator, context.default_catalog
    )
    plan_evaluator._push(plan)
    promotion_result = plan_evaluator._promote(plan)
    plan_evaluator._update_views(plan, promotion_result)


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


@pytest.fixture()
def sushi_no_default_catalog(mocker: MockerFixture) -> Context:
    mocker.patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.default_catalog",
        PropertyMock(return_value=None),
    )
    context, plan = init_and_plan_context("examples/sushi", mocker)
    assert context.default_catalog is None
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


@pytest.fixture
def make_temp_table_name(mocker: MockerFixture) -> t.Callable:
    def _make_function(table_name: str, random_id: str) -> exp.Table:
        temp_table = exp.to_table(table_name)
        temp_table.set("this", exp.to_identifier(f"__temp_{temp_table.name}_{random_id}"))
        return temp_table

    return _make_function
