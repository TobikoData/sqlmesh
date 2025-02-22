import logging
import pathlib
import typing as t
import re
from datetime import date, timedelta
from tempfile import TemporaryDirectory
from unittest.mock import PropertyMock, call, patch

import time_machine
import pytest
import pandas as pd
from pathlib import Path
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one, Dialect
from sqlglot.errors import SchemaError

from sqlmesh.core.config.gateway import GatewayConfig
from sqlmesh.core.config.plan import PlanConfig
import sqlmesh.core.constants
from sqlmesh.core import dialect as d, constants as c
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    EnvironmentSuffixTarget,
    ModelDefaultsConfig,
    SnowflakeConnectionConfig,
    load_configs,
)
from sqlmesh.core.context import Context
from sqlmesh.core.console import create_console
from sqlmesh.core.dialect import parse, schema_
from sqlmesh.core.engine_adapter.duckdb import DuckDBEngineAdapter
from sqlmesh.core.environment import Environment, EnvironmentNamingInfo
from sqlmesh.core.model import load_sql_based_model, model
from sqlmesh.core.renderer import render_statements
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core.plan import BuiltInPlanEvaluator, PlanBuilder
from sqlmesh.core.state_sync.cache import CachingStateSync
from sqlmesh.core.state_sync.db import EngineAdapterStateSync
from sqlmesh.utils.connection_pool import SingletonConnectionPool, ThreadLocalConnectionPool
from sqlmesh.utils.date import (
    make_inclusive_end,
    now,
    to_date,
    to_timestamp,
    yesterday_ds,
)
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.metaprogramming import Executable
from tests.utils.test_filesystem import create_temp_file


def test_global_config(copy_to_temp_path: t.Callable):
    context = Context(paths=copy_to_temp_path("examples/sushi"))
    assert context.config.dialect == "duckdb"
    assert context.config.time_column_format == "%Y-%m-%d"


def test_named_config(copy_to_temp_path: t.Callable):
    context = Context(paths=copy_to_temp_path("examples/sushi"), config="test_config")
    assert len(context.config.gateways) == 1


def test_invalid_named_config(copy_to_temp_path: t.Callable):
    with pytest.raises(
        ConfigError,
        match="Config 'blah' was not found.",
    ):
        Context(paths=copy_to_temp_path("examples/sushi"), config="blah")


def test_missing_named_config(copy_to_temp_path: t.Callable):
    with pytest.raises(ConfigError, match=r"Config 'imaginary_config' was not found."):
        Context(paths=copy_to_temp_path("examples/sushi"), config="imaginary_config")


def test_config_parameter(copy_to_temp_path: t.Callable):
    config = Config(model_defaults=ModelDefaultsConfig(dialect="presto"), project="test_project")
    context = Context(paths=copy_to_temp_path("examples/sushi"), config=config)
    assert context.config.dialect == "presto"
    assert context.config.project == "test_project"


def test_generate_table_name_in_dialect(mocker: MockerFixture):
    context = Context(config=Config(model_defaults=ModelDefaultsConfig(dialect="bigquery")))
    mocker.patch(
        "sqlmesh.core.context.GenericContext._model_tables",
        PropertyMock(return_value={'"project-id"."dataset"."table"': '"project-id".dataset.table'}),
    )
    assert (
        context.resolve_table('"project-id"."dataset"."table"') == "`project-id`.`dataset`.`table`"
    )


def test_config_not_found(copy_to_temp_path: t.Callable):
    with pytest.raises(
        ConfigError,
        match=r".*config could not be found.*",
    ):
        Context(paths="nonexistent/directory", config="config")


def test_custom_macros(sushi_context):
    assert "add_one" in sushi_context._macros


def test_dag(sushi_context):
    assert set(sushi_context.dag.upstream('"memory"."sushi"."customer_revenue_by_day"')) == {
        '"memory"."sushi"."items"',
        '"memory"."sushi"."orders"',
        '"memory"."sushi"."order_items"',
    }


@pytest.mark.slow
def test_render_sql_model(sushi_context, assert_exp_eq, copy_to_temp_path: t.Callable):
    assert_exp_eq(
        sushi_context.render(
            "sushi.waiter_revenue_by_day",
            start=date(2021, 1, 1),
            end=date(2021, 1, 1),
            expand=True,
        ),
        """
        SELECT
          CAST("o"."waiter_id" AS INT) AS "waiter_id", /* Waiter id */
          CAST(SUM("oi"."quantity" * "i"."price") AS DOUBLE) AS "revenue", /* Revenue from orders taken by this waiter */
          CAST("o"."event_date" AS DATE) AS "event_date" /* Date */
        FROM (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "customer_id",
            CAST(NULL AS INT) AS "waiter_id",
            CAST(NULL AS INT) AS "start_ts",
            CAST(NULL AS INT) AS "end_ts",
            CAST(NULL AS DATE) AS "event_date"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "o"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "order_id",
            CAST(NULL AS INT) AS "item_id",
            CAST(NULL AS INT) AS "quantity",
            CAST(NULL AS DATE) AS "event_date"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "oi"
          ON "o"."event_date" = "oi"."event_date" AND "o"."id" = "oi"."order_id"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS TEXT) AS "name",
            CAST(NULL AS DOUBLE) AS "price",
            CAST(NULL AS DATE) AS "event_date"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "i"
          ON "i"."event_date" = "oi"."event_date" AND "i"."id" = "oi"."item_id"
        WHERE
          "o"."event_date" <= CAST('2021-01-01' AS DATE)
          AND "o"."event_date" >= CAST('2021-01-01' AS DATE)
        GROUP BY
          "o"."waiter_id",
          "o"."event_date"
        """,
    )

    # unpushed render should not expand
    unpushed = Context(paths=copy_to_temp_path("examples/sushi"))
    assert_exp_eq(
        unpushed.render("sushi.waiter_revenue_by_day"),
        """
        SELECT
          CAST("o"."waiter_id" AS INT) AS "waiter_id", /* Waiter id */
          CAST(SUM("oi"."quantity" * "i"."price") AS DOUBLE) AS "revenue", /* Revenue from orders taken by this waiter */
          CAST("o"."event_date" AS DATE) AS "event_date" /* Date */
        FROM "memory"."sushi"."orders" AS "o"
        LEFT JOIN "memory"."sushi"."order_items" AS "oi"
          ON "o"."event_date" = "oi"."event_date" AND "o"."id" = "oi"."order_id"
        LEFT JOIN "memory"."sushi"."items" AS "i"
          ON "i"."event_date" = "oi"."event_date" AND "i"."id" = "oi"."item_id"
        WHERE
          "o"."event_date" <= CAST('1970-01-01' AS DATE) AND "o"."event_date" >= CAST('1970-01-01' AS DATE)
        GROUP BY
          "o"."waiter_id",
          "o"."event_date"
        """,
    )


@pytest.mark.slow
def test_render_non_deployable_parent(sushi_context, assert_exp_eq, copy_to_temp_path: t.Callable):
    model = sushi_context.get_model("sushi.waiter_revenue_by_day")
    forward_only_kind = model.kind.copy(update={"forward_only": True})
    model = model.copy(update={"kind": forward_only_kind})
    sushi_context.upsert_model(model)
    sushi_context.plan("dev", no_prompts=True, auto_apply=True)

    expected_table_name = parse_one(
        sushi_context.get_snapshot("sushi.waiter_revenue_by_day").table_name(is_deployable=False),
        into=exp.Table,
    ).this.this

    assert_exp_eq(
        sushi_context.render(
            "sushi.top_waiters",
            start=date(2021, 1, 1),
            end=date(2021, 1, 1),
        ),
        f"""
        WITH "test_macros" AS (
          SELECT
            2 AS "lit_two",
            "waiter_revenue_by_day"."revenue" * 2.0 AS "sql_exp",
            CAST("waiter_revenue_by_day"."revenue" AS TEXT) AS "sql_lit"
          FROM "memory"."sqlmesh__sushi"."{expected_table_name}" AS "waiter_revenue_by_day" /* memory.sushi.waiter_revenue_by_day */
        )
        SELECT
          CAST("waiter_revenue_by_day"."waiter_id" AS INT) AS "waiter_id",
          CAST("waiter_revenue_by_day"."revenue" AS DOUBLE) AS "revenue"
        FROM "memory"."sqlmesh__sushi"."{expected_table_name}" AS "waiter_revenue_by_day" /* memory.sushi.waiter_revenue_by_day */
        WHERE
          "waiter_revenue_by_day"."event_date" = (
            SELECT
              MAX("waiter_revenue_by_day"."event_date") AS "_col_0"
            FROM "memory"."sqlmesh__sushi"."{expected_table_name}" AS "waiter_revenue_by_day" /* memory.sushi.waiter_revenue_by_day */
          )
        ORDER BY
          "revenue" DESC
        LIMIT 10
        """,
    )


@pytest.mark.slow
def test_render_seed_model(sushi_context, assert_exp_eq):
    assert_exp_eq(
        sushi_context.render(
            "sushi.waiter_names",
            start=date(2021, 1, 1),
            end=date(2021, 1, 1),
        ),
        """
        SELECT
          CAST(id AS BIGINT) AS id,
          CAST(name AS TEXT) AS name
        FROM (VALUES
          (0, 'Toby'),
          (1, 'Tyson'),
          (2, 'Ryan'),
          (3, 'George'),
          (4, 'Chris')) AS t(id, name)
        """,
    )


@pytest.mark.slow
def test_diff(sushi_context: Context, mocker: MockerFixture):
    mock_console = mocker.Mock()
    sushi_context.console = mock_console
    yesterday = yesterday_ds()
    success = sushi_context.run(start=yesterday, end=yesterday)

    plan_evaluator = BuiltInPlanEvaluator(
        sushi_context.state_sync,
        sushi_context.snapshot_evaluator,
        sushi_context.create_scheduler,
        sushi_context.default_catalog,
    )

    plan = PlanBuilder(
        context_diff=sushi_context._context_diff("prod"),
        engine_schema_differ=sushi_context.engine_adapter.SCHEMA_DIFFER,
    ).build()

    # stringify used to trigger an unhashable exception due to
    # https://github.com/pydantic/pydantic/issues/8016
    assert str(plan) != ""

    promotion_result = plan_evaluator._promote(plan.to_evaluatable(), plan.snapshots)
    plan_evaluator._update_views(plan.to_evaluatable(), plan.snapshots, promotion_result)

    sushi_context.upsert_model("sushi.customers", query=parse_one("select 1 as customer_id"))
    sushi_context.diff("test")
    assert mock_console.show_model_difference_summary.called
    assert success


@pytest.mark.slow
def test_evaluate_limit():
    context = Context(config=Config())

    context.upsert_model(
        load_sql_based_model(
            parse(
                """
        MODEL(name with_limit, kind FULL);
        SELECT t.v as v FROM (VALUES (1), (2), (3), (4), (5)) AS t(v) LIMIT 1 + 2"""
            )
        )
    )

    assert context.evaluate("with_limit", "2020-01-01", "2020-01-02", "2020-01-02").size == 3
    assert context.evaluate("with_limit", "2020-01-01", "2020-01-02", "2020-01-02", 4).size == 3
    assert context.evaluate("with_limit", "2020-01-01", "2020-01-02", "2020-01-02", 2).size == 2

    context.upsert_model(
        load_sql_based_model(
            parse(
                """
        MODEL(name without_limit, kind FULL);
        SELECT t.v as v FROM (VALUES (1), (2), (3), (4), (5)) AS t(v)"""
            )
        )
    )

    assert context.evaluate("without_limit", "2020-01-01", "2020-01-02", "2020-01-02").size == 5
    assert context.evaluate("without_limit", "2020-01-01", "2020-01-02", "2020-01-02", 4).size == 4
    assert context.evaluate("without_limit", "2020-01-01", "2020-01-02", "2020-01-02", 2).size == 2


def test_gateway_specific_adapters(copy_to_temp_path, mocker):
    path = copy_to_temp_path("examples/sushi")
    ctx = Context(paths=path, config="isolated_systems_config", gateway="prod")
    assert len(ctx._engine_adapters) == 1
    assert ctx.engine_adapter == ctx._engine_adapters["prod"]

    with pytest.raises(SQLMeshError):
        assert ctx._get_engine_adapter("non_existing")

    # This will create the requested engine adapter
    assert ctx._get_engine_adapter("dev") == ctx._engine_adapters["dev"]

    ctx = Context(paths=path, config="isolated_systems_config")
    assert len(ctx._engine_adapters) == 1
    assert ctx.engine_adapter == ctx._engine_adapters["dev"]

    mocker.patch.object(
        Context,
        "_snapshot_gateways",
        new_callable=mocker.PropertyMock(return_value={"test_snapshot": "test"}),
    )

    ctx = Context(paths=path, config="isolated_systems_config")

    assert len(ctx.engine_adapters) == 3
    assert ctx.engine_adapter == ctx._get_engine_adapter()
    assert ctx._get_engine_adapter("test") == ctx._engine_adapters["test"]


def test_multiple_gateways(tmp_path: Path):
    db_path = str(tmp_path / "db.db")
    gateways = {
        "staging": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
        "final": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
    }

    config = Config(gateways=gateways, default_gateway="final")
    context = Context(config=config)

    gateway_model = load_sql_based_model(
        parse(
            """
    MODEL(name staging.stg_model, start '2024-01-01',kind FULL, gateway staging);
    SELECT t.v as v FROM (VALUES (1), (2), (3), (4), (5)) AS t(v)"""
        ),
        default_catalog="db",
    )

    assert gateway_model.gateway == "staging"
    context.upsert_model(gateway_model)
    assert context.evaluate("staging.stg_model", "2020-01-01", "2020-01-02", "2020-01-02").size == 5

    default_model = load_sql_based_model(
        parse(
            """
    MODEL(name main.final_model, start '2024-01-01',kind FULL);
    SELECT v FROM staging.stg_model"""
        ),
        default_catalog="db",
    )

    assert not default_model.gateway
    context.upsert_model(default_model)

    context.plan(
        execution_time="2024-01-02",
        auto_apply=True,
        no_prompts=True,
    )

    sorted_snapshots = sorted(context.snapshots.values())

    physical_schemas = [snapshot.physical_schema for snapshot in sorted_snapshots]
    assert physical_schemas == ["sqlmesh__main", "sqlmesh__staging"]

    view_schemas = [snapshot.qualified_view_name.schema_name for snapshot in sorted_snapshots]
    assert view_schemas == ["main", "staging"]

    assert (
        str(context.fetchdf("select * from staging.stg_model"))
        == "   v\n0  1\n1  2\n2  3\n3  4\n4  5"
    )
    assert str(context.fetchdf("select * from final_model")) == "   v\n0  1\n1  2\n2  3\n3  4\n4  5"

    assert (
        context.snapshots['"db"."main"."final_model"'].parents[0].name
        == '"db"."staging"."stg_model"'
    )
    assert context.dag._sorted == ['"db"."staging"."stg_model"', '"db"."main"."final_model"']


def test_plan_execution_time():
    context = Context(config=Config())
    context.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL(
                    name db.x,
                    start '2024-01-01',
                    kind FULL
                );

                SELECT @execution_date AS execution_date
                """
            )
        )
    )

    context.plan(
        "dev",
        execution_time="2024-01-02",
        auto_apply=True,
        no_prompts=True,
    )
    assert (
        str(list(context.fetchdf("select * from db__dev.x")["execution_date"])[0])
        == "2024-01-02 00:00:00"
    )


def test_override_builtin_audit_blocking_mode():
    context = Context(config=Config())
    context.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL(
                    name db.x,
                    kind FULL,
                    audits (
                        not_null(columns := [c], blocking := false),
                        unique_values(columns := [c]),
                    )
                );

                SELECT NULL AS c
                """
            )
        )
    )

    with patch.object(context.console, "log_warning") as mock_logger:
        plan = context.plan(auto_apply=True, no_prompts=True)
        new_snapshot = next(iter(plan.context_diff.new_snapshots.values()))

        assert mock_logger.call_args_list[0][0][0] == "\n'not_null' audit error: 1 row failed."

    # Even though there are two builtin audits referenced in the above definition, we only
    # store the one that overrides `blocking` in the snapshot; the other one isn't needed
    audits_with_args = new_snapshot.model.audits_with_args
    assert len(audits_with_args) == 2
    audit, args = audits_with_args[0]
    assert audit.name == "not_null"
    assert list(args) == ["columns", "blocking"]

    context = Context(config=Config())
    context.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL(
                    name db.x,
                    kind FULL,
                    audits (
                        not_null_non_blocking(columns := [c], blocking := true)
                    )
                );

                SELECT NULL AS c
                """
            )
        )
    )

    with pytest.raises(SQLMeshError):
        context.plan(auto_apply=True, no_prompts=True)


def test_python_model_empty_df_raises(sushi_context, capsys):
    sushi_context.console = create_console()

    @model(
        "memory.sushi.test_model",
        columns={"col": "int"},
        kind=dict(name=ModelKindName.FULL),
    )
    def entrypoint(context, **kwargs):
        yield pd.DataFrame({"col": []})

    test_model = model.get_registry()["memory.sushi.test_model"].model(
        module_path=Path("."), path=Path(".")
    )
    sushi_context.upsert_model(test_model)

    capsys.readouterr()
    with pytest.raises(SQLMeshError):
        sushi_context.plan(no_prompts=True, auto_apply=True)

    assert (
        "Cannot construct source query from an empty DataFrame. This error is commonly related to Python models that produce no data. For such models, consider yielding from an empty generator if the resulting set is empty, i.e. use"
    ) in capsys.readouterr().out.replace("\n", "")


def test_env_and_default_schema_normalization(mocker: MockerFixture):
    from sqlglot.dialects import DuckDB
    from sqlglot.dialects.dialect import NormalizationStrategy

    mocker.patch.object(DuckDB, "NORMALIZATION_STRATEGY", NormalizationStrategy.UPPERCASE)

    context = Context(config=Config())
    context.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL(
                    name x,
                    kind FULL
                );

                SELECT 1 AS c
                """
            )
        )
    )
    context.plan("dev", auto_apply=True, no_prompts=True)
    assert list(context.fetchdf('select c from "DEFAULT__DEV"."X"')["c"])[0] == 1


def test_clear_caches(tmp_path: pathlib.Path):
    models_dir = tmp_path / "models"

    cache_dir = models_dir / sqlmesh.core.constants.CACHE
    cache_dir.mkdir(parents=True)
    model_cache_file = cache_dir / "test.txt"
    model_cache_file.write_text("test")

    assert model_cache_file.exists()
    assert len(list(cache_dir.iterdir())) == 1

    context = Context(config=Config(), paths=str(models_dir))
    context.clear_caches()

    assert not cache_dir.exists()
    assert models_dir.exists()


def test_ignore_files(mocker: MockerFixture, tmp_path: pathlib.Path):
    mocker.patch.object(
        sqlmesh.core.constants,
        "IGNORE_PATTERNS",
        [".ipynb_checkpoints/*.sql"],
    )

    models_dir = pathlib.Path("models")
    macros_dir = pathlib.Path("macros")

    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "ignore", "ignore_model.sql"),
        "MODEL(name ignore.ignore_model); SELECT 1 AS cola",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "ignore", "inner_ignore", "inner_ignore_model.sql"),
        "MODEL(name ignore.inner_ignore_model); SELECT 1 AS cola",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "macro_ignore.py"),
        """
from sqlmesh.core.macros import macro

@macro()
def test():
    return "test"
""",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, ".ipynb_checkpoints", "ignore_model2.sql"),
        "MODEL(name ignore_model2); SELECT cola::bigint AS cola FROM db.other_table",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "db", "actual_test.sql"),
        "MODEL(name db.actual_test, kind full); SELECT 1 AS cola",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "macro_file.py"),
        """
from sqlmesh.core.macros import macro

@macro()
def test():
    return "test"
""",
    )
    config = Config(
        ignore_patterns=["models/ignore/**/*.sql", "macro_ignore.py", ".ipynb_checkpoints/*"]
    )
    context = Context(paths=tmp_path, config=config)

    assert ['"memory"."db"."actual_test"'] == list(context.models)
    assert "test" in context._macros


@pytest.mark.slow
def test_plan_apply(sushi_context_pre_scheduling) -> None:
    logger = logging.getLogger("sqlmesh.core.renderer")
    with patch.object(logger, "warning") as mock_logger:
        sushi_context_pre_scheduling.plan(
            "dev",
            auto_apply=True,
            no_prompts=True,
            include_unmodified=True,
        )
        mock_logger.assert_not_called()
    assert sushi_context_pre_scheduling.state_reader.get_environment("dev")


def test_project_config_person_config_overrides(tmp_path: pathlib.Path):
    with TemporaryDirectory() as td:
        home_path = pathlib.Path(td)
        create_temp_file(
            home_path,
            pathlib.Path("config.yaml"),
            """
gateways:
    snowflake:
        connection:
            type: snowflake
            password: XYZ
            user: ABC
""",
        )
        project_config = create_temp_file(
            tmp_path,
            pathlib.Path("config.yaml"),
            """
gateways:
    snowflake:
        connection:
            type: snowflake
            account: abc123
            user: CDE

model_defaults:
    dialect: snowflake
""",
        )
        with pytest.raises(
            ConfigError,
            match="User and password must be provided if using default authentication",
        ):
            load_configs("config", Config, paths=project_config.parent)
        loaded_configs: t.Dict[pathlib.Path, Config] = load_configs(
            "config", Config, paths=project_config.parent, sqlmesh_path=home_path
        )
        assert len(loaded_configs) == 1
        snowflake_connection = list(loaded_configs.values())[0].gateways["snowflake"].connection  # type: ignore
        assert isinstance(snowflake_connection, SnowflakeConnectionConfig)
        assert snowflake_connection.account == "abc123"
        assert snowflake_connection.user == "ABC"
        assert snowflake_connection.password == "XYZ"
        assert snowflake_connection.application == "Tobiko_SQLMesh"


@pytest.mark.slow
def test_physical_schema_override(copy_to_temp_path: t.Callable) -> None:
    def get_schemas(context: Context):
        return {
            snapshot.physical_schema for snapshot in context.snapshots.values() if snapshot.is_model
        }

    def get_view_schemas(context: Context):
        return {
            snapshot.qualified_view_name.schema_name
            for snapshot in context.snapshots.values()
            if snapshot.is_model
        }

    def get_sushi_fingerprints(context: Context):
        return {
            snapshot.fingerprint.to_identifier()
            for snapshot in context.snapshots.values()
            if snapshot.is_model and snapshot.model.schema_name == "sushi"
        }

    project_path = copy_to_temp_path("examples/sushi")
    no_mapping_context = Context(paths=project_path)
    assert no_mapping_context.config.physical_schema_mapping == {}
    assert get_schemas(no_mapping_context) == {"sqlmesh__sushi", "sqlmesh__raw"}
    assert get_view_schemas(no_mapping_context) == {"sushi", "raw"}
    no_mapping_fingerprints = get_sushi_fingerprints(no_mapping_context)
    context = Context(paths=project_path, config="map_config")
    assert context.config.physical_schema_mapping == {re.compile("^sushi$"): "company_internal"}
    assert get_schemas(context) == {"company_internal", "sqlmesh__raw"}
    assert get_view_schemas(context) == {"sushi", "raw"}
    sushi_fingerprints = get_sushi_fingerprints(context)
    assert (
        len(sushi_fingerprints)
        == len(no_mapping_fingerprints)
        == len(no_mapping_fingerprints - sushi_fingerprints)
    )


@pytest.mark.slow
def test_physical_schema_mapping(tmp_path: pathlib.Path) -> None:
    create_temp_file(
        tmp_path,
        pathlib.Path(pathlib.Path("models"), "a.sql"),
        "MODEL(name foo_staging.model_a); SELECT 1;",
    )

    create_temp_file(
        tmp_path,
        pathlib.Path(pathlib.Path("models"), "b.sql"),
        "MODEL(name testone.model_b); SELECT 1;",
    )

    create_temp_file(
        tmp_path,
        pathlib.Path(pathlib.Path("models"), "c.sql"),
        "MODEL(name untouched.model_c); SELECT 1;",
    )

    ctx = Context(
        config=Config(
            model_defaults=ModelDefaultsConfig(dialect="duckdb"),
            physical_schema_mapping={
                # anything ending with 'staging' becomes 'overridden_staging'
                "^.*staging$": "overridden_staging",
                # anything starting with 'test' becomes 'testing'
                "^test.*": "testing",
            },
        ),
        paths=tmp_path,
    )

    ctx.load()

    physical_schemas = [snapshot.physical_schema for snapshot in sorted(ctx.snapshots.values())]

    view_schemas = [
        snapshot.qualified_view_name.schema_name for snapshot in sorted(ctx.snapshots.values())
    ]

    assert len(physical_schemas) == len(view_schemas) == 3
    assert physical_schemas == ["overridden_staging", "testing", "sqlmesh__untouched"]
    assert view_schemas == ["foo_staging", "testone", "untouched"]


@pytest.mark.slow
def test_janitor(sushi_context, mocker: MockerFixture) -> None:
    adapter_mock = mocker.MagicMock()
    adapter_mock.dialect = "duckdb"
    state_sync_mock = mocker.MagicMock()
    state_sync_mock.delete_expired_environments.return_value = [
        Environment(
            name="test_environment",
            suffix_target=EnvironmentSuffixTarget.TABLE,
            snapshots=[x.table_info for x in sushi_context.snapshots.values()],
            start_at="2022-01-01",
            end_at="2022-01-01",
            plan_id="test_plan_id",
            previous_plan_id="test_plan_id",
        ),
        Environment(
            name="test_environment",
            suffix_target=EnvironmentSuffixTarget.SCHEMA,
            snapshots=[x.table_info for x in sushi_context.snapshots.values()],
            start_at="2022-01-01",
            end_at="2022-01-01",
            plan_id="test_plan_id",
            previous_plan_id="test_plan_id",
        ),
    ]
    sushi_context._engine_adapters = {sushi_context.config.default_gateway: adapter_mock}
    sushi_context._state_sync = state_sync_mock
    sushi_context._run_janitor()
    # Assert that the schemas are dropped just twice for the schema based environment
    # Make sure that external model schemas/tables are not dropped
    adapter_mock.drop_schema.assert_has_calls(
        [
            call(
                schema_("sushi__test_environment", "memory"),
                cascade=True,
                ignore_if_not_exists=True,
            ),
        ]
    )
    # Assert that the views are dropped for each snapshot just once and make sure that the name used is the
    # view name with the environment as a suffix
    assert adapter_mock.drop_view.call_count == 14
    adapter_mock.drop_view.assert_has_calls(
        [
            call(
                "memory.sushi.waiter_as_customer_by_day__test_environment",
                ignore_if_not_exists=True,
            ),
        ]
    )


@pytest.mark.slow
def test_plan_default_end(sushi_context_pre_scheduling: Context):
    prod_plan_builder = sushi_context_pre_scheduling.plan_builder("prod")
    # Simulate that the prod is 3 days behind.
    plan_end = to_date(now()) - timedelta(days=3)
    prod_plan_builder._end = plan_end
    prod_plan_builder.apply()

    dev_plan = sushi_context_pre_scheduling.plan(
        "test_env", no_prompts=True, include_unmodified=True, skip_backfill=True, auto_apply=True
    )
    assert dev_plan.end is not None
    assert to_date(make_inclusive_end(dev_plan.end)) == plan_end

    forward_only_dev_plan = sushi_context_pre_scheduling.plan_builder(
        "test_env_forward_only", include_unmodified=True, forward_only=True
    ).build()
    assert forward_only_dev_plan.end is not None
    assert to_date(make_inclusive_end(forward_only_dev_plan.end)) == plan_end
    assert to_timestamp(forward_only_dev_plan.start) == to_timestamp(plan_end)


@pytest.mark.slow
def test_plan_start_ahead_of_end(copy_to_temp_path):
    path = copy_to_temp_path("examples/sushi")
    with time_machine.travel("2024-01-02 00:00:00 UTC"):
        context = Context(paths=path, gateway="duckdb_persistent")
        context.plan("prod", no_prompts=True, auto_apply=True)
        assert all(
            i == to_timestamp("2024-01-02")
            for i in context.state_sync.max_interval_end_per_model("prod").values()
        )
        context.close()
    with time_machine.travel("2024-01-03 00:00:00 UTC"):
        context = Context(paths=path, gateway="duckdb_persistent")
        expression = d.parse(
            """
                MODEL(
            name sushi.hourly,
            KIND FULL,
            cron '@hourly',
            start '2024-01-02 12:00:00',
        );

        SELECT 1"""
        )
        model = load_sql_based_model(expression, default_catalog=context.default_catalog)
        context.upsert_model(model)
        context.plan("prod", no_prompts=True, auto_apply=True)
        # Since the new start is ahead of the latest end loaded for prod, the table is deployed as empty
        # This isn't considered a gap since prod has not loaded these intervals yet
        # As a results the max interval end is unchanged and the table is empty
        assert all(
            i == to_timestamp("2024-01-02")
            for i in context.state_sync.max_interval_end_per_model("prod").values()
        )
        assert context.engine_adapter.fetchone("SELECT COUNT(*) FROM sushi.hourly")[0] == 0
        context.close()


@pytest.mark.slow
def test_schema_error_no_default(sushi_context_pre_scheduling) -> None:
    context = sushi_context_pre_scheduling

    with pytest.raises(SchemaError):
        context.upsert_model(
            load_sql_based_model(
                parse(
                    """
            MODEL(name c);
            SELECT x FROM a
            """
                )
            )
        )


@pytest.mark.slow
def test_unrestorable_snapshot(sushi_context: Context) -> None:
    model_v1 = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_unrestorable);
        SELECT 1 AS one;
        """
        ),
        default_catalog=sushi_context.default_catalog,
        dialect=sushi_context.default_dialect,
    )
    model_v2 = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_unrestorable);
        SELECT 2 AS two;
        """
        ),
        default_catalog=sushi_context.default_catalog,
        dialect=sushi_context.default_dialect,
    )

    sushi_context.upsert_model(model_v1)
    sushi_context.plan(auto_apply=True, no_prompts=True)
    model_v1_old_snapshot = sushi_context.get_snapshot(
        "sushi.test_unrestorable", raise_if_missing=True
    )

    sushi_context.upsert_model(model_v2)
    sushi_context.plan(
        auto_apply=True,
        no_prompts=True,
        forward_only=True,
        allow_destructive_models=["memory.sushi.test_unrestorable"],
    )

    sushi_context.upsert_model(model_v1)
    sushi_context.plan(
        auto_apply=True,
        no_prompts=True,
        forward_only=True,
        allow_destructive_models=["memory.sushi.test_unrestorable"],
    )
    model_v1_new_snapshot = sushi_context.get_snapshot(
        "memory.sushi.test_unrestorable", raise_if_missing=True
    )

    assert (
        model_v1_new_snapshot.node.stamp
        == f"revert to {model_v1_old_snapshot.snapshot_id.identifier}"
    )
    assert model_v1_old_snapshot.snapshot_id != model_v1_new_snapshot.snapshot_id
    assert model_v1_old_snapshot.fingerprint != model_v1_new_snapshot.fingerprint


def test_default_catalog_connections(copy_to_temp_path: t.Callable):
    with patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.default_catalog",
        PropertyMock(return_value=None),
    ):
        context = Context(paths=copy_to_temp_path("examples/sushi"))
        assert context.default_catalog is None

    # Verify that providing a catalog gets set as default catalog
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="presto"),
        default_connection=DuckDBConnectionConfig(catalogs={"catalog": ":memory:"}),
    )
    context = Context(paths=copy_to_temp_path("examples/sushi"), config=config)
    assert context.default_catalog == "catalog"


def test_load_external_models(copy_to_temp_path):
    path = copy_to_temp_path("examples/sushi")

    context = Context(paths=path)

    external_model_names = [
        m.name for m in context.models.values() if m.kind.name == ModelKindName.EXTERNAL
    ]

    assert len(external_model_names) > 0

    # from default external_models.yaml in root dir
    assert "raw.demographics" in external_model_names

    # from external_models/model1.yaml
    assert "raw.model1" in external_model_names

    # from external_models/model2.yaml
    assert "raw.model2" in external_model_names

    # from external_models/prod.yaml, should not show unless --gateway=prod
    assert "prod_raw.model1" not in external_model_names

    # get physical table names of external models using table
    assert context.resolve_table("raw.model1") == '"memory"."raw"."model1"'
    assert context.resolve_table("raw.demographics") == '"memory"."raw"."demographics"'
    assert context.resolve_table("raw.model2") == '"memory"."raw"."model2"'

    with patch.object(context.console, "log_warning") as mock_logger:
        context.table("raw.model1") == '"memory"."raw"."model1"'

        assert mock_logger.mock_calls == [
            call(
                "The SQLMesh context's `table` method is deprecated and will be removed "
                "in a future release. Please use the `resolve_table` method instead."
            )
        ]


def test_load_gateway_specific_external_models(copy_to_temp_path):
    path = copy_to_temp_path("examples/sushi")

    def _get_external_model_names(gateway=None):
        context = Context(paths=path, config="isolated_systems_config", gateway=gateway)

        external_model_names = [
            m.name for m in context.models.values() if m.kind.name == ModelKindName.EXTERNAL
        ]

        assert len(external_model_names) > 0

        return external_model_names

    # default gateway is dev, prod model should not show
    assert "prod_raw.model1" not in _get_external_model_names()

    # gateway explicitly set to prod; prod model should now show
    assert "prod_raw.model1" in _get_external_model_names(gateway="prod")


def test_disabled_model(copy_to_temp_path):
    path = copy_to_temp_path("examples/sushi")

    context = Context(paths=path)

    assert (path[0] / "models" / "disabled.sql").exists()
    assert not context.get_model("sushi.disabled")

    assert (path[0] / "models" / "disabled.py").exists()
    assert not context.get_model("sushi.disabled_py")


def test_disabled_model_python_macro(sushi_context):
    @model(
        "memory.sushi.disabled_model_2",
        columns={"col": "int"},
        enabled="@IF(@gateway = 'dev', True, False)",
    )
    def entrypoint(context, **kwargs):
        yield pd.DataFrame({"col": []})

    test_model = model.get_registry()["memory.sushi.disabled_model_2"].model(
        module_path=Path("."), path=Path("."), variables={"gateway": "prod"}
    )
    assert not test_model.enabled

    with pytest.raises(
        SQLMeshError,
        match="The disabled model 'memory.sushi.disabled_model_2' cannot be upserted",
    ):
        sushi_context.upsert_model(test_model)


def test_get_model_mixed_dialects(copy_to_temp_path):
    path = copy_to_temp_path("examples/sushi")

    context = Context(paths=path)
    expression = d.parse(
        """
    MODEL(
        name sushi.snowflake_dialect,
        dialect snowflake,
    );

    SELECT 1"""
    )
    model = load_sql_based_model(expression, default_catalog=context.default_catalog)
    context.upsert_model(model)

    assert context.get_model("sushi.snowflake_dialect").dict() == model.dict()


def test_override_dialect_normalization_strategy():
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb,normalization_strategy=lowercase")
    )

    # This has the side-effect of mutating DuckDB globally to override its normalization strategy
    Context(config=config)

    from sqlglot.dialects import DuckDB
    from sqlglot.dialects.dialect import NormalizationStrategy

    assert DuckDB.NORMALIZATION_STRATEGY == NormalizationStrategy.LOWERCASE

    # The above change is applied globally so we revert it to avoid breaking other tests
    DuckDB.NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_INSENSITIVE


def test_different_gateway_normalization_strategy(tmp_path: pathlib.Path):
    config = Config(
        gateways={
            "duckdb": GatewayConfig(
                connection=DuckDBConnectionConfig(database="db.db"),
                model_defaults=ModelDefaultsConfig(
                    dialect="snowflake, normalization_strategy=case_insensitive"
                ),
            )
        },
        model_defaults=ModelDefaultsConfig(dialect="snowflake"),
        default_gateway="duckdb",
    )

    from sqlglot.dialects import Snowflake
    from sqlglot.dialects.dialect import NormalizationStrategy

    assert Snowflake.NORMALIZATION_STRATEGY == NormalizationStrategy.UPPERCASE

    ctx = Context(paths=tmp_path, config=config, gateway="duckdb")

    dialect = Dialect.get_or_raise(ctx.config.dialect)

    assert dialect == "snowflake"
    assert Snowflake.NORMALIZATION_STRATEGY == NormalizationStrategy.CASE_INSENSITIVE

    Snowflake.NORMALIZATION_STRATEGY = NormalizationStrategy.UPPERCASE


def test_access_self_columns_to_types_in_macro(tmp_path: pathlib.Path):
    create_temp_file(
        tmp_path,
        pathlib.Path(pathlib.Path("models"), "test.sql"),
        "MODEL(name test); SELECT 1 AS c; @post_statement()",
    )
    create_temp_file(
        tmp_path,
        pathlib.Path(pathlib.Path("macros"), "post_statement.py"),
        """
from sqlglot import exp
from sqlmesh.core.macros import macro

@macro()
def post_statement(evaluator):
    if evaluator.runtime_stage != 'loading':
        assert evaluator.columns_to_types("test") == {"c": exp.DataType.build("int")}
    return None
""",
    )

    context = Context(paths=tmp_path, config=Config())
    context.plan(auto_apply=True, no_prompts=True)


def test_wildcard(copy_to_temp_path: t.Callable):
    parent_path = copy_to_temp_path("examples/multi")[0]

    context = Context(paths=f"{parent_path}/*", gateway="memory")
    assert len(context.models) == 5


def test_duckdb_state_connection_automatic_multithreaded_mode(tmp_path):
    single_threaded_config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        default_gateway="duckdb",
        gateways={
            "duckdb": GatewayConfig(
                connection=DuckDBConnectionConfig(concurrent_tasks=1),
                state_connection=DuckDBConnectionConfig(concurrent_tasks=1),
            )
        },
    )

    # main connection 4 concurrent tasks, state connection 1 concurrent task,
    # context should adjust concurrent tasks on state connection to match main connection
    multi_threaded_config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        default_gateway="duckdb",
        gateways={
            "duckdb": GatewayConfig(
                connection=DuckDBConnectionConfig(concurrent_tasks=4),
                state_connection=DuckDBConnectionConfig(concurrent_tasks=1),
            )
        },
    )

    context = Context(paths=[tmp_path], config=single_threaded_config)
    assert isinstance(context.state_sync, CachingStateSync)
    state_sync = context.state_sync.state_sync
    assert isinstance(state_sync, EngineAdapterStateSync)
    assert isinstance(state_sync.engine_adapter, DuckDBEngineAdapter)
    assert isinstance(state_sync.engine_adapter._connection_pool, SingletonConnectionPool)

    context = Context(paths=[tmp_path], config=multi_threaded_config)
    assert isinstance(context.state_sync, CachingStateSync)
    state_sync = context.state_sync.state_sync
    assert isinstance(state_sync, EngineAdapterStateSync)
    assert isinstance(state_sync.engine_adapter, DuckDBEngineAdapter)
    assert isinstance(state_sync.engine_adapter._connection_pool, ThreadLocalConnectionPool)


def test_requirements(copy_to_temp_path: t.Callable):
    from sqlmesh.utils.metaprogramming import Executable

    context_path = copy_to_temp_path("examples/sushi")[0]

    with open(context_path / c.REQUIREMENTS, "w") as f:
        # Add pandas and test_package and exclude ruamel.yaml
        f.write("pandas==2.2.2\ntest_package==1.0.0\n^ruamel.yaml\n^ruamel.yaml.clib")

    context = Context(paths=context_path)

    model = context.get_model("sushi.items")
    model.python_env["ruamel"] = Executable(payload="import ruamel", kind="import")
    model.python_env["Image"] = Executable(
        payload="from ipywidgets.widgets.widget_media import Image", kind="import"
    )

    environment = context.plan(
        "dev", no_prompts=True, skip_tests=True, skip_backfill=True, auto_apply=True
    ).environment
    requirements = {"ipywidgets", "numpy", "pandas", "test_package"}
    assert environment.requirements["pandas"] == "2.2.2"
    assert set(environment.requirements) == requirements

    context._requirements = {"numpy": "2.1.2", "pandas": "2.2.1"}
    context._excluded_requirements = {"ipywidgets", "ruamel.yaml", "ruamel.yaml.clib"}
    diff = context.plan_builder("dev", skip_tests=True, skip_backfill=True).build().context_diff
    assert set(diff.previous_requirements) == requirements
    assert set(diff.requirements) == {"numpy", "pandas"}


@pytest.mark.slow
def test_rendered_diff():
    ctx = Context(config=Config())

    ctx.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL (
                    name test,
                );

                CREATE TABLE IF NOT EXISTS foo AS (SELECT @OR(FALSE, TRUE));

                SELECT 4 + 2;

                CREATE TABLE IF NOT EXISTS foo2 AS (SELECT @AND(TRUE, FALSE));

                ON_VIRTUAL_UPDATE_BEGIN;
                DROP VIEW @this_model
                ON_VIRTUAL_UPDATE_END;

                """
            )
        )
    )

    ctx.plan("dev", auto_apply=True, no_prompts=True)

    # Alter the model's query and pre/post/virtual statements to cause the diff
    ctx.upsert_model(
        load_sql_based_model(
            parse(
                """
                MODEL (
                    name test,
                );

                CREATE TABLE IF NOT EXISTS foo AS (SELECT @AND(TRUE, NULL));

                SELECT 5 + 2;

                CREATE TABLE IF NOT EXISTS foo2 AS (SELECT @OR(TRUE, NULL));

                ON_VIRTUAL_UPDATE_BEGIN;
                DROP VIEW IF EXISTS @this_model
                ON_VIRTUAL_UPDATE_END;
                """
            )
        )
    )

    plan = ctx.plan("dev", auto_apply=True, no_prompts=True, diff_rendered=True)

    assert '''@@ -4,13 +4,13 @@

 CREATE TABLE IF NOT EXISTS "foo" AS
 (
   SELECT
-    FALSE OR TRUE
+    TRUE
 )
 SELECT
-  6 AS "_col_0"
+  7 AS "_col_0"
 CREATE TABLE IF NOT EXISTS "foo2" AS
 (
   SELECT
-    TRUE AND FALSE
+    TRUE
 )
-DROP VIEW "test"
+DROP VIEW IF EXISTS "test"''' in plan.context_diff.text_diff('"test"')


def test_plan_enable_preview_default(sushi_context: Context, sushi_dbt_context: Context):
    assert sushi_context._plan_preview_enabled
    assert not sushi_dbt_context._plan_preview_enabled

    sushi_dbt_context.engine_adapter.SUPPORTS_CLONING = True
    assert sushi_dbt_context._plan_preview_enabled


def test_catalog_name_needs_to_be_quoted():
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        default_connection=DuckDBConnectionConfig(catalogs={'"foo--bar"': ":memory:"}),
    )
    context = Context(config=config)
    parsed_model = parse("MODEL(name db.x, kind FULL); SELECT 1 AS c")
    context.upsert_model(load_sql_based_model(parsed_model, default_catalog='"foo--bar"'))
    context.plan(auto_apply=True, no_prompts=True)
    assert context.fetchdf('select * from "foo--bar".db.x').to_dict() == {"c": {0: 1}}


def test_plan_runs_audits_on_dev_previews(sushi_context: Context, capsys, caplog):
    sushi_context.console = create_console()

    test_model = """
    MODEL (
        name sushi.test_audit_model,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column event_date,
            forward_only true
        ),
        audits (
            number_of_rows(threshold := 10),
            not_null(columns := id),
            at_least_one_non_blocking(column := waiter_id)
        )
    );

    SELECT * FROM sushi.orders WHERE event_date BETWEEN @start_ts AND @end_ts
    """

    sushi_context.upsert_model(
        load_sql_based_model(parse(test_model), default_catalog=sushi_context.default_catalog)
    )
    plan = sushi_context.plan(auto_apply=True)

    assert plan.new_snapshots[0].name == '"memory"."sushi"."test_audit_model"'
    assert plan.deployability_index.is_deployable(plan.new_snapshots[0])

    # now, we mutate the model and run a plan in dev to create a dev preview
    test_model = """
    MODEL (
        name sushi.test_audit_model,
        kind INCREMENTAL_BY_TIME_RANGE (
            time_column event_date,
            forward_only true
        ),
        audits (
            not_null(columns := new_col),
            at_least_one_non_blocking(column := new_col)
        )
    );

    SELECT *, null as new_col FROM sushi.orders WHERE event_date BETWEEN @start_ts AND @end_ts
    """

    sushi_context.upsert_model(
        load_sql_based_model(parse(test_model), default_catalog=sushi_context.default_catalog)
    )

    capsys.readouterr()  # clear output buffer
    plan = sushi_context.plan(environment="dev", auto_apply=True)

    assert len(plan.new_snapshots) == 1
    dev_preview = plan.new_snapshots[0]
    assert dev_preview.name == '"memory"."sushi"."test_audit_model"'
    assert dev_preview.is_forward_only
    assert not plan.deployability_index.is_deployable(
        dev_preview
    )  # if something is not deployable to prod, then its by definiton a dev preview

    # we only see audit results if they fail
    stdout = capsys.readouterr().out
    log = caplog.text
    assert "'not_null' audit error:" in log
    assert "'at_least_one_non_blocking' audit error:" in log
    assert "Target environment updated successfully" in stdout


def test_environment_statements(tmp_path: pathlib.Path):
    models_dir = pathlib.Path("models")
    macros_dir = pathlib.Path("macros")
    dialect = "postgres"

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=dialect),
        plan=PlanConfig(
            before_all=[
                "CREATE TABLE IF NOT EXISTS analytic_stats (physical_table VARCHAR, evaluation_time VARCHAR)"
            ],
            after_all=[
                "@grant_schema_usage()",
                "@grant_select_privileges()",
            ],
        ),
    )

    expression = """
MODEL(
  name db.test_after_model,
  kind full
);

SELECT 1 AS col_a;
    """

    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "db", "test_after_model.sql"),
        expression,
    )

    create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "grant_select_file.py"),
        """
from sqlmesh.core.macros import macro
from sqlmesh.core.snapshot.definition import to_view_mapping

@macro()
def grant_select_privileges(evaluator):
    if evaluator._environment_naming_info:
        mapping = to_view_mapping(
            evaluator._snapshots.values(), evaluator._environment_naming_info
        )
        return [
            stmt
            for stmt in [
                f"GRANT SELECT ON VIEW {view_name} TO ROLE admin_role;"
                for view_name in mapping.values()
            ]
        ]
""",
    )

    create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "grant_schema_file.py"),
        """
from sqlmesh import macro

@macro()
def grant_schema_usage(evaluator):
    if evaluator._environment_naming_info:
        schemas = {
            snapshot.qualified_view_name.schema_for_environment(
                evaluator._environment_naming_info
            )
            for snapshot in evaluator._snapshots.values()
            if snapshot.is_model
        }
        return [
            f"GRANT USAGE ON SCHEMA {schema} TO user_role;"
            for schema in schemas
        ]
""",
    )

    context = Context(paths=tmp_path, config=config)
    snapshots = {s.name: s for s in context.snapshots.values()}

    environment_statements = context._environment_statements[0]
    before_all = environment_statements.before_all
    after_all = environment_statements.after_all
    python_env = environment_statements.python_env

    assert isinstance(python_env["to_view_mapping"], Executable)
    assert isinstance(python_env["grant_select_privileges"], Executable)

    before_all_rendered = render_statements(
        statements=before_all, dialect=dialect, python_env=python_env
    )

    assert before_all_rendered == [
        "CREATE TABLE IF NOT EXISTS analytic_stats (physical_table VARCHAR, evaluation_time VARCHAR)"
    ]

    after_all_rendered = render_statements(
        statements=after_all,
        dialect=dialect,
        python_env=python_env,
        snapshots=snapshots,
        environment_naming_info=EnvironmentNamingInfo(name="prod"),
    )

    assert after_all_rendered == [
        "GRANT USAGE ON SCHEMA db TO user_role",
        "GRANT SELECT ON VIEW memory.db.test_after_model TO ROLE admin_role",
    ]

    after_all_rendered_dev = render_statements(
        statements=after_all,
        dialect=dialect,
        python_env=python_env,
        snapshots=snapshots,
        environment_naming_info=EnvironmentNamingInfo(name="dev"),
    )

    assert after_all_rendered_dev == [
        "GRANT USAGE ON SCHEMA db__dev TO user_role",
        "GRANT SELECT ON VIEW memory.db__dev.test_after_model TO ROLE admin_role",
    ]


def test_plan_environment_statements(tmp_path: pathlib.Path):
    models_dir = pathlib.Path("models")
    macros_dir = pathlib.Path("macros")
    dialect = "duckdb"

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect=dialect),
        plan=PlanConfig(
            before_all=["@create_stats_table()"],
            after_all=["CREATE TABLE IF NOT EXISTS after_table AS SELECT @some_var"],
        ),
        variables={"some_var": 5},
    )

    model_file = """
MODEL(
  name db.test_stats_model,
  kind full,
);

@IF(
  @runtime_stage = 'evaluating',
  SET VARIABLE stats_model_start = now()
);

SELECT 1 AS cola;

@IF(
  @runtime_stage = 'evaluating',
  INSERT INTO analytic_stats (physical_table, evaluation_start, evaluation_end, evaluation_time)
  VALUES (@resolve_template('@{schema_name}.@{table_name}'), getvariable('stats_model_start'), now(), now() - getvariable('stats_model_start'))
);

    """

    create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "db", "test_stats_model.sql"),
        model_file,
    )

    create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "create_stats_table.py"),
        """
from sqlmesh.core.macros import macro

@macro()
def create_stats_table(evaluator):
    return "CREATE TABLE IF NOT EXISTS analytic_stats (physical_table VARCHAR, evaluation_start VARCHAR,  evaluation_end VARCHAR, evaluation_time VARCHAR)"
""",
    )

    context = Context(paths=tmp_path, config=config)

    assert context._environment_statements[0].before_all == ["@create_stats_table()"]
    assert context._environment_statements[0].after_all == [
        "CREATE TABLE IF NOT EXISTS after_table AS SELECT @some_var"
    ]
    assert context._environment_statements[0].python_env["create_stats_table"]

    context.plan(auto_apply=True, no_prompts=True)

    model = context.get_model("db.test_stats_model")
    snapshot = context.get_snapshot("db.test_stats_model")
    assert snapshot and snapshot.version

    assert (
        model.pre_statements[0].sql()
        == "@IF(@runtime_stage = 'evaluating', SET VARIABLE stats_model_start = now())"
    )
    assert (
        model.post_statements[0].sql()
        == "@IF(@runtime_stage = 'evaluating', INSERT INTO analytic_stats (physical_table, evaluation_start, evaluation_end, evaluation_time) VALUES (@resolve_template('@{schema_name}.@{table_name}'), GETVARIABLE('stats_model_start'), NOW(), NOW() - GETVARIABLE('stats_model_start')))"
    )

    stats_table = context.fetchdf("select * from memory.analytic_stats").to_dict()
    assert stats_table.keys() == {
        "physical_table",
        "evaluation_start",
        "evaluation_end",
        "evaluation_time",
    }
    assert (
        stats_table["physical_table"][0] == f"sqlmesh__db.db__test_stats_model__{snapshot.version}"
    )

    assert context.fetchdf("select * from memory.after_table").to_dict()["5"][0] == 5

    state_table = context.state_reader.get_environment_statements(c.PROD)
    assert state_table[0].before_all == context._environment_statements[0].before_all
    assert state_table[0].after_all == context._environment_statements[0].after_all
    assert state_table[0].python_env == context._environment_statements[0].python_env
