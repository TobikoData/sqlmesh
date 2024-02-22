import logging
import pathlib
import typing as t
from datetime import date, timedelta
from tempfile import TemporaryDirectory
from unittest.mock import PropertyMock, call, patch

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one
from sqlglot.errors import SchemaError

import sqlmesh.core.constants
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    EnvironmentSuffixTarget,
    ModelDefaultsConfig,
    SnowflakeConnectionConfig,
    load_configs,
)
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse, schema_
from sqlmesh.core.environment import Environment
from sqlmesh.core.model import load_sql_based_model
from sqlmesh.core.plan import BuiltInPlanEvaluator, PlanBuilder
from sqlmesh.utils.date import make_inclusive_end, now, to_date, yesterday_ds
from sqlmesh.utils.errors import ConfigError
from tests.utils.test_filesystem import create_temp_file


def test_global_config():
    context = Context(paths="examples/sushi")
    assert context.config.dialect == "duckdb"
    assert context.config.time_column_format == "%Y-%m-%d"


def test_named_config():
    context = Context(paths="examples/sushi", config="local_config")
    assert len(context.config.gateways) == 1


def test_invalid_named_config():
    with pytest.raises(
        ConfigError,
        match="Config 'blah' was not found.",
    ):
        Context(paths="examples/sushi", config="blah")


def test_missing_named_config():
    with pytest.raises(ConfigError, match=r"Config 'imaginary_config' was not found."):
        Context(paths="examples/sushi", config="imaginary_config")


def test_config_parameter():
    config = Config(model_defaults=ModelDefaultsConfig(dialect="presto"), project="test_project")
    context = Context(paths="examples/sushi", config=config)
    assert context.config.dialect == "presto"
    assert context.config.project == "test_project"


def test_config_not_found():
    with pytest.raises(
        ConfigError,
        match=r".*config could not be found.*",
    ):
        Context(paths="nonexistent/directory", config="local_config")


def test_custom_macros(sushi_context):
    assert "add_one" in sushi_context._macros


def test_dag(sushi_context):
    assert set(sushi_context.dag.upstream('"memory"."sushi"."customer_revenue_by_day"')) == {
        '"memory"."sushi"."items"',
        '"memory"."sushi"."orders"',
        '"memory"."sushi"."order_items"',
    }


@pytest.mark.slow
def test_render_sql_model(sushi_context, assert_exp_eq):
    assert_exp_eq(
        sushi_context.render(
            "sushi.waiter_revenue_by_day",
            start=date(2021, 1, 1),
            end=date(2021, 1, 1),
            expand=True,
        ),
        f"""
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
    unpushed = Context(paths="examples/sushi")
    assert_exp_eq(
        unpushed.render("sushi.waiter_revenue_by_day"),
        f"""
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
        sushi_context.state_sync, sushi_context.snapshot_evaluator, sushi_context.default_catalog
    )
    plan = PlanBuilder(
        context_diff=sushi_context._context_diff("prod"),
    ).build()

    promotion_result = plan_evaluator._promote(plan)
    plan_evaluator._update_views(plan, promotion_result)

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

    ignore_model_file = create_temp_file(
        tmp_path,
        pathlib.Path(models_dir, "ignore", "ignore_model.sql"),
        "MODEL(name ignore.ignore_model); SELECT 1 AS cola",
    )
    ignore_macro_file = create_temp_file(
        tmp_path,
        pathlib.Path(macros_dir, "macro_ignore.py"),
        """
from sqlmesh.core.macros import macro

@macro()
def test():
    return "test"
""",
    )
    constant_ignore_model_file = create_temp_file(
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
        ignore_patterns=["models/ignore/*.sql", "macro_ignore.py", ".ipynb_checkpoints/*"]
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
    context = Context(paths="examples/sushi")
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


@pytest.mark.slow
def test_physical_schema_override() -> None:
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

    no_mapping_context = Context(paths="examples/sushi")
    assert no_mapping_context.config.physical_schema_override == {}
    assert get_schemas(no_mapping_context) == {"sqlmesh__sushi", "sqlmesh__raw"}
    assert get_view_schemas(no_mapping_context) == {"sushi", "raw"}
    no_mapping_fingerprints = get_sushi_fingerprints(no_mapping_context)
    context = Context(paths="examples/sushi", config="map_config")
    assert context.config.physical_schema_override == {"sushi": "company_internal"}
    assert get_schemas(context) == {"company_internal", "sqlmesh__raw"}
    assert get_view_schemas(context) == {"sushi", "raw"}
    sushi_fingerprints = get_sushi_fingerprints(context)
    assert (
        len(sushi_fingerprints)
        == len(no_mapping_fingerprints)
        == len(no_mapping_fingerprints - sushi_fingerprints)
    )


@pytest.mark.slow
def test_janitor(sushi_context, mocker: MockerFixture) -> None:
    adapter_mock = mocker.MagicMock()
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
    sushi_context._engine_adapter = adapter_mock
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
    assert adapter_mock.drop_view.call_count == 12
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

    forward_only_dev_plan = sushi_context_pre_scheduling.plan(
        "test_env_forward_only", no_prompts=True, include_unmodified=True, forward_only=True
    )
    assert forward_only_dev_plan.end is not None
    assert to_date(make_inclusive_end(forward_only_dev_plan.end)) == plan_end
    assert forward_only_dev_plan.start == plan_end


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
def test_gateway_macro(sushi_context: Context) -> None:
    sushi_context.upsert_model(
        load_sql_based_model(
            parse(
                """
            MODEL(name sushi.test_gateway_macro);
            SELECT @gateway AS gateway;
            """
            ),
            macros=sushi_context._macros,
            default_catalog=sushi_context.default_catalog,
        )
    )

    assert (
        sushi_context.render("sushi.test_gateway_macro").sql()
        == "SELECT 'in_memory' AS \"gateway\""
    )

    sushi_context.upsert_model(
        load_sql_based_model(
            parse(
                """
            MODEL(name sushi.test_gateway_macro_jinja);
            JINJA_QUERY_BEGIN;
            SELECT '{{ gateway }}' AS gateway_jinja;
            JINJA_END;
            """
            ),
            jinja_macros=sushi_context._jinja_macros,
            default_catalog=sushi_context.default_catalog,
        )
    )

    assert (
        sushi_context.render("sushi.test_gateway_macro_jinja").sql()
        == "SELECT 'in_memory' AS \"gateway_jinja\""
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
    )
    model_v2 = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_unrestorable);
        SELECT 2 AS two;
        """
        ),
        default_catalog=sushi_context.default_catalog,
    )

    sushi_context.upsert_model(model_v1)
    sushi_context.plan(auto_apply=True, no_prompts=True)
    model_v1_old_snapshot = sushi_context.get_snapshot(
        "sushi.test_unrestorable", raise_if_missing=True
    )

    sushi_context.upsert_model(model_v2)
    sushi_context.plan(auto_apply=True, no_prompts=True, forward_only=True)

    sushi_context.upsert_model(model_v1)
    sushi_context.plan(auto_apply=True, no_prompts=True, forward_only=True)
    model_v1_new_snapshot = sushi_context.get_snapshot(
        "memory.sushi.test_unrestorable", raise_if_missing=True
    )

    assert (
        model_v1_new_snapshot.node.stamp
        == f"revert to {model_v1_old_snapshot.snapshot_id.identifier}"
    )
    assert model_v1_old_snapshot.snapshot_id != model_v1_new_snapshot.snapshot_id
    assert model_v1_old_snapshot.fingerprint != model_v1_new_snapshot.fingerprint


def test_default_catalog_connections(mocker: MockerFixture):
    with patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.default_catalog",
        PropertyMock(return_value=None),
    ):
        context = Context(paths="examples/sushi")
        assert context.default_catalog is None

    # Verify that providing a catalog gets set as default catalog
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="presto"),
        default_connection=DuckDBConnectionConfig(catalogs={"catalog": ":memory:"}),
    )
    context = Context(paths="examples/sushi", config=config)
    assert context.default_catalog == "catalog"
