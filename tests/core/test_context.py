import pathlib
from datetime import date
from tempfile import TemporaryDirectory

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import parse_one

import sqlmesh.core.constants
from sqlmesh.core.config import Config, ModelDefaultsConfig, SnowflakeConnectionConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.model import load_model
from sqlmesh.core.plan import BuiltInPlanEvaluator, Plan
from sqlmesh.utils.date import yesterday_ds
from sqlmesh.utils.errors import ConfigError
from tests.utils.test_filesystem import create_temp_file


def test_global_config():
    context = Context(paths="examples/sushi")
    assert context.config.dialect is None
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
    assert set(sushi_context.dag.upstream("sushi.customer_revenue_by_day")) == {
        "sushi.items",
        "sushi.orders",
        "sushi.order_items",
    }


def test_render(sushi_context, assert_exp_eq):
    snapshot = sushi_context.snapshots["sushi.waiter_revenue_by_day"]

    assert_exp_eq(
        sushi_context.render(
            snapshot.model,
            start=date(2021, 1, 1),
            end=date(2021, 1, 1),
            latest=date(2021, 1, 1),
            expand=True,
        ),
        f"""
        SELECT
          CAST("o"."waiter_id" AS INT) AS "waiter_id", /* Waiter id */
          CAST(SUM("oi"."quantity" * "i"."price") AS DOUBLE) AS "revenue", /* Revenue from orders taken by this waiter */
          CAST("o"."ds" AS TEXT) AS "ds" /* Date */
        FROM (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "customer_id",
            CAST(NULL AS INT) AS "waiter_id",
            CAST(NULL AS INT) AS "start_ts",
            CAST(NULL AS INT) AS "end_ts",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "o"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "order_id",
            CAST(NULL AS INT) AS "item_id",
            CAST(NULL AS INT) AS "quantity",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "oi"
          ON "o"."ds" = "oi"."ds" AND "o"."id" = "oi"."order_id"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS TEXT) AS "name",
            CAST(NULL AS DOUBLE) AS "price",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "i"
          ON "oi"."ds" = "i"."ds" AND "oi"."item_id" = "i"."id"
        WHERE
          "o"."ds" <= '2021-01-01' AND "o"."ds" >= '2021-01-01'
        GROUP BY
          "o"."waiter_id",
          "o"."ds"
        """,
    )

    # unpushed render still works
    unpushed = Context(paths="examples/sushi")
    assert_exp_eq(
        unpushed.render(snapshot.name),
        f"""
        SELECT
          CAST("o"."waiter_id" AS INT) AS "waiter_id", /* Waiter id */
          CAST(SUM("oi"."quantity" * "i"."price") AS DOUBLE) AS "revenue", /* Revenue from orders taken by this waiter */
          CAST("o"."ds" AS TEXT) AS "ds" /* Date */
        FROM (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "customer_id",
            CAST(NULL AS INT) AS "waiter_id",
            CAST(NULL AS INT) AS "start_ts",
            CAST(NULL AS INT) AS "end_ts",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "o"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS INT) AS "order_id",
            CAST(NULL AS INT) AS "item_id",
            CAST(NULL AS INT) AS "quantity",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "oi"
          ON "o"."ds" = "oi"."ds" AND "o"."id" = "oi"."order_id"
        LEFT JOIN (
          SELECT
            CAST(NULL AS INT) AS "id",
            CAST(NULL AS TEXT) AS "name",
            CAST(NULL AS DOUBLE) AS "price",
            CAST(NULL AS TEXT) AS "ds"
          FROM (VALUES
            (1)) AS "t"("dummy")
        ) AS "i"
          ON "oi"."ds" = "i"."ds" AND "oi"."item_id" = "i"."id"
        WHERE
          "o"."ds" <= '1970-01-01' AND "o"."ds" >= '1970-01-01'
        GROUP BY
          "o"."waiter_id",
          "o"."ds"
        """,
    )


def test_diff(sushi_context: Context, mocker: MockerFixture):
    mock_console = mocker.Mock()
    sushi_context.console = mock_console
    yesterday = yesterday_ds()
    sushi_context.run(start=yesterday, end=yesterday, latest=yesterday)

    plan_evaluator = BuiltInPlanEvaluator(
        sushi_context.state_sync, sushi_context.snapshot_evaluator
    )
    plan_evaluator._promote(
        Plan(
            context_diff=sushi_context._context_diff("prod"),
        )
    )

    sushi_context.upsert_model("sushi.customers", query=parse_one("select 1 as customer_id"))
    sushi_context.diff("test")
    assert mock_console.show_model_difference_summary.called


def test_evaluate_limit():
    context = Context(config=Config())

    context.upsert_model(
        load_model(
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
        load_model(
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
    context = Context(paths=str(tmp_path), config=config)

    assert ["db.actual_test"] == list(context.models)
    assert "test" == list(context._macros)[-1]


def test_plan_apply(sushi_context) -> None:
    plan = sushi_context.plan(
        "dev",
        start=yesterday_ds(),
        end=yesterday_ds(),
        promote_all=True,
    )
    sushi_context.apply(plan)
    assert sushi_context.state_reader.get_environment("dev")


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
            account: 123
            user: CDE
""",
        )
        with pytest.raises(
            ConfigError,
            match="User and password must be provided if using default authentication",
        ):
            context._load_configs("config", [project_config.parent])
        context.sqlmesh_path = home_path
        loaded_configs = context._load_configs("config", [project_config.parent])
        assert len(loaded_configs) == 1
        snowflake_connection = list(loaded_configs.values())[0].gateways["snowflake"].connection  # type: ignore
        assert isinstance(snowflake_connection, SnowflakeConnectionConfig)
        assert snowflake_connection.account == "123"
        assert snowflake_connection.user == "ABC"
        assert snowflake_connection.password == "XYZ"
