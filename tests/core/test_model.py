# ruff: noqa: F811
import json
import logging
import typing as t
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch, PropertyMock

import pandas as pd
import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one
from sqlglot.schema import MappingSchema
from sqlmesh.cli.example_project import init_example_project

from sqlmesh.core import constants as c
from sqlmesh.core import dialect as d
from sqlmesh.core.audit import ModelAudit, load_audit
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    NameInferenceConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.context import Context, ExecutionContext
from sqlmesh.core.dialect import parse
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.core.model import (
    CustomKind,
    PythonModel,
    FullKind,
    IncrementalByTimeRangeKind,
    IncrementalUnmanagedKind,
    ModelCache,
    ModelMeta,
    SeedKind,
    SqlModel,
    TimeColumn,
    ExternalKind,
    create_external_model,
    create_seed_model,
    create_sql_model,
    load_sql_based_model,
    model,
)
from sqlmesh.core.model.common import parse_expression
from sqlmesh.core.model.kind import ModelKindName, _model_kind_validator
from sqlmesh.core.model.seed import CsvSettings
from sqlmesh.core.node import IntervalUnit, _Node
from sqlmesh.core.snapshot import Snapshot, SnapshotChangeCategory
from sqlmesh.utils.date import TimeLike, to_datetime, to_ds, to_timestamp
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroInfo, MacroExtractor
from sqlmesh.utils.metaprogramming import Executable


def missing_schema_warning_msg(model, deps):
    deps = ", ".join(f"'{dep}'" for dep in sorted(deps))
    return (
        f"SELECT * cannot be expanded due to missing schema(s) for model(s): {deps}. "
        f"Run `sqlmesh create_external_models` and / or make sure that the model '{model}' "
        "can be rendered at parse time."
    )


def test_load(assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            storage_format iceberg,
            partitioned_by d,
            clustered_by e,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
            ),
            tags [tag_foo, tag_bar],
            grain [a, b],
            references (
                f,
                g
            ),
        );

        @DEF(x, 1);
        @DEF(y, @x + 1);
        CACHE TABLE x AS SELECT 1;
        ADD JAR 's3://my_jar.jar';

        SELECT
            1::int AS a,
            CAST(2 AS double) AS b,
            c::bool,
            1::int AS d, -- d
            CAST(2 AS double) AS e, --e
            f::bool, --f
            @y::int AS g,
        FROM
            db.other_table t1
            LEFT JOIN
            db.table t2
            ON
                t1.a = t2.a;

        DROP TABLE x;
    """
    )

    model = load_sql_based_model(expressions)
    assert model.name == "db.table"
    assert model.owner == "owner_name"
    assert model.dialect == "spark"
    assert model.storage_format == "iceberg"
    assert [col.sql() for col in model.partitioned_by] == ['"a"', '"d"']
    assert model.clustered_by == ["e"]
    assert model.columns_to_types == {
        "a": exp.DataType.build("int"),
        "b": exp.DataType.build("double"),
        "c": exp.DataType.build("boolean"),
        "d": exp.DataType.build("int"),
        "e": exp.DataType.build("double"),
        "f": exp.DataType.build("boolean"),
        "g": exp.DataType.build("int"),
    }
    assert model.annotated
    assert model.view_name == "table"
    assert model.macro_definitions == [d.parse_one("@DEF(x, 1)"), d.parse_one("@DEF(y, @x + 1)")]
    assert list(model.pre_statements) == [
        d.parse_one("@DEF(x, 1)"),
        d.parse_one("@DEF(y, @x + 1)"),
        d.parse_one("CACHE TABLE x AS SELECT 1"),
        d.parse_one("ADD JAR 's3://my_jar.jar'", dialect="spark"),
    ]
    assert list(model.post_statements) == [
        d.parse_one("DROP TABLE x"),
    ]
    assert model.depends_on == {'"db"."other_table"'}

    assert (
        model.render_query().sql(pretty=True, dialect="spark")
        == """SELECT
  CAST(1 AS INT) AS `a`,
  CAST(2 AS DOUBLE) AS `b`,
  CAST(`c` AS BOOLEAN) AS `c`,
  CAST(1 AS INT) AS `d`, /* d */
  CAST(2 AS DOUBLE) AS `e`, /* e */
  CAST(`f` AS BOOLEAN) AS `f`, /* f */
  CAST(1 + 1 AS INT) AS `g`
FROM `db`.`other_table` AS `t1`
LEFT JOIN `db`.`table` AS `t2`
  ON `t1`.`a` = `t2`.`a`"""
    )

    assert model.tags == ["tag_foo", "tag_bar"]
    assert [r.dict() for r in model.all_references] == [
        {"model_name": "db.table", "expression": d.parse_one("[a, b]"), "unique": True},
        {"model_name": "db.table", "expression": d.parse_one("f"), "unique": True},
        {"model_name": "db.table", "expression": d.parse_one("g"), "unique": True},
    ]


def test_model_multiple_select_statements():
    # Make sure the load_model raises an exception for model with multiple select statements.
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
        );

        SELECT 1, ds;
        SELECT 2, ds;
        """
    )
    with pytest.raises(ConfigError, match=r"^Only one SELECT.*"):
        load_sql_based_model(expressions)


@pytest.mark.parametrize(
    "query, error",
    [
        ("y::int, x::int AS y", "duplicate"),
        ("* FROM db.table", "require inferrable column types"),
    ],
)
def test_model_validation(query, error):
    expressions = d.parse(
        f"""
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT {query}
        """
    )

    model = load_sql_based_model(expressions)
    with pytest.raises(ConfigError) as ex:
        model.validate_definition()
    assert error in str(ex.value)


def test_model_union_query(sushi_context, assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT a, b UNION SELECT c, c
        """
    )

    load_sql_based_model(expressions)

    expressions = d.parse(
        """
        MODEL (
            name sushi.test,
            kind FULL,
        );

        @union('all', sushi.marketing, sushi.marketing)
        """
    )
    sushi_context.upsert_model(load_sql_based_model(expressions, default_catalog="memory"))
    assert_exp_eq(
        sushi_context.get_model("sushi.test").render_query(),
        """SELECT
  CAST("marketing"."customer_id" AS INT) AS "customer_id",
  CAST("marketing"."status" AS TEXT) AS "status",
  CAST("marketing"."updated_at" AS TIMESTAMP) AS "updated_at",
  CAST("marketing"."valid_from" AS TIMESTAMP) AS "valid_from",
  CAST("marketing"."valid_to" AS TIMESTAMP) AS "valid_to"
FROM "memory"."sushi"."marketing" AS "marketing"
UNION ALL
SELECT
  CAST("marketing"."customer_id" AS INT) AS "customer_id",
  CAST("marketing"."status" AS TEXT) AS "status",
  CAST("marketing"."updated_at" AS TIMESTAMP) AS "updated_at",
  CAST("marketing"."valid_from" AS TIMESTAMP) AS "valid_from",
  CAST("marketing"."valid_to" AS TIMESTAMP) AS "valid_to"
FROM "memory"."sushi"."marketing" AS "marketing"
        """,
    )


def test_model_validation_union_query():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT a, a UNION SELECT c, c
        """
    )

    model = load_sql_based_model(expressions)
    with pytest.raises(ConfigError, match=r"Found duplicate outer select name 'a'"):
        model.validate_definition()


def test_model_qualification():
    logger = logging.getLogger("sqlmesh.core.renderer")
    with patch.object(logger, "warning") as mock_logger:
        expressions = d.parse(
            """
            MODEL (
                name db.table,
                kind FULL,
            );

            SELECT a
            """
        )

        model = load_sql_based_model(expressions)
        model.render_query(needs_optimization=True)
        assert (
            mock_logger.call_args[0][0]
            == "%s for model '%s', the column may not exist or is ambiguous"
        )


@pytest.mark.parametrize(
    "partition_by_input, partition_by_output, output_dialect, expected_exception",
    [
        ("a", ["`a`"], "bigquery", None),
        ("(a, b)", ["`a`", "`b`"], "bigquery", None),
        ("TIMESTAMP_TRUNC(`a`, DAY)", ["TIMESTAMP_TRUNC(`a`, DAY)"], "bigquery", None),
        ("e", "", "bigquery", ConfigError),
        ("(a, e)", "", "bigquery", ConfigError),
        ("(a, a)", "", "bigquery", ConfigError),
        ("(day(a),b)", ['DAY("a")', '"b"'], "trino", None),
    ],
)
def test_partitioned_by(
    partition_by_input, partition_by_output, output_dialect, expected_exception
):
    expressions = d.parse(
        f"""
        MODEL (
            name db.table,
            dialect bigquery,
            owner owner_name,
            partitioned_by {partition_by_input},
            clustered_by (c, d),
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
            ),
        );

        SELECT 1::int AS a, 2::int AS b, 3 AS c, 4 as d;
    """
    )

    model = load_sql_based_model(expressions)
    assert model.clustered_by == ["c", "d"]
    if expected_exception:
        with pytest.raises(expected_exception):
            model.validate_definition()
    else:
        model.validate_definition()
        assert [
            col.sql(dialect=output_dialect) for col in model.partitioned_by
        ] == partition_by_output


def test_no_model_statement(tmp_path: Path):
    # No name inference => MODEL (...) is required
    expressions = d.parse("SELECT 1 AS x")
    with pytest.raises(
        ConfigError,
        match="The MODEL statement is required as the first statement in the definition, unless model name inference is enabled. at '.'",
    ):
        load_sql_based_model(expressions)

    # Name inference is enabled => MODEL (...) not required
    init_example_project(tmp_path, dialect="duckdb")

    test_sql_file = tmp_path / "models/test_schema/test_model.sql"
    test_sql_file.parent.mkdir(parents=True, exist_ok=True)
    test_sql_file.write_text("SELECT 1 AS c")

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        model_naming=NameInferenceConfig(infer_names=True),
    )
    context = Context(paths=tmp_path, config=config)

    model = context.get_model("test_schema.test_model")
    assert isinstance(model, SqlModel)
    assert model.name == "test_schema.test_model"


def test_unordered_model_statements():
    expressions = d.parse(
        """
        SELECT 1 AS x;

        MODEL (
            name db.table,
            dialect spark,
            owner owner_name
        );
    """
    )

    with pytest.raises(ConfigError) as ex:
        load_sql_based_model(expressions)
    assert "MODEL statement is required" in str(ex.value)


def test_no_query():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name
        );

        @DEF(x, 1)
        """
    )

    with pytest.raises(ConfigError) as ex:
        model = load_sql_based_model(expressions, path=Path("test_location"))
        model.validate_definition()

    assert "Model query needs to be a SELECT or a UNION, got @DEF(x, 1)." in str(ex.value)


def test_single_macro_as_query(assert_exp_eq):
    @macro()
    def select_query(evaluator, *projections):
        return exp.select(*[f'{p} AS "{p}"' for p in projections])

    expressions = d.parse(
        """
        MODEL (
            name test
        );

        @SELECT_QUERY(1, 2, 3)
        """
    )
    model = load_sql_based_model(expressions)
    assert_exp_eq(
        model.render_query(),
        """
        SELECT
          1 AS "1",
          2 AS "2",
          3 AS "3"
        """,
    )


def test_partition_key_is_missing_in_query():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            kind INCREMENTAL_BY_TIME_RANGE(
              time_column a
            ),
            partitioned_by (b, c, d)
        );

        SELECT 1::int AS a, 2::int AS b;
    """
    )

    model = load_sql_based_model(expressions)
    with pytest.raises(ConfigError) as ex:
        model.validate_definition()
    assert "['c', 'd'] are missing" in str(ex.value)


def test_cluster_key_is_missing_in_query():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            kind INCREMENTAL_BY_TIME_RANGE(
              time_column a
            ),
            clustered_by (b, c, d)
        );

        SELECT 1::int AS a, 2::int AS b;
    """
    )

    model = load_sql_based_model(expressions)
    with pytest.raises(ConfigError) as ex:
        model.validate_definition()
    assert "['c', 'd'] are missing" in str(ex.value)


def test_partition_key_and_select_star():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            kind INCREMENTAL_BY_TIME_RANGE(
              time_column a
            ),
            partitioned_by (b, c, d)
        );

        SELECT * FROM tbl;
    """
    )

    load_sql_based_model(expressions)


def test_json_serde():
    expressions = parse(
        """
        MODEL (
            name test_model,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column ds
            ),
            owner test_owner,
            dialect spark,
            cron '@daily',
            storage_format parquet,
            partitioned_by a,
            physical_properties (
                key_a = 'value_a',
                'key_b' = 1,
                key_c = true,
                "key_d" = 2.0,
            ),
        );

        @DEF(key, 'value');

        SELECT a, ds FROM `tbl`
    """
    )

    model = load_sql_based_model(expressions)

    model_json = model.json()
    model_json_parsed = json.loads(model.json())
    assert model_json_parsed["kind"]["dialect"] == "spark"
    assert model_json_parsed["kind"]["time_column"]["column"] == "`ds`"
    assert model_json_parsed["partitioned_by"] == ["`a`"]

    deserialized_model = SqlModel.parse_raw(model_json)

    assert deserialized_model == model


def test_scd_type_2_by_col_serde():
    expressions = parse(
        """
        MODEL(
            name test_model,
            KIND SCD_TYPE_2_BY_COLUMN (
                unique_key a,
                columns *,
            ),
            cron '@daily',
            owner 'reakman',
            grain a,
            dialect bigquery
        );

        SELECT a, ds FROM `tbl`
    """,
        default_dialect="bigquery",
    )

    model = load_sql_based_model(expressions)

    model_json = model.json()
    model_json_parsed = json.loads(model.json())
    assert model_json_parsed["kind"]["dialect"] == "bigquery"
    assert model_json_parsed["kind"]["unique_key"] == ["`a`"]
    assert model_json_parsed["kind"]["columns"] == "*"
    # Bigquery converts TIMESTAMP -> DATETIME
    assert model_json_parsed["kind"]["time_data_type"] == "DATETIME"

    deserialized_model = SqlModel.parse_raw(model_json)
    assert deserialized_model.dict() == model.dict()


def test_column_descriptions(sushi_context, assert_exp_eq):
    assert sushi_context.models[
        '"memory"."sushi"."customer_revenue_by_day"'
    ].column_descriptions == {
        "customer_id": "Customer id",
        "country code": "Customer country code, used for testing spaces",
        "revenue": "Revenue from orders made by this customer",
        "event_date": "Date",
    }

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT
          -- this is the id column, used in ...
          id::int, -- primary key
          foo::int, -- bar
        FROM table
    """
    )
    model = load_sql_based_model(expressions, default_catalog="memory")

    assert_exp_eq(
        model.query,
        """
        SELECT
          -- this is the id column, used in ...
          id::int, -- primary key
          foo::int, -- bar
        FROM table
        """,
    )
    assert model.column_descriptions == {"id": "primary key", "foo": "bar"}


def test_model_jinja_macro_reference_extraction():
    @macro()
    def test_macro(**kwargs) -> None:
        pass

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
        );

        JINJA_STATEMENT_BEGIN;
        {{ test_macro() }}
        JINJA_END;

        SELECT 1 AS x;
    """
    )
    model = load_sql_based_model(expressions)
    assert "test_macro" in model.python_env


def test_model_pre_post_statements():
    @macro()
    def foo(**kwargs) -> None:
        pass

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
        );

        @foo();

        JINJA_STATEMENT_BEGIN;
        CREATE TABLE x{{ 1 + 1 }};
        JINJA_END;

        SELECT 1 AS x;

        @foo(bar='x', val=@this);

        DROP TABLE x2;
    """
    )
    model = load_sql_based_model(expressions)

    expected_pre = [
        *d.parse("@foo()"),
        d.jinja_statement("CREATE TABLE x{{ 1 + 1 }};"),
    ]
    assert model.pre_statements == expected_pre

    expected_post = d.parse("@foo(bar='x', val=@this); DROP TABLE x2;")
    assert model.post_statements == expected_post

    assert model.query == d.parse("SELECT 1 AS x")[0]

    @macro()
    def multiple_statements(evaluator, t1_value=exp.Literal.number(1)):
        return [f"CREATE TABLE t1 AS SELECT {t1_value} AS c", "CREATE TABLE t2 AS SELECT 2 AS c"]

    expressions = d.parse(
        """
        MODEL (name db.table);

        SELECT 1 AS col;

        @multiple_statements()
        """
    )
    model = load_sql_based_model(expressions)

    expected_post = d.parse(
        'CREATE TABLE "t1" AS SELECT 1 AS "c"; CREATE TABLE "t2" AS SELECT 2 AS "c"'
    )
    assert model.render_post_statements() == expected_post
    assert "exp" in model.python_env


def test_seed_hydration():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            )
        );
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert model.is_hydrated
    assert not model.derived_columns_to_types

    column_hashes = model.column_hashes

    dehydrated_model = model.to_dehydrated()
    assert not dehydrated_model.is_hydrated
    assert dehydrated_model.column_hashes == column_hashes
    assert dehydrated_model.derived_columns_to_types == {
        "id": exp.DataType.build("bigint"),
        "name": exp.DataType.build("text"),
    }
    assert dehydrated_model.seed.content == ""

    hydrated_model = dehydrated_model.to_hydrated(model.seed.content)
    assert hydrated_model.is_hydrated
    assert hydrated_model.column_hashes == column_hashes
    assert hydrated_model.seed.content == model.seed.content
    assert hydrated_model.column_hashes_ is None


def test_seed():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            )
        );
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.kind.path == "../seeds/waiter_names.csv"
    assert model.kind.batch_size == 100
    assert model.seed is not None
    assert len(model.seed.content) > 0

    assert model.columns_to_types == {
        "id": exp.DataType.build("bigint"),
        "name": exp.DataType.build("text"),
    }

    assert model.batch_size is None


def test_seed_model_creation_error():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path 'gibberish',
            )
        );
    """
    )
    with pytest.raises(ConfigError, match="No such file or directory"):
        load_sql_based_model(expressions)


def test_seed_provided_columns():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            ),
            columns (
              id double,
              alias varchar
            )
        );
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.kind.path == "../seeds/waiter_names.csv"
    assert model.kind.batch_size == 100
    assert model.seed is not None
    assert len(model.seed.content) > 0

    assert model.columns_to_types == {
        "id": exp.DataType.build("double"),
        "alias": exp.DataType.build("varchar"),
    }


def test_seed_case_sensitive_columns(tmp_path):
    model_csv_path = (tmp_path / "model.csv").absolute()

    with open(model_csv_path, "w", encoding="utf-8") as fd:
        fd.write(
            """camelCaseId,camelCaseBool,camelCaseString,normalisedCaseDate,camelCaseTimestamp
1,false,Alice,2022-01-01,2022-01-01
"""
        )

    expressions = d.parse(
        f"""
        MODEL (
            name db.seed,
            dialect postgres,
            kind SEED (
              path '{str(model_csv_path)}',
            ),
            columns (
              "camelCaseId" int,
              "camelCaseBool" boolean,
              "camelCaseString" text,
              "camelCaseTimestamp" timestamp
            )
        );
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.seed is not None
    assert len(model.seed.content) > 0
    assert model.columns_to_types == {
        "camelCaseId": exp.DataType.build("int"),
        "camelCaseBool": exp.DataType.build("boolean"),
        "camelCaseString": exp.DataType.build("text"),
        "camelCaseTimestamp": exp.DataType.build("TIMESTAMP"),
    }
    df = next(model.render(context=None))

    assert df["camelCaseId"].dtype == "int64"
    assert df["camelCaseId"].iloc[0] == 1

    assert df["camelCaseBool"].dtype == "bool"
    assert not df["camelCaseBool"].iloc[0]

    assert df["camelCaseString"].dtype == "object"
    assert df["camelCaseString"].iloc[0] == "Alice"

    assert df["normalisedcasedate"].dtype == "object"
    assert df["normalisedcasedate"].iloc[0] == "2022-01-01"

    assert df["camelCaseTimestamp"].dtype == "datetime64[ns]"
    assert df["camelCaseTimestamp"].iloc[0] == pd.Timestamp("2022-01-01 00:00:00")


def test_seed_csv_settings():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
              csv_settings (
                quotechar = '''',
                escapechar = '\\',
              ),
            ),
            columns (
              id double,
              alias varchar
            )
        );
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.kind.csv_settings == CsvSettings(quotechar="'", escapechar="\\")


def test_seed_marker_substitution():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '$root/seeds/waiter_names.csv',
              batch_size 100,
            )
        );
    """
    )

    model = load_sql_based_model(
        expressions,
        path=Path("./examples/sushi/models/test_model.sql"),
        module_path=Path("./examples/sushi"),
    )

    assert isinstance(model.kind, SeedKind)
    assert model.kind.path == "examples/sushi/seeds/waiter_names.csv"
    assert model.seed is not None
    assert len(model.seed.content) > 0


def test_seed_pre_post_statements():
    @macro()
    def bar(**kwargs) -> None:
        pass

    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            )
        );

        @bar();

        JINJA_STATEMENT_BEGIN;
        CREATE TABLE x{{ 1 + 1 }};
        JINJA_END;

        @INSERT_SEED();

        @bar(foo='x', val=@this);

        DROP TABLE x2;
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    expected_pre = [
        *d.parse("@bar()"),
        d.jinja_statement("CREATE TABLE x{{ 1 + 1 }};"),
    ]
    assert model.pre_statements == expected_pre

    expected_post = [
        *d.parse("@bar(foo='x', val=@this)"),
        *d.parse("DROP TABLE x2;"),
    ]
    assert model.post_statements == expected_post


def test_seed_pre_statements_only():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            )
        );

        JINJA_STATEMENT_BEGIN;
        CREATE TABLE x{{ 1 + 1 }};
        JINJA_END;

        DROP TABLE x2;
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    expected_pre = [
        d.jinja_statement("CREATE TABLE x{{ 1 + 1 }};"),
        *d.parse("DROP TABLE x2;"),
    ]
    assert model.pre_statements == expected_pre
    assert not model.post_statements


def test_seed_model_custom_types(tmp_path):
    model_csv_path = (tmp_path / "model.csv").absolute()

    with open(model_csv_path, "w", encoding="utf-8") as fd:
        fd.write(
            """key,ds_date,ds_timestamp,b_a,b_b,i,i_str,empty_date
123,2022-01-01,2022-01-01,false,0,321,321,
"""
        )

    model = create_seed_model(
        "test_db.test_model",
        SeedKind(path=str(model_csv_path)),
        columns={
            "key": "string",
            "ds_date": "date",
            "ds_timestamp": "timestamp",
            "b_a": "boolean",
            "b_b": "boolean",
            "i": "int",
            "i_str": "text",
            "empty_date": "date",
        },
    )

    df = next(model.render(context=None))

    assert df["ds_date"].dtype == "object"
    assert df["ds_date"].iloc[0] == date(2022, 1, 1)

    assert df["ds_timestamp"].dtype == "datetime64[ns]"
    assert df["ds_timestamp"].iloc[0] == pd.Timestamp("2022-01-01 00:00:00")

    assert df["key"].dtype == "object"
    assert df["key"].iloc[0] == "123"

    assert df["b_a"].dtype == "bool"
    assert not df["b_a"].iloc[0]

    assert df["b_b"].dtype == "bool"
    assert not df["b_b"].iloc[0]

    assert df["i"].dtype == "int64"
    assert df["i"].iloc[0] == 321

    assert df["i_str"].dtype == "object"
    assert df["i_str"].iloc[0] == "321"

    assert df["empty_date"].dtype == "object"
    assert df["empty_date"].iloc[0] is None


def test_seed_with_special_characters_in_column(tmp_path, assert_exp_eq):
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    context = Context(config=config)

    model_csv_path = (tmp_path / "model.csv").absolute()
    with open(model_csv_path, "w", encoding="utf-8") as fd:
        fd.write("col.\tcol!@#$\n123\tfoo")

    expressions = d.parse(
        f"""
        MODEL (
            name memory.test_db.test_model,
            kind SEED (
              path '{model_csv_path}',
              csv_settings (
                delimiter = '\\t'
              )
            ),
        );
    """
    )

    context.upsert_model(load_sql_based_model(expressions))
    assert_exp_eq(
        context.render("memory.test_db.test_model").sql(),
        "SELECT "
        'CAST("col." AS BIGINT) AS "col.", '
        'CAST("col!@#$" AS TEXT) AS "col!@#$" '
        """FROM (VALUES (123, 'foo')) AS t("col.", "col!@#$")""",
    )


def test_python_model_jinja_pre_post_statements():
    macros = """
    {% macro test_macro(v) %}{{ v }}{% endmacro %}
    {% macro extra_macro(v) %}{{ v + 1 }}{% endmacro %}
    """

    jinja_macros = JinjaMacroRegistry()
    jinja_macros.add_macros(MacroExtractor().extract(macros))

    @model(
        "db.test_model",
        kind="full",
        columns={"id": "string", "name": "string"},
        pre_statements=[
            "JINJA_STATEMENT_BEGIN;\n{% set table_name = 'x' %}\nCREATE OR REPLACE TABLE {{table_name}}{{ 1 + 1 }};\nJINJA_END;"
        ],
        post_statements=[
            "JINJA_STATEMENT_BEGIN;\nCREATE INDEX {{test_macro('idx')}} ON db.test_model(id);\nJINJA_END;",
            parse_one("DROP TABLE x2;"),
        ],
    )
    def model_with_statements(context, **kwargs):
        return pd.DataFrame(
            [
                {
                    "id": context.var("1"),
                    "name": context.var("var"),
                }
            ]
        )

    python_model = model.get_registry()["db.test_model"].model(
        module_path=Path("."), path=Path("."), dialect="duckdb", jinja_macros=jinja_macros
    )

    assert len(jinja_macros.root_macros) == 2
    assert len(python_model.jinja_macros.root_macros) == 1
    assert "test_macro" in python_model.jinja_macros.root_macros
    assert "extra_macro" not in python_model.jinja_macros.root_macros

    expected_pre = [
        d.jinja_statement(
            "{% set table_name = 'x' %}\nCREATE OR REPLACE TABLE {{table_name}}{{ 1 + 1 }};"
        ),
    ]
    assert python_model.pre_statements == expected_pre
    assert python_model.render_pre_statements()[0].sql() == 'CREATE OR REPLACE TABLE "x2"'

    expected_post = [
        d.jinja_statement("CREATE INDEX {{test_macro('idx')}} ON db.test_model(id);"),
        *d.parse("DROP TABLE x2;"),
    ]
    assert python_model.post_statements == expected_post
    assert (
        python_model.render_post_statements()[0].sql()
        == 'CREATE INDEX "idx" ON "db"."test_model"("id" NULLS LAST)'
    )
    assert python_model.render_post_statements()[1].sql() == 'DROP TABLE "x2"'


def test_audits():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            audits (
                audit_a,
                audit_b(key='value')
            ),
            tags (foo)
        );
        SELECT 1, ds;
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert model.audits == [
        ("audit_a", {}),
        ("audit_b", {"key": exp.Literal.string("value")}),
    ]
    assert model.tags == ["foo"]


def test_enable_audits_from_model_defaults():
    expressions = d.parse(
        """
        MODEL (
            name db.audit_model,
        );
        SELECT 1 as id;

        AUDIT (
    name assert_positive_order_ids,
    );
    SELECT *
    FROM @this_model
    WHERE
    id < 0;
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert len(model.audits) == 0
    assert len(model.inline_audits) == 1

    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb", audits=["assert_positive_order_ids"])
    )
    assert config.model_defaults.audits[0] == ("assert_positive_order_ids", {})

    snapshot = Snapshot.from_node(model, nodes={}, config=config)
    assert len(snapshot.audits) == 1
    assert type(snapshot.audits[0]) == ModelAudit
    assert snapshot.audits[0].query.sql() == "SELECT * FROM @this_model WHERE id < 0"


def test_description(sushi_context):
    assert sushi_context.models['"memory"."sushi"."orders"'].description == "Table of sushi orders."


def test_render_definition():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            owner owner_name,
            dialect spark,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column (`a`, 'yyyymmdd'),
                forward_only FALSE,
                disable_restatement FALSE,
                on_destructive_change 'ERROR'
            ),
            storage_format iceberg,
            partitioned_by `a`,
            grains (
                [a, b],
                c
            ),
            references (
                [a, b],
                c
            ),
        );

        @DEF(x, 1);
        CACHE TABLE x AS SELECT 1;
        ADD JAR 's3://my_jar.jar';

        SELECT
            1::int AS a,
            CAST(2 AS double) AS b,
            c::bool,
            1::int AS d, -- d
            CAST(2 AS double) AS e, --e
            f::bool, --f
            @test_macro(1),
        FROM
            db.other_table t1
            LEFT JOIN
            db.table t2
            ON
                t1.a = t2.a;

        @IF( @runtime_stage = 'creating', create index db_table_idx on db.table(a) );
    """
    )

    model = load_sql_based_model(
        expressions,
        python_env={
            "test_macro": Executable(payload="def test_macro(evaluator, v):\n    return v"),
        },
        default_catalog="catalog",
    )

    # Should not include the macro implementation.
    assert d.format_model_expressions(
        model.render_definition(include_python=False)
    ) == d.format_model_expressions(expressions)

    # Should include the macro implementation.
    assert "def test_macro(evaluator, v):" in d.format_model_expressions(model.render_definition())


def test_render_definition_with_defaults():
    query = """
        SELECT
            1::int AS a,
            CAST(2 AS double) AS b,
        FROM
            db.other_table t1
            LEFT JOIN
            db.table t2
            ON
                t1.a = t2.a
        """

    expressions = d.parse(
        f"""
        MODEL (
            name db.table,
            owner owner_name,
            dialect spark,
            kind VIEW,
        );

        {query}
        """
    )

    model = load_sql_based_model(
        expressions,
        default_catalog="catalog",
    )

    expected_expressions = d.parse(
        f"""
        MODEL (
            name db.table,
            owner owner_name,
            cron '@daily',
            dialect spark,
            kind VIEW (
                materialized FALSE
            )
        );

        {query}
        """
    )

    # Should not include the macro implementation.
    assert d.format_model_expressions(
        model.render_definition(include_python=False, include_defaults=True)
    ) == d.format_model_expressions(expected_expressions)


def test_cron():
    daily = _Node(name="x", cron="@daily")
    assert to_datetime(daily.cron_prev("2020-01-01")) == to_datetime("2019-12-31")
    assert to_datetime(daily.cron_floor("2020-01-01")) == to_datetime("2020-01-01")
    assert to_timestamp(daily.cron_floor("2020-01-01 10:00:00")) == to_timestamp("2020-01-01")
    assert to_timestamp(daily.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02")
    interval = daily.interval_unit
    assert to_datetime(interval.cron_prev("2020-01-01")) == to_datetime("2019-12-31")
    assert to_datetime(interval.cron_floor("2020-01-01")) == to_datetime("2020-01-01")
    assert to_timestamp(interval.cron_floor("2020-01-01 10:00:00")) == to_timestamp("2020-01-01")
    assert to_timestamp(interval.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02")

    offset = _Node(name="x", cron="1 0 * * *")
    assert to_datetime(offset.cron_prev("2020-01-01")) == to_datetime("2019-12-31 00:01")
    assert to_datetime(offset.cron_floor("2020-01-01")) == to_datetime("2019-12-31 00:01")
    assert to_timestamp(offset.cron_floor("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01 00:01"
    )
    assert to_timestamp(offset.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02 00:01")
    interval = offset.interval_unit
    assert to_datetime(interval.cron_prev("2020-01-01")) == to_datetime("2019-12-31")
    assert to_datetime(interval.cron_floor("2020-01-01")) == to_datetime("2020-01-01")
    assert to_timestamp(interval.cron_floor("2020-01-01 10:00:00")) == to_timestamp("2020-01-01")
    assert to_timestamp(interval.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02")

    hourly = _Node(name="x", cron="1 * * * *")
    assert to_timestamp(hourly.cron_prev("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01 09:01:00"
    )
    assert to_timestamp(hourly.cron_prev("2020-01-01 10:02:00")) == to_timestamp(
        "2020-01-01 10:01:00"
    )
    assert to_timestamp(hourly.cron_floor("2020-01-01 10:01:00")) == to_timestamp(
        "2020-01-01 10:01:00"
    )
    interval = hourly.interval_unit
    assert to_timestamp(interval.cron_prev("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01 09:00:00"
    )
    assert to_timestamp(interval.cron_prev("2020-01-01 10:01:00")) == to_timestamp(
        "2020-01-01 10:00:00"
    )
    assert to_timestamp(interval.cron_floor("2020-01-01 10:01:00")) == to_timestamp(
        "2020-01-01 10:00:00"
    )

    monthly = _Node(name="x", cron="0 0 2 * *")
    assert to_timestamp(monthly.cron_prev("2020-01-01 00:00:00")) == to_timestamp(
        "2019-12-02 00:00:00"
    )
    assert to_timestamp(monthly.cron_prev("2020-02-01 00:00:00")) == to_timestamp(
        "2020-01-02 00:00:00"
    )
    assert to_timestamp(monthly.cron_next("2020-01-01 00:00:00")) == to_timestamp(
        "2020-01-02 00:00:00"
    )
    assert to_timestamp(monthly.cron_next("2020-01-02 00:00:00")) == to_timestamp(
        "2020-02-02 00:00:00"
    )
    assert to_timestamp(monthly.cron_floor("2020-01-17 00:00:00")) == to_timestamp(
        "2020-01-02 00:00:00"
    )
    interval = monthly.interval_unit
    assert to_timestamp(interval.cron_prev("2020-01-01 00:00:00")) == to_timestamp(
        "2019-12-01 00:00:00"
    )
    assert to_timestamp(interval.cron_prev("2020-02-01 00:00:00")) == to_timestamp(
        "2020-01-01 00:00:00"
    )
    assert to_timestamp(interval.cron_next("2020-01-01 00:00:00")) == to_timestamp(
        "2020-02-01 00:00:00"
    )
    assert to_timestamp(interval.cron_floor("2020-01-17 00:00:00")) == to_timestamp(
        "2020-01-01 00:00:00"
    )

    yearly = ModelMeta(name="x", cron="0 0 1 2 *")
    assert to_timestamp(yearly.cron_prev("2020-01-01 00:00:00")) == to_timestamp(
        "2019-02-01 00:00:00"
    )
    assert to_timestamp(yearly.cron_next("2020-01-01 00:00:00")) == to_timestamp(
        "2020-02-01 00:00:00"
    )
    assert to_timestamp(yearly.cron_floor("2020-12-10 00:00:00")) == to_timestamp(
        "2020-02-01 00:00:00"
    )
    interval = yearly.interval_unit
    assert to_timestamp(interval.cron_prev("2020-01-01 00:00:00")) == to_timestamp(
        "2019-01-01 00:00:00"
    )
    assert to_timestamp(interval.cron_next("2020-01-01 00:00:00")) == to_timestamp(
        "2021-01-01 00:00:00"
    )
    assert to_timestamp(interval.cron_floor("2020-12-10 00:00:00")) == to_timestamp(
        "2020-01-01 00:00:00"
    )


def test_lookback():
    model = ModelMeta(
        name="x", cron="@hourly", kind=IncrementalByTimeRangeKind(time_column="ts", lookback=2)
    )
    assert to_timestamp(model.lookback_start("Jan 8 2020 04:00:00")) == to_timestamp(
        "Jan 8 2020 02:00:00"
    )

    model = ModelMeta(
        name="x", cron="@daily", kind=IncrementalByTimeRangeKind(time_column="ds", lookback=2)
    )
    assert to_timestamp(model.lookback_start("Jan 8 2020")) == to_timestamp("Jan 6 2020")

    model = ModelMeta(
        name="x", cron="0 0 1 * *", kind=IncrementalByTimeRangeKind(time_column="ds", lookback=2)
    )
    assert to_timestamp(model.lookback_start("April 1 2020")) == to_timestamp("Feb 1 2020")

    model = ModelMeta(
        name="x", cron="0 0 1 1 *", kind=IncrementalByTimeRangeKind(time_column="ds", lookback=2)
    )
    assert to_timestamp(model.lookback_start("Jan 1 2020")) == to_timestamp("Jan 1 2018")


def test_render_query(assert_exp_eq, sushi_context):
    model = SqlModel(
        name="test",
        cron="1 0 * * *",
        kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="y")),
        query=d.parse_one(
            """
        SELECT y
        FROM x
        WHERE
          y BETWEEN @start_date and @end_date AND
          y BETWEEN @start_ds and @end_ds
        """
        ),
    )
    assert_exp_eq(
        model.render_query(start="2020-10-28", end="2020-10-28"),
        """
        SELECT
          "y" AS "y"
        FROM "x" AS "x"
        WHERE
          "y" BETWEEN DATE_STR_TO_DATE('2020-10-28') AND DATE_STR_TO_DATE('2020-10-28')
          AND "y" BETWEEN '2020-10-28' AND '2020-10-28'
        """,
    )
    assert_exp_eq(
        model.render_query(start="2020-10-28", end="2020-10-28", table_mapping={"x": "x_mapped"}),
        """
        SELECT
          "y" AS "y"
        FROM "x_mapped" AS "x"
        WHERE
          "y" BETWEEN DATE_STR_TO_DATE('2020-10-28') AND DATE_STR_TO_DATE('2020-10-28')
          AND "y" BETWEEN '2020-10-28' AND '2020-10-28'
        """,
    )

    expressions = d.parse(
        """
        MODEL (
          name dummy.model,
          kind FULL,
          dialect postgres
        );

        SELECT COUNT(DISTINCT a) FILTER (WHERE b > 0) AS c FROM x
        """
    )
    model = load_sql_based_model(expressions, dialect="postgres")
    assert_exp_eq(
        model.render_query(),
        'SELECT COUNT(DISTINCT "a") FILTER (WHERE "b" > 0) AS "c" FROM "x" AS "x"',
    )

    assert_exp_eq(
        sushi_context.models['"memory"."sushi"."waiters"'].render_query().sql(),
        """
        SELECT DISTINCT
          CAST("o"."waiter_id" AS INT) AS "waiter_id",
          CAST("o"."event_date" AS DATE) AS "event_date"
        FROM "memory"."sushi"."orders" AS "o"
        WHERE
          "o"."event_date" <= CAST('1970-01-01' AS DATE) AND "o"."event_date" >= CAST('1970-01-01' AS DATE)
        """,
    )

    expressions = d.parse(
        """
        MODEL (
          name dummy.model,
          kind FULL,
          dialect duckdb
        );

        @DEF(x, ['1', '2', '3']);

        SELECT @x AS "x"
        """
    )
    model = load_sql_based_model(expressions, dialect="duckdb")
    assert model.render_query().sql("duckdb") == '''SELECT ['1', '2', '3'] AS "x"'''

    expressions = d.parse(
        """
        MODEL (
          name dummy.model,
          kind FULL
        );

        @DEF(area, r -> pi() * r * r);

        SELECT route, centroid, @area(route_radius) AS area
        """
    )
    model = load_sql_based_model(expressions)
    assert (
        model.render_query().sql()
        == 'SELECT "route" AS "route", "centroid" AS "centroid", PI() * "route_radius" * "route_radius" AS "area"'
    )

    expressions = d.parse(
        """
        MODEL (
          name dummy.model,
          kind FULL
        );

        @DEF(area, r -> pi() * r * r);
        @DEF(container_volume, (r, h) -> @area(@r) * h);

        SELECT container_id, @container_volume((cont_di / 2), cont_hi) AS area
        """
    )
    model = load_sql_based_model(expressions)
    assert (
        model.render_query().sql()
        == 'SELECT "container_id" AS "container_id", PI() * ("cont_di" / 2) * ("cont_di" / 2) * "cont_hi" AS "area"'
    )

    expressions = d.parse(
        """
        MODEL (
          name dummy.model,
          kind FULL
        );

        @DEF(times2, x -> x * 2);

        SELECT @times4(10) AS "i dont exist"
        """
    )
    model = load_sql_based_model(expressions)
    with pytest.raises(SQLMeshError, match=r"Macro 'times4' does not exist.*"):
        model.render_query()

    assert_exp_eq(
        sushi_context.models['"memory"."sushi"."top_waiters"'].render_query().sql(),
        """
        WITH "test_macros" AS (
          SELECT
            2 AS "lit_two",
            "waiter_revenue_by_day"."revenue" * 2.0 AS "sql_exp",
            CAST("waiter_revenue_by_day"."revenue" AS TEXT) AS "sql_lit"
          FROM "memory"."sushi"."waiter_revenue_by_day" AS "waiter_revenue_by_day"
        )
        SELECT
          CAST("waiter_revenue_by_day"."waiter_id" AS INT) AS "waiter_id",
          CAST("waiter_revenue_by_day"."revenue" AS DOUBLE) AS "revenue"
        FROM "memory"."sushi"."waiter_revenue_by_day" AS "waiter_revenue_by_day"
        WHERE
          "waiter_revenue_by_day"."event_date" = (
            SELECT
              MAX("waiter_revenue_by_day"."event_date") AS "_col_0"
            FROM "memory"."sushi"."waiter_revenue_by_day" AS "waiter_revenue_by_day"
          )
        ORDER BY
          "revenue" DESC
        LIMIT 10
        """,
    )


def test_time_column():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column ds
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions)
    assert model.time_column.column == exp.to_column("ds", quoted=True)
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == parse_one("(\"ds\", '%Y-%m-%d')")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds)
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions)
    assert model.time_column.column == exp.to_column("ds", quoted=True)
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == d.parse_one("(\"ds\", '%Y-%m-%d')")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect 'hive',
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds, 'yyyy-MM'),
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions)
    assert model.time_column.column == exp.to_column("ds", quoted=True)
    assert model.time_column.format == "%Y-%m"
    assert model.time_column.expression == d.parse_one("(\"ds\", '%Y-%m')")


def test_default_time_column():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column ds
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions, time_column_format="%Y")
    assert model.time_column.format == "%Y"

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds, "%Y")
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions, time_column_format="%m")
    assert model.time_column.format == "%Y"

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect hive,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds, "dd")
            )
        );

        SELECT col::text, ds::text
    """
    )
    model = load_sql_based_model(expressions, dialect="duckdb", time_column_format="%Y")
    assert model.time_column.format == "%d"


def test_convert_to_time_column():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds)
            )
        );

        SELECT ds::text
    """
    )
    model = load_sql_based_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == d.parse_one("'2022-01-01'")
    assert model.convert_to_time_column(to_datetime("2022-01-01")) == d.parse_one("'2022-01-01'")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds, '%d/%m/%Y')
            )
        );

        SELECT ds::text
    """
    )
    model = load_sql_based_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == d.parse_one("'01/01/2022'")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (di, '%Y%m%d')
            )
        );

        SELECT di::int
    """
    )
    model = load_sql_based_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == d.parse_one("20220101")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column (ds, '%Y%m%d')
            )
        );

        SELECT ds::date
    """
    )
    model = load_sql_based_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == d.parse_one("CAST('2022-01-01' AS DATE)")

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column ds
            )
        );

        SELECT ds::timestamp
    """
    )
    model = load_sql_based_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == d.parse_one(
        "CAST('2022-01-01 00:00:00' AS TIMESTAMP)"
    )


def test_parse(assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (
          name sushi.items,
          kind INCREMENTAL_BY_TIME_RANGE(
            time_column ds
          ),
          dialect '',
        );

        SELECT
          id::INT AS id,
          ds
        FROM x
        WHERE ds BETWEEN '{{ start_ds }}' AND @end_ds
    """
    )
    model = load_sql_based_model(expressions, dialect="hive")
    assert model.columns_to_types == {
        "ds": exp.DataType.build("unknown"),
        "id": exp.DataType.build("int"),
    }
    assert not model.annotated
    assert model.dialect == ""
    assert isinstance(model.query, exp.Select)
    assert isinstance(SqlModel.parse_raw(model.json()).query, exp.Select)
    assert_exp_eq(
        model.render_query(),
        """
      SELECT
        CAST("id" AS INT) AS "id",
        "ds" AS "ds"
      FROM "x" AS "x"
      WHERE
        "ds" BETWEEN '1970-01-01' AND '1970-01-01'
    """,
    )


CONST = "bar"


def test_python_model(assert_exp_eq) -> None:
    from functools import reduce

    @model(
        name="my_model",
        kind="full",
        columns={'"COL"': "int"},
        pre_statements=["CACHE TABLE x AS SELECT 1;"],
        post_statements=["DROP TABLE x;"],
        enabled=True,
    )
    def my_model(context, **kwargs):
        context.table("foo")
        context.table(model_name=CONST + ".baz")

        # This checks that built-in functions are serialized properly
        a = reduce(lambda x, y: x + y, [1, 2, 3, 4])  # noqa: F841

    m = model.get_registry()["my_model"].model(
        module_path=Path("."),
        path=Path("."),
        dialect="duckdb",
    )

    assert list(m.pre_statements) == [
        d.parse_one("CACHE TABLE x AS SELECT 1"),
    ]
    assert list(m.post_statements) == [
        d.parse_one("DROP TABLE x"),
    ]
    assert m.enabled
    assert m.dialect == "duckdb"
    assert m.depends_on == {'"foo"', '"bar"."baz"'}
    assert m.columns_to_types == {"col": exp.DataType.build("int")}
    assert_exp_eq(
        m.ctas_query(),
        """
SELECT
  CAST(NULL AS INT) AS "col"
FROM (VALUES
  (1)) AS t(dummy)
WHERE
  FALSE
LIMIT 0
""",
    )


def test_python_model_depends_on() -> None:
    @model(
        name="model_with_depends_on",
        kind="full",
        columns={'"COL"': "int"},
        depends_on={"foo.bar"},
    )
    def my_model(context, **kwargs):
        context.table("foo")
        context.table(model_name=CONST + ".baz")

    m = model.get_registry()["model_with_depends_on"].model(
        module_path=Path("."),
        path=Path("."),
    )

    # We are not expecting the context.table() calls to be reflected in the model's depends_on since we
    # explicitly specified the depends_on argument.
    assert m.depends_on == {'"foo"."bar"'}


def test_python_model_with_session_properties():
    @model(
        name="python_model_prop",
        kind="full",
        columns={"some_col": "int"},
        session_properties={"some_string": "string_prop", "some_bool": True, "some_float": 1.0},
    )
    def python_model_prop(context, **kwargs):
        context.table("foo")

    m = model.get_registry()["python_model_prop"].model(
        module_path=Path("."),
        path=Path("."),
        dialect="duckdb",
        defaults={
            "session_properties": {
                "some_string": "default_string",
                "default_value": "default_value",
            }
        },
    )
    assert m.session_properties == {
        "some_string": "string_prop",
        "some_bool": True,
        "some_float": 1.0,
        "default_value": "default_value",
    }


def test_python_models_returning_sql(assert_exp_eq) -> None:
    config = Config(model_defaults=ModelDefaultsConfig(dialect="snowflake"))
    context = Context(config=config)

    @model(
        name="model1",
        is_sql=True,
        description="A dummy model.",
        kind="full",
        dialect="snowflake",
        post_statements=["PUT file:///dir/tmp.csv @%table"],
    )
    def model1_entrypoint(evaluator: MacroEvaluator) -> exp.Select:
        return exp.select("x", "y").from_(exp.values([("1", 2), ("2", 3)], "_v", ["x", "y"]))

    @model(name="model2", is_sql=True, kind="full", dialect="snowflake")
    def model2_entrypoint(evaluator: MacroEvaluator) -> str:
        return "select * from model1"

    model1 = model.get_registry()["model1"].model(module_path=Path("."), path=Path("."))
    model2 = model.get_registry()["model2"].model(module_path=Path("."), path=Path("."))

    context.upsert_model(model1)
    context.upsert_model(model2)

    assert isinstance(model1, SqlModel)
    assert isinstance(model1.query, d.MacroFunc)
    assert model1.description == "A dummy model."
    assert model1.depends_on == set()
    assert_exp_eq(
        context.render("model1"),
        """
        SELECT
          "_V"."X" AS "X",
          "_V"."Y" AS "Y"
        FROM (VALUES ('1', 2), ('2', 3)) AS "_V"("X", "Y")
        """,
    )
    post_statement = context.get_model("model1").post_statements[0].sql(dialect="snowflake")  # type: ignore
    assert post_statement == "PUT file:///dir/tmp.csv @%table"

    assert isinstance(model2, SqlModel)
    assert isinstance(model2.query, d.MacroFunc)
    assert model2.depends_on == {'"MODEL1"'}
    assert_exp_eq(
        context.render(
            "model2",
            expand=[
                d.normalize_model_name("model1", context.default_catalog, context.config.dialect)
            ],
        ),
        """
        SELECT
          "MODEL1"."X" AS "X",
          "MODEL1"."Y" AS "Y"
        FROM (
          SELECT
            "_V"."X" AS "X",
            "_V"."Y" AS "Y"
          FROM (VALUES ('1', 2), ('2', 3)) AS "_V"("X", "Y")
        ) AS "MODEL1"
        """,
    )


def test_python_model_decorator_kind() -> None:
    logger = logging.getLogger("sqlmesh.core.model.decorator")

    # no kind specified -> default Full kind
    @model("default_kind", columns={'"COL"': "int"})
    def a_model(context):
        pass

    python_model = model.get_registry()["default_kind"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert isinstance(python_model.kind, FullKind)

    # string kind name specified
    @model("kind_string", kind="external", columns={'"COL"': "int"})
    def b_model(context):
        pass

    python_model = model.get_registry()["kind_string"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert isinstance(python_model.kind, ExternalKind)

    @model("kind_empty_dict", kind=dict(), columns={'"COL"': "int"})
    def my_model(context):
        pass

    # error if kind dict with no `name` key
    with pytest.raises(ConfigError, match="`kind` dictionary must contain a `name` key"):
        python_model = model.get_registry()["kind_empty_dict"].model(
            module_path=Path("."),
            path=Path("."),
        )

    @model("kind_view", kind="view", columns={'"COL"': "int"})
    def kind_view(context):
        pass

    # error if kind = view
    with pytest.raises(
        SQLMeshError, match=r".*Cannot create Python model.*doesnt support Python models"
    ):
        python_model = (
            model.get_registry()["kind_view"]
            .model(
                module_path=Path("."),
                path=Path("."),
            )
            .validate_definition()
        )

    @model("kind_dict_badname", kind=dict(name="test"), columns={'"COL"': "int"})
    def my_model_1(context):
        pass

    # error if kind dict with `name` key whose type is not a ModelKindName enum
    with pytest.raises(ConfigError, match="with a valid ModelKindName enum value"):
        python_model = model.get_registry()["kind_dict_badname"].model(
            module_path=Path("."),
            path=Path("."),
        )

    @model("kind_instance", kind=FullKind(), columns={'"COL"': "int"})
    def my_model_2(context):
        pass

    # warning if kind is ModelKind instance
    with patch.object(logger, "warning") as mock_logger:
        python_model = model.get_registry()["kind_instance"].model(
            module_path=Path("."),
            path=Path("."),
        )

        assert (
            mock_logger.call_args[0][0]
            == """Python model "kind_instance"'s `kind` argument was passed a SQLMesh `FullKind` object. This may result in unexpected behavior - provide a dictionary instead."""
        )

    # no warning with valid kind dict
    with patch.object(logger, "warning") as mock_logger:

        @model("kind_valid_dict", kind=dict(name=ModelKindName.FULL), columns={'"COL"': "int"})
        def my_model(context):
            pass

        python_model = model.get_registry()["kind_valid_dict"].model(
            module_path=Path("."),
            path=Path("."),
        )

        assert isinstance(python_model.kind, FullKind)

        assert not mock_logger.call_args


def test_python_model_decorator_col_descriptions() -> None:
    # `columns` and `column_descriptions` column names are different cases, but name normalization makes both lower
    @model("col_descriptions", columns={"col": "int"}, column_descriptions={"COL": "a column"})
    def a_model(context):
        pass

    py_model = model.get_registry()["col_descriptions"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert py_model.columns_to_types.keys() == py_model.column_descriptions.keys()

    # error: `columns` and `column_descriptions` column names are different cases, quoting preserves case
    @model(
        "col_descriptions_quoted",
        columns={'"col"': "int"},
        column_descriptions={'"COL"': "a column"},
    )
    def b_model(context):
        pass

    with pytest.raises(ConfigError, match="a description is provided for column 'COL'"):
        py_model = model.get_registry()["col_descriptions_quoted"].model(
            module_path=Path("."),
            path=Path("."),
        )


def test_star_expansion(assert_exp_eq) -> None:
    context = Context(config=Config())

    model1 = load_sql_based_model(
        d.parse(
            """
        MODEL (name db.model1, kind full);

        SELECT
            id::INT AS id,
            item_id::INT AS item_id,
            ds::TEXT AS ds,
        FROM
            (VALUES
                (1, 1, '2020-01-01'),
                (1, 2, '2020-01-01'),
                (2, 1, '2020-01-01'),
                (3, 3, '2020-01-03'),
                (4, 1, '2020-01-04'),
                (5, 1, '2020-01-05'),
                (6, 1, '2020-01-06'),
                (7, 1, '2020-01-07')
            ) AS t (id, item_id, ds)
        """
        ),
        default_catalog=context.default_catalog,
    )

    model2 = load_sql_based_model(
        d.parse(
            """
        MODEL (name db.model2, kind full);

        SELECT * FROM db.model1 AS model1
        """
        ),
        default_catalog=context.default_catalog,
    )

    model3 = load_sql_based_model(
        d.parse(
            """
            MODEL(name db.model3, kind full);

            SELECT * FROM db.model2 AS model2
        """
        ),
        default_catalog=context.default_catalog,
    )

    context.upsert_model(model1)
    context.upsert_model(model2)
    context.upsert_model(model3)

    assert_exp_eq(
        context.render("db.model2", expand=["db.model1"]),
        """
        SELECT
          "model1"."id" AS "id",
          "model1"."item_id" AS "item_id",
          "model1"."ds" AS "ds"
        FROM (
          SELECT
            CAST("t"."id" AS INT) AS "id",
            CAST("t"."item_id" AS INT) AS "item_id",
            CAST("t"."ds" AS TEXT) AS "ds"
          FROM (VALUES
            (1, 1, '2020-01-01'),
            (1, 2, '2020-01-01'),
            (2, 1, '2020-01-01'),
            (3, 3, '2020-01-03'),
            (4, 1, '2020-01-04'),
            (5, 1, '2020-01-05'),
            (6, 1, '2020-01-06'),
            (7, 1, '2020-01-07')) AS "t"("id", "item_id", "ds")
        ) AS "model1"
        """,
    )
    assert_exp_eq(
        context.render("db.model3", expand=["db.model1", "db.model2"]),
        """
        SELECT
          "model2"."id" AS "id",
          "model2"."item_id" AS "item_id",
          "model2"."ds" AS "ds"
        FROM (
          SELECT
            "model1"."id" AS "id",
            "model1"."item_id" AS "item_id",
            "model1"."ds" AS "ds"
          FROM (
            SELECT
              CAST("t"."id" AS INT) AS "id",
              CAST("t"."item_id" AS INT) AS "item_id",
              CAST("t"."ds" AS TEXT) AS "ds"
            FROM (VALUES
              (1, 1, '2020-01-01'),
              (1, 2, '2020-01-01'),
              (2, 1, '2020-01-01'),
              (3, 3, '2020-01-03'),
              (4, 1, '2020-01-04'),
              (5, 1, '2020-01-05'),
              (6, 1, '2020-01-06'),
              (7, 1, '2020-01-07')) AS "t"("id", "item_id", "ds")
          ) AS "model1"
        ) AS "model2"
        """,
    )

    snapshots = context.snapshots
    snapshots['"memory"."db"."model1"'].categorize_as(SnapshotChangeCategory.BREAKING)
    snapshots['"memory"."db"."model2"'].categorize_as(SnapshotChangeCategory.BREAKING)
    snapshots['"memory"."db"."model3"'].categorize_as(SnapshotChangeCategory.BREAKING)

    assert_exp_eq(
        snapshots['"memory"."db"."model2"'].model.render_query(snapshots=snapshots),
        f"""
            SELECT
              "model1"."id" AS "id",
              "model1"."item_id" AS "item_id",
              "model1"."ds" AS "ds"
            FROM "memory"."sqlmesh__db"."db__model1__{snapshots['"memory"."db"."model1"'].version}" AS "model1"
            """,
    )

    assert_exp_eq(
        context.models['"memory"."db"."model3"'].render_query(snapshots=snapshots),
        f"""
            SELECT
              "model2"."id" AS "id",
              "model2"."item_id" AS "item_id",
              "model2"."ds" AS "ds"
            FROM "memory"."sqlmesh__db"."db__model2__{snapshots['"memory"."db"."model2"'].version}" AS "model2"
            """,
    )


def test_case_sensitivity(assert_exp_eq):
    config = Config(model_defaults=ModelDefaultsConfig(dialect="snowflake"))
    context = Context(config=config)

    source = load_sql_based_model(
        d.parse(
            """
            MODEL (name example.source, kind EMBEDDED);

            SELECT 'id' AS "id", 'name' AS "name", 'payload' AS "payload"
            """
        ),
        dialect="snowflake",
        default_catalog=context.default_catalog,
    )

    # Ensure that when manually specifying dependencies, they're normalized correctly
    downstream = load_sql_based_model(
        d.parse(
            """
            MODEL (name example.model, kind FULL, depends_on [ExAmPlE.SoUrCe]);

            SELECT JSON_EXTRACT_PATH_TEXT("payload", 'field') AS "new_field", * FROM example.source
            """
        ),
        dialect="snowflake",
        default_catalog=context.default_catalog,
    )

    context.upsert_model(source)
    context.upsert_model(downstream)

    assert_exp_eq(
        context.render("example.model"),
        """
        SELECT
          JSON_EXTRACT_PATH_TEXT("SOURCE"."payload", 'field') AS "new_field",
          "SOURCE"."id" AS "id",
          "SOURCE"."name" AS "name",
          "SOURCE"."payload" AS "payload"
        FROM (
          SELECT
            'id' AS "id",
            'name' AS "name",
            'payload' AS "payload"
        ) AS "SOURCE"
        """,
    )

    model = load_sql_based_model(
        d.parse("MODEL (name test, dialect hive); SELECT 1 AS c"),
        dialect="snowflake",
    )
    assert model.dialect == "hive"


def test_batch_size_validation():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            ),
            columns (
              id double,
              alias varchar
            ),
            batch_size 100,
        );
    """
    )

    with pytest.raises(ConfigError):
        load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))


def test_model_cache(tmp_path: Path, mocker: MockerFixture):
    cache = ModelCache(tmp_path)

    expressions = d.parse(
        """
        MODEL (
            name db.seed,
        );
        SELECT 1, ds;
    """
    )

    model = load_sql_based_model([e for e in expressions if e])

    loader = mocker.Mock(return_value=model)

    assert cache.get_or_load("test_model", "test_entry_a", loader=loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_a", loader=loader).dict() == model.dict()

    assert cache.get_or_load("test_model", "test_entry_b", loader=loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_b", loader=loader).dict() == model.dict()

    assert cache.get_or_load("test_model", "test_entry_a", loader=loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_a", loader=loader).dict() == model.dict()

    assert loader.call_count == 2


@pytest.mark.slow
def test_model_cache_gateway(tmp_path: Path, mocker: MockerFixture):
    init_example_project(tmp_path, dialect="duckdb")

    db_path = str(tmp_path / "db.db")
    config = Config(
        gateways={
            "main": GatewayConfig(connection=DuckDBConnectionConfig(database=db_path)),
            "secondary": GatewayConfig(connection=DuckDBConnectionConfig()),
        },
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
    )
    Context(paths=tmp_path, config=config)

    patched_cache_put = mocker.patch("sqlmesh.utils.cache.FileCache.put")

    Context(paths=tmp_path, config=config)
    assert patched_cache_put.call_count == 0

    Context(paths=tmp_path, config=config, gateway="secondary")
    assert patched_cache_put.call_count == 4


@pytest.mark.slow
def test_model_cache_default_catalog(tmp_path: Path, mocker: MockerFixture):
    init_example_project(tmp_path, dialect="duckdb")
    Context(paths=tmp_path)

    patched_cache_put = mocker.patch("sqlmesh.utils.cache.FileCache.put")

    Context(paths=tmp_path)
    assert patched_cache_put.call_count == 0

    with patch(
        "sqlmesh.core.engine_adapter.base.EngineAdapter.default_catalog",
        PropertyMock(return_value=None),
    ):
        Context(paths=tmp_path)
        assert patched_cache_put.call_count == 4


def test_model_ctas_query():
    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 as a FROM x WHERE TRUE LIMIT 2
    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "x" AS "x" WHERE TRUE AND FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name db.table, kind FULL);
        SELECT 1 as a FROM b
        """
    )

    assert (
        load_sql_based_model(expressions).ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "b" AS "b" WHERE FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 AS a FROM t UNION ALL SELECT 2 AS a FROM t UNION ALL SELECT 3 AS a FROM t UNION ALL SELECT 4 AS a FROM t
    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "t" AS "t" WHERE FALSE UNION ALL SELECT 2 AS "a" FROM "t" AS "t" WHERE FALSE UNION ALL SELECT 3 AS "a" FROM "t" AS "t" WHERE FALSE UNION ALL SELECT 4 AS "a" FROM "t" AS "t" WHERE FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 AS a FROM t UNION ALL SELECT 2 AS a FROM t
    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "t" AS "t" WHERE FALSE UNION ALL SELECT 2 AS "a" FROM "t" AS "t" WHERE FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 AS a FROM t UNION ALL SELECT 2 AS a FROM t ORDER BY 1
    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "t" AS "t" WHERE FALSE UNION ALL SELECT 2 AS "a" FROM "t" AS "t" WHERE FALSE ORDER BY 1 LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        WITH RECURSIVE a AS (
            SELECT * FROM x
        ), b AS (
            SELECT * FROM a UNION ALL SELECT * FROM a
        )
        SELECT * FROM b

    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'WITH RECURSIVE "a" AS (SELECT * FROM "x" AS "x" WHERE FALSE), "b" AS (SELECT * FROM "a" AS "a" WHERE FALSE UNION ALL SELECT * FROM "a" AS "a" WHERE FALSE) SELECT * FROM "b" AS "b" WHERE FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        WITH RECURSIVE a AS (
            SELECT * FROM (SELECT * FROM (SELECT * FROM x))
        ), b AS (
            SELECT * FROM a UNION ALL SELECT * FROM a
        )
        SELECT * FROM b

    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'WITH RECURSIVE "a" AS (SELECT * FROM (SELECT * FROM (SELECT * FROM "x" AS "x" WHERE FALSE) AS "_q_0" WHERE FALSE) AS "_q_1" WHERE FALSE), "b" AS (SELECT * FROM "a" AS "a" WHERE FALSE UNION ALL SELECT * FROM "a" AS "a" WHERE FALSE) SELECT * FROM "b" AS "b" WHERE FALSE LIMIT 0'
    )

    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        WITH RECURSIVE a AS (
            WITH nested_a AS (
                SELECT * FROM (SELECT * FROM (SELECT * FROM x))
            )
            SELECT * FROM nested_a
        ), b AS (
            SELECT * FROM a UNION ALL SELECT * FROM a
        )
        SELECT * FROM b

    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'WITH RECURSIVE "a" AS (WITH "nested_a" AS (SELECT * FROM (SELECT * FROM (SELECT * FROM "x" AS "x" WHERE FALSE) AS "_q_0" WHERE FALSE) AS "_q_1" WHERE FALSE) SELECT * FROM "nested_a" AS "nested_a" WHERE FALSE), "b" AS (SELECT * FROM "a" AS "a" WHERE FALSE UNION ALL SELECT * FROM "a" AS "a" WHERE FALSE) SELECT * FROM "b" AS "b" WHERE FALSE LIMIT 0'
    )


def test_is_breaking_change():
    model = create_external_model("a", columns={"a": "int", "limit": "int"})
    assert model.is_breaking_change(create_external_model("a", columns={"a": "int"})) is False
    assert model.is_breaking_change(create_external_model("a", columns={"a": "text"})) is None
    assert (
        model.is_breaking_change(create_external_model("a", columns={"a": "int", "limit": "int"}))
        is False
    )
    assert (
        model.is_breaking_change(
            create_external_model("a", columns={"a": "int", "limit": "int", "c": "int"})
        )
        is None
    )


def test_parse_expression_list_with_jinja():
    input = [
        "JINJA_STATEMENT_BEGIN;\n{{ log('log message') }}\nJINJA_END;",
        "GRANT SELECT ON TABLE foo TO DEV",
    ]
    assert input == [val.sql() for val in parse_expression(SqlModel, input, {})]


def test_no_depends_on_runtime_jinja_query():
    @macro()
    def runtime_macro(**kwargs) -> None:
        from sqlmesh.utils.errors import ParsetimeAdapterCallError

        raise ParsetimeAdapterCallError("")

    expressions = d.parse(
        """
        MODEL (name db.table);

        JINJA_QUERY_BEGIN;
        SELECT {{ runtime_macro() }} as a FROM b
        JINJA_QUERY_END;
        """
    )

    model = load_sql_based_model(expressions)
    with pytest.raises(
        ConfigError,
        match=r"Dependencies must be provided explicitly for models that can be rendered only at runtime at.*",
    ):
        model.validate_definition()


def test_update_schema():
    expressions = d.parse(
        """
        MODEL (name db.table);

        SELECT * FROM table_a JOIN table_b
        """
    )

    model = load_sql_based_model(expressions)
    schema = MappingSchema(normalize=False)

    # Even though the partial schema is applied, we won't optimize the model
    schema.add_table('"table_a"', {"a": exp.DataType.build("int")})
    model.update_schema(schema)
    assert model.mapping_schema == {'"table_a"': {"a": "INT"}}

    logger = logging.getLogger("sqlmesh.core.renderer")
    with patch.object(logger, "warning") as mock_logger:
        model.render_query(needs_optimization=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg(
            '"db"."table"', ('"table_b"',)
        )

    schema.add_table('"table_b"', {"b": exp.DataType.build("int")})
    model.update_schema(schema)
    assert model.mapping_schema == {
        '"table_a"': {"a": "INT"},
        '"table_b"': {"b": "INT"},
    }
    model.render_query(needs_optimization=True)


def test_missing_schema_warnings():
    logger = logging.getLogger("sqlmesh.core.renderer")

    full_schema = MappingSchema(
        {
            "a": {"x": exp.DataType.build("int")},
            "b": {"y": exp.DataType.build("int")},
        },
        normalize=False,
    )

    partial_schema = MappingSchema(
        {
            '"a"': {"x": exp.DataType.build("int")},
        },
    )

    # star, no schema, no deps
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM (SELECT 1 a) x"))
        model.render_query(needs_optimization=True)
        mock_logger.assert_not_called()

    # star, full schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM a CROSS JOIN b"))
        model.update_schema(full_schema)
        model.render_query(needs_optimization=True)
        mock_logger.assert_not_called()

    # star, partial schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM a CROSS JOIN b"))
        model.update_schema(partial_schema)
        model.render_query(needs_optimization=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg('"test"', ('"b"',))

    # star, no schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM b JOIN a"))
        model.render_query(needs_optimization=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg('"test"', ('"a"', '"b"'))

    # no star, full schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.update_schema(full_schema)
        model.render_query(needs_optimization=True)
        mock_logger.assert_not_called()

    # no star, partial schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.update_schema(partial_schema)
        model.render_query(needs_optimization=True)
        mock_logger.assert_not_called()

    # no star, empty schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.render_query(needs_optimization=True)
        mock_logger.assert_not_called()


def test_user_provided_depends_on():
    expressions = d.parse(
        """
        MODEL (name db.table, depends_on [table_b]);

        SELECT a FROM table_a
        """
    )

    model = load_sql_based_model(expressions)

    assert model.depends_on == {'"table_a"', '"table_b"'}


def test_check_schema_mapping_when_rendering_at_runtime(assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (name db.table, depends_on [table_b]);

        SELECT * FROM table_a JOIN table_b
        """
    )

    model = load_sql_based_model(expressions)

    # Simulate a query that cannot be rendered at parse time.
    with patch.object(SqlModel, "render_query", return_value=None) as render_query_mock:
        assert not model.columns_to_types

        schema = MappingSchema(normalize=False)
        schema.add_table('"table_b"', {"b": exp.DataType.build("int")})
        model.update_schema(schema)

    assert '"table_b"' in model.mapping_schema
    assert model.depends_on == {'"table_b"'}

    render_query_mock.assert_called_once()

    # Simulate rendering at runtime.
    assert_exp_eq(
        model.render_query(), """SELECT * FROM "table_a" AS "table_a", "table_b" AS "table_b" """
    )


def test_model_normalization():
    expr = d.parse(
        """
        MODEL (
            name `project-1.db.tbl`,
            kind FULL,
            columns ( a STRUCT <`a` INT64> ),
            partitioned_by foo(`ds`),
            dialect bigquery,
            grain [id, ds]
        );
        SELECT * FROM project-1.db.raw
        """
    )

    model = SqlModel.parse_raw(load_sql_based_model(expr, depends_on={"project-2.db.raw"}).json())
    assert model.name == "`project-1.db.tbl`"
    assert model.columns_to_types["a"].sql(dialect="bigquery") == "STRUCT<`a` INT64>"
    assert model.partitioned_by[0].sql(dialect="bigquery") == "foo(`ds`)"
    # The values are quoted with double quotes instead of backticks because of how model name normalization works
    # since it normalizes values according to the target dialect but not the quotes
    assert model.depends_on == {'"project-1"."db"."raw"', '"project-2"."db"."raw"'}

    expr = d.parse(
        """
        MODEL (
            name foo,
            kind INCREMENTAL_BY_TIME_RANGE (
              time_column a
            ),
            columns (a int),
            partitioned_by foo("ds"),
            dialect snowflake,
            grain [id, ds],
            tags (pii, fact),
            clustered_by a
        );
        SELECT * FROM bla
        """
    )

    model = SqlModel.parse_raw(load_sql_based_model(expr).json())
    assert model.name == "foo"
    assert model.time_column.column == exp.to_column("A", quoted=True)
    assert model.columns_to_types["A"].sql(dialect="snowflake") == "INT"
    assert model.partitioned_by[0].sql(dialect="snowflake") == '"A"'
    assert model.partitioned_by[1].sql(dialect="snowflake") == 'FOO("ds")'
    assert model.tags == ["pii", "fact"]
    assert model.clustered_by == ["A"]
    assert model.depends_on == {'"BLA"'}

    # Check possible variations of unique_key definitions
    for key in ("""[a, COALESCE(b, ''), "c"]""", """(a, COALESCE(b, ''), "c")"""):
        expr = d.parse(
            f"""
            MODEL (
                name foo,
                kind INCREMENTAL_BY_UNIQUE_KEY (
                    unique_key {key}
                ),
                dialect snowflake
            );

            SELECT
              x.a AS a,
              x.b AS b,
              x."c" AS c
            FROM test.x AS x
            """
        )
        model = SqlModel.parse_raw(load_sql_based_model(expr).json())
        assert model.unique_key == [
            exp.column("A", quoted=True),
            exp.func("COALESCE", exp.column("B", quoted=True), "''"),
            exp.column("c", quoted=True),
        ]

    expr = d.parse(
        """
        MODEL (
            name foo,
            dialect snowflake,
            kind INCREMENTAL_BY_UNIQUE_KEY(unique_key a),
              audits (
                not_null(COLUMNS := id)
            ),
        );

        SELECT x.a AS a FROM test.x AS x
        """
    )
    model = SqlModel.parse_raw(load_sql_based_model(expr).json())
    assert model.unique_key == [exp.column("A", quoted=True)]
    # we should never normalize the model meta, additionally, we should force lower case
    assert model.audits[0][1] == {"columns": exp.column("id")}

    model = create_sql_model(
        "foo",
        parse_one("SELECT * FROM bla"),
        columns={"a": "int"},
        kind=IncrementalByTimeRangeKind(time_column=exp.column("a"), dialect="snowflake"),
        dialect="snowflake",
        grain=[exp.to_column("id"), exp.to_column("ds")],
        tags=["pii", "fact"],
        clustered_by=exp.to_column("a"),
    )
    assert model.name == "foo"
    assert model.time_column.column == exp.column("A", quoted=True)
    assert model.columns_to_types["A"].sql(dialect="snowflake") == "INT"
    assert model.tags == ["pii", "fact"]
    assert model.clustered_by == ["A"]
    assert model.depends_on == {'"BLA"'}

    model = create_sql_model(
        "foo",
        parse_one("SELECT * FROM bla"),
        kind=IncrementalByTimeRangeKind(time_column=exp.to_column("a")),
        dialect="snowflake",
        grain=[exp.to_column("id"), exp.to_column("ds")],
        tags=["pii", "fact"],
        clustered_by=[exp.column("a"), exp.column("b")],
    )
    assert model.clustered_by == ["A", "B"]

    model = create_sql_model(
        "foo",
        parse_one("SELECT * FROM bla"),
        kind=IncrementalByTimeRangeKind(time_column=exp.to_column("a")),
        dialect="snowflake",
        grain=[exp.to_column("id"), exp.to_column("ds")],
        tags=["pii", "fact"],
        clustered_by=["a", "b"],
    )
    assert model.clustered_by == ["A", "B"]


def test_incremental_unmanaged_validation():
    model = create_sql_model(
        "a",
        d.parse_one("SELECT a, ds FROM table_a"),
        kind=IncrementalUnmanagedKind(insert_overwrite=True),
    )

    with pytest.raises(
        ConfigError,
        match=r"Unmanaged incremental models with insert / overwrite enabled must specify the partitioned_by field.*",
    ):
        model.validate_definition()

    model = model.copy(update={"partitioned_by_": [exp.to_column("ds")]})
    model.validate_definition()


def test_custom_interval_unit():
    assert (
        load_sql_based_model(
            d.parse("MODEL (name db.table, interval_unit FIVE_MINUTE); SELECT a FROM tbl;")
        ).interval_unit
        == IntervalUnit.FIVE_MINUTE
    )

    assert (
        load_sql_based_model(
            d.parse("MODEL (name db.table, interval_unit hour); SELECT a FROM tbl;")
        ).interval_unit
        == IntervalUnit.HOUR
    )

    assert (
        load_sql_based_model(
            d.parse("MODEL (name db.table, interval_unit Hour, cron '@daily'); SELECT a FROM tbl;")
        ).interval_unit
        == IntervalUnit.HOUR
    )

    assert (
        load_sql_based_model(
            d.parse("MODEL (name db.table, cron '@daily'); SELECT a FROM tbl;")
        ).interval_unit
        == IntervalUnit.DAY
    )

    assert (
        load_sql_based_model(
            d.parse("MODEL (name db.table, cron '0 5 * * *'); SELECT a FROM tbl;")
        ).interval_unit
        == IntervalUnit.DAY
    )

    assert (
        load_sql_based_model(
            d.parse(
                "MODEL (name db.table, cron '0 5 * * *', interval_unit 'quarter_hour'); SELECT a FROM tbl;"
            )
        ).interval_unit
        == IntervalUnit.QUARTER_HOUR
    )

    with pytest.raises(
        ConfigError, match=r"Interval unit of '.*' is larger than cron period of '@daily'"
    ):
        load_sql_based_model(
            d.parse("MODEL (name db.table, interval_unit month); SELECT a FROM tbl;")
        )

    with pytest.raises(
        ConfigError, match=r"Interval unit of '.*' is larger than cron period of '@hourly'"
    ):
        load_sql_based_model(
            d.parse("MODEL (name db.table, interval_unit Day, cron '@hourly'); SELECT a FROM tbl;")
        )


def test_model_physical_properties() -> None:
    # Validate python model table properties
    @model(
        "my_model",
        kind="full",
        columns={"id": "int"},
        physical_properties={
            "format": "PARQUET",
            "bucket_count": 0,
            "orc_bloom_filter_fpp": 0.05,
            "auto_purge": False,
        },
    )
    def my_model(context, **kwargs):
        pass

    python_model = model.get_registry()["my_model"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert python_model.physical_properties == {
        "format": exp.Literal.string("PARQUET"),
        "bucket_count": exp.Literal.number(0),
        "orc_bloom_filter_fpp": exp.Literal.number(0.05),
        "auto_purge": exp.false(),
    }

    # Validate a tuple.
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            physical_properties (
                key_a = 'value_a',
                'key_b' = 1,
                key_c = true,
                "key_d" = 2.0,
            )
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert sql_model.physical_properties_ == d.parse_one(
        """(key_a = 'value_a', 'key_b' = 1, key_c = TRUE, "key_d" = 2.0)"""
    )

    # Validate a tuple with one item.
    sql_model = load_sql_based_model(
        d.parse(
            """
            MODEL (
                name test_schema.test_model,
                physical_properties (key_a = 'value_a')
            );
            SELECT a FROM tbl;
            """
        )
    )
    assert sql_model.physical_properties == {"key_a": exp.convert("value_a")}
    assert (
        sql_model.physical_properties_.sql()  # type: ignore
        == exp.Tuple(expressions=[d.parse_one("key_a = 'value_a'")]).sql()
    )

    # Validate an array.
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            physical_properties [
                key_a = 'value_a',
                'key_b' = 1,
            ]
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
    }
    assert sql_model.physical_properties_ == d.parse_one("""(key_a = 'value_a', 'key_b' = 1)""")

    # Validate empty.
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {}
    assert sql_model.physical_properties_ is None

    # Validate sql expression.
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            physical_properties [
                key = ['value']
            ]
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {"key": d.parse_one("['value']")}
    assert sql_model.physical_properties_ == exp.Tuple(expressions=[d.parse_one("key = ['value']")])

    # Validate dict parsing.
    sql_model = create_sql_model(
        name="test_schema.test_model",
        query=d.parse_one("SELECT a FROM tbl"),
        physical_properties={
            "key_a": exp.Literal.string("value_a"),
            "key_b": exp.Literal.number(1),
            "key_c": exp.true(),
            "key_d": exp.Literal.number(2.0),
        },
    )
    assert sql_model.physical_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert sql_model.physical_properties_ == d.parse_one(
        """('key_a' = 'value_a', 'key_b' = 1, 'key_c' = TRUE, 'key_d' = 2.0)"""
    )

    with pytest.raises(ConfigError, match=r"Invalid property 'invalid'.*"):
        load_sql_based_model(
            d.parse(
                """
                MODEL (
                    name test_schema.test_model,
                    physical_properties [
                        invalid
                    ]
                );
                SELECT a FROM tbl;
                """
            )
        )


def test_model_physical_properties_labels() -> None:
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            physical_properties [
                labels = [('test-label', 'label-value')]
            ]
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {"labels": exp.array("('test-label', 'label-value')")}


def test_physical_and_virtual_table_properties() -> None:
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            physical_properties (
                partition_expiration_days = 7,
                labels = [('test-physical-label', 'label-physical-value')],
            ),
            virtual_properties (
                labels = [('test-virtual-label', 'label-virtual-value')],
            )
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {
        "partition_expiration_days": exp.convert(7),
        "labels": exp.array("('test-physical-label', 'label-physical-value')"),
    }

    assert sql_model.virtual_properties == {
        "labels": exp.array("('test-virtual-label', 'label-virtual-value')"),
    }


def test_model_table_properties() -> None:
    # Ensure backward compatibility to table_properties.
    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            table_properties (
                key_a = 'value_a',
                'key_b' = 1,
                key_c = true,
                "key_d" = 2.0,
            )
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert sql_model.physical_properties_ == d.parse_one(
        """(key_a = 'value_a', 'key_b' = 1, key_c = TRUE, "key_d" = 2.0)"""
    )

    sql_model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            table_properties (
                partition_expiration_days = 7,
            )
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert sql_model.physical_properties == {
        "partition_expiration_days": exp.convert(7),
    }
    assert sql_model.physical_properties_ == d.parse_one("""(partition_expiration_days = 7,)""")


def test_model_table_properties_conflicts() -> None:
    # Throw an error on conflicting usage of table_properties and physical_properties.
    with pytest.raises(ConfigError, match=r"Cannot use argument 'table_properties'*"):
        sql_model = load_sql_based_model(
            d.parse(
                """
                MODEL (
                    name test_schema.test_model,
                    table_properties (
                        key_a = 'value_a',
                        'key_b' = 1,
                        key_c = true,
                        "key_d" = 2.0,
                    ),
                    physical_properties (key_a = 'value_a')
                );
                SELECT a FROM tbl;
                """
            )
        )
        sql_model.physical_properties


def test_session_properties_on_model_and_project(sushi_context):
    model_defaults = ModelDefaultsConfig(
        session_properties={
            "some_bool": False,
            "quoted_identifier": "value_you_wont_see",
            "project_level_property": "project_property",
        }
    )

    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            session_properties (
                'spark.executor.cores' = 2,
                'spark.executor.memory' = '1G',
                some_bool = True,
                some_float = 0.1,
                quoted_identifier = "quoted identifier",
                unquoted_identifier = unquoted_identifier,
            )
        );
        SELECT a FROM tbl;
        """,
            default_dialect="snowflake",
        ),
        defaults=model_defaults.dict(),
    )

    assert model.session_properties == {
        "spark.executor.cores": 2,
        "spark.executor.memory": "1G",
        "some_bool": True,
        "some_float": 0.1,
        "quoted_identifier": exp.column("quoted identifier", quoted=True),
        "unquoted_identifier": exp.column("unquoted_identifier", quoted=False),
        "project_level_property": "project_property",
    }


def test_project_level_session_properties(sushi_context):
    model_defaults = ModelDefaultsConfig(
        session_properties={
            "some_bool": False,
            "some_float": 0.1,
            "project_level_property": "project_property",
        }
    )

    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
        );
        SELECT a FROM tbl;
        """,
            default_dialect="snowflake",
        ),
        defaults=model_defaults.dict(),
    )

    assert model.session_properties == {
        "some_bool": False,
        "some_float": 0.1,
        "project_level_property": "project_property",
    }


def test_model_session_properties(sushi_context):
    assert sushi_context.models['"memory"."sushi"."items"'].session_properties == {
        "string_prop": "some_value",
        "int_prop": 1,
        "float_prop": 1.0,
        "bool_prop": True,
    }
    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            session_properties (
                'spark.executor.cores' = 2,
                'spark.executor.memory' = '1G',
                some_bool = True,
                some_float = 0.1,
                quoted_identifier = "quoted identifier",
                unquoted_identifier = unquoted_identifier,
            )
        );
        SELECT a FROM tbl;
        """,
            default_dialect="snowflake",
        )
    )

    assert model.session_properties == {
        "spark.executor.cores": 2,
        "spark.executor.memory": "1G",
        "some_bool": True,
        "some_float": 0.1,
        "quoted_identifier": exp.column("quoted identifier", quoted=True),
        "unquoted_identifier": exp.column("unquoted_identifier", quoted=False),
    }

    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            session_properties (
                'warehouse' = 'test_warehouse'
            )
        );
        SELECT a FROM tbl;
        """,
            default_dialect="snowflake",
        )
    )
    assert model.session_properties == {
        "warehouse": "test_warehouse",
    }


def test_model_jinja_macro_rendering():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
        );

        JINJA_STATEMENT_BEGIN;
        {{ test_package.macro_a() }}
        {{ macro_b() }}
        JINJA_END;

        SELECT 1 AS x;
    """
    )

    jinja_macros = JinjaMacroRegistry(
        packages={
            "test_package": {"macro_a": MacroInfo(definition="macro_a_body", depends_on=[])},
        },
        root_macros={"macro_b": MacroInfo(definition="macro_b_body", depends_on=[])},
        global_objs={"test_int": 1, "test_str": "value"},
    )

    model = load_sql_based_model(expressions, jinja_macros=jinja_macros)
    definition = model.render_definition()

    assert definition[1].sql() == "JINJA_STATEMENT_BEGIN;\nmacro_b_body\nJINJA_END;"
    assert definition[2].sql() == "JINJA_STATEMENT_BEGIN;\nmacro_a_body\nJINJA_END;"


def test_view_model_data_hash():
    view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW,
        );
        SELECT 1;
        """
    )
    view_model_hash = load_sql_based_model(view_model_expressions).data_hash

    materialized_view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW (
              materialized true
            ),
        );
        SELECT 1;
        """
    )
    materialized_view_model_hash = load_sql_based_model(
        materialized_view_model_expressions
    ).data_hash

    assert view_model_hash != materialized_view_model_hash


def test_view_materialized_partition_by_clustered_by():
    materialized_view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW (
              materialized true
            ),
            partitioned_by ds,
            clustered_by a
        );
        SELECT 1;
        """
    )
    materialized_view_model = load_sql_based_model(materialized_view_model_expressions)
    assert materialized_view_model.partitioned_by == [exp.column("ds", quoted=True)]
    assert materialized_view_model.clustered_by == ["a"]


def test_view_non_materialized_partition_by():
    view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW,
            partitioned_by ds,
        );
        SELECT 1;
        """
    )
    with pytest.raises(ConfigError, match=r".*partitioned_by_ field cannot be set for ViewKind.*"):
        load_sql_based_model(view_model_expressions)


def test_view_non_materialized_clustered_by():
    view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW,
            clustered_by ds,
        );
        SELECT 1;
        """
    )
    with pytest.raises(ConfigError, match=r".*clustered_by field cannot be set for ViewKind.*"):
        load_sql_based_model(view_model_expressions)


def test_seed_model_data_hash():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
            )
        );
    """
    )
    seed_model = load_sql_based_model(
        expressions, path=Path("./examples/sushi/models/test_model.sql")
    )

    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              csv_settings (
                quotechar = '''',
              )
            )
        );
    """
    )
    new_seed_model = load_sql_based_model(
        expressions, path=Path("./examples/sushi/models/test_model.sql")
    )

    assert seed_model.data_hash != new_seed_model.data_hash


def test_interval_unit_validation():
    assert (
        create_sql_model(
            "a",
            d.parse_one("SELECT a, ds FROM table_a"),
            interval_unit=IntervalUnit.HOUR,
        ).interval_unit
        == IntervalUnit.HOUR
    )

    assert (
        create_sql_model(
            "a",
            d.parse_one("SELECT a, ds FROM table_a"),
            interval_unit="HOUR",
        ).interval_unit
        == IntervalUnit.HOUR
    )

    assert (
        create_sql_model(
            "a",
            d.parse_one("SELECT a, ds FROM table_a"),
            interval_unit=None,
        ).interval_unit_
        is None
    )


def test_scd_type_2_by_time_defaults():
    model_def = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2 (
                unique_key (COALESCE("ID", '') || '|' || COALESCE("ds", ''), COALESCE("ds", '')),
            ),
        );
        SELECT
            1 as "ID",
            '2020-01-01' as ds,
            '2020-01-01' as test_updated_at,
            '2020-01-01' as test_valid_from,
            '2020-01-01' as test_valid_to
        ;
        """
    )
    scd_type_2_model = load_sql_based_model(model_def)
    assert scd_type_2_model.unique_key == [
        parse_one("""COALESCE("ID", '') || '|' || COALESCE("ds", '')"""),
        parse_one("""COALESCE("ds", '')"""),
    ]
    assert scd_type_2_model.columns_to_types == {
        "ID": exp.DataType.build("int"),
        "ds": exp.DataType.build("varchar"),
        "test_updated_at": exp.DataType.build("varchar"),
        "test_valid_from": exp.DataType.build("varchar"),
        "test_valid_to": exp.DataType.build("varchar"),
        "valid_from": exp.DataType.build("TIMESTAMP"),
        "valid_to": exp.DataType.build("TIMESTAMP"),
    }
    assert scd_type_2_model.managed_columns == {
        "valid_from": exp.DataType.build("TIMESTAMP"),
        "valid_to": exp.DataType.build("TIMESTAMP"),
    }
    assert scd_type_2_model.kind.updated_at_name == exp.column("updated_at", quoted=True)
    assert scd_type_2_model.kind.valid_from_name == exp.column("valid_from", quoted=True)
    assert scd_type_2_model.kind.valid_to_name == exp.column("valid_to", quoted=True)
    assert not scd_type_2_model.kind.updated_at_as_valid_from
    assert scd_type_2_model.kind.is_scd_type_2_by_time
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert scd_type_2_model.kind.forward_only
    assert scd_type_2_model.kind.disable_restatement


def test_scd_type_2_by_time_overrides():
    model_def = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_TIME (
                unique_key ["iD", ds],
                updated_at_name test_updated_at,
                valid_from_name test_valid_from,
                valid_to_name test_valid_to,
                time_data_type TIMESTAMPTZ,
                updated_at_as_valid_from True,
                forward_only False,
                disable_restatement False,
                invalidate_hard_deletes False,
            ),
            dialect snowflake
        );
        SELECT
            1 as "iD",
            '2020-01-01' as ds,
            '2020-01-01' as test_updated_at,
            '2020-01-01' as test_valid_from,
            '2020-01-01' as test_valid_to
        ;
        """
    )
    scd_type_2_model = load_sql_based_model(model_def)
    assert scd_type_2_model.unique_key == [
        exp.column("iD", quoted=True),
        exp.column("DS", quoted=True),
    ]
    assert scd_type_2_model.managed_columns == {
        "TEST_VALID_FROM": exp.DataType.build("TIMESTAMPTZ"),
        "TEST_VALID_TO": exp.DataType.build("TIMESTAMPTZ"),
    }
    assert scd_type_2_model.kind.updated_at_name == exp.column("TEST_UPDATED_AT", quoted=True)
    assert scd_type_2_model.kind.valid_from_name == exp.column("TEST_VALID_FROM", quoted=True)
    assert scd_type_2_model.kind.valid_to_name == exp.column("TEST_VALID_TO", quoted=True)
    assert scd_type_2_model.kind.updated_at_as_valid_from
    assert scd_type_2_model.kind.is_scd_type_2_by_time
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert not scd_type_2_model.kind.invalidate_hard_deletes
    assert not scd_type_2_model.kind.forward_only
    assert not scd_type_2_model.kind.disable_restatement

    model_kind_dict = scd_type_2_model.kind.dict()
    assert scd_type_2_model.kind == _model_kind_validator(None, model_kind_dict, {})


def test_scd_type_2_by_column_defaults():
    model_def = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_COLUMN (
                unique_key "ID",
                columns ["value_to_track"]
            ),
        );
        SELECT
            1 as "ID",
            2 as "value_to_track",
            '2020-01-01' as ds,
        ;
        """
    )
    scd_type_2_model = load_sql_based_model(model_def)
    assert scd_type_2_model.unique_key == [exp.to_column("ID", quoted=True)]
    assert scd_type_2_model.kind.columns == [exp.to_column("value_to_track", quoted=True)]
    assert scd_type_2_model.columns_to_types == {
        "ID": exp.DataType.build("int"),
        "value_to_track": exp.DataType.build("int"),
        "ds": exp.DataType.build("varchar"),
        "valid_from": exp.DataType.build("TIMESTAMP"),
        "valid_to": exp.DataType.build("TIMESTAMP"),
    }
    assert scd_type_2_model.managed_columns == {
        "valid_from": exp.DataType.build("TIMESTAMP"),
        "valid_to": exp.DataType.build("TIMESTAMP"),
    }
    assert scd_type_2_model.kind.valid_from_name == exp.column("valid_from", quoted=True)
    assert scd_type_2_model.kind.valid_to_name == exp.column("valid_to", quoted=True)
    assert not scd_type_2_model.kind.execution_time_as_valid_from
    assert scd_type_2_model.kind.is_scd_type_2_by_column
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert scd_type_2_model.kind.forward_only
    assert scd_type_2_model.kind.disable_restatement


def test_scd_type_2_by_column_overrides():
    model_def = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_COLUMN (
                unique_key ["iD", ds],
                columns "value_to_track",
                valid_from_name test_valid_from,
                valid_to_name test_valid_to,
                execution_time_as_valid_from True,
                time_data_type TIMESTAMPTZ,
                forward_only False,
                disable_restatement False,
                invalidate_hard_deletes False,
            ),
        );
        SELECT
            1 as "ID",
            2 as "value_to_track",
            '2020-01-01' as ds,
        ;
        """
    )
    scd_type_2_model = load_sql_based_model(model_def)
    assert scd_type_2_model.unique_key == [
        exp.column("iD", quoted=True),
        exp.column("ds", quoted=True),
    ]
    assert scd_type_2_model.managed_columns == {
        "test_valid_from": exp.DataType.build("TIMESTAMPTZ"),
        "test_valid_to": exp.DataType.build("TIMESTAMPTZ"),
    }
    assert scd_type_2_model.kind.valid_from_name == exp.column("test_valid_from", quoted=True)
    assert scd_type_2_model.kind.valid_to_name == exp.column("test_valid_to", quoted=True)
    assert scd_type_2_model.kind.execution_time_as_valid_from
    assert scd_type_2_model.kind.is_scd_type_2_by_column
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert scd_type_2_model.kind.time_data_type == exp.DataType.build("TIMESTAMPTZ")
    assert not scd_type_2_model.kind.invalidate_hard_deletes
    assert not scd_type_2_model.kind.forward_only
    assert not scd_type_2_model.kind.disable_restatement

    model_kind_dict = scd_type_2_model.kind.dict()
    assert scd_type_2_model.kind == _model_kind_validator(None, model_kind_dict, {})


def test_scd_type_2_python_model() -> None:
    @model(
        "test_scd_type_2_python_model",
        kind=dict(
            name=ModelKindName.SCD_TYPE_2_BY_TIME,
            unique_key="a",
            updated_at_name="b",
            updated_at_as_valid_from=True,
        ),
        columns={"a": "string", "b": "string"},
    )
    def scd_type_2_model(context, **kwargs):
        return pd.DataFrame(
            [
                {
                    "a": "val1",
                    "b": "2024-01-01",
                }
            ]
        )

    python_model = model.get_registry()["test_scd_type_2_python_model"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert python_model.columns_to_types == {
        "a": exp.DataType.build("string"),
        "b": exp.DataType.build("string"),
        "valid_from": exp.DataType.build("TIMESTAMP"),
        "valid_to": exp.DataType.build("TIMESTAMP"),
    }


@pytest.mark.parametrize(
    "input_columns,expected_columns",
    [
        (
            "col1",
            [exp.to_column("col1", quoted=True)],
        ),
        (
            "[col1]",
            [exp.to_column("col1", quoted=True)],
        ),
        (
            "[col1, col2]",
            [exp.to_column("col1", quoted=True), exp.to_column("col2", quoted=True)],
        ),
        (
            '"col1"',
            [exp.to_column("col1", quoted=True)],
        ),
        (
            '["col1"]',
            [exp.to_column("col1", quoted=True)],
        ),
        ("*", exp.Star()),
    ],
)
def test_check_column_variants(input_columns, expected_columns):
    model_def = d.parse(
        f"""
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_COLUMN (
                unique_key "ID",
                columns {input_columns}
            ),
        );
        SELECT 1
        ;
        """
    )
    scd_type_2_model = load_sql_based_model(model_def)
    assert scd_type_2_model.kind.columns == expected_columns


def test_model_dialect_name():
    expressions = d.parse(
        """
        MODEL (
            name `project-1`.`db`.`tbl1`,
            dialect bigquery
        );
        SELECT 1;
        """
    )

    model = load_sql_based_model(expressions)
    assert model.fqn == '"project-1"."db"."tbl1"'

    model = create_external_model(
        "`project-1`.`db`.`tbl1`", columns={"x": "STRING"}, dialect="bigquery"
    )
    assert "name `project-1`.`db`.`tbl1`" in model.render_definition()[0].sql(dialect="bigquery")


def test_model_allow_partials():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            allow_partials true,
        );
        SELECT 1;
        """
    )

    model = load_sql_based_model(expressions)

    assert model.allow_partials

    assert "allow_partials TRUE" in model.render_definition()[0].sql()


def test_signals():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            signals [
                (
                    table_name = 'table_a',
                    ds = @end_ds,
                ),
                (
                    table_name = 'table_b',
                    ds = @end_ds,
                    hour = @end_hour,
                ),
                (
                    bool_key = True,
                    int_key = 1,
                    float_key = 1.0,
                    string_key = 'string',
                )
            ],
        );
        SELECT 1;
        """
    )

    model = load_sql_based_model(expressions)
    assert model.signals == [
        exp.Tuple(
            expressions=[
                exp.to_column("table_name").eq("table_a"),
                exp.to_column("ds").eq(d.MacroVar(this="end_ds")),
            ]
        ),
        exp.Tuple(
            expressions=[
                exp.to_column("table_name").eq("table_b"),
                exp.to_column("ds").eq(d.MacroVar(this="end_ds")),
                exp.to_column("hour").eq(d.MacroVar(this="end_hour")),
            ]
        ),
        exp.Tuple(
            expressions=[
                exp.to_column("bool_key").eq(True),
                exp.to_column("int_key").eq(1),
                exp.to_column("float_key").eq(1.0),
                exp.to_column("string_key").eq("string"),
            ]
        ),
    ]

    rendered_signals = model.render_signals(start="2023-01-01", end="2023-01-02 15:00:00")
    assert rendered_signals == [
        {"table_name": "table_a", "ds": "2023-01-02"},
        {"table_name": "table_b", "ds": "2023-01-02", "hour": 14},
        {"bool_key": True, "int_key": 1, "float_key": 1.0, "string_key": "string"},
    ]

    assert (
        "signals ((table_name = 'table_a', ds = @end_ds), (table_name = 'table_b', ds = @end_ds, hour = @end_hour), (bool_key = TRUE, int_key = 1, float_key = 1.0, string_key = 'string')"
        in model.render_definition()[0].sql()
    )


def test_null_column_type():
    expressions = d.parse(
        """
        MODEL (
          name test_db.test_model,
          columns (
            id INT,
            ds NULL,
          )
        );

        SELECT
          id::INT AS id,
          ds
        FROM x
    """
    )
    model = load_sql_based_model(expressions, dialect="hive")
    assert model.columns_to_types == {
        "ds": exp.DataType.build("null"),
        "id": exp.DataType.build("int"),
    }
    assert not model.annotated


def test_when_matched():
    expressions = d.parse(
        """
        MODEL (
          name db.employees,
          kind INCREMENTAL_BY_UNIQUE_KEY (
            unique_key name,
            when_matched WHEN MATCHED THEN UPDATE SET target.salary = COALESCE(source.salary, target.salary)
          )
        );
        SELECT 'name' AS name, 1 AS salary;
    """
    )

    expected_when_matched = "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.salary = COALESCE(__MERGE_SOURCE__.salary, __MERGE_TARGET__.salary)"

    model = load_sql_based_model(expressions, dialect="hive")
    assert len(model.kind.when_matched) == 1
    assert model.kind.when_matched[0].sql() == expected_when_matched

    model = SqlModel.parse_raw(model.json())
    assert len(model.kind.when_matched) == 1
    assert model.kind.when_matched[0].sql() == expected_when_matched


def test_when_matched_multiple():
    expressions = d.parse(
        """
        MODEL (
          name @{schema}.employees,
          kind INCREMENTAL_BY_UNIQUE_KEY (
            unique_key name,
            when_matched WHEN MATCHED AND source.x = 1 THEN UPDATE SET target.salary = COALESCE(source.salary, target.salary),
            WHEN MATCHED THEN UPDATE SET target.salary = COALESCE(source.salary, target.salary)
            
          )
        );
        SELECT 'name' AS name, 1 AS salary;
    """
    )

    expected_when_matched = [
        "WHEN MATCHED AND __MERGE_SOURCE__.x = 1 THEN UPDATE SET __MERGE_TARGET__.salary = COALESCE(__MERGE_SOURCE__.salary, __MERGE_TARGET__.salary)",
        "WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.salary = COALESCE(__MERGE_SOURCE__.salary, __MERGE_TARGET__.salary)",
    ]

    model = load_sql_based_model(expressions, dialect="hive", variables={"schema": "db"})
    assert len(model.kind.when_matched) == 2
    assert model.kind.when_matched[0].sql() == expected_when_matched[0]
    assert model.kind.when_matched[1].sql() == expected_when_matched[1]

    model = SqlModel.parse_raw(model.json())
    assert len(model.kind.when_matched) == 2
    assert model.kind.when_matched[0].sql() == expected_when_matched[0]
    assert model.kind.when_matched[1].sql() == expected_when_matched[1]


def test_default_catalog_sql(assert_exp_eq):
    """
    This test validates the hashing behavior of the system as it relates to the default catalog.
    The system is not designed to actually support having an engine that doesn't support default catalog
    to start supporting it or the reverse of that. If that did happen then bugs would occur.
    """
    HASH_WITH_CATALOG = "3198762995"

    # Test setting default catalog doesn't change hash if it matches existing logic
    expressions = d.parse(
        """
        MODEL (
            name catalog.db.table
        );
        SELECT x
        FROM catalog.db.source
        """
    )

    model = load_sql_based_model(expressions, default_catalog="catalog")
    assert model.default_catalog == "catalog"
    assert model.name == "catalog.db.table"
    assert model.fqn == '"catalog"."db"."table"'
    assert model.depends_on == {'"catalog"."db"."source"'}

    assert_exp_eq(
        model.render_query(),
        """
        SELECT
          "x" AS "x"
          FROM "catalog"."db"."source" AS "source"
        """,
    )

    assert model.data_hash == HASH_WITH_CATALOG

    expressions = d.parse(
        """
        MODEL (
            name catalog.db.table,
        );
        SELECT x
        FROM catalog.db.source
        """
    )

    model = load_sql_based_model(expressions)
    assert model.default_catalog is None
    assert model.name == "catalog.db.table"
    assert model.fqn == '"catalog"."db"."table"'
    assert model.depends_on == {'"catalog"."db"."source"'}

    assert_exp_eq(
        model.render_query(),
        """
        SELECT
          "x" AS "x"
          FROM "catalog"."db"."source" AS "source"
        """,
    )

    assert model.data_hash == HASH_WITH_CATALOG

    # Test setting default catalog to a different catalog but everything if fully qualified then no hash change
    expressions = d.parse(
        """
        MODEL (
            name catalog.db.table
        );
        SELECT x
        FROM catalog.db.source
        """
    )

    model = load_sql_based_model(expressions, default_catalog="other_catalog")
    assert model.default_catalog == "other_catalog"
    assert model.name == "catalog.db.table"
    assert model.fqn == '"catalog"."db"."table"'
    assert model.depends_on == {'"catalog"."db"."source"'}

    assert_exp_eq(
        model.render_query(),
        """
        SELECT
          "x" AS "x"
          FROM "catalog"."db"."source" AS "source"
        """,
    )

    assert model.data_hash == HASH_WITH_CATALOG

    # test that hash changes if model contains a non-fully-qualified reference
    expressions = d.parse(
        """
        MODEL (
            name catalog.db.table
        );
        SELECT x
        FROM db.source
        """
    )

    model = load_sql_based_model(expressions, default_catalog="other_catalog")
    assert model.default_catalog == "other_catalog"
    assert model.name == "catalog.db.table"
    assert model.fqn == '"catalog"."db"."table"'
    assert model.depends_on == {'"other_catalog"."db"."source"'}

    # The query changed so the hash should change
    assert model.data_hash != HASH_WITH_CATALOG

    # test that hash is the same but the fqn is different so the snapshot is different so this is
    # a new snapshot but with the same hash as before
    expressions = d.parse(
        """
        MODEL (
            name db.table,
        );
        SELECT x
        FROM catalog.db.source
        """
    )

    model = load_sql_based_model(expressions)
    assert model.default_catalog is None
    assert model.name == "db.table"
    assert model.fqn == '"db"."table"'
    assert model.depends_on == {'"catalog"."db"."source"'}

    assert model.data_hash == HASH_WITH_CATALOG

    # This will also have the same hash but the fqn is different so the snapshot is different so this is
    # a new snapshot but with the same hash as before
    expressions = d.parse(
        """
        MODEL (
            name db.table
        );
        SELECT x
        FROM catalog.db.source
        """
    )

    model = load_sql_based_model(expressions, default_catalog="catalog")
    assert model.default_catalog == "catalog"
    assert model.name == "db.table"
    assert model.fqn == '"catalog"."db"."table"'
    assert model.depends_on == {'"catalog"."db"."source"'}

    assert model.data_hash == HASH_WITH_CATALOG

    # Query is different since default catalog does not apply and therefore the hash is different
    expressions = d.parse(
        """
        MODEL (
            name table
        );
        SELECT x
        FROM source
        """
    )

    model = load_sql_based_model(expressions, default_catalog="catalog")
    assert model.default_catalog == "catalog"
    assert model.name == "table"
    assert model.fqn == '"table"'
    assert model.depends_on == {'"source"'}

    assert model.data_hash != HASH_WITH_CATALOG


def test_default_catalog_python():
    HASH_WITH_CATALOG = "2928466080"

    @model(name="db.table", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("dependency.table")

    m = model.get_registry()["db.table"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert m.default_catalog is None
    assert m.name == "db.table"
    assert m.fqn == '"db"."table"'
    assert m.depends_on == {'"dependency"."table"'}

    assert m.data_hash != HASH_WITH_CATALOG

    m = model.get_registry()["db.table"].model(
        module_path=Path("."),
        path=Path("."),
        default_catalog="catalog",
    )

    assert m.default_catalog == "catalog"
    assert m.name == "db.table"
    assert m.fqn == '"catalog"."db"."table"'
    assert m.depends_on == {'"catalog"."dependency"."table"'}

    # This ideally would be `m.data_hash == HASH_WITH_CATALOG`. The reason it is not is because when we hash
    # the python function we make the hash out of the actual logic of the function which means `context.table("dependency.table")`
    # is used when really is should be `context.table("catalog.dependency.table")`.
    assert m.data_hash != HASH_WITH_CATALOG

    @model(name="catalog.db.table", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("catalog.dependency.table")

    m = model.get_registry()["catalog.db.table"].model(
        module_path=Path("."),
        path=Path("."),
        default_catalog="other_catalog",
    )

    assert m.default_catalog == "other_catalog"
    assert m.name == "catalog.db.table"
    assert m.fqn == '"catalog"."db"."table"'
    assert m.depends_on == {'"catalog"."dependency"."table"'}

    assert m.data_hash == HASH_WITH_CATALOG

    @model(name="catalog.db.table2", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("dependency.table")

    m = model.get_registry()["catalog.db.table2"].model(
        module_path=Path("."),
        path=Path("."),
        default_catalog="other_catalog",
    )

    assert m.default_catalog == "other_catalog"
    assert m.name == "catalog.db.table2"
    assert m.fqn == '"catalog"."db"."table2"'
    assert m.depends_on == {'"other_catalog"."dependency"."table"'}

    assert m.data_hash != HASH_WITH_CATALOG

    @model(name="table", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("table2")

    m = model.get_registry()["table"].model(
        module_path=Path("."),
        path=Path("."),
        default_catalog="catalog",
    )

    assert m.default_catalog == "catalog"
    assert m.name == "table"
    assert m.fqn == '"table"'
    assert m.depends_on == {'"table2"'}

    assert m.data_hash != HASH_WITH_CATALOG


def test_default_catalog_external_model():
    """
    Since external models fqns are the only thing affected by default catalog, and when they change new snapshots
    are made, the hash will be the same across different names.
    """
    EXPECTED_HASH = "1837375494"

    model = create_external_model("db.table", columns={"a": "int", "limit": "int"})
    assert model.default_catalog is None
    assert model.name == "db.table"
    assert model.fqn == '"db"."table"'

    assert model.data_hash == EXPECTED_HASH

    model = create_external_model(
        "db.table", columns={"a": "int", "limit": "int"}, default_catalog="catalog"
    )
    assert model.default_catalog == "catalog"
    assert model.name == "db.table"
    assert model.fqn == '"catalog"."db"."table"'

    assert model.data_hash == EXPECTED_HASH

    model = create_external_model(
        "catalog.db.table", columns={"a": "int", "limit": "int"}, default_catalog="other_catalog"
    )
    assert model.default_catalog == "other_catalog"
    assert model.name == "catalog.db.table"
    assert model.fqn == '"catalog"."db"."table"'

    assert model.data_hash == EXPECTED_HASH

    # Since there is no schema defined, the default physical schema is used which changes the hash
    model = create_external_model(
        "table", columns={"a": "int", "limit": "int"}, default_catalog="catalog"
    )

    assert model.default_catalog == "catalog"
    assert model.name == "table"
    assert model.fqn == '"table"'

    assert model.data_hash != EXPECTED_HASH


def test_user_cannot_set_default_catalog():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            default_catalog some_catalog
        );

        SELECT 1::int AS a, 2::int AS b, 3 AS c, 4 as d;
    """
    )

    with pytest.raises(ConfigError, match="`default_catalog` cannot be set on a per-model basis"):
        load_sql_based_model(expressions)

    with pytest.raises(ConfigError, match="`default_catalog` cannot be set on a per-model basis"):

        @model(name="db.table", kind="full", columns={'"COL"': "int"}, default_catalog="catalog")
        def my_model(context, **kwargs):
            context.table("dependency.table")


def test_depends_on_default_catalog_python():
    @model(name="some.table", kind="full", columns={'"COL"': "int"}, depends_on={"other.table"})
    def my_model(context, **kwargs):
        context.table("dependency.table")

    m = model.get_registry()["some.table"].model(
        module_path=Path("."),
        path=Path("."),
        default_catalog="catalog",
    )

    assert m.default_catalog == "catalog"
    assert m.depends_on == {'"catalog"."other"."table"'}


def test_end_date():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts,
            ),
            start '2023-01-01',
            end '2023-06-01'
        );

        SELECT 1::int AS a, 2::int AS b, now::timestamp as ts
        """
    )
    model = load_sql_based_model(expressions)

    assert model.start == "2023-01-01"
    assert model.end == "2023-06-01"
    assert model.interval_unit == IntervalUnit.DAY

    with pytest.raises(ConfigError, match=".*Start date.+can't be greater than end date.*"):
        load_sql_based_model(
            d.parse(
                """
            MODEL (
                name db.table,
                kind INCREMENTAL_BY_TIME_RANGE (
                    time_column ts,
                ),
                start '2024-01-01',
                end '2023-06-01'
            );

            SELECT 1::int AS a, 2::int AS b, now::timestamp as ts
            """
            )
        )


def test_end_no_start():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column ts,
            ),
            end '2023-06-01'
        );

        SELECT 1::int AS a, 2::int AS b, now::timestamp as ts
        """
    )
    with pytest.raises(ConfigError, match="Must define a start date if an end date is defined"):
        load_sql_based_model(expressions)
    load_sql_based_model(expressions, defaults={"start": "2023-01-01"})


def test_variables():
    @macro()
    def test_macro_var(evaluator) -> exp.Expression:
        return exp.convert(evaluator.var("TEST_VAR_D") + 10)

    expressions = parse(
        """
        MODEL(
            name test_model,
            kind FULL,
        );

        SELECT
          @VAR('TEST_VAR_A') AS a,
          @VAR('test_var_b', 'default_value') AS b,
          @VAR('test_var_c') AS c,
          @TEST_MACRO_VAR() AS d,
          @'foo_@{test_var_e}' AS e,
          @SQL(foo_@{test_var_f}) AS f,
          'foo_@{test_var_unused}' AS g
    """,
        default_dialect="bigquery",
    )

    model = load_sql_based_model(
        expressions,
        variables={
            "test_var_a": "test_value",
            "test_var_d": 1,
            "test_var_e": 4,
            "test_var_f": 5,
            "test_var_unused": 2,
        },
    )
    assert model.python_env[c.SQLMESH_VARS] == Executable.value(
        {"test_var_a": "test_value", "test_var_d": 1, "test_var_e": 4, "test_var_f": 5}
    )
    assert (
        model.render_query().sql(dialect="bigquery")
        == "SELECT 'test_value' AS `a`, 'default_value' AS `b`, NULL AS `c`, 11 AS `d`, 'foo_4' AS `e`, `foo_5` AS `f`, 'foo_@{test_var_unused}' AS `g`"
    )

    with pytest.raises(ConfigError, match=r"Macro VAR requires at least one argument.*"):
        expressions = parse(
            """
            MODEL(
                name test_model,
            );

            SELECT @VAR() AS a;
        """,
            default_dialect="bigquery",
        )
        load_sql_based_model(expressions)

    with pytest.raises(
        ConfigError, match=r"The variable name must be a string literal, '123' was given instead.*"
    ):
        expressions = parse(
            """
            MODEL(
                name test_model,
            );

            SELECT @VAR(123) AS a;
        """,
            default_dialect="bigquery",
        )
        load_sql_based_model(expressions)

    with pytest.raises(
        ConfigError,
        match=r"The variable name must be a string literal, '@VAR_NAME' was given instead.*",
    ):
        expressions = parse(
            """
            MODEL(
                name test_model,
            );

            @DEF(VAR_NAME, 'var_name');
            SELECT @VAR(@VAR_NAME) AS a;
        """,
            default_dialect="bigquery",
        )
        load_sql_based_model(expressions)


def test_named_variable_macros() -> None:
    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro);
        @DEF(overridden_var, 'overridden_value');
        SELECT @gateway AS gateway, @TEST_VAR_A AS test_var_a, @overridden_var AS overridden_var
        """
        ),
        variables={
            c.GATEWAY: "in_memory",
            "test_var_a": "test_value",
            "test_var_unused": "unused",
            "overridden_var": "initial_value",
        },
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value(
        {c.GATEWAY: "in_memory", "test_var_a": "test_value", "overridden_var": "initial_value"}
    )
    assert (
        model.render_query_or_raise().sql()
        == "SELECT 'in_memory' AS \"gateway\", 'test_value' AS \"test_var_a\", 'overridden_value' AS \"overridden_var\""
    )


def test_variables_in_templates() -> None:
    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro);
        @DEF(overridden_var, overridden_value);
        SELECT 'gateway' AS col_@gateway, 'test_var_a' AS @{test_var_a}_col, 'overridden_var' AS col_@{overridden_var}_col
        """
        ),
        variables={
            c.GATEWAY: "in_memory",
            "test_var_a": "test_value",
            "test_var_unused": "unused",
            "overridden_var": "initial_value",
        },
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value(
        {c.GATEWAY: "in_memory", "test_var_a": "test_value", "overridden_var": "initial_value"}
    )
    assert (
        model.render_query_or_raise().sql()
        == "SELECT 'gateway' AS \"col_in_memory\", 'test_var_a' AS \"test_value_col\", 'overridden_var' AS \"col_overridden_value_col\""
    )

    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro);
        @DEF(overridden_var, overridden_value);
        SELECT 'combo' AS col_@{test_var_a}_@{overridden_var}_col_@gateway
        """
        ),
        variables={
            c.GATEWAY: "in_memory",
            "test_var_a": "test_value",
            "test_var_unused": "unused",
            "overridden_var": "initial_value",
        },
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value(
        {c.GATEWAY: "in_memory", "test_var_a": "test_value", "overridden_var": "initial_value"}
    )
    assert (
        model.render_query_or_raise().sql()
        == "SELECT 'combo' AS \"col_test_value_overridden_value_col_in_memory\""
    )

    model = load_sql_based_model(
        parse(
            """
        MODEL(
          name @{some_var}.bar,
          dialect snowflake
        );

        SELECT 1 AS c
        """
        ),
        variables={
            "some_var": "foo",
        },
    )

    assert model.name == "foo.bar"


def test_variables_jinja():
    expressions = parse(
        """
        MODEL(
            name test_model,
            kind FULL,
        );

        JINJA_QUERY_BEGIN;
        SELECT '{{ var('TEST_VAR_A') }}' AS a, '{{ var('test_var_b', 'default_value') }}' AS b, '{{ var('test_var_c') }}' AS c, {{ test_macro_var() }} AS d;
        JINJA_END;
    """,
        default_dialect="bigquery",
    )

    jinja_macros = JinjaMacroRegistry(
        root_macros={
            "test_macro_var": MacroInfo(
                definition="{% macro test_macro_var() %}{{ var('test_var_d') + 10 }}{% endmacro %}",
                depends_on=[],
            )
        },
    )

    model = load_sql_based_model(
        expressions,
        variables={"test_var_a": "test_value", "test_var_d": 1, "test_var_unused": 2},
        jinja_macros=jinja_macros,
    )
    assert model.python_env[c.SQLMESH_VARS] == Executable.value(
        {"test_var_a": "test_value", "test_var_d": 1}
    )
    assert (
        model.render_query().sql(dialect="bigquery")
        == "SELECT 'test_value' AS `a`, 'default_value' AS `b`, 'None' AS `c`, 11 AS `d`"
    )


def test_variables_python_model(mocker: MockerFixture) -> None:
    @model(
        "test_variables_python_model",
        kind="full",
        columns={"a": "string", "b": "string", "c": "string"},
    )
    def model_with_variables(context, **kwargs):
        return pd.DataFrame(
            [
                {
                    "a": context.var("TEST_VAR_A"),
                    "b": context.var("test_var_b", "default_value"),
                    "c": context.var("test_var_c"),
                }
            ]
        )

    python_model = model.get_registry()["test_variables_python_model"].model(
        module_path=Path("."),
        path=Path("."),
        variables={"test_var_a": "test_value", "test_var_unused": 2},
    )

    assert python_model.python_env[c.SQLMESH_VARS] == Executable.value({"test_var_a": "test_value"})

    context = ExecutionContext(mocker.Mock(), {}, None, None)
    df = list(python_model.render(context=context))[0]
    assert df.to_dict(orient="records") == [{"a": "test_value", "b": "default_value", "c": None}]


def test_load_external_model_python(sushi_context) -> None:
    @model(
        "test_load_external_model_python",
        columns={"customer_id": "int", "zip": "str"},
        kind={"name": ModelKindName.FULL},
    )
    def external_model_python(context, **kwargs):
        demographics_table = context.table("memory.raw.demographics")
        return context.fetchdf(
            exp.select("customer_id", "zip").from_(demographics_table),
        )

    python_model = model.get_registry()["test_load_external_model_python"].model(
        module_path=Path("."),
        path=Path("."),
    )

    context = ExecutionContext(sushi_context.engine_adapter, sushi_context.snapshots, None, None)
    df = list(python_model.render(context=context))[0]

    assert df.to_dict(orient="records") == [{"customer_id": 1, "zip": "00000"}]


def test_variables_python_sql_model(mocker: MockerFixture) -> None:
    @model(
        "test_variables_python_model",
        is_sql=True,
        kind="full",
        columns={"a": "string", "b": "string", "c": "string"},
    )
    def model_with_variables(evaluator, **kwargs):
        return exp.select(
            exp.convert(evaluator.var("TEST_VAR_A")).as_("a"),
            exp.convert(evaluator.var("test_var_b", "default_value")).as_("b"),
            exp.convert(evaluator.var("test_var_c")).as_("c"),
        )

    python_sql_model = model.get_registry()["test_variables_python_model"].model(
        module_path=Path("."),
        path=Path("."),
        variables={"test_var_a": "test_value", "test_var_unused": 2},
    )

    assert python_sql_model.python_env[c.SQLMESH_VARS] == Executable.value(
        {"test_var_a": "test_value"}
    )

    context = ExecutionContext(mocker.Mock(), {}, None, None)
    query = list(python_sql_model.render(context=context))[0]
    assert (
        query.sql()
        == """SELECT 'test_value' AS "a", 'default_value' AS "b", NULL AS "c" """.strip()
    )


def test_columns_python_sql_model() -> None:
    @model(
        "test_columns_python_model",
        is_sql=True,
        kind="full",
        columns={"d": "Date", "s": "String", "dt": "DateTime"},
    )
    def model_with_columns(evaluator, **kwargs):
        return exp.select("*").from_("fake")

    python_sql_model = model.get_registry()["test_columns_python_model"].model(
        module_path=Path("."),
        path=Path("."),
    )

    columns_to_types = python_sql_model.columns_to_types

    assert columns_to_types is not None
    assert isinstance(columns_to_types["d"], exp.DataType)
    assert columns_to_types["d"].this == exp.DataType.Type.DATE
    assert isinstance(columns_to_types["s"], exp.DataType)
    assert columns_to_types["s"].this == exp.DataType.Type.TEXT
    assert isinstance(columns_to_types["dt"], exp.DataType)
    assert columns_to_types["dt"].this == exp.DataType.Type.DATETIME


def test_named_variables_python_model(mocker: MockerFixture) -> None:
    @model(
        "test_named_variables_python_model",
        kind="full",
        columns={"a": "string", "b": "string", "c": "string"},
    )
    def model_with_named_variables(
        context, start: TimeLike, test_var_a: str, test_var_b: t.Optional[str] = None, **kwargs
    ):
        return pd.DataFrame(
            [{"a": test_var_a, "b": test_var_b, "start": start.strftime("%Y-%m-%d")}]  # type: ignore
        )

    python_model = model.get_registry()["test_named_variables_python_model"].model(
        module_path=Path("."),
        path=Path("."),
        # Passing `start` in variables to make sure that built-in arguments can't be overridden.
        variables={
            "test_var_a": "test_value",
            "test_var_unused": 2,
            "start": "2024-01-01",
        },
    )

    assert python_model.python_env[c.SQLMESH_VARS] == Executable.value(
        {"test_var_a": "test_value", "start": "2024-01-01"}
    )

    context = ExecutionContext(mocker.Mock(), {}, None, None)
    df = list(python_model.render(context=context))[0]
    assert df.to_dict(orient="records") == [{"a": "test_value", "b": None, "start": to_ds(c.EPOCH)}]


def test_gateway_macro() -> None:
    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro);
        SELECT @gateway AS gateway
        """
        ),
        variables={c.GATEWAY: "in_memory"},
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value({c.GATEWAY: "in_memory"})
    assert model.render_query_or_raise().sql() == "SELECT 'in_memory' AS \"gateway\""

    @macro()
    def macro_uses_gateway(evaluator) -> exp.Expression:
        return exp.convert(evaluator.gateway + "_from_macro")

    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro);
        SELECT @macro_uses_gateway() AS gateway_from_macro
        """
        ),
        variables={c.GATEWAY: "in_memory"},
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value({c.GATEWAY: "in_memory"})
    assert (
        model.render_query_or_raise().sql()
        == "SELECT 'in_memory_from_macro' AS \"gateway_from_macro\""
    )


def test_gateway_macro_jinja() -> None:
    model = load_sql_based_model(
        parse(
            """
        MODEL(name sushi.test_gateway_macro_jinja);
        JINJA_QUERY_BEGIN;
        SELECT '{{ gateway() }}' AS gateway_jinja;
        JINJA_END;
        """
        ),
        variables={c.GATEWAY: "in_memory"},
    )

    assert model.python_env[c.SQLMESH_VARS] == Executable.value({c.GATEWAY: "in_memory"})
    assert model.render_query_or_raise().sql() == "SELECT 'in_memory' AS \"gateway_jinja\""


def test_gateway_python_model(mocker: MockerFixture) -> None:
    @model(
        "test_gateway_python_model",
        kind="full",
        columns={"gateway_python": "string"},
    )
    def model_with_variables(context, **kwargs):
        return pd.DataFrame([{"gateway_python": context.gateway + "_from_python"}])

    python_model = model.get_registry()["test_gateway_python_model"].model(
        module_path=Path("."),
        path=Path("."),
        variables={c.GATEWAY: "in_memory"},
    )

    assert python_model.python_env[c.SQLMESH_VARS] == Executable.value({c.GATEWAY: "in_memory"})

    context = ExecutionContext(mocker.Mock(), {}, None, None)
    df = list(python_model.render(context=context))[0]
    assert df.to_dict(orient="records") == [{"gateway_python": "in_memory_from_python"}]


@pytest.mark.parametrize("dialect", ["spark", "trino"])
def test_view_render_no_quote_identifiers(dialect: str) -> None:
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind VIEW,
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, dialect=dialect)
    assert (
        model.render_query_or_raise().sql(dialect=dialect)
        == "SELECT a AS a, b AS b, c AS c FROM source_table AS source_table"
    )


@pytest.mark.parametrize(
    "dialect,kind",
    [
        ("spark", "FULL"),
        ("trino", "FULL"),
        ("duckdb", "VIEW"),
        ("duckdb", "FULL"),
    ],
)
def test_render_quote_identifiers(dialect: str, kind: str) -> None:
    expressions = d.parse(
        f"""
        MODEL (
            name db.table,
            kind {kind},
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, dialect=dialect)
    assert (
        model.render_query_or_raise().sql(dialect="duckdb")
        == 'SELECT "a" AS "a", "b" AS "b", "c" AS "c" FROM "source_table" AS "source_table"'
    )


def test_this_model() -> None:
    expressions = d.parse(
        """
        MODEL (
            name `project-1.table`,
            dialect bigquery,
        );

        JINJA_STATEMENT_BEGIN;
        VACUUM {{ this_model }} TO 'a';
        JINJA_END;

        JINJA_QUERY_BEGIN;
        SELECT '{{ this_model }}' as x
        JINJA_END;

        JINJA_STATEMENT_BEGIN;
        VACUUM {{ this_model }} TO 'b';
        JINJA_END;
        """
    )
    model = load_sql_based_model(expressions)

    assert (
        model.render_query_or_raise().sql(dialect="bigquery")
        == """SELECT '`project-1`.`table`' AS `x`"""
    )

    assert (
        model.render_pre_statements()[0].sql(dialect="bigquery")
        == """VACUUM `project-1`.`table` TO 'a'"""
    )
    assert (
        model.render_post_statements()[0].sql(dialect="bigquery")
        == """VACUUM `project-1`.`table` TO 'b'"""
    )

    snapshot = Snapshot.from_node(model, nodes={})

    assert (
        model.render_query_or_raise(
            start="2020-01-01",
            snapshots={snapshot.name: snapshot},
        ).sql(dialect="bigquery")
        == """SELECT '`project-1`.`table`' AS `x`"""
    )

    snapshot.categorize_as(SnapshotChangeCategory.BREAKING)

    assert (
        model.render_query_or_raise(
            start="2020-01-01",
            snapshots={snapshot.name: snapshot},
        ).sql(dialect="bigquery")
        == f"SELECT '`sqlmesh__project-1`.`project_1__table__{snapshot.version}`' AS `x`"
    )


def test_macros_in_model_statement(sushi_context, assert_exp_eq):
    @macro()
    def session_properties(evaluator, value):
        return exp.Property(
            this=exp.var("session_properties"),
            value=exp.convert([exp.convert("foo").eq(exp.var(f"bar_{value}"))]),
        )

    expressions = d.parse(
        """
        MODEL (
            name @{gateway}__@{gateway}.test_model,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column @{time_column}

            ),
            start @IF(@gateway = 'test_gateway', '2023-01-01', '2024-01-02'),
            @session_properties(baz)
        );

        SELECT a, b UNION SELECT c, c
        """
    )

    model = load_sql_based_model(
        expressions, variables={"gateway": "test_gateway", "time_column": "a"}
    )
    assert model.name == "test_gateway__test_gateway.test_model"
    assert model.time_column
    assert model.time_column.column == exp.column("a", quoted=True)
    assert model.start == "2023-01-01"
    assert model.session_properties == {"foo": exp.column("bar_baz", quoted=False)}


def test_macro_references_in_audits():
    @macro()
    def zero_value(evaluator: MacroEvaluator) -> int:
        return 0

    @macro()
    def min_value(evaluator: MacroEvaluator) -> int:
        return 1

    @macro()
    def not_loaded_macro(evaluator: MacroEvaluator) -> int:
        return 10

    @macro()
    def max_value(evaluator: MacroEvaluator) -> int:
        return 1000

    audit_expression = parse(
        """
        AUDIT (
    name assert_max_value,
    );
    SELECT *
    FROM @this_model
    WHERE
    id > @max_value;
    """
    )

    not_zero_audit = parse(
        """
        AUDIT (
    name assert_not_zero,
    );
    SELECT *
    FROM @this_model
    WHERE
    id = @zero_value;
    """
    )

    model_expression = d.parse(
        """
        MODEL (
            name db.audit_model,
            audits (assert_max_value, assert_positive_ids),
        );
        SELECT 1 as id;

        AUDIT (
    name assert_positive_ids,
    );
    SELECT *
    FROM @this_model
    WHERE
    id < @min_value;
    """
    )

    audits = {
        "assert_max_value": load_audit(audit_expression, dialect="duckdb"),
        "assert_not_zero": load_audit(not_zero_audit, dialect="duckdb"),
    }
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb", audits=["assert_not_zero"])
    )
    model = load_sql_based_model(
        model_expression, audits=audits, default_audits=config.model_defaults.audits
    )

    assert len(model.audits) == 2
    assert len(model.inline_audits) == 1
    assert len(model.python_env) == 3
    assert config.model_defaults.audits == [("assert_not_zero", {})]
    assert model.audits == [("assert_max_value", {}), ("assert_positive_ids", {})]
    assert isinstance(model.inline_audits["assert_positive_ids"], ModelAudit)
    assert isinstance(model.python_env["min_value"], Executable)
    assert isinstance(model.python_env["max_value"], Executable)
    assert isinstance(model.python_env["zero_value"], Executable)
    assert "not_loaded_macro" not in model.python_env


def test_python_model_dialect():
    model._dialect = "snowflake"

    @model(
        name="a",
        kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="x", format="YYMMDD")),
        columns={},
    )
    def test(context, **kwargs):
        return None

    m = model.get_registry()["a"].model(
        module_path=Path("."),
        path=Path("."),
        dialect="snowflake",
    )

    assert m.time_column.column.sql() == '"X"'
    assert m.time_column.format == "%y%m%d"

    @model(
        name="b",
        kind=IncrementalByTimeRangeKind(time_column="y"),
        columns={},
    )
    def test(context, **kwargs):
        return None

    m = model.get_registry()["b"].model(
        module_path=Path("."),
        path=Path("."),
        dialect="snowflake",
    )

    assert m.time_column.column.sql() == '"Y"'
    assert m.time_column.format == "%Y-%m-%d"

    model._dialect = None


def test_jinja_runtime_stage(assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (
            name test.jinja
        );

        JINJA_QUERY_BEGIN;

        SELECT '{{ runtime_stage }}' as a, {{ runtime_stage == 'loading' }} as b

        JINJA_END;
        """
    )

    model = load_sql_based_model(expressions)
    assert_exp_eq(model.render_query(), '''SELECT 'loading' as "a", TRUE as "b"''')


def test_forward_only_on_destructive_change_config() -> None:
    # global default to ALLOW for non-incremental models
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    context = Context(config=config)

    expressions = d.parse(
        """
        MODEL (
            name memory.db.table,
            kind FULL,
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, defaults=config.model_defaults.dict())
    context.upsert_model(model)
    context_model = context.get_model("memory.db.table")
    assert context_model.on_destructive_change.is_allow

    # global default to ERROR for incremental models
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    context = Context(config=config)

    expressions = d.parse(
        """
        MODEL (
            name memory.db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column c,
                forward_only True
            ),
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, defaults=config.model_defaults.dict())
    context.upsert_model(model)
    context_model = context.get_model("memory.db.table")
    assert context_model.on_destructive_change.is_error

    # WARN specified in model definition, overrides incremental model sqlmesh default ERROR
    config = Config(model_defaults=ModelDefaultsConfig(dialect="duckdb"))
    context = Context(config=config)

    expressions = d.parse(
        """
        MODEL (
            name memory.db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column c,
                forward_only True,
                on_destructive_change warn
            ),
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, defaults=config.model_defaults.dict())
    context.upsert_model(model)
    context_model = context.get_model("memory.db.table")
    assert context_model.on_destructive_change.is_warn

    # WARN specified as model default, overrides incremental model sqlmesh default ERROR
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb", on_destructive_change="warn")
    )
    context = Context(config=config)

    expressions = d.parse(
        """
        MODEL (
            name memory.db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column c,
                forward_only True
            ),
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, defaults=config.model_defaults.dict())
    context.upsert_model(model)
    context_model = context.get_model("memory.db.table")
    assert context_model.on_destructive_change.is_warn

    # WARN specified as model default, does not override non-incremental sqlmesh default ALLOW
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb", on_destructive_change="warn")
    )
    context = Context(config=config)

    expressions = d.parse(
        """
        MODEL (
            name memory.db.table,
            kind FULL,
        );
        SELECT a, b, c FROM source_table;
        """
    )
    model = load_sql_based_model(expressions, defaults=config.model_defaults.dict())
    context.upsert_model(model)
    context_model = context.get_model("memory.db.table")
    assert context_model.on_destructive_change.is_allow


def test_incremental_by_partition(sushi_context, assert_exp_eq):
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_PARTITION,
            partitioned_by [a],
        );

        SELECT a, b
        """
    )
    model = load_sql_based_model(expressions)
    assert model.kind.is_incremental_by_partition
    assert model.kind.disable_restatement

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_PARTITION (
                disable_restatement false
            ),
            partitioned_by [a],
        );

        SELECT a, b
        """
    )
    model = load_sql_based_model(expressions)
    assert model.kind.is_incremental_by_partition
    assert not model.kind.disable_restatement

    with pytest.raises(
        ConfigError,
        match=r".*partitioned_by field is required for INCREMENTAL_BY_PARTITION models.*",
    ):
        expressions = d.parse(
            """
            MODEL (
                name db.table,
                kind INCREMENTAL_BY_PARTITION,
            );

            SELECT a, b
            """
        )
        load_sql_based_model(expressions)

    with pytest.raises(
        ConfigError,
        match=r".*Do not specify the `forward_only` configuration key.*",
    ):
        expressions = d.parse(
            """
            MODEL (
                name db.table,
                kind INCREMENTAL_BY_PARTITION (
                    forward_only true
                ),
            );

            SELECT a, b
            """
        )
        load_sql_based_model(expressions)


@pytest.mark.parametrize(
    ["model_def", "path", "expected_name"],
    [
        [
            """dialect duckdb,""",
            """models/test_schema/test_model.sql,""",
            "test_schema.test_model",
        ],
        [
            """dialect duckdb,""",
            """models/test_model.sql,""",
            "test_model",
        ],
        [
            """dialect duckdb,""",
            """models/inventory/db/test_schema/test_model.sql,""",
            "db.test_schema.test_model",
        ],
        ["""name test_model,""", """models/schema/test_model.sql,""", "test_model"],
    ],
)
def test_model_table_name_inference(
    sushi_context: Context, model_def: str, path: str, expected_name: str
):
    model = load_sql_based_model(
        d.parse(
            f"""
        MODEL (
            {model_def}
        );
        SELECT a FROM tbl;
        """,
            default_dialect="duckdb",
        ),
        path=Path(f"$root/{path}"),
        infer_names=True,
    )
    assert model.name == expected_name


@pytest.mark.parametrize(
    ["path", "expected_name"],
    [
        [
            """models/test_schema/test_model.py""",
            "test_schema.test_model",
        ],
        [
            """models/inventory/db/test_schema/test_model.py""",
            "db.test_schema.test_model",
        ],
    ],
)
def test_python_model_name_inference(tmp_path: Path, path: str, expected_name: str) -> None:
    init_example_project(tmp_path, dialect="duckdb")
    config = Config(
        model_defaults=ModelDefaultsConfig(dialect="duckdb"),
        model_naming=NameInferenceConfig(infer_names=True),
    )

    foo_py_file = tmp_path / path
    foo_py_file.parent.mkdir(parents=True, exist_ok=True)
    foo_py_file.write_text("""from sqlmesh import model
@model(
    columns={'"COL"': "int"},
)
def my_model(context, **kwargs):
    pass""")
    context = Context(paths=tmp_path, config=config)
    assert context.get_model(expected_name).name == expected_name
    assert isinstance(context.get_model(expected_name), PythonModel)


def test_custom_kind():
    from sqlmesh import CustomMaterialization

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind CUSTOM (
                materialization 'MyTestStrategy',
                forward_only true,
                disable_restatement true,
                materialization_properties (
                  'key_a' = 'value_a',
                  key_b = 2,
                  'key_c' = true,
                  'key_d' = 1.23,
                ),
                batch_size 1,
                batch_concurrency 2,
                lookback 3,
            )
        );

        SELECT a, b
        """
    )

    with pytest.raises(
        ConfigError, match=r"Materialization strategy with name 'MyTestStrategy' was not found.*"
    ):
        load_sql_based_model(expressions)

    class MyTestStrategy(CustomMaterialization):
        pass

    model = load_sql_based_model(expressions)
    assert model.kind.is_custom

    kind = t.cast(CustomKind, model.kind)
    assert kind.disable_restatement
    assert kind.forward_only
    assert kind.materialization == "MyTestStrategy"
    assert kind.materialization_properties == {
        "key_a": "value_a",
        "key_b": 2,
        "key_c": True,
        "key_d": 1.23,
    }
    assert kind.batch_size == 1
    assert kind.batch_concurrency == 2
    assert kind.lookback == 3

    assert (
        kind.to_expression().sql()
        == """CUSTOM (
materialization 'MyTestStrategy',
materialization_properties ('key_a' = 'value_a', key_b = 2, 'key_c' = TRUE, 'key_d' = 1.23),
forward_only TRUE,
disable_restatement TRUE,
batch_size 1,
batch_concurrency 2,
lookback 3
)"""
    )


def test_model_kind_to_expression():
    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
            ),
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_TIME_RANGE (
time_column ("a", '%Y-%m-%d'),
forward_only FALSE,
disable_restatement FALSE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
                batch_size 1,
                batch_concurrency 2,
                lookback 3,
                forward_only TRUE,
                disable_restatement TRUE,
                on_destructive_change WARN,
            ),
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_TIME_RANGE (
time_column ("a", '%Y-%m-%d'),
batch_size 1,
batch_concurrency 2,
lookback 3,
forward_only TRUE,
disable_restatement TRUE,
on_destructive_change 'WARN'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_UNIQUE_KEY(
                unique_key a,
            ),
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_UNIQUE_KEY (
unique_key ("a"),
batch_concurrency 1,
forward_only FALSE,
disable_restatement FALSE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_UNIQUE_KEY(
                unique_key a,
                when_matched WHEN MATCHED THEN UPDATE SET target.b = COALESCE(source.b, target.b)
            ),
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_UNIQUE_KEY (
unique_key ("a"),
when_matched ARRAY(WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.b = COALESCE(__MERGE_SOURCE__.b, __MERGE_TARGET__.b)),
batch_concurrency 1,
forward_only FALSE,
disable_restatement FALSE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
            MODEL (
                name db.table,
                kind INCREMENTAL_BY_UNIQUE_KEY(
                    unique_key a,
                    when_matched WHEN MATCHED AND source.x = 1 THEN UPDATE SET target.b = COALESCE(source.b, target.b),
                    WHEN MATCHED THEN UPDATE SET target.b = COALESCE(source.b, target.b)
                ),
            );
            SELECT a, b
            """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_UNIQUE_KEY (
unique_key ("a"),
when_matched ARRAY(WHEN MATCHED AND __MERGE_SOURCE__.x = 1 THEN UPDATE SET __MERGE_TARGET__.b = COALESCE(__MERGE_SOURCE__.b, __MERGE_TARGET__.b), WHEN MATCHED THEN UPDATE SET __MERGE_TARGET__.b = COALESCE(__MERGE_SOURCE__.b, __MERGE_TARGET__.b)),
batch_concurrency 1,
forward_only FALSE,
disable_restatement FALSE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_PARTITION,
            partitioned_by ["a"],
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """INCREMENTAL_BY_PARTITION (
forward_only TRUE,
disable_restatement TRUE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
            )
        );
        """
            ),
            path=Path("./examples/sushi/models/test_model.sql"),
        )
        .kind.to_expression()
        .sql()
        == """SEED (
path '../seeds/waiter_names.csv',
batch_size 1000
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_TIME (
                unique_key [a, b]
            )
        );
        SELECT a, b
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """SCD_TYPE_2_BY_TIME (
updated_at_name "updated_at",
updated_at_as_valid_from FALSE,
unique_key ("a", "b"),
valid_from_name "valid_from",
valid_to_name "valid_to",
invalidate_hard_deletes FALSE,
time_data_type TIMESTAMP,
forward_only TRUE,
disable_restatement TRUE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_COLUMN (
                unique_key [a, b],
                columns [b]
            )
        );
        SELECT a, b, c
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """SCD_TYPE_2_BY_COLUMN (
columns ("b"),
execution_time_as_valid_from FALSE,
unique_key ("a", "b"),
valid_from_name "valid_from",
valid_to_name "valid_to",
invalidate_hard_deletes FALSE,
time_data_type TIMESTAMP,
forward_only TRUE,
disable_restatement TRUE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind SCD_TYPE_2_BY_COLUMN (
                unique_key [a, b],
                columns *
            )
        );
        SELECT a, b, c
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """SCD_TYPE_2_BY_COLUMN (
columns *,
execution_time_as_valid_from FALSE,
unique_key ("a", "b"),
valid_from_name "valid_from",
valid_to_name "valid_to",
invalidate_hard_deletes FALSE,
time_data_type TIMESTAMP,
forward_only TRUE,
disable_restatement TRUE,
on_destructive_change 'ERROR'
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind FULL
        );
        SELECT a, b, c
        """
            )
        )
        .kind.to_expression()
        .sql()
        == "FULL"
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind VIEW
        );
        SELECT a, b, c
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """VIEW (
materialized FALSE
)"""
    )

    assert (
        load_sql_based_model(
            d.parse(
                """
        MODEL (
            name db.table,
            kind VIEW (materialized true)
        );
        SELECT a, b, c
        """
            )
        )
        .kind.to_expression()
        .sql()
        == """VIEW (
materialized TRUE
)"""
    )


@pytest.mark.parametrize(
    "metadata_only",
    [True, False],
)
def test_macro_func_hash(metadata_only):
    macro.set_registry({})

    @macro(metadata_only=metadata_only)
    def noop(evaluator) -> None:
        return None

    expressions = d.parse(
        """
        MODEL (
            name db.model,
        );

        SELECT 1;
    """
    )
    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    expressions = d.parse(
        """
        MODEL (
            name db.model,
        );

        SELECT 1;

        @noop();
    """
    )
    new_model = load_sql_based_model(
        expressions, path=Path("./examples/sushi/models/test_model.sql")
    )
    if metadata_only:
        assert "noop" not in new_model._data_hash_values[0]
        assert "noop" in new_model._additional_metadata[0]
        assert model.data_hash == new_model.data_hash
        assert model.metadata_hash(audits={}) != new_model.metadata_hash(audits={})
    else:
        assert "noop" in new_model._data_hash_values[0]
        assert not new_model._additional_metadata
        assert model.data_hash != new_model.data_hash
        assert model.metadata_hash(audits={}) == new_model.metadata_hash(audits={})

    @macro(metadata_only=metadata_only)
    def noop(evaluator) -> None:
        print("noop")
        return None

    updated_model = load_sql_based_model(
        expressions, path=Path("./examples/sushi/models/test_model.sql")
    )
    if metadata_only:
        assert "print" not in new_model._additional_metadata[0]
        assert "print" in updated_model._additional_metadata[0]
        assert new_model.data_hash == updated_model.data_hash
        assert new_model.metadata_hash(audits={}) != updated_model.metadata_hash(audits={})
    else:
        assert "print" not in new_model._data_hash_values[0]
        assert "print" in updated_model._data_hash_values[0]
        assert new_model.data_hash != updated_model.data_hash
        assert new_model.metadata_hash(audits={}) == updated_model.metadata_hash(audits={})


def test_managed_kind_sql():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind MANAGED,
            physical_properties (
                warehouse = small,
                target_lag = '20 minutes',
                refresh_mode = auto
            )
        );

        SELECT a, b
        """
    )

    model = load_sql_based_model(expressions)

    assert model.kind.is_managed

    with pytest.raises(ConfigError, match=r".*must specify the 'target_lag' physical property.*"):
        load_sql_based_model(
            d.parse(
                """
            MODEL (
                name db.table,
                kind MANAGED,
                dialect snowflake
            );

            SELECT a, b
            """
            )
        ).validate_definition()


def test_managed_kind_python():
    @model("test_managed_python_model", kind="managed", columns={"a": "int"})
    def execute(
        context: ExecutionContext,
        start: datetime,
        end: datetime,
        execution_time: datetime,
        **kwargs: t.Any,
    ) -> pd.DataFrame:
        return pd.DataFrame.from_dict(data={"a": 1}, orient="index")

    with pytest.raises(
        SQLMeshError,
        match=r".*Cannot create Python model.*the 'MANAGED' kind doesnt support Python models",
    ):
        model.get_registry()["test_managed_python_model"].model(
            module_path=Path("."),
            path=Path("."),
        ).validate_definition()


def test_trailing_comments():
    expressions = d.parse(
        """
        MODEL (name db.table);

        /* some comment A */

        SELECT 1;
        /* some comment B */
        """
    )
    model = load_sql_based_model(expressions)
    assert not model.render_pre_statements()
    assert not model.render_post_statements()

    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            kind SEED (
              path '../seeds/waiter_names.csv',
              batch_size 100,
            )
        );

        /* some comment A */
        """
    )
    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert not model.render_pre_statements()
    assert not model.render_post_statements()


def test_staged_file_path():
    expressions = d.parse(
        """
        MODEL (name test, dialect snowflake);

        SELECT * FROM @a.b/c/d.csv(FILE_FORMAT => 'b.ff')
        """
    )
    model = load_sql_based_model(expressions)
    query = model.render_query()
    assert query.sql(dialect="snowflake") == "SELECT * FROM @a.b/c/d.csv (FILE_FORMAT => 'b.ff')"


def test_cache():
    expressions = d.parse(
        """
        MODEL (name test);

        SELECT 1 x
        FROM y

        """
    )
    model = load_sql_based_model(expressions)
    assert model.depends_on == {'"y"'}
    assert model.copy(update={"depends_on_": {'"z"'}}).depends_on == {'"z"', '"y"'}
