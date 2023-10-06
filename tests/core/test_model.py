import logging
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse_one
from sqlglot.schema import MappingSchema

import sqlmesh.core.dialect as d
from sqlmesh.core.config import Config
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import parse
from sqlmesh.core.macros import MacroEvaluator, macro
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    IncrementalUnmanagedKind,
    ModelCache,
    ModelMeta,
    SeedKind,
    SqlModel,
    TimeColumn,
    create_external_model,
    create_seed_model,
    create_sql_model,
    load_sql_based_model,
    model,
)
from sqlmesh.core.model.common import parse_expression
from sqlmesh.core.model.seed import CsvSettings
from sqlmesh.core.node import IntervalUnit, _Node
from sqlmesh.core.renderer import QueryRenderer
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroInfo
from sqlmesh.utils.metaprogramming import Executable


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
    assert [col.sql() for col in model.partitioned_by] == ["a", "d"]
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
    assert model.depends_on == {"db.other_table"}
    assert_exp_eq(
        model.render_query(),
        """
    SELECT
      TRY_CAST(1 AS INT) AS "a",
      TRY_CAST(2 AS DOUBLE) AS "b",
      TRY_CAST("c" AS BOOLEAN) AS "c",
      TRY_CAST(1 AS INT) AS "d", /* d */
      TRY_CAST(2 AS DOUBLE) AS "e", /* e */
      TRY_CAST("f" AS BOOLEAN) AS "f", /* f */
      TRY_CAST(1 + 1 AS INT) AS "g",
    FROM "db"."other_table" AS "t1"
    LEFT JOIN "db"."table" AS "t2"
      ON "t1"."a" = "t2"."a"
    """,
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


def test_model_union_query():
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
    with patch.object(logger, "error") as mock_logger:
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
        model.render_query(optimize=True)
        assert (
            mock_logger.call_args[0][0]
            == "%s for model '%s', the column may not exist or is ambiguous"
        )


@pytest.mark.parametrize(
    "partition_by_input, partition_by_output, expected_exception",
    [
        ("a", ["a"], None),
        ("(a, b)", ["a", "b"], None),
        ("TIMESTAMP_TRUNC(a, DAY)", ["TIMESTAMP_TRUNC(a, DAY)"], None),
        ("e", "", ConfigError),
        ("(a, e)", "", ConfigError),
        ("(a, a)", "", ConfigError),
    ],
)
def test_partitioned_by(partition_by_input, partition_by_output, expected_exception):
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
        assert [col.sql(dialect="bigquery") for col in model.partitioned_by] == partition_by_output


def test_no_model_statement():
    expressions = d.parse(
        """
        SELECT 1 AS x
    """
    )

    with pytest.raises(
        ConfigError,
        match="MODEL statement is required as the first statement in the definition at '.",
    ):
        load_sql_based_model(expressions)


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
        load_sql_based_model(expressions, path=Path("test_location"))
    assert "have a SELECT" in str(ex.value)


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
            table_properties (
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
    deserialized_model = SqlModel.parse_raw(model.json())

    assert deserialized_model == model


def test_column_descriptions(sushi_context, assert_exp_eq):
    assert sushi_context.models["sushi.customer_revenue_by_day"].column_descriptions == {
        "customer_id": "Customer id",
        "revenue": "Revenue from orders made by this customer",
        "ds": "Date",
    }

    expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT
          id::int, -- primary key
          foo::int, -- bar
        FROM table
    """
    )
    model = load_sql_based_model(expressions)

    assert_exp_eq(
        model.query,
        """
        SELECT
          id::int, -- primary key
          foo::int, -- bar
        FROM table
    """,
    )


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

    expected_post = [
        *d.parse("@foo(bar='x', val=@this)"),
        *d.parse("DROP TABLE x2;"),
    ]
    assert model.post_statements == expected_post

    assert model.query == d.parse("SELECT 1 AS x")[0]


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

    column_hashes = model.column_hashes

    dehydrated_model = model.to_dehydrated()
    assert not dehydrated_model.is_hydrated
    assert dehydrated_model.column_hashes == column_hashes
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


def test_seed_model_diff(tmp_path):
    model_a_csv_path = (tmp_path / "model_a.csv").absolute()
    model_b_csv_path = (tmp_path / "model_b.csv").absolute()

    with open(model_a_csv_path, "w") as fd:
        fd.write(
            """key,value
1,value_a
"""
        )

    with open(model_b_csv_path, "w") as fd:
        fd.write(
            """key,value
2,value_b
"""
        )

    model_a = create_seed_model("test_db.test_model", SeedKind(path=str(model_a_csv_path)))
    model_b = create_seed_model("test_db.test_model", SeedKind(path=str(model_b_csv_path)))

    diff = model_a.text_diff(model_b)
    assert diff.endswith("-1,value_a\n+2,value_b")


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

    with open(model_csv_path, "w") as fd:
        fd.write(
            """key,ds,b_a,b_b,i,i_str
123,2022-01-01,false,0,321,321
"""
        )

    model = create_seed_model(
        "test_db.test_model",
        SeedKind(path=str(model_csv_path)),
        columns={
            "key": "string",
            "ds": "date",
            "b_a": "boolean",
            "b_b": "boolean",
            "i": "int",
            "i_str": "text",
        },
    )

    df = next(model.render(context=None))

    assert df["ds"].dtype == "datetime64[ns]"
    assert df["ds"].iloc[0].date() == date(2022, 1, 1)

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


def test_audits():
    expressions = d.parse(
        """
        MODEL (
            name db.seed,
            audits (
                audit_a,
                audit_b(key='value')
            )
        );
        SELECT 1, ds;
    """
    )

    model = load_sql_based_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert model.audits == [
        ("audit_a", {}),
        ("audit_b", {"key": exp.Literal.string("value")}),
    ]


def test_description(sushi_context):
    assert sushi_context.models["sushi.orders"].description == "Table of sushi orders."


def test_render_definition():
    expressions = d.parse(
        """
        MODEL (
            name db.table,
            owner owner_name,
            dialect spark,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column (a, 'yyyymmdd')
            ),
            storage_format iceberg,
            partitioned_by a,
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
                t1.a = t2.a
    """
    )

    model = load_sql_based_model(
        expressions,
        python_env={
            "test_macro": Executable(payload="def test_macro(evaluator, v):\n    return v"),
        },
    )

    # Should not include the macro implementation.
    assert d.format_model_expressions(
        model.render_definition(include_python=False)
    ) == d.format_model_expressions(expressions)

    # Should include the macro implementation.
    assert "def test_macro(evaluator, v):" in d.format_model_expressions(model.render_definition())


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


def test_render_query(assert_exp_eq):
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
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == parse_one("(ds, '%Y-%m-%d')")

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
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == d.parse_one("(ds, '%Y-%m-%d')")

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
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m"
    assert model.time_column.expression == d.parse_one("(ds, '%Y-%m')")


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
    assert model.convert_to_time_column("2022-01-01") == d.parse_one("CAST('20220101' AS date)")


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
    @model(name="my_model", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("foo")
        context.table(model_name=CONST + ".baz")

    m = model.get_registry()["my_model"].model(
        module_path=Path("."),
        path=Path("."),
    )

    assert m.depends_on == {"foo", "bar.baz"}
    assert m.columns_to_types == {"COL": exp.DataType.build("int")}
    assert_exp_eq(
        m.ctas_query(),
        """
SELECT
  CAST(NULL AS INT) AS "COL"
FROM (VALUES
  (1)) AS t(dummy)
WHERE
  FALSE
	""",
    )


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
    post_statement = context.models["MODEL1"].post_statements[0].sql(dialect="snowflake")  # type: ignore
    assert post_statement == "PUT file:///dir/tmp.csv @%table"

    assert isinstance(model2, SqlModel)
    assert isinstance(model2.query, d.MacroFunc)
    assert model2.depends_on == {"MODEL1"}
    assert_exp_eq(
        context.render("model2"),
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
    )

    model2 = load_sql_based_model(
        d.parse(
            """
        MODEL (name db.model2, kind full);

        SELECT * FROM db.model1 AS model1
        """
        ),
    )

    model3 = load_sql_based_model(
        d.parse(
            """
            MODEL(name db.model3, kind full);

            SELECT * FROM db.model2 AS model2
        """
        ),
    )

    context.upsert_model(model1)
    context.upsert_model(model2)
    context.upsert_model(model3)

    assert_exp_eq(
        context.render("db.model2"),
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
        context.render("db.model3"),
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
    snapshots["db.model1"].categorize_as(SnapshotChangeCategory.BREAKING)
    snapshots["db.model2"].categorize_as(SnapshotChangeCategory.BREAKING)
    snapshots["db.model3"].categorize_as(SnapshotChangeCategory.BREAKING)

    assert_exp_eq(
        context.models["db.model2"].render_query(snapshots=snapshots),
        f"""
        SELECT
          "model1"."id" AS "id",
          "model1"."item_id" AS "item_id",
          "model1"."ds" AS "ds"
        FROM "sqlmesh__db"."db__model1__{snapshots['db.model1'].version}" AS "model1"
        """,
    )

    assert_exp_eq(
        context.models["db.model3"].render_query(snapshots=snapshots),
        f"""
        SELECT
          "model2"."id" AS "id",
          "model2"."item_id" AS "item_id",
          "model2"."ds" AS "ds"
        FROM "sqlmesh__db"."db__model2__{snapshots['db.model2'].version}" AS "model2"
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

    assert cache.get_or_load("test_model", "test_entry_a", loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_a", loader).dict() == model.dict()

    assert cache.get_or_load("test_model", "test_entry_b", loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_b", loader).dict() == model.dict()

    assert cache.get_or_load("test_model", "test_entry_a", loader).dict() == model.dict()
    assert cache.get_or_load("test_model", "test_entry_a", loader).dict() == model.dict()

    assert loader.call_count == 3


def test_model_ctas_query():
    expressions = d.parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 as a
    """
    )

    assert (
        load_sql_based_model(expressions, dialect="bigquery").ctas_query().sql()
        == 'SELECT 1 AS "a"'
    )

    expressions = d.parse(
        """
        MODEL (name db.table, kind FULL);
        SELECT 1 as a FROM b
        """
    )

    assert (
        load_sql_based_model(expressions).ctas_query().sql()
        == 'SELECT 1 AS "a" FROM "b" AS "b" WHERE FALSE'
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

        SELECT a, b FROM table_a JOIN table_b
        """
    )

    model = load_sql_based_model(expressions)

    schema = MappingSchema(normalize=False)
    schema.add_table("table_a", {"a": exp.DataType.build("int")})

    # Make sure that the partial schema is not applied.
    model.update_schema(schema)
    assert not model.mapping_schema

    schema.add_table("table_b", {"b": exp.DataType.build("int")})

    model.update_schema(schema)
    assert model.mapping_schema == {
        "table_a": {"a": "INT"},
        "table_b": {"b": "INT"},
    }


def test_user_provided_depends_on():
    expressions = d.parse(
        """
        MODEL (name db.table, depends_on [table_b]);

        SELECT a FROM table_a
        """
    )

    model = load_sql_based_model(expressions)

    assert model.depends_on == {"table_a", "table_b"}


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
        schema.add_table("table_b", {"b": exp.DataType.build("int")})
        model.update_schema(schema)

    assert "table_b" in model.mapping_schema
    assert model.depends_on == {"table_b"}

    render_query_mock.assert_called_once()

    # Simulate rendering at runtime.
    assert_exp_eq(
        model.render_query(), """SELECT * FROM "table_a" AS "table_a", "table_b" AS "table_b" """
    )


def test_contains_star_projection():
    expression_with_star = d.parse(
        """
        MODEL (name db.table);
        SELECT * FROM table_a
        """
    )

    model = load_sql_based_model(expression_with_star)
    assert model.contains_star_projection
    assert model.columns_to_types is None

    # Simulate a query that cannot be rendered at parse time.
    with patch.object(QueryRenderer, "render", return_value=None) as render_query_mock:
        model._columns_to_types = None
        assert model.contains_star_projection is None
        assert model.columns_to_types is None

    expression_without_star = d.parse(
        """
        MODEL (name db.table);
        SELECT a FROM table_a
        """
    )

    model = load_sql_based_model(expression_without_star)
    assert model.contains_star_projection is False
    assert "a" in model.columns_to_types


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
    assert model.name == '"project-1".db.tbl'
    assert model.columns_to_types["a"].sql(dialect="bigquery") == "STRUCT<`a` INT64>"
    assert model.partitioned_by[0].sql(dialect="bigquery") == "foo(`ds`)"
    assert model.depends_on == {'"project-1".db.raw', '"project-2".db.raw'}

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
    assert model.name == "FOO"
    assert model.time_column.column == "A"
    assert model.columns_to_types["A"].sql(dialect="snowflake") == "INT"
    assert model.partitioned_by[0].sql(dialect="snowflake") == "A"
    assert model.partitioned_by[1].sql(dialect="snowflake") == 'FOO("ds")'
    assert model.tags == ["pii", "fact"]
    assert model.clustered_by == ["A"]
    assert model.depends_on == {"BLA"}


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


def test_model_table_properties(sushi_context):
    # # Validate python model table properties
    assert sushi_context.models["sushi.items"].table_properties == {
        "string_prop": exp.Literal.string("some_value"),
        "int_prop": exp.Literal.number(1),
        "float_prop": exp.Literal.number(1.0),
        "bool_prop": exp.true(),
    }
    # Validate a tuple.
    model = load_sql_based_model(
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
    assert model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert model.table_properties_ == d.parse_one(
        """(key_a = 'value_a', 'key_b' = 1, key_c = TRUE, "key_d" = 2.0)"""
    )

    # Validate a tuple with one item.
    model = load_sql_based_model(
        d.parse(
            """
            MODEL (
                name test_schema.test_model,
                table_properties (key_a = 'value_a')
            );
            SELECT a FROM tbl;
            """
        )
    )
    assert model.table_properties == {"key_a": exp.convert("value_a")}
    assert (
        model.table_properties_.sql()
        == exp.Tuple(expressions=[d.parse_one("key_a = 'value_a'")]).sql()
    )

    # Validate an array.
    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            table_properties [
                key_a = 'value_a',
                'key_b' = 1,
            ]
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
    }
    assert model.table_properties_ == d.parse_one("""(key_a = 'value_a', 'key_b' = 1)""")

    # Validate empty.
    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert model.table_properties == {}
    assert model.table_properties_ is None

    # Validate sql expression.
    model = load_sql_based_model(
        d.parse(
            """
        MODEL (
            name test_schema.test_model,
            table_properties [
                key = ['value']
            ]
        );
        SELECT a FROM tbl;
        """
        )
    )
    assert model.table_properties == {"key": d.parse_one("['value']")}
    assert model.table_properties_ == exp.Tuple(expressions=[d.parse_one("key = ['value']")])

    # Validate dict parsing.
    model = create_sql_model(
        name="test_schema.test_model",
        query=d.parse_one("SELECT a FROM tbl"),
        table_properties={
            "key_a": exp.Literal.string("value_a"),
            "key_b": exp.Literal.number(1),
            "key_c": exp.true(),
            "key_d": exp.Literal.number(2.0),
        },
    )
    assert model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert model.table_properties_ == d.parse_one(
        """('key_a' = 'value_a', 'key_b' = 1, 'key_c' = TRUE, 'key_d' = 2.0)"""
    )

    with pytest.raises(ConfigError, match=r"Invalid property 'invalid'.*"):
        load_sql_based_model(
            d.parse(
                """
                MODEL (
                    name test_schema.test_model,
                    table_properties [
                        invalid
                    ]
                );
                SELECT a FROM tbl;
                """
            )
        )


def test_model_session_properties(sushi_context):
    assert sushi_context.models["sushi.items"].session_properties == {
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
            )
        );
        SELECT a FROM tbl;
        """
        )
    )

    assert model.session_properties == {
        "spark.executor.cores": 2,
        "spark.executor.memory": "1G",
        "some_bool": True,
        "some_float": 0.1,
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

    assert definition[1].sql() == "test_int = 1\ntest_str = 'value'"
    assert definition[2].sql() == "JINJA_STATEMENT_BEGIN;\nmacro_b_body\nJINJA_END;"
    assert definition[3].sql() == "JINJA_STATEMENT_BEGIN;\nmacro_a_body\nJINJA_END;"


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


def test_scd_type_2_defaults():
    view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2 (
                unique_key "ID",
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
    scd_type_2_model = load_sql_based_model(view_model_expressions)
    assert scd_type_2_model.unique_key == [exp.to_column("ID", quoted=True)]
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
    assert scd_type_2_model.kind.updated_at_name == "updated_at"
    assert scd_type_2_model.kind.valid_from_name == "valid_from"
    assert scd_type_2_model.kind.valid_to_name == "valid_to"
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert scd_type_2_model.kind.forward_only
    assert scd_type_2_model.kind.disable_restatement


def test_scd_type_2_overrides():
    view_model_expressions = d.parse(
        """
        MODEL (
            name db.table,
            kind SCD_TYPE_2 (
                unique_key ["iD", COALESCE("ds", '')],
                updated_at_name test_updated_at,
                valid_from_name test_valid_from,
                valid_to_name test_valid_to,
                forward_only False,
                disable_restatement False,
            ),
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
    scd_type_2_model = load_sql_based_model(view_model_expressions)
    assert scd_type_2_model.unique_key == [
        exp.column("iD", quoted=True),
        exp.func("COALESCE", '"ds"', "''"),
    ]
    assert scd_type_2_model.managed_columns == {
        "test_valid_from": exp.DataType.build("TIMESTAMP"),
        "test_valid_to": exp.DataType.build("TIMESTAMP"),
    }
    assert scd_type_2_model.kind.updated_at_name == "test_updated_at"
    assert scd_type_2_model.kind.valid_from_name == "test_valid_from"
    assert scd_type_2_model.kind.valid_to_name == "test_valid_to"
    assert scd_type_2_model.kind.is_scd_type_2
    assert scd_type_2_model.kind.is_materialized
    assert not scd_type_2_model.kind.forward_only
    assert not scd_type_2_model.kind.disable_restatement


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
