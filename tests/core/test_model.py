import logging
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
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
from sqlmesh.core.model.kind import _model_kind_validator
from sqlmesh.core.model.seed import CsvSettings
from sqlmesh.core.node import IntervalUnit, _Node
from sqlmesh.core.snapshot import SnapshotChangeCategory
from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.errors import ConfigError, SQLMeshError
from sqlmesh.utils.jinja import JinjaMacroRegistry, MacroInfo
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
    assert sushi_context.models[
        '"memory"."sushi"."customer_revenue_by_day"'
    ].column_descriptions == {
        "customer_id": "Customer id",
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
            """key,ds_date,ds_timestamp,b_a,b_b,i,i_str
123,2022-01-01,2022-01-01,false,0,321,321
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
    assert sushi_context.models['"memory"."sushi"."orders"'].description == "Table of sushi orders."


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
        default_catalog="catalog",
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
    @model(name="my_model", kind="full", columns={'"COL"': "int"})
    def my_model(context, **kwargs):
        context.table("foo")
        context.table(model_name=CONST + ".baz")

    m = model.get_registry()["my_model"].model(
        module_path=Path("."),
        path=Path("."),
        dialect="duckdb",
    )

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
        model.render_query(optimize=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg(
            '"db"."table"', ('"table_b"',)
        )

    schema.add_table('"table_b"', {"b": exp.DataType.build("int")})
    model.update_schema(schema)
    assert model.mapping_schema == {
        '"table_a"': {"a": "INT"},
        '"table_b"': {"b": "INT"},
    }
    model.render_query(optimize=True)


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
        model.render_query(optimize=True)
        mock_logger.assert_not_called()

    # star, full schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM a CROSS JOIN b"))
        model.update_schema(full_schema)
        model.render_query(optimize=True)
        mock_logger.assert_not_called()

    # star, partial schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM a CROSS JOIN b"))
        model.update_schema(partial_schema)
        model.render_query(optimize=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg('"test"', ('"b"',))

    # star, no schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(d.parse("MODEL (name test); SELECT * FROM b JOIN a"))
        model.render_query(optimize=True)
        assert mock_logger.call_args[0][0] == missing_schema_warning_msg('"test"', ('"a"', '"b"'))

    # no star, full schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.update_schema(full_schema)
        model.render_query(optimize=True)
        mock_logger.assert_not_called()

    # no star, partial schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.update_schema(partial_schema)
        model.render_query(optimize=True)
        mock_logger.assert_not_called()

    # no star, empty schema
    with patch.object(logger, "warning") as mock_logger:
        model = load_sql_based_model(
            d.parse("MODEL (name test); SELECT x::INT FROM a CROSS JOIN b")
        )
        model.render_query(optimize=True)
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
    assert model.time_column.column == "A"
    assert model.columns_to_types["A"].sql(dialect="snowflake") == "INT"
    assert model.partitioned_by[0].sql(dialect="snowflake") == "A"
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
            exp.column("A", quoted=False),
            exp.func("COALESCE", exp.column("B", quoted=False), "''"),
            exp.column("c", quoted=True),
        ]

    expr = d.parse(
        """
        MODEL (
            name foo,
            dialect snowflake,
            kind INCREMENTAL_BY_UNIQUE_KEY(unique_key a)
        );

        SELECT x.a AS a FROM test.x AS x
        """
    )
    model = SqlModel.parse_raw(load_sql_based_model(expr).json())
    assert model.unique_key == [exp.column("A", quoted=False)]


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


def test_model_table_properties() -> None:
    # Validate python model table properties
    @model(
        "my_model",
        kind="full",
        columns={"id": "int"},
        table_properties={
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

    assert python_model.table_properties == {
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
    assert sql_model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert sql_model.table_properties_ == d.parse_one(
        """(key_a = 'value_a', 'key_b' = 1, key_c = TRUE, "key_d" = 2.0)"""
    )

    # Validate a tuple with one item.
    sql_model = load_sql_based_model(
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
    assert sql_model.table_properties == {"key_a": exp.convert("value_a")}
    assert (
        sql_model.table_properties_.sql()  # type: ignore
        == exp.Tuple(expressions=[d.parse_one("key_a = 'value_a'")]).sql()
    )

    # Validate an array.
    sql_model = load_sql_based_model(
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
    assert sql_model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
    }
    assert sql_model.table_properties_ == d.parse_one("""(key_a = 'value_a', 'key_b' = 1)""")

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
    assert sql_model.table_properties == {}
    assert sql_model.table_properties_ is None

    # Validate sql expression.
    sql_model = load_sql_based_model(
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
    assert sql_model.table_properties == {"key": d.parse_one("['value']")}
    assert sql_model.table_properties_ == exp.Tuple(expressions=[d.parse_one("key = ['value']")])

    # Validate dict parsing.
    sql_model = create_sql_model(
        name="test_schema.test_model",
        query=d.parse_one("SELECT a FROM tbl"),
        table_properties={
            "key_a": exp.Literal.string("value_a"),
            "key_b": exp.Literal.number(1),
            "key_c": exp.true(),
            "key_d": exp.Literal.number(2.0),
        },
    )
    assert sql_model.table_properties == {
        "key_a": exp.convert("value_a"),
        "key_b": exp.convert(1),
        "key_c": exp.convert(True),
        "key_d": exp.convert(2.0),
    }
    assert sql_model.table_properties_ == d.parse_one(
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
    assert scd_type_2_model.kind.updated_at_name == "updated_at"
    assert scd_type_2_model.kind.valid_from_name == "valid_from"
    assert scd_type_2_model.kind.valid_to_name == "valid_to"
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
        exp.column("ds", quoted=False),
    ]
    assert scd_type_2_model.managed_columns == {
        "test_valid_from": exp.DataType.build("TIMESTAMPTZ"),
        "test_valid_to": exp.DataType.build("TIMESTAMPTZ"),
    }
    assert scd_type_2_model.kind.updated_at_name == "test_updated_at"
    assert scd_type_2_model.kind.valid_from_name == "test_valid_from"
    assert scd_type_2_model.kind.valid_to_name == "test_valid_to"
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
    assert scd_type_2_model.kind.valid_from_name == "valid_from"
    assert scd_type_2_model.kind.valid_to_name == "valid_to"
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
        exp.column("ds", quoted=False),
    ]
    assert scd_type_2_model.managed_columns == {
        "test_valid_from": exp.DataType.build("TIMESTAMPTZ"),
        "test_valid_to": exp.DataType.build("TIMESTAMPTZ"),
    }
    assert scd_type_2_model.kind.valid_from_name == "test_valid_from"
    assert scd_type_2_model.kind.valid_to_name == "test_valid_to"
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


@pytest.mark.parametrize(
    "input_columns,expected_columns",
    [
        (
            "col1",
            [exp.to_column("col1")],
        ),
        (
            "[col1]",
            [exp.to_column("col1")],
        ),
        (
            "[col1, col2]",
            [exp.to_column("col1"), exp.to_column("col2")],
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
    assert model.kind.when_matched.sql() == expected_when_matched

    model = SqlModel.parse_raw(model.json())
    assert model.kind.when_matched.sql() == expected_when_matched


def test_default_catalog_sql(assert_exp_eq):
    """
    This test validates the hashing behavior of the system as it relates to the default catalog.
    The system is not designed to actually support having an engine that doesn't support default catalog
    to start supporting it or the reverse of that. If that did happen then bugs would occur.
    """
    HASH_WITH_CATALOG = "1074956187"

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
        f"""
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
