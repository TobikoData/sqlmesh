from datetime import date
from pathlib import Path

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import exp, parse, parse_one

import sqlmesh.core.dialect as d
from sqlmesh.core.config import Config
from sqlmesh.core.config.model import ModelDefaultsConfig
from sqlmesh.core.context import Context
from sqlmesh.core.macros import macro
from sqlmesh.core.model import (
    IncrementalByTimeRangeKind,
    ModelCache,
    ModelMeta,
    SeedKind,
    SqlModel,
    TimeColumn,
    create_external_model,
    create_seed_model,
    load_model,
    model,
)
from sqlmesh.core.model.common import parse_expression
from sqlmesh.utils.date import to_datetime, to_timestamp
from sqlmesh.utils.errors import ConfigError
from sqlmesh.utils.metaprogramming import Executable


def test_load(assert_exp_eq):
    expressions = parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            storage_format iceberg,
            partitioned_by d,
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
            ),
            tags [tag_foo, tag_bar],
            grain [a, b],
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
        FROM
            db.other_table t1
            LEFT JOIN
            db.table t2
            ON
                t1.a = t2.a;

        DROP TABLE x;
    """,
        read="spark",
    )

    model = load_model(expressions)
    assert model.name == "db.table"
    assert model.owner == "owner_name"
    assert model.dialect == "spark"
    assert model.storage_format == "iceberg"
    assert model.partitioned_by == ["a", "d"]
    assert model.columns_to_types == {
        "a": exp.DataType.build("int"),
        "b": exp.DataType.build("double"),
        "c": exp.DataType.build("boolean"),
        "d": exp.DataType.build("int"),
        "e": exp.DataType.build("double"),
        "f": exp.DataType.build("boolean"),
    }
    assert model.view_name == "table"
    assert model.macro_definitions == [
        parse_one("@DEF(x, 1)"),
    ]
    assert list(model.pre_statements) == [
        parse_one("@DEF(x, 1)"),
        parse_one("CACHE TABLE x AS SELECT 1"),
        parse_one("ADD JAR 's3://my_jar.jar'", read="spark"),
    ]
    assert list(model.post_statements) == [
        parse_one("DROP TABLE x"),
    ]
    assert model.depends_on == {"db.other_table"}
    assert_exp_eq(
        model.render_query(),
        """
    SELECT
        TRY_CAST(1 AS INT) AS a,
        TRY_CAST(2 AS DOUBLE) AS b,
        TRY_CAST(c AS BOOL) AS c,
        TRY_CAST(1 AS INT) AS d, -- d
        TRY_CAST(2 AS DOUBLE) AS e, -- e
        TRY_CAST(f AS BOOL) AS f, -- f
    FROM
        db.other_table t1
        LEFT JOIN
        db.table t2
        ON
            t1.a = t2.a
    """,
    )
    assert model.tags == ["tag_foo", "tag_bar"]
    assert model.grain == ["a", "b"]


def test_model_multiple_select_statements():
    # Make sure the load_model raises an exception for model with multiple select statements.
    expressions = parse(
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
        load_model(expressions)


@pytest.mark.parametrize(
    "query, error",
    [
        ("y::int, x::int AS y", "duplicate"),
    ],
)
def test_model_validation(query, error):
    expressions = parse(
        f"""
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT {query}
        """
    )

    with pytest.raises(ConfigError) as ex:
        load_model(expressions)
    assert error in str(ex.value)


def test_model_union_query():
    expressions = parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT a, b UNION SELECT c, c
        """
    )

    load_model(expressions)


def test_model_validation_union_query():
    expressions = parse(
        """
        MODEL (
            name db.table,
            kind FULL,
        );

        SELECT a, a UNION SELECT c, c
        """
    )

    with pytest.raises(ConfigError, match=r"Found duplicate outer select name 'a'"):
        load_model(expressions)


def test_partitioned_by():
    expressions = parse(
        """
        MODEL (
            name db.table,
            dialect spark,
            owner owner_name,
            partitioned_by (a, b),
            kind INCREMENTAL_BY_TIME_RANGE(
                time_column a,
            ),
        );

        SELECT 1::int AS a, 2::int AS b;
    """
    )

    model = load_model(expressions)
    assert model.partitioned_by == ["a", "b"]


def test_no_model_statement():
    expressions = parse(
        """
        SELECT 1 AS x
    """
    )

    with pytest.raises(
        ConfigError,
        match="MODEL statement is required as the first statement in the definition at '.",
    ):
        load_model(expressions)


def test_unordered_model_statements():
    expressions = parse(
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
        load_model(expressions)
    assert "MODEL statement is required" in str(ex.value)


def test_no_query():
    expressions = parse(
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
        load_model(expressions, path=Path("test_location"))
    assert "have a SELECT" in str(ex.value)


def test_partition_key_is_missing_in_query():
    expressions = parse(
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

    with pytest.raises(ConfigError) as ex:
        load_model(expressions)
    assert "['c', 'd'] are missing" in str(ex.value)


def test_partition_key_and_select_star():
    expressions = parse(
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

    load_model(expressions)


def test_json_serde():
    model = SqlModel(
        name="test_model",
        kind=IncrementalByTimeRangeKind(time_column="ds"),
        owner="test_owner",
        dialect="spark",
        cron="@daily",
        storage_format="parquet",
        partitioned_by=["a"],
        query=parse_one("SELECT a FROM tbl"),
        pre_statements=[
            parse_one("@DEF(key, 'value')"),
        ],
    )
    model_json_str = model.json()

    deserialized_model = SqlModel.parse_raw(model_json_str)
    assert deserialized_model == model


def test_column_descriptions(sushi_context, assert_exp_eq):
    assert sushi_context.models["sushi.customer_revenue_by_day"].column_descriptions == {
        "customer_id": "Customer id",
        "revenue": "Revenue from orders made by this customer",
        "ds": "Date",
    }

    expressions = parse(
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
    model = load_model(expressions)

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
    model = load_model(expressions)
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
    model = load_model(expressions)

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
    expressions = parse(
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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
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
    expressions = parse(
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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.kind.path == "../seeds/waiter_names.csv"
    assert model.kind.batch_size == 100
    assert model.seed is not None
    assert len(model.seed.content) > 0

    assert model.columns_to_types == {
        "id": exp.DataType.build("bigint"),
        "name": exp.DataType.build("varchar"),
    }

    assert model.batch_size is None


def test_seed_provided_columns():
    expressions = parse(
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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

    assert isinstance(model.kind, SeedKind)
    assert model.kind.path == "../seeds/waiter_names.csv"
    assert model.kind.batch_size == 100
    assert model.seed is not None
    assert len(model.seed.content) > 0

    assert model.columns_to_types == {
        "id": exp.DataType.build("double"),
        "alias": exp.DataType.build("varchar"),
    }


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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))

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
    expressions = parse(
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

    model = load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))
    assert model.audits == [
        ("audit_a", {}),
        ("audit_b", {"key": exp.Literal.string("value")}),
    ]


def test_description(sushi_context):
    assert sushi_context.models["sushi.orders"].description == "Table of sushi orders."


def test_render_definition():
    expressions = parse(
        """
        MODEL (
            dialect spark,
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column (a, 'yyyymmdd')
            ),
            owner owner_name,
            storage_format iceberg,
            partitioned_by a,
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
    """,
        read="spark",
    )

    model = load_model(
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
    daily = ModelMeta(name="x", cron="@daily")
    assert to_datetime(daily.cron_prev("2020-01-01")) == to_datetime("2019-12-31")
    assert to_datetime(daily.cron_floor("2020-01-01")) == to_datetime("2020-01-01")
    assert to_timestamp(daily.cron_floor("2020-01-01 10:00:00")) == to_timestamp("2020-01-01")
    assert to_timestamp(daily.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02")

    offset = ModelMeta(name="x", cron="1 0 * * *")
    assert to_datetime(offset.cron_prev("2020-01-01")) == to_datetime("2019-12-31")
    assert to_datetime(offset.cron_floor("2020-01-01")) == to_datetime("2020-01-01")
    assert to_timestamp(offset.cron_floor("2020-01-01 10:00:00")) == to_timestamp("2020-01-01")
    assert to_timestamp(offset.cron_next("2020-01-01 10:00:00")) == to_timestamp("2020-01-02")

    hourly = ModelMeta(name="x", cron="1 * * * *")
    assert hourly.normalized_cron() == "0 * * * *"
    assert to_timestamp(hourly.cron_prev("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01 09:00:00"
    )
    assert to_timestamp(hourly.cron_prev("2020-01-01 10:01:00")) == to_timestamp(
        "2020-01-01 10:00:00"
    )
    assert to_timestamp(hourly.cron_floor("2020-01-01 10:01:00")) == to_timestamp(
        "2020-01-01 10:00:00"
    )


def test_render_query(assert_exp_eq):
    model = SqlModel(
        name="test",
        cron="1 0 * * *",
        kind=IncrementalByTimeRangeKind(time_column=TimeColumn(column="y")),
        query=parse_one(
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
          y AS y
        FROM x AS x
        WHERE
          y <= '2020-10-28'
          AND y <= DATE_STR_TO_DATE('2020-10-28')
          AND y >= '2020-10-28'
          AND y >= DATE_STR_TO_DATE('2020-10-28')
        """,
    )
    assert_exp_eq(
        model.render_query(
            start="2020-10-28", end=to_datetime("2020-10-29"), add_incremental_filter=True
        ),
        """
        SELECT
          *
        FROM (
          SELECT
            y AS y
          FROM x AS x
          WHERE
            y <= '2020-10-28'
            AND y <= DATE_STR_TO_DATE('2020-10-28')
            AND y >= '2020-10-28'
            AND y >= DATE_STR_TO_DATE('2020-10-28')
        ) AS _subquery
        WHERE
          y BETWEEN TIME_STR_TO_TIME('2020-10-28T00:00:00+00:00') AND TIME_STR_TO_TIME('2020-10-28T23:59:59.999000+00:00')

        """,
    )


def test_time_column():
    expressions = parse(
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
    model = load_model(expressions)
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == parse_one("(ds, '%Y-%m-%d')")

    expressions = parse(
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
    model = load_model(expressions)
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m-%d"
    assert model.time_column.expression == parse_one("(ds, '%Y-%m-%d')")

    expressions = parse(
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
    model = load_model(expressions)
    assert model.time_column.column == "ds"
    assert model.time_column.format == "%Y-%m"
    assert model.time_column.expression == parse_one("(ds, '%Y-%m')")


def test_default_time_column():
    expressions = parse(
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
    model = load_model(expressions, time_column_format="%Y")
    assert model.time_column.format == "%Y"

    expressions = parse(
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
    model = load_model(expressions, time_column_format="%m")
    assert model.time_column.format == "%Y"

    expressions = parse(
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
    model = load_model(expressions, dialect="duckdb", time_column_format="%Y")
    assert model.time_column.format == "%d"


def test_convert_to_time_column():
    expressions = parse(
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
    model = load_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one("'2022-01-01'")
    assert model.convert_to_time_column(to_datetime("2022-01-01")) == parse_one("'2022-01-01'")

    expressions = parse(
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
    model = load_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one("'01/01/2022'")

    expressions = parse(
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
    model = load_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one("20220101")

    expressions = parse(
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
    model = load_model(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one("CAST('20220101' AS date)")


def test_filter_time_column(assert_exp_eq):
    expressions = parse(
        """
        MODEL (
          name sushi.items,
          kind INCREMENTAL_BY_TIME_RANGE(
            time_column (ds, '%Y%m%d')
          )
        );

        SELECT
          id::INT AS id,
          name::TEXT AS name,
          price::DOUBLE AS price,
          ds::TEXT AS ds
        FROM raw.items
    """
    )
    model = load_model(expressions)

    assert_exp_eq(
        model.render_query(
            start="2021-01-01", end="2021-01-01", latest="2021-01-01", add_incremental_filter=True
        ),
        """
        SELECT
          *
        FROM (
          SELECT
            CAST(id AS INT) AS id,
            CAST(name AS TEXT) AS name,
            CAST(price AS DOUBLE) AS price,
            CAST(ds AS TEXT) AS ds
          FROM raw.items AS items
        ) AS _subquery
        WHERE
          ds BETWEEN '20210101' AND '20210101'
        """,
    )

    expressions = parse(
        """
        MODEL (
          name sushi.items,
          kind INCREMENTAL_BY_TIME_RANGE(
            time_column (ds, '%Y%m%d')
          )
        );

        SELECT
          id::INT AS id,
          name::TEXT AS name,
          price::DOUBLE AS price,
          ds::TEXT AS ds
        FROM raw.items
        WHERE
          CAST(ds AS TEXT) <= '20210101' AND CAST(ds as TEXT) >= '20210101'
    """
    )
    model = load_model(expressions)

    assert_exp_eq(
        model.render_query(
            start="2021-01-01", end="2021-01-01", latest="2021-01-01", add_incremental_filter=True
        ),
        """
        SELECT
          *
        FROM (
          SELECT
            CAST(id AS INT) AS id,
            CAST(name AS TEXT) AS name,
            CAST(price AS DOUBLE) AS price,
            CAST(ds AS TEXT) AS ds
          FROM raw.items AS items
          WHERE
            CAST(ds AS TEXT) <= '20210101' AND CAST(ds AS TEXT) >= '20210101'
        ) AS _subquery
        WHERE
          ds BETWEEN '20210101' AND '20210101'
        """,
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
    model = load_model(expressions, dialect="hive")
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
        CAST(id AS INT) AS id,
        ds AS ds
      FROM x AS x
      WHERE
        ds <= '1970-01-01' AND ds >= '1970-01-01'
    """,
    )


CONST = "bar"


def test_python_model_deps() -> None:
    @model(name="my_model", kind="full", columns={"foo": "int"})
    def my_model(context, **kwargs):
        context.table("foo")
        context.table(model_name=CONST + ".baz")

    assert model.get_registry()["my_model"].model(
        module_path=Path("."),
        path=Path("."),
    ).depends_on == {"foo", "bar.baz"}


def test_star_expansion(assert_exp_eq) -> None:
    context = Context(config=Config())

    model1 = load_model(
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

    model2 = load_model(
        d.parse(
            """
        MODEL (name db.model2, kind full);

        SELECT * FROM db.model1 AS model1
        """
        ),
    )

    model3 = load_model(
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


def test_case_sensitivity(assert_exp_eq):
    config = Config(model_defaults=ModelDefaultsConfig(dialect="snowflake"))
    context = Context(config=config)

    source = load_model(
        d.parse(
            """
            MODEL (name example.source, kind EMBEDDED);

            SELECT "id", "name", "payload" FROM db.schema."table"
            """
        ),
        dialect="snowflake",
    )

    # Ensure that when manually specifying dependencies, they're normalized correctly
    downstream = load_model(
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
            "id" AS "id",
            "name" AS "name",
            "payload" AS "payload"
          FROM "DB"."SCHEMA"."table" AS "table"
        ) AS "SOURCE"
        """,
    )


def test_batch_size_validation():
    expressions = parse(
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
        load_model(expressions, path=Path("./examples/sushi/models/test_model.sql"))


def test_model_cache(tmp_path: Path, mocker: MockerFixture):
    cache = ModelCache(tmp_path)

    expressions = parse(
        """
        MODEL (
            name db.seed,
        );
        SELECT 1, ds;
    """
    )

    model = load_model([e for e in expressions if e])

    loader = mocker.Mock(return_value=model)

    assert cache.get_or_load("test_model", "test_entry_a", loader) == model
    assert cache.get_or_load("test_model", "test_entry_a", loader) == model

    assert cache.get_or_load("test_model", "test_entry_b", loader) == model
    assert cache.get_or_load("test_model", "test_entry_b", loader) == model

    assert cache.get_or_load("test_model", "test_entry_a", loader) == model
    assert cache.get_or_load("test_model", "test_entry_a", loader) == model

    assert loader.call_count == 3


def test_model_ctas_query():
    expressions = parse(
        """
        MODEL (name `a-b-c.table`, kind FULL, dialect bigquery);
        SELECT 1 as a
        """,
        read="bigquery",
    )

    assert load_model(expressions, dialect="bigquery").ctas_query().sql() == 'SELECT 1 AS "a"'

    expressions = parse(
        """
        MODEL (name db.table, kind FULL);
        SELECT 1 as a FROM b
        """
    )

    assert (
        load_model(expressions).ctas_query().sql() == 'SELECT 1 AS "a" FROM "b" AS "b" WHERE FALSE'
    )


def test_is_breaking_change():
    model = create_external_model("a", columns={"a": "int", "b": "int"})
    assert model.is_breaking_change(create_external_model("a", columns={"a": "int"})) is False
    assert model.is_breaking_change(create_external_model("a", columns={"a": "text"})) is None
    assert (
        model.is_breaking_change(create_external_model("a", columns={"a": "int", "b": "int"}))
        is False
    )
    assert (
        model.is_breaking_change(
            create_external_model("a", columns={"a": "int", "b": "int", "c": "int"})
        )
        is None
    )


def test_parse_expression_list_with_jinja():
    input = [
        "JINJA_STATEMENT_BEGIN;\n{{ log('log message') }}\nJINJA_END;",
        "GRANT SELECT ON TABLE foo TO DEV",
    ]
    assert input == [val.sql() for val in parse_expression(input)]


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

    with pytest.raises(
        ConfigError,
        match=r"Dependencies must be provided explicitly for models that can be rendered only at runtime at.*",
    ):
        load_model(expressions)
