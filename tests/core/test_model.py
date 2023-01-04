from pathlib import Path

import pytest
from sqlglot import exp, parse, parse_one

from sqlmesh.core.config import Config
from sqlmesh.core.context import Context
from sqlmesh.core.dialect import Jinja, format_model_expressions, parse_model
from sqlmesh.core.model import (
    IncrementalByTimeRange,
    Model,
    ModelMeta,
    TimeColumn,
    model,
)
from sqlmesh.utils.date import to_date, to_datetime, to_timestamp
from sqlmesh.utils.errors import ConfigError


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
                t1.a = t2.a
    """,
        read="spark",
    )

    model = Model.load(expressions)
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
    assert model.sql_statements == [
        parse_one("CACHE TABLE x AS SELECT 1"),
        parse_one("ADD JAR 's3://my_jar.jar'", read="spark"),
    ]
    assert model.depends_on == {"db.other_table"}
    assert_exp_eq(
        model.query,
        """
    SELECT
        CAST(1 AS INT) AS a,
        TRY_CAST(2 AS DOUBLE) AS b,
        CAST(c AS BOOL),
        CAST(1 AS INT) AS d, -- d
        TRY_CAST(2 AS DOUBLE) AS e, -- e
        CAST(f AS BOOL), -- f
    FROM
        db.other_table t1
        LEFT JOIN
        db.table t2
        ON
            t1.a = t2.a
    """,
    )


@pytest.mark.parametrize(
    "query, error",
    [
        ("sum(x)::int", "must have inferrable names"),
        ("CAST(x + 1 AS INT)", "must have inferrable names"),
        ("y::int, x::int AS y", "duplicate"),
        ("sum(x)::int -- annotation", "must have inferrable names"),
    ],
)
def test_model_validation(query, error):
    expressions = parse(
        f"""
        MODEL (
            name db.table,
        );

        SELECT {query}
        """
    )

    with pytest.raises(ConfigError) as ex:
        Model.load(expressions)
    assert error in str(ex.value)


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

    model = Model.load(expressions)
    assert model.partitioned_by == ["a", "b"]


def test_no_model_statement():
    expressions = parse(
        """
        SELECT 1 AS x
    """
    )

    with pytest.raises(ConfigError) as ex:
        Model.load(expressions)
    assert "Incomplete model definition" in str(ex.value)


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
        Model.load(expressions)
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
        Model.load(expressions, path=Path("test_location"))
    assert "must be a SELECT" in str(ex.value)


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
        Model.load(expressions)
    assert "['c', 'd'] are missing" in str(ex.value)


def test_json_serde():
    model = Model(
        name="test_model",
        owner="test_owner",
        dialect="spark",
        cron="@daily",
        storage_format="parquet",
        partitioned_by=["a"],
        query=parse_one("SELECT a FROM tbl"),
        expressions=[
            parse_one("@DEF(key, 'value')"),
        ],
    )
    model_json_str = model.json()

    deserialized_model = Model.parse_raw(model_json_str)
    assert deserialized_model == model


def test_column_descriptions(sushi_context, assert_exp_eq):
    assert sushi_context.models[
        "sushi.customer_revenue_by_day"
    ].column_descriptions == {
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
    model = Model.load(expressions)

    assert_exp_eq(
        model.query,
        """
        SELECT
          id::int, -- primary key
          foo::int, -- bar
        FROM table
    """,
    )


def test_description(sushi_context):
    assert sushi_context.models["sushi.orders"].description == "Table of sushi orders."


def test_render():
    expressions = parse(
        """
        MODEL (
            name db.table,
            kind INCREMENTAL_BY_TIME_RANGE (
                time_column (a, 'yyyymmdd')
            ),
            dialect spark,
            cron '@daily',
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
        FROM
            db.other_table t1
            LEFT JOIN
            db.table t2
            ON
                t1.a = t2.a
    """,
        read="spark",
    )

    model = Model.load(expressions)
    assert format_model_expressions(model.render()) == format_model_expressions(
        expressions
    )


def test_cron():
    daily = ModelMeta(name="x", cron="@daily")
    assert daily.cron_prev("2020-01-01") == to_date("2019-12-31")
    assert daily.cron_floor("2020-01-01") == to_date("2020-01-01")
    assert to_timestamp(daily.cron_floor("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01"
    )
    assert to_timestamp(daily.cron_next("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-02"
    )

    offset = ModelMeta(name="x", cron="1 0 * * *")
    assert offset.cron_prev("2020-01-01") == to_date("2019-12-31")
    assert offset.cron_floor("2020-01-01") == to_date("2020-01-01")
    assert to_timestamp(offset.cron_floor("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-01"
    )
    assert to_timestamp(offset.cron_next("2020-01-01 10:00:00")) == to_timestamp(
        "2020-01-02"
    )

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
    model = Model(
        name="test",
        cron="1 0 * * *",
        kind=IncrementalByTimeRange(time_column=TimeColumn(column="y")),
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
        SELECT y
        FROM x
        WHERE
          y <= '2020-10-28'
          AND y <= TIME_STR_TO_TIME('2020-10-28T23:59:59.999000+00:00')
          AND y >= '2020-10-28'
          AND y >= TIME_STR_TO_TIME('2020-10-28T00:00:00+00:00')
        """,
    )
    assert_exp_eq(
        model.render_query(start="2020-10-28", end=to_datetime("2020-10-29")),
        """
        SELECT y
        FROM x
        WHERE
          y <= '2020-10-28'
          AND y <= TIME_STR_TO_TIME('2020-10-28T23:59:59.999000+00:00')
          AND y >= '2020-10-28'
          AND y >= TIME_STR_TO_TIME('2020-10-28T00:00:00+00:00')
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
    model = Model.load(expressions)
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
    model = Model.load(expressions)
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
    model = Model.load(expressions)
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
    model = Model.load(expressions, time_column_format="%Y")
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
    model = Model.load(expressions, time_column_format="%m")
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
    model = Model.load(expressions, dialect="duckdb", time_column_format="%Y")
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
    model = Model.load(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one("'2022-01-01'")
    assert model.convert_to_time_column(to_datetime("2022-01-01")) == parse_one(
        "'2022-01-01'"
    )

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
    model = Model.load(expressions)
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
    model = Model.load(expressions)
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
    model = Model.load(expressions)
    assert model.convert_to_time_column("2022-01-01") == parse_one(
        "CAST('20220101' AS date)"
    )


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
    model = Model.load(expressions)

    assert_exp_eq(
        model.render_query(start="2021-01-01", end="2021-01-01", latest="2021-01-01"),
        """
        SELECT
          id::INT AS id,
          name::TEXT AS name,
          price::DOUBLE AS price,
          ds::TEXT AS ds
        FROM raw.items
        WHERE
          CAST(ds AS TEXT) <= '20210101' AND CAST(ds as TEXT) >= '20210101'
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
    model = Model.load(expressions)

    assert_exp_eq(
        model.render_query(start="2021-01-01", end="2021-01-01", latest="2021-01-01"),
        """
        SELECT
          id::INT AS id,
          name::TEXT AS name,
          price::DOUBLE AS price,
          ds::TEXT AS ds
        FROM raw.items
        WHERE
          CAST(ds AS TEXT) <= '20210101' AND CAST(ds as TEXT) >= '20210101'
        """,
    )


def test_parse_model(assert_exp_eq):
    expressions = parse_model(
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
    model = Model.load(expressions, dialect="hive")
    assert model.columns_to_types == {
        "ds": exp.DataType.build("unknown"),
        "id": exp.DataType.build("int"),
    }
    assert model.dialect == ""
    assert isinstance(model.query, Jinja)
    assert isinstance(Model.parse_raw(model.json()).query, Jinja)
    assert_exp_eq(
        model.render_query(),
        """
      SELECT
        CAST(id AS INT) AS id,
        ds
      FROM x
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

    model1 = Model.load(
        parse_model(
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
        path=context.path,
        dialect=context.dialect,
    )

    model2 = Model.load(
        parse_model(
            """
        MODEL (name db.model2, kind full);

        SELECT * FROM db.model1 AS model1
		"""
        ),
        path=context.path,
        dialect=context.dialect,
    )

    model3 = Model.load(
        parse_model(
            """
            MODEL(name db.model3, kind full);

            SELECT * FROM db.model2 AS model2
        """
        ),
        path=context.path,
        dialect=context.dialect,
    )

    context.upsert_model(model1)
    context.upsert_model(model2)
    context.upsert_model(model3)

    assert_exp_eq(
        context.render("db.model2"),
        f"""
        SELECT
          model1.id AS id,
          model1.item_id AS item_id,
          model1.ds AS ds
        FROM (
          SELECT
            CAST(t.id AS INT) AS id,
            CAST(t.item_id AS INT) AS item_id,
            CAST(t.ds AS TEXT) AS ds
          FROM (VALUES
            (1, 1, '2020-01-01'),
            (1, 2, '2020-01-01'),
            (2, 1, '2020-01-01'),
            (3, 3, '2020-01-03'),
            (4, 1, '2020-01-04'),
            (5, 1, '2020-01-05'),
            (6, 1, '2020-01-06'),
            (7, 1, '2020-01-07')) AS t(id, item_id, ds)
        ) AS model1
        """,
    )
    assert_exp_eq(
        context.render("db.model3"),
        f"""
        SELECT
          model2.id AS id,
          model2.item_id AS item_id,
          model2.ds AS ds
        FROM (
          SELECT
            model1.id AS id,
            model1.item_id AS item_id,
            model1.ds AS ds
          FROM (
            SELECT
              CAST(t.id AS INT) AS id,
              CAST(t.item_id AS INT) AS item_id,
              CAST(t.ds AS TEXT) AS ds
            FROM (VALUES
              (1, 1, '2020-01-01'),
              (1, 2, '2020-01-01'),
              (2, 1, '2020-01-01'),
              (3, 3, '2020-01-03'),
              (4, 1, '2020-01-04'),
              (5, 1, '2020-01-05'),
              (6, 1, '2020-01-06'),
              (7, 1, '2020-01-07')) AS t(id, item_id, ds)
          ) AS model1
        ) AS model2
        """,
    )
