import pytest
from pytest_mock.plugin import MockerFixture
import pandas as pd
from sqlglot import exp
from sqlmesh.core import dialect as d
from sqlmesh.core.context import Context
from sqlmesh.core.config import AutoCategorizationMode, CategorizerConfig
from sqlmesh.core.model import SqlModel, load_sql_based_model
from sqlmesh.core.table_diff import TableDiff


@pytest.mark.slow
def test_data_diff(sushi_context_fixed_date):
    model = sushi_context_fixed_date.models['"memory"."sushi"."customer_revenue_by_day"']

    model.query.select(exp.cast("'1'", "VARCHAR").as_("modified_col"), "1 AS y", copy=False)
    sushi_context_fixed_date.upsert_model(model)

    sushi_context_fixed_date.plan(
        "source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-31",
        end="2023-01-31",
    )
    model = sushi_context_fixed_date.models['"memory"."sushi"."customer_revenue_by_day"']

    for column in model.query.find_all(exp.Column):
        if column.name == "total":
            column.replace(
                exp.func(
                    "IF",
                    exp.column("total", "ot") < 4,
                    exp.column("total", "ot") * 0.1,
                    exp.column("total", "ot"),
                )
            )

    for alias in model.query.find_all(exp.Alias):
        if alias.alias == "modified_col":
            alias.this.set("to", exp.DataType.build("INT"))
        if alias.alias == "y":
            alias.pop()

    model.query.select("1 AS z", copy=False).limit(10000, copy=False)
    # add row to table with UNION
    modified_model = model.dict()
    modified_model["query"] = (
        exp.select("*")
        .from_(model.query.subquery())
        .union("select -1, 9999.00, 0, CAST('2023-01-31' AS DATE), 1, 1")
    )
    modified_sqlmodel = SqlModel(**modified_model)
    sushi_context_fixed_date.upsert_model(modified_sqlmodel)

    sushi_context_fixed_date.auto_categorize_changes = CategorizerConfig(
        sql=AutoCategorizationMode.FULL
    )
    sushi_context_fixed_date.plan(
        "target_dev",
        create_from="source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-31",
        end="2023-01-31",
    )

    diff = sushi_context_fixed_date.table_diff(
        source="source_dev",
        target="target_dev",
        on=exp.condition("s.customer_id = t.customer_id AND s.event_date = t.event_date"),
        model_or_snapshot="sushi.customer_revenue_by_day",
    )

    schema_diff = diff.schema_diff()
    assert schema_diff.added == [("z", exp.DataType.build("int"))]
    assert schema_diff.modified == {
        "modified_col": (exp.DataType.build("text"), exp.DataType.build("int"))
    }
    assert schema_diff.removed == [("y", exp.DataType.build("int"))]

    row_diff = diff.row_diff()
    assert row_diff.source_count == 17
    assert row_diff.target_count == 18
    assert row_diff.join_count == 17
    assert row_diff.full_match_count == 17
    assert row_diff.full_match_pct == 97.14
    assert row_diff.partial_match_count == 0.0
    assert row_diff.partial_match_pct == 0.0
    assert row_diff.s_only_count == 0
    assert row_diff.t_only_count == 1
    assert row_diff.sample.shape == (1, 12)
    assert row_diff.joined_sample.shape == (0, 2)
    assert row_diff.s_sample.shape == (0, 6)
    assert row_diff.t_sample.shape == (1, 6)


@pytest.mark.slow
def test_data_diff_decimals(sushi_context_fixed_date):
    engine_adapter = sushi_context_fixed_date.engine_adapter

    engine_adapter.ctas(
        "table_diff_source",
        pd.DataFrame(
            {
                "key": [1, 2, 3],
                "value": [1.0, 2.0, 3.1233],
            }
        ),
    )

    engine_adapter.ctas(
        "table_diff_target",
        pd.DataFrame(
            {
                "key": [1, 2, 3],
                "value": [1.0, 2.0, 3.1234],
            }
        ),
    )

    diff = sushi_context_fixed_date.table_diff(
        source="table_diff_source",
        target="table_diff_target",
        on=["key"],
    )
    assert diff.row_diff().full_match_count == 3
    assert diff.row_diff().partial_match_count == 0

    diff = sushi_context_fixed_date.table_diff(
        source="table_diff_source",
        target="table_diff_target",
        on=["key"],
        decimals=4,
    )

    row_diff = diff.row_diff()
    joined_sample_columns = row_diff.joined_sample.columns
    assert row_diff.full_match_count == 2
    assert row_diff.partial_match_count == 1
    assert "s__value" in joined_sample_columns
    assert "t__value" in joined_sample_columns

    table_diff = TableDiff(
        adapter=engine_adapter,
        source="table_diff_source",
        target="table_diff_target",
        source_alias="dev",
        target_alias="prod",
        on=["key"],
        decimals=4,
    )

    aliased_joined_sample = table_diff.row_diff().joined_sample.columns
    assert "DEV__value" in aliased_joined_sample
    assert "PROD__value" in aliased_joined_sample


@pytest.mark.slow
def test_grain_check(sushi_context_fixed_date):
    expressions = d.parse(
        """
        MODEL (name memory.sushi.grain_items, kind full, grain("key_1", KEY_2));
        SELECT
            key_1 as "key_1",
            KEY_2,
            value,
        FROM
            (VALUES
                (1, 1, 1),
                (7, 4, 2),
                (NULL, 3, 3),
                (NULL, NULL, 3),
                (1, 2, 2),
                (4, NULL, 3),
                (2, 3, 2),
            ) AS t (key_1,KEY_2, value)
    """
    )
    model_s = load_sql_based_model(expressions, dialect="snowflake")
    sushi_context_fixed_date.upsert_model(model_s)
    sushi_context_fixed_date.plan(
        "source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-31",
        end="2023-01-31",
    )

    model = sushi_context_fixed_date.models['"MEMORY"."SUSHI"."GRAIN_ITEMS"']

    modified_model = model.dict()
    modified_model["query"] = (
        exp.select("*")
        .from_(model.query.subquery())
        .union(
            'SELECT key_1 as "key_1", KEY_2, value FROM (VALUES (1, 6, 1),(1, 5, 3),(NULL, 2, 3),) AS t (key_1, KEY_2, value)'
        )
    )

    modified_sqlmodel = SqlModel(**modified_model)
    sushi_context_fixed_date.upsert_model(modified_sqlmodel)

    sushi_context_fixed_date.auto_categorize_changes = CategorizerConfig(
        sql=AutoCategorizationMode.FULL
    )
    sushi_context_fixed_date.plan(
        "target_dev",
        create_from="source_dev",
        no_prompts=True,
        auto_apply=True,
        skip_tests=True,
        start="2023-01-31",
        end="2023-01-31",
    )

    diff = sushi_context_fixed_date.table_diff(
        source="source_dev",
        target="target_dev",
        on=["'key_1'", "key_2"],
        model_or_snapshot="SUSHI.GRAIN_ITEMS",
        skip_grain_check=False,
    )

    row_diff = diff.row_diff()
    assert row_diff.full_match_count == 7
    assert row_diff.full_match_pct == 93.33
    assert row_diff.s_only_count == 2
    assert row_diff.t_only_count == 5
    assert row_diff.stats["join_count"] == 4
    assert row_diff.stats["null_grain_count"] == 4
    assert row_diff.stats["s_count"] != row_diff.stats["distinct_count_s"]
    assert row_diff.stats["distinct_count_s"] == 7
    assert row_diff.stats["t_count"] != row_diff.stats["distinct_count_t"]
    assert row_diff.stats["distinct_count_t"] == 10
    assert row_diff.s_sample.shape == (0, 3)
    assert row_diff.t_sample.shape == (3, 3)


@pytest.mark.slow
def test_generated_sql(sushi_context_fixed_date: Context, mocker: MockerFixture):
    engine_adapter = sushi_context_fixed_date.engine_adapter

    engine_adapter.ctas(
        "table_diff_source",
        pd.DataFrame(
            {
                "key": [1, 2, 3],
                "value": [1.0, 4.2, 4.1],
                "ignored": [1, 2, 3],
            }
        ),
    )

    engine_adapter.ctas(
        "table_diff_target",
        pd.DataFrame(
            {
                "key": [1, 2, 6],
                "value": [1.0, 3.0, -2.2],
                "ignored": [1, 2, 3],
            }
        ),
    )

    query_sql = 'CREATE TABLE IF NOT EXISTS "memory"."sqlmesh_temp_test"."__temp_diff_abcdefgh" AS WITH "__source" AS (SELECT "key", "value", "key" AS "__sqlmesh_join_key" FROM "table_diff_source"), "__target" AS (SELECT "key", "value", "key" AS "__sqlmesh_join_key" FROM "table_diff_target"), "__stats" AS (SELECT "s"."key" AS "s__key", "s"."value" AS "s__value", "s"."__sqlmesh_join_key" AS "s____sqlmesh_join_key", "t"."key" AS "t__key", "t"."value" AS "t__value", "t"."__sqlmesh_join_key" AS "t____sqlmesh_join_key", CASE WHEN NOT "s"."key" IS NULL THEN 1 ELSE 0 END AS "s_exists", CASE WHEN NOT "t"."key" IS NULL THEN 1 ELSE 0 END AS "t_exists", CASE WHEN "s"."__sqlmesh_join_key" = "t"."__sqlmesh_join_key" AND (NOT "s"."key" IS NULL AND NOT "t"."key" IS NULL) THEN 1 ELSE 0 END AS "row_joined", CASE WHEN "s"."key" IS NULL AND "t"."key" IS NULL THEN 1 ELSE 0 END AS "null_grain", CASE WHEN "s"."key" = "t"."key" THEN 1 WHEN ("s"."key" IS NULL) AND ("t"."key" IS NULL) THEN 1 WHEN ("s"."key" IS NULL) OR ("t"."key" IS NULL) THEN 0 ELSE 0 END AS "key_matches", CASE WHEN ROUND("s"."value", 3) = ROUND("t"."value", 3) THEN 1 WHEN ("s"."value" IS NULL) AND ("t"."value" IS NULL) THEN 1 WHEN ("s"."value" IS NULL) OR ("t"."value" IS NULL) THEN 0 ELSE 0 END AS "value_matches" FROM "__source" AS "s" FULL JOIN "__target" AS "t" ON "s"."__sqlmesh_join_key" = "t"."__sqlmesh_join_key") SELECT *, CASE WHEN "key_matches" = 1 AND "value_matches" = 1 THEN 1 ELSE 0 END AS "row_full_match" FROM "__stats"'
    summary_query_sql = 'SELECT SUM("s_exists") AS "s_count", SUM("t_exists") AS "t_count", SUM("row_joined") AS "join_count", SUM("null_grain") AS "null_grain_count", SUM("row_full_match") AS "full_match_count", SUM("key_matches") AS "key_matches", SUM("value_matches") AS "value_matches", COUNT(DISTINCT ("s____sqlmesh_join_key")) AS "distinct_count_s", COUNT(DISTINCT ("t____sqlmesh_join_key")) AS "distinct_count_t" FROM "memory"."sqlmesh_temp_test"."__temp_diff_abcdefgh"'
    compare_sql = 'SELECT ROUND(100 * (CAST(SUM("key_matches") AS DECIMAL) / COUNT("key_matches")), 1) AS "key_matches", ROUND(100 * (CAST(SUM("value_matches") AS DECIMAL) / COUNT("value_matches")), 1) AS "value_matches" FROM "memory"."sqlmesh_temp_test"."__temp_diff_abcdefgh" WHERE "row_joined" = 1'
    sample_query_sql = 'SELECT "s_exists", "t_exists", "row_joined", "row_full_match", "s__key", "s__value", "s____sqlmesh_join_key", "t__key", "t__value", "t____sqlmesh_join_key" FROM "memory"."sqlmesh_temp_test"."__temp_diff_abcdefgh" WHERE "key_matches" = 0 OR "value_matches" = 0 ORDER BY "s__key" NULLS FIRST, "t__key" NULLS FIRST LIMIT 20'
    drop_sql = 'DROP TABLE IF EXISTS "memory"."sqlmesh_temp_test"."__temp_diff_abcdefgh"'

    spy_execute = mocker.spy(engine_adapter, "_execute")
    mocker.patch("sqlmesh.core.engine_adapter.base.random_id", return_value="abcdefgh")

    sushi_context_fixed_date.table_diff(
        source="table_diff_source",
        target="table_diff_target",
        on=["key"],
        skip_columns=["ignored"],
        temp_schema="sqlmesh_temp_test",
    )

    spy_execute.assert_any_call(query_sql)
    spy_execute.assert_any_call(summary_query_sql)
    spy_execute.assert_any_call(compare_sql)
    spy_execute.assert_any_call(sample_query_sql)
    spy_execute.assert_any_call(drop_sql)

    spy_execute.reset_mock()

    # Also check WHERE clause is propagated correctly
    sushi_context_fixed_date.table_diff(
        source="table_diff_source",
        target="table_diff_target",
        on=["key"],
        skip_columns=["ignored"],
        where="key = 2",
    )

    query_sql_where = 'CREATE TABLE IF NOT EXISTS "memory"."sqlmesh_temp"."__temp_diff_abcdefgh" AS WITH "__source" AS (SELECT "key", "value", "key" AS "__sqlmesh_join_key" FROM "table_diff_source" WHERE "key" = 2), "__target" AS (SELECT "key", "value", "key" AS "__sqlmesh_join_key" FROM "table_diff_target" WHERE "key" = 2), "__stats" AS (SELECT "s"."key" AS "s__key", "s"."value" AS "s__value", "s"."__sqlmesh_join_key" AS "s____sqlmesh_join_key", "t"."key" AS "t__key", "t"."value" AS "t__value", "t"."__sqlmesh_join_key" AS "t____sqlmesh_join_key", CASE WHEN NOT "s"."key" IS NULL THEN 1 ELSE 0 END AS "s_exists", CASE WHEN NOT "t"."key" IS NULL THEN 1 ELSE 0 END AS "t_exists", CASE WHEN "s"."__sqlmesh_join_key" = "t"."__sqlmesh_join_key" AND (NOT "s"."key" IS NULL AND NOT "t"."key" IS NULL) THEN 1 ELSE 0 END AS "row_joined", CASE WHEN "s"."key" IS NULL AND "t"."key" IS NULL THEN 1 ELSE 0 END AS "null_grain", CASE WHEN "s"."key" = "t"."key" THEN 1 WHEN ("s"."key" IS NULL) AND ("t"."key" IS NULL) THEN 1 WHEN ("s"."key" IS NULL) OR ("t"."key" IS NULL) THEN 0 ELSE 0 END AS "key_matches", CASE WHEN ROUND("s"."value", 3) = ROUND("t"."value", 3) THEN 1 WHEN ("s"."value" IS NULL) AND ("t"."value" IS NULL) THEN 1 WHEN ("s"."value" IS NULL) OR ("t"."value" IS NULL) THEN 0 ELSE 0 END AS "value_matches" FROM "__source" AS "s" FULL JOIN "__target" AS "t" ON "s"."__sqlmesh_join_key" = "t"."__sqlmesh_join_key") SELECT *, CASE WHEN "key_matches" = 1 AND "value_matches" = 1 THEN 1 ELSE 0 END AS "row_full_match" FROM "__stats"'
    spy_execute.assert_any_call(query_sql_where)
