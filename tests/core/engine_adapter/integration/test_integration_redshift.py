import typing as t
import pytest
from tests.core.engine_adapter.integration import TestContext
from sqlglot import exp

pytestmark = [pytest.mark.remote, pytest.mark.engine, pytest.mark.redshift]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "redshift", "inttest_redshift"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_columns(ctx: TestContext):
    ctx.init()

    table = ctx.table("column_types")
    col_strings = {
        "char": ["char", "character", "nchar"],
        "varchar": ["varchar", "character varying", "nvarchar"],
        "varbinary": ["varbyte", "varbinary", "binary varying"],
        "decimal": ["decimal", "numeric"],
    }

    # raw ddl
    sql = f"CREATE TABLE {table} ("
    sql += (
        ", ".join(
            f"{col.replace(' ', '_')}10 {col}(10)"
            for col in [*col_strings["char"], *col_strings["varchar"], *col_strings["varbinary"]]
        )
        + ", "
    )
    # bare types that should have their default lengths of 1 added by columns()
    sql += ", ".join(f"{col.replace(' ', '_')}1 {col}" for col in col_strings["char"]) + ", "
    # bare types that should have their default lengths of 256 added by columns()
    sql += ", ".join(f"{col.replace(' ', '_')}256 {col}" for col in col_strings["varchar"]) + ", "
    sql += (
        ", ".join(f"{col.replace(' ', '_')}172 {col}(17, 2)" for col in col_strings["decimal"])
        + ")"
    )

    ctx.engine_adapter.cursor.execute(sql)
    columns = ctx.engine_adapter.columns(table)

    # columns to types
    cols_to_types = {
        f"{col.replace(' ', '_')}10": exp.DataType.build(f"{col}(10)", dialect=ctx.dialect)
        for col in [*col_strings["char"], *col_strings["varchar"], *col_strings["varbinary"]]
    }
    cols_to_types.update(
        {
            f"{col.replace(' ', '_')}1": exp.DataType.build(f"{col}(1)", dialect=ctx.dialect)
            for col in col_strings["char"]
        }
    )
    cols_to_types.update(
        {
            f"{col.replace(' ', '_')}256": exp.DataType.build(f"{col}(256)", dialect=ctx.dialect)
            for col in col_strings["varchar"]
        }
    )
    cols_to_types.update(
        {
            f"{col.replace(' ', '_')}172": exp.DataType.build(f"{col}(17, 2)", dialect=ctx.dialect)
            for col in col_strings["decimal"]
        }
    )

    # did we convert the types from redshift correctly?
    assert [col.sql(ctx.dialect) for col in columns.values()] == [
        col.sql(ctx.dialect) for col in cols_to_types.values()
    ]

    # did we replace default char/varchar lengths with MAX correctly?
    max_cols = [col for col in columns if col.endswith("1") or col.endswith("256")]
    assert [
        col.sql(ctx.dialect)
        for col in ctx.engine_adapter._default_precision_to_max(  # type: ignore
            {k: columns[k] for k in max_cols}
        ).values()
    ] == ["CHAR(max)", "CHAR(max)", "CHAR(max)", "VARCHAR(max)", "VARCHAR(max)", "VARCHAR(max)"]


def test_fetch_native_df_respects_case_sensitivity(ctx: TestContext):
    adapter = ctx.engine_adapter
    adapter.execute("SET enable_case_sensitive_identifier TO true")
    assert adapter.fetchdf('WITH t AS (SELECT 1 AS "C", 2 AS "c") SELECT * FROM t').to_dict() == {
        "C": {0: 1},
        "c": {0: 2},
    }
