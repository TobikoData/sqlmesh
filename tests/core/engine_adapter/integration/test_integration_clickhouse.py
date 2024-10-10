import typing as t
import pytest
from tests.core.engine_adapter.integration import TestContext
import pandas as pd
from sqlglot import exp, parse_one

if t.TYPE_CHECKING:
    pass

pytestmark = [pytest.mark.docker, pytest.mark.engine, pytest.mark.clickhouse]


@pytest.fixture
def mark_gateway() -> t.Tuple[str, str]:
    return "clickhouse", "inttest_clickhouse"


@pytest.fixture
def test_type() -> str:
    return "query"


def test_insert_overwrite_by_condition_partition(ctx: TestContext):
    ctx.init()

    existing_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2024-01-01"},
            {"id": 2, "ds": "2024-02-01"},
            {"id": 3, "ds": "2024-02-28"},
            {"id": 4, "ds": "2024-03-01"},
        ]
    )
    existing_table_name = ctx.table("data_existing")
    ctx.engine_adapter.ctas(
        existing_table_name.sql(),
        ctx.input_data(existing_data),
        {
            "id": exp.DataType.build("Int8", ctx.dialect),
            "ds": exp.DataType.build("Date", ctx.dialect),
        },
        partitioned_by=[parse_one("toMonth(ds)", dialect=ctx.dialect)],
    )

    insert_data = pd.DataFrame(
        [
            {"id": 5, "ds": "2024-02-29"},
            {"id": 6, "ds": "2024-04-01"},
        ]
    )
    insert_table_name = ctx.table("data_insert")

    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data), insert_table_name.sql()
    ) as insert_table:
        source_queries, columns_to_types = (
            ctx.engine_adapter._get_source_queries_and_columns_to_types(
                parse_one(f"SELECT * FROM {insert_table.sql()}"),  # type: ignore
                {"id": exp.DataType.build("INT"), "ds": exp.DataType.build("Date")},
                target_table=existing_table_name.sql(),
            )
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            exp.Between(
                this=exp.column("ds"), low=parse_one("'2024-02-15'"), high=parse_one("'2024-04-30'")
            ),
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                {"id": 2, "ds": pd.Timestamp("2024-02-01")},
                {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                {"id": 6, "ds": pd.Timestamp("2024-04-01")},
            ]
        ),
    )
    ctx.engine_adapter.drop_table(existing_table_name.sql())
