import typing as t
import pytest
from tests.core.engine_adapter.integration import TestContext
import pandas as pd
from sqlglot import exp, parse_one


@pytest.fixture(
    params=[
        pytest.param(
            "clickhouse",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.clickhouse,
            ],
        ),
        pytest.param(
            "clickhouse_cluster",
            marks=[
                pytest.mark.docker,
                pytest.mark.engine,
                pytest.mark.clickhouse_cluster,
            ],
        ),
        pytest.param(
            "clickhouse_cloud",
            marks=[
                pytest.mark.engine,
                pytest.mark.remote,
                pytest.mark.clickhouse_cloud,
            ],
        ),
    ]
)
def mark_gateway(request) -> t.Tuple[str, str]:
    return request.param, f"inttest_{request.param}"


@pytest.fixture
def test_type() -> str:
    return "query"


def _get_source_queries_and_columns_to_types(
    ctx,
    insert_table: exp.Table,
    target_table: exp.Table,
    columns_to_types: t.Dict[str, exp.DataType] = {
        "id": exp.DataType.build("Int8", dialect="clickhouse"),
        "ds": exp.DataType.build("Date", dialect="clickhouse"),
    },
):
    return ctx.engine_adapter._get_source_queries_and_columns_to_types(
        parse_one(f"SELECT * FROM {insert_table.sql()}"),  # type: ignore
        columns_to_types,
        target_table=target_table.sql(),
    )


def _create_table_and_insert_existing_data(
    ctx: TestContext,
    existing_data: pd.DataFrame = pd.DataFrame(
        [
            {"id": 1, "ds": "2024-01-01"},
            {"id": 2, "ds": "2024-02-01"},
            {"id": 3, "ds": "2024-02-28"},
            {"id": 4, "ds": "2024-03-01"},
        ]
    ),
    columns_to_types: t.Dict[str, exp.DataType] = {
        "id": exp.DataType.build("Int8", "clickhouse"),
        "ds": exp.DataType.build("Date", "clickhouse"),
    },
    table_name: str = "data_existing",
    partitioned_by: t.Optional[t.List[exp.Expression]] = [
        parse_one("toMonth(ds)", dialect="clickhouse")
    ],
) -> exp.Table:
    existing_data = existing_data
    existing_table_name: exp.Table = ctx.table(table_name)
    ctx.engine_adapter.ctas(
        existing_table_name.sql(),
        ctx.input_data(existing_data, columns_to_types),
        columns_to_types,
        partitioned_by=partitioned_by,
    )
    return existing_table_name


def test_insert_overwrite_by_condition_replace_partitioned(ctx: TestContext):
    existing_table_name = _create_table_and_insert_existing_data(ctx)

    # new data to insert
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
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )

        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                {"id": 6, "ds": pd.Timestamp("2024-04-01")},
            ]
        ),
    )


def test_insert_overwrite_by_condition_replace(ctx: TestContext):
    existing_table_name = _create_table_and_insert_existing_data(ctx, partitioned_by=None)

    # new data to insert
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
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )

        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                {"id": 6, "ds": pd.Timestamp("2024-04-01")},
            ]
        ),
    )


def test_insert_overwrite_by_condition_where_partitioned(ctx: TestContext):
    # `where` time window
    start_date = "2024-02-15"
    end_date = "2024-04-30"

    # data currently in target table
    existing_table_name = _create_table_and_insert_existing_data(ctx)

    # new data to insert
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
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            exp.Between(
                this=exp.column("ds"),
                low=parse_one(f"'{start_date}'"),
                high=parse_one(f"'{end_date}'"),
            ),
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 1, "ds": pd.Timestamp("2024-01-01")},  # retained
                {"id": 2, "ds": pd.Timestamp("2024-02-01")},  # retained
                {"id": 5, "ds": pd.Timestamp("2024-02-29")},  # inserted
                {"id": 6, "ds": pd.Timestamp("2024-04-01")},  # inserted
            ]
        ),
    )


def test_insert_overwrite_by_condition_where_compound_partitioned(ctx: TestContext):
    # `where` time window
    start_date = "2024-02-15"
    end_date = "2024-04-30"

    compound_columns_to_types = {
        "id": exp.DataType.build("Int8", ctx.dialect),
        "ds": exp.DataType.build("Date", ctx.dialect),
        "city": exp.DataType.build("String", ctx.dialect),
    }

    # data currently in target table
    existing_table_name = _create_table_and_insert_existing_data(
        ctx,
        existing_data=pd.DataFrame(
            [
                {"id": 1, "ds": "2024-01-01", "city": "1"},
                {"id": 2, "ds": "2024-01-02", "city": "2"},
                {"id": 3, "ds": "2024-02-01", "city": "1"},
                {"id": 4, "ds": "2024-02-02", "city": "2"},
                {"id": 5, "ds": "2024-02-27", "city": "1"},
                {"id": 6, "ds": "2024-02-28", "city": "2"},
                {"id": 7, "ds": "2024-03-01", "city": "1"},
                {"id": 8, "ds": "2024-03-02", "city": "2"},
            ]
        ),
        columns_to_types=compound_columns_to_types,
        partitioned_by=[parse_one("toMonth(ds)", dialect=ctx.dialect), exp.column("city")],
    )

    # new data to insert
    insert_data = pd.DataFrame(
        [
            {"id": 9, "ds": "2024-02-26", "city": "1"},
            {"id": 10, "ds": "2024-02-29", "city": "2"},
            {"id": 11, "ds": "2024-04-01", "city": "1"},
            {"id": 12, "ds": "2024-04-02", "city": "2"},
        ]
    )
    insert_table_name = ctx.table("data_insert")

    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data, compound_columns_to_types), insert_table_name.sql()
    ) as insert_table:
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx,
            insert_table,
            existing_table_name,
            compound_columns_to_types,
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            exp.Between(
                this=exp.column("ds"),
                low=parse_one(f"'{start_date}'"),
                high=parse_one(f"'{end_date}'"),
            ),
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 1, "ds": pd.Timestamp("2024-01-01"), "city": "1"},
                {"id": 2, "ds": pd.Timestamp("2024-01-02"), "city": "2"},
                {"id": 3, "ds": pd.Timestamp("2024-02-01"), "city": "1"},
                {"id": 4, "ds": pd.Timestamp("2024-02-02"), "city": "2"},
                {"id": 9, "ds": pd.Timestamp("2024-02-26"), "city": "1"},
                {"id": 10, "ds": pd.Timestamp("2024-02-29"), "city": "2"},
                {"id": 11, "ds": pd.Timestamp("2024-04-01"), "city": "1"},
                {"id": 12, "ds": pd.Timestamp("2024-04-02"), "city": "2"},
            ]
        ),
    )


def test_insert_overwrite_by_condition_by_key(ctx: TestContext):
    # key parameters
    key = [exp.column("id")]
    key_exp = key[0]

    # data currently in target table
    existing_table_name = _create_table_and_insert_existing_data(ctx, partitioned_by=None)

    # new data to insert
    insert_data = pd.DataFrame(
        [
            {"id": 4, "ds": "2024-05-01"},  # will overwrite existing record
            {"id": 4, "ds": "2024-05-02"},  # only inserted if unique_key = False
            {"id": 5, "ds": "2024-02-29"},
            {"id": 6, "ds": "2024-04-01"},
        ]
    )

    # unique_key = True
    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data), ctx.table("data_insert").sql()
    ) as insert_table:
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            dynamic_key=key,
            dynamic_key_exp=key_exp,
            dynamic_key_unique=True,
        )

        ctx.compare_with_current(
            existing_table_name,
            pd.DataFrame(
                [
                    {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                    {"id": 2, "ds": pd.Timestamp("2024-02-01")},
                    {"id": 3, "ds": pd.Timestamp("2024-02-28")},
                    {"id": 4, "ds": pd.Timestamp("2024-05-01")},
                    {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                    {"id": 6, "ds": pd.Timestamp("2024-04-01")},
                ]
            ),
        )
    ctx.engine_adapter.drop_table(existing_table_name.sql())

    # unique_key = False
    existing_table_name = _create_table_and_insert_existing_data(
        ctx, table_name="data_existing_no_unique", partitioned_by=None
    )

    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data), ctx.table("data_insert_no_unique").sql()
    ) as insert_table:
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            dynamic_key=key,
            dynamic_key_exp=key_exp,
            dynamic_key_unique=False,
        )

        ctx.compare_with_current(
            existing_table_name,
            pd.DataFrame(
                [
                    {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                    {"id": 2, "ds": pd.Timestamp("2024-02-01")},
                    {"id": 3, "ds": pd.Timestamp("2024-02-28")},
                    {"id": 4, "ds": pd.Timestamp("2024-05-01")},
                    {"id": 4, "ds": pd.Timestamp("2024-05-02")},
                    {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                    {"id": 6, "ds": pd.Timestamp("2024-04-01")},
                ]
            ),
        )


def test_insert_overwrite_by_condition_by_key_partitioned(ctx: TestContext):
    # key parameters
    key = [exp.column("id")]
    key_exp = key[0]

    # data currently in target table
    existing_table_name = _create_table_and_insert_existing_data(ctx)

    # new data to insert
    insert_data = pd.DataFrame(
        [
            {"id": 4, "ds": "2024-05-01"},  # will overwrite existing record
            {"id": 4, "ds": "2024-05-02"},  # only inserted if unique_key = False
            {"id": 5, "ds": "2024-02-29"},
            {"id": 6, "ds": "2024-04-01"},
        ]
    )

    # unique_key = True
    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data), ctx.table("data_insert").sql()
    ) as insert_table:
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            dynamic_key=key,
            dynamic_key_exp=key_exp,
            dynamic_key_unique=True,
        )

        ctx.compare_with_current(
            existing_table_name,
            pd.DataFrame(
                [
                    {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                    {"id": 2, "ds": pd.Timestamp("2024-02-01")},
                    {"id": 3, "ds": pd.Timestamp("2024-02-28")},
                    {"id": 4, "ds": pd.Timestamp("2024-05-01")},
                    {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                    {"id": 6, "ds": pd.Timestamp("2024-04-01")},
                ]
            ),
        )
    ctx.engine_adapter.drop_table(existing_table_name.sql())

    # unique_key = False
    existing_table_name = _create_table_and_insert_existing_data(
        ctx, table_name="data_existing_no_unique"
    )

    with ctx.engine_adapter.temp_table(
        ctx.input_data(insert_data), ctx.table("data_insert_no_unique").sql()
    ) as insert_table:
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            dynamic_key=key,
            dynamic_key_exp=key_exp,
            dynamic_key_unique=False,
        )

        ctx.compare_with_current(
            existing_table_name,
            pd.DataFrame(
                [
                    {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                    {"id": 2, "ds": pd.Timestamp("2024-02-01")},
                    {"id": 3, "ds": pd.Timestamp("2024-02-28")},
                    {"id": 4, "ds": pd.Timestamp("2024-05-01")},
                    {
                        "id": 4,
                        "ds": pd.Timestamp("2024-05-02"),
                    },  # second ID=4 row because unique_key=False
                    {"id": 5, "ds": pd.Timestamp("2024-02-29")},
                    {"id": 6, "ds": pd.Timestamp("2024-04-01")},
                ]
            ),
        )


def test_insert_overwrite_by_condition_inc_by_partition(ctx: TestContext):
    existing_table_name = _create_table_and_insert_existing_data(ctx)

    # new data to insert
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
        source_queries, columns_to_types = _get_source_queries_and_columns_to_types(
            ctx, insert_table, existing_table_name
        )
        ctx.engine_adapter._insert_overwrite_by_condition(
            existing_table_name.sql(),
            source_queries,
            columns_to_types,
            keep_existing_partition_rows=False,
        )

    ctx.compare_with_current(
        existing_table_name,
        pd.DataFrame(
            [
                {"id": 1, "ds": pd.Timestamp("2024-01-01")},
                {
                    "id": 5,
                    "ds": pd.Timestamp("2024-02-29"),
                },  # all existing Feb records overwritten by this row
                {"id": 4, "ds": pd.Timestamp("2024-03-01")},
                {"id": 6, "ds": pd.Timestamp("2024-04-01")},
            ]
        ),
    )
